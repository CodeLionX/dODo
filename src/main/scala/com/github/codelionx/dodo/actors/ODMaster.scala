package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, SubscribeAck}
import com.github.codelionx.dodo.Settings
import com.github.codelionx.dodo.Settings.DefaultValues
import com.github.codelionx.dodo.actors.ClusterListener.{GetNumberOfNodes, NumberOfNodes}
import com.github.codelionx.dodo.actors.DataHolder.{DataRef, LoadDataFromDisk}
import com.github.codelionx.dodo.actors.ResultCollector.{ConstColumns, OrderEquivalencies}
import com.github.codelionx.dodo.actors.Worker._
import com.github.codelionx.dodo.discovery.{CandidateGenerator, DependencyChecking}
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.language.postfixOps



object ODMaster {

  val name = "odmaster"

  def props(): Props = Props(new ODMaster)

  case class FindODs(dataHolder: ActorRef)

  case object GetWorkLoad
  case class WorkLoad(queueSize: Int)
  case object WorkLoadTimeout
  case class StealWork(amount: Int)
  case class SendWork(work: Queue[(Seq[Int], Seq[Int])])
  case object AckWorkReceived
  case class CheckAckWorkReceived(workThief: ActorRef)

}


class ODMaster() extends Actor with ActorLogging with DependencyChecking with CandidateGenerator {

  import ODMaster._


  private val settings = Settings(context.system)
  private val nWorkers: Int = settings.workers

  private val resultCollector: ActorRef = context.actorOf(ResultCollector.props(), ResultCollector.name)
  private val dataHolder: ActorRef = context.actorOf(DataHolder.props(DefaultValues.HOST), DataHolder.name)
  private val clusterListener: ActorRef = context.actorOf(ClusterListener.props, ClusterListener.name)
  private val workers: Seq[ActorRef] = (0 until nWorkers).map(i =>
  context.actorOf(Worker.props(resultCollector), s"${Worker.name}-$i")
  )


  private var reducedColumns: Set[Int] = Set.empty
  private var pendingPruningResponses = 0

  private var odsToCheck: Queue[(Seq[Int], Seq[Int])] = Queue.empty
  private var waitingForODStatus: Map[ActorRef, Queue[(Seq[Int], Seq[Int])]] = Map.empty

  private var othersWorkloads: Seq[(Int, ActorRef)] = Seq.empty

  val workStealingMediator: ActorRef = DistributedPubSub(context.system).mediator
  val workStealingTopic = "workStealing"


  override def preStart(): Unit = {
    log.info("Starting {}", name)
    Reaper.watchWithDefault(self)
    workStealingMediator ! Subscribe(workStealingTopic, self)
  }

  override def postStop(): Unit =
    log.info("Stopping {}", name)

  override def receive: Receive = unsubscribed

  def unsubscribed: Receive = {
    case SubscribeAck(Subscribe(`workStealingTopic`, None, `self`)) =>
      log.info("subscribed to the workStealing mediator")
      clusterListener ! GetNumberOfNodes
    case NumberOfNodes(number) =>
      dataHolder ! LoadDataFromDisk(settings.inputFilePath)
      context.become(uninitialized(number <= 1))
    case _ => log.info("Unknown message received")
  }

  def uninitialized(first:Boolean): Receive = {
    case DataRef(table) =>
      if (table.length <= 1) {
        log.info("No order dependencies due to length of table")
        shutdown()
      }
      if (first) {
        log.debug("Looking for constant columns and generating column tuples for equality checking")
        val orderEquivalencies = Array.fill(table.length){Seq.empty[Int]}
        val columnIndexTuples = table.indices.combinations(2).map(l => l.head -> l(1))

        reducedColumns = table.indices.toSet
        val constColumns = pruneConstColumns(table)
        log.debug("Found {} constant columns, starting pruning", constColumns.length)
        resultCollector ! ConstColumns(constColumns.map(table(_).name))
        workers.foreach(actor => actor ! DataRef(table))
        context.become(pruning(table, orderEquivalencies, columnIndexTuples))
      } else {
        getWorkloads()
        context.become(findingODs(table))
      }

    case m => log.debug("Unknown message received: {}", m)
  }

  def pruning(table: Array[TypedColumn[Any]], orderEquivalencies: Array[Seq[Int]], columnIndexTuples: Iterator[(Int, Int)]): Receive = {
    case GetTask =>
      if(columnIndexTuples.isEmpty) {
        // TODO: Remember to send task once all pruning answers are in and the state has been changed
        log.warning("TODO: no task scheduling implemented in this branch")
      } else {
        var nextTuple = columnIndexTuples.next()
        while (!(reducedColumns.contains(nextTuple._1) && reducedColumns.contains(nextTuple._2)) && columnIndexTuples.nonEmpty) {
          nextTuple = columnIndexTuples.next()
        }

        log.debug("Scheduling task to check equivalence to worker {}", sender.path.name)
        sender ! CheckForEquivalency(nextTuple)
        pendingPruningResponses += 1
      }

    case OrderEquivalent(od, isOrderEquiv) =>
      if (isOrderEquiv) {
        reducedColumns -= od._2
        orderEquivalencies(od._1) :+= od._2
      }
      pendingPruningResponses -= 1
      if (pendingPruningResponses == 0 && columnIndexTuples.isEmpty) {
        log.info("Pruning done")

        val equivalenceClasses = reducedColumns.foldLeft(Map.empty[Int, Seq[Int]])(
          (map, ind) => map + (ind -> orderEquivalencies(ind))
        )
        resultCollector ! OrderEquivalencies(
          equivalenceClasses.map{ case (key, cols) =>
            table(key).name -> cols.map(table(_).name)
          }
        )
        log.debug("Generating first candidates and starting search")
        odsToCheck ++= generateFirstCandidates(reducedColumns)
        context.become(findingODs(table))
      }

    case _ => log.debug("Unknown message received")
  }

  def findingODs(table: Array[TypedColumn[Any]]): Receive = {
    case GetTask =>
      if (odsToCheck.nonEmpty) {
        val batchLength = math.min(math.max(odsToCheck.length / nWorkers, odsToCheck.length), settings.maxBatchSize)
        val (workerODs, newQueue) = odsToCheck.splitAt(batchLength)
        odsToCheck = newQueue

        log.debug("Scheduling task to check OCD to worker {}", sender.path.name)
        sender ! CheckForOD(workerODs, reducedColumns)
        waitingForODStatus += (sender -> workerODs)
        if (odsToCheck.isEmpty) {
          othersWorkloads = Seq.empty
          getWorkloads()
        }
      }

    case GetWorkLoad =>
      if (sender != self) {
        sender ! WorkLoad(odsToCheck.length)
        log.info("Asked for workload")
      }

    case WorkLoad(queueSize: Int) =>
      othersWorkloads :+ (queueSize, sender)

    case WorkLoadTimeout =>
      val sortedWorkloads = othersWorkloads.sorted
      val averageWl: Int = sortedWorkloads.foldLeft(0)(_ + _._1)/(sortedWorkloads.size + 1)
      var ownWorkLoad = 0
      for (master <- sortedWorkloads) {
        val amountToSteal = math.min(master._1 - averageWl, averageWl - ownWorkLoad)
        if (amountToSteal > 0) {
          master._2 ! StealWork(amountToSteal)
          ownWorkLoad += amountToSteal
        }
      }

    case CheckAckWorkReceived(workThief) =>
      // TODO: refine technique of how to handle message loss
      if (waitingForODStatus.contains(workThief)) {
        odsToCheck ++= waitingForODStatus(workThief)
        waitingForODStatus -= workThief
      }

    case StealWork(amount: Int) =>
      val (stolenQueue, newQueue) = odsToCheck.splitAt(amount)
      odsToCheck = newQueue
      waitingForODStatus += (sender -> stolenQueue)
      sender ! SendWork(stolenQueue)
      import context.dispatcher
      context.system.scheduler.scheduleOnce(2 seconds, self, CheckAckWorkReceived(sender))

    case SendWork(stolenQueue) =>
      odsToCheck ++= stolenQueue
      sender ! AckWorkReceived

    case AckWorkReceived =>
      waitingForODStatus -= sender

    case ODsToCheck(newODs) =>
      odsToCheck ++= newODs
      waitingForODStatus -= sender
      if (waitingForODStatus.isEmpty && odsToCheck.isEmpty) {
        log.info("Found all ODs")
        shutdown()
      }

    case _ => log.info("Unknown message received")
  }

  def pruneConstColumns(table: Array[TypedColumn[Any]]): Seq[Int] = {
    val constColumns = for {
      (column, index) <- table.zipWithIndex
      if checkConstant(column)
    } yield index

    reducedColumns --= constColumns
    constColumns
  }

  def shutdown(): Unit = {
    context.children.foreach(_ ! PoisonPill)
    context.stop(self)
  }

  def getWorkloads(): Unit = {
    workStealingMediator ! Publish(workStealingTopic, GetWorkLoad)
    import context.dispatcher
    context.system.scheduler.scheduleOnce(3 second, self, WorkLoadTimeout)
    log.info("Asking for workloads")
  }
}