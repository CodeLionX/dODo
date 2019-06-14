package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, NotInfluenceReceiveTimeout, Props, ReceiveTimeout}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, SubscribeAck}
import com.github.codelionx.dodo.Settings
import com.github.codelionx.dodo.actors.ClusterListener.{GetNumberOfNodes, NumberOfNodes}
import com.github.codelionx.dodo.actors.DataHolder.{DataRef, GetDataRef}
import com.github.codelionx.dodo.actors.ResultCollector.{ConstColumns, OrderEquivalencies}
import com.github.codelionx.dodo.actors.SystemCoordinator.Finished
import com.github.codelionx.dodo.actors.Worker.{CheckForEquivalency, CheckForOD, GetTask, ODsToCheck, OrderEquivalent}
import com.github.codelionx.dodo.discovery.{CandidateGenerator, DependencyChecking}
import com.github.codelionx.dodo.types.TypedColumn
import com.sun.org.apache.xpath.internal.functions.FuncFalse

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.language.postfixOps


object ODMaster {

  val name = "odmaster"

  def props(nWorkers: Int, resultCollector: ActorRef, systemCoordinator: ActorRef): Props = Props(new ODMaster(nWorkers, resultCollector, systemCoordinator))

  case class FindODs(dataHolder: ActorRef)

  case object GetWorkLoad extends NotInfluenceReceiveTimeout
  case class WorkLoad(queueSize: Int) extends NotInfluenceReceiveTimeout
  case class StealWork(amount: Int) extends NotInfluenceReceiveTimeout
  case class SendWork(work: Queue[(Seq[Int], Seq[Int])]) extends NotInfluenceReceiveTimeout
  case object AckWorkReceived extends NotInfluenceReceiveTimeout
}


class ODMaster(nWorkers: Int, resultCollector: ActorRef, systemCoordinator: ActorRef) extends Actor with ActorLogging with DependencyChecking with CandidateGenerator {

  import ODMaster._
  private val settings = Settings(context.system)
  private val workers: Seq[ActorRef] = (0 until nWorkers).map(i =>
    context.actorOf(Worker.props(resultCollector), s"${Worker.name}-$i")
  )
  private val clusterListener: ActorRef = context.actorOf(ClusterListener.props, ClusterListener.name)

  private var reducedColumns: Set[Int] = Set.empty
  private var pendingPruningResponses = 0

  private var odsToCheck: Queue[(Seq[Int], Seq[Int])] = Queue.empty
  private var waitingForODStatus: Map[ActorRef, Queue[(Seq[Int], Seq[Int])]] = Map.empty

  private var othersWorkloads: Seq[(Int, ActorRef)] = Seq.empty
  private var waitingForWorkloads: Boolean = false
  private var lastWorkThief: ActorRef = null

  val workStealingMediator: ActorRef = DistributedPubSub(context.system).mediator
  workStealingMediator ! Subscribe("workStealing", self)

  override def preStart(): Unit = {
    log.info(s"Starting $name")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    log.info(s"Stopping $name")

  override def receive: Receive = unsubscribed

  def unsubscribed: Receive = {
    case SubscribeAck(Subscribe("workStealing", None, `self`)) =>
      log.info("subscribed to the workStealing mediator")
      clusterListener ! GetNumberOfNodes
    case NumberOfNodes(number) =>
      context.become(uninitialized(number <= 1))
    //TODO: case FindODs(dataHolder):
    case _ => log.info("Unknown message received")
  }

  def uninitialized(first:Boolean): Receive = {
    case FindODs(dataHolder) =>
      dataHolder ! GetDataRef

    case DataRef(table) =>
      if (table.length <= 1) {
        log.info("No order dependencies due to length of table")
        systemCoordinator ! Finished
        context.stop(self)
      }
      if (first) {
        val orderEquivalencies = Array.fill(table.length){Seq.empty[Int]}
        val columnIndexTuples = table.indices.combinations(2).map(l => l.head -> l(1))

        reducedColumns = table.indices.toSet
        val constColumns = pruneConstColumns(table)
        resultCollector ! ConstColumns(constColumns.map(table(_).name))
        workers.foreach(actor => actor ! DataRef(table))
        context.become(pruning(table, orderEquivalencies, columnIndexTuples))
      }
      else {
        workStealingMediator ! Publish("workStealing", GetWorkLoad)
        context.become(findingODs(table))
      }

    case _ => log.info("Unknown message received")
  }

  def pruning(table: Array[TypedColumn[Any]], orderEquivalencies: Array[Seq[Int]], columnIndexTuples: Iterator[(Int, Int)]): Receive = {
    case GetTask =>
      if(columnIndexTuples.isEmpty) {
        // TODO: Remember to send task once all pruning answers are in and the state has been changed
      }
      else {
        var nextTuple = columnIndexTuples.next()
        while (!(reducedColumns.contains(nextTuple._1) && reducedColumns.contains(nextTuple._2)) && columnIndexTuples.nonEmpty) {
          nextTuple = columnIndexTuples.next()
        }

        log.debug(s"Scheduling task to check equivalence of $nextTuple to worker ${sender.path.name}")
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
        odsToCheck ++= generateFirstCandidates(reducedColumns)
        context.become(findingODs(table))
      }

    case _ => log.info("Unknown message received")
  }

  def findingODs(table: Array[TypedColumn[Any]]): Receive = {
    case GetTask =>
      if (odsToCheck.nonEmpty) {
        val batchLength = math.min(math.max(odsToCheck.length / nWorkers, odsToCheck.length), settings.maxBatchSize)
        val (workerODs, newQueue) = odsToCheck.splitAt(batchLength)
        odsToCheck = newQueue

        log.debug(s"Scheduling task to check OCD $workerODs to worker ${sender.path.name}")
        sender ! CheckForOD(workerODs, reducedColumns)
        waitingForODStatus += (sender -> workerODs)
        if (odsToCheck.isEmpty) {
          othersWorkloads = Seq.empty
          workStealingMediator ! Publish("workStealing", GetWorkLoad)
          waitingForWorkloads = true
          context.setReceiveTimeout(1 second)
        }
      }

    case GetWorkLoad =>
      if (lastWorkThief == null) {
        sender ! WorkLoad(odsToCheck.length)
      }
      else {
        log.info("{} is already sending work to somebody else right now", self.path.name)
      }

    case WorkLoad(queueSize: Int) =>
      othersWorkloads :+ (queueSize, sender)

    case ReceiveTimeout if waitingForWorkloads =>
      waitingForWorkloads = false
      context.setReceiveTimeout(Duration.Undefined)
      val sortedWorkloads = othersWorkloads.sorted
      val averageWl: Int = sortedWorkloads.foldLeft(0)(_ + _._1)/(sortedWorkloads.size + 1)
      var ownWorkLoad = 0
      for (master <- sortedWorkloads) {
        val amountToSteal = math.min(master._1 - averageWl, averageWl - ownWorkLoad)
        if (amountToSteal > 0) {
          master._2 ! StealWork(amountToSteal)
          ownWorkLoad -= amountToSteal
        }
      }

    case ReceiveTimeout if !waitingForWorkloads && lastWorkThief != null =>
      context.setReceiveTimeout(Duration.Undefined)
      odsToCheck ++= waitingForODStatus(lastWorkThief)
      waitingForODStatus -= lastWorkThief
      lastWorkThief = null
      // TODO: refine technique of how to handle message loss

    case StealWork(amount: Int) =>
      val (stolenQueue, newQueue) = odsToCheck.splitAt(amount)
      odsToCheck = newQueue
      waitingForODStatus += (sender -> stolenQueue)
      sender ! SendWork(stolenQueue)
      lastWorkThief = sender
      context.setReceiveTimeout(2 seconds)

    case SendWork(stolenQueue) =>
      odsToCheck ++= stolenQueue
      sender ! AckWorkReceived

    case AckWorkReceived =>
      lastWorkThief = null
      waitingForODStatus -= sender

    case ODsToCheck(originalODs, newODs) =>
      odsToCheck ++= newODs
      waitingForODStatus -= sender
      if (waitingForODStatus.isEmpty && odsToCheck.isEmpty) {
        log.info("Found all ODs")
        systemCoordinator ! Finished
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
}