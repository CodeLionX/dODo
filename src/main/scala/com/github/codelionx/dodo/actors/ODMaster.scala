package com.github.codelionx.dodo.actors

import java.io.File

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props, Terminated}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Publish, Subscribe, SubscribeAck}
import com.github.codelionx.dodo.{DodoException, ODCandidateQueue, Settings}
import com.github.codelionx.dodo.actors.ClusterListener.{GetNumberOfNodes, NumberOfNodes}
import com.github.codelionx.dodo.actors.DataHolder.{DataNotReady, DataRef, FetchDataFromCluster, LoadDataFromDisk}
import com.github.codelionx.dodo.actors.ResultCollector.{ConstColumns, OrderEquivalencies}
import com.github.codelionx.dodo.actors.Worker._
import com.github.codelionx.dodo.discovery.{CandidateGenerator, DependencyChecking}
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.language.postfixOps


object ODMaster {

  val name = "odmaster"

  def props(inputFile: Option[File]): Props = Props(new ODMaster(inputFile))

  case class FindODs(dataHolder: ActorRef)

  case object GetWorkLoad

  case class WorkLoad(queueSize: Int)

  case object WorkLoadTimeout

  case class WorkToSend(amount: Int)

  case class StolenWork(work: Queue[(Seq[Int], Seq[Int])])

  case object AckWorkReceived

  case class AckReceivedTimeout(workThief: ActorRef)

}


class ODMaster(inputFile: Option[File])
  extends Actor
    with ActorLogging
    with DependencyChecking
    with CandidateGenerator {

  import ODMaster._


  private val settings = Settings(context.system)
  private val nWorkers: Int = settings.workers

  private val clusterListener: ActorRef = context.actorOf(ClusterListener.props, ClusterListener.name)
  private val dataHolder: ActorRef = context.actorOf(DataHolder.props(clusterListener), DataHolder.name)
  val resultCollector: ActorRef = context.actorOf(ResultCollector.props(), ResultCollector.name)
  val workers: Seq[ActorRef] = (0 until nWorkers).map(i =>
    context.actorOf(Worker.props(resultCollector), s"${Worker.name}-$i")
  )


  private var reducedColumns: Set[Int] = Set.empty
  private var pendingPruningResponses = 0

  private val state: ODCandidateQueue = ODCandidateQueue.empty(settings.maxBatchSize)

  private var idleWorkers: Seq[ActorRef] = Seq.empty

  private var otherWorkloads: Seq[(Int, ActorRef)] = Seq.empty

  private val workStealingMediator: ActorRef = DistributedPubSub(context.system).mediator
  private val workStealingTopic = "workStealing"

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
      log.debug("subscribed to the workStealing mediator")
      clusterListener ! GetNumberOfNodes

    case NumberOfNodes(number) =>
      val isFirstNode = number <= 1
      val notFirstNode = !isFirstNode
      inputFile match {
        case Some(file) =>
          dataHolder ! LoadDataFromDisk(file)
        case None if notFirstNode =>
          dataHolder ! FetchDataFromCluster
        case None if isFirstNode =>
          log.error("No input data file was specified and this node is the only cluster member: no dataset found!")
          shutdown()
      }
      context.become(uninitialized(isFirstNode))

    case m => log.debug("Unknown message received: {}", m)
  }

  def uninitialized(first: Boolean): Receive = {
    case DataRef(table) =>
      if (table.length <= 1) {
        log.info("No order dependencies due to length of table")
        shutdown()
      } else if (first) {
        log.debug("Looking for constant columns and generating column tuples for equality checking")
        val orderEquivalencies = Array.fill(table.length) {
          Seq.empty[Int]
        }
        val columnIndexTuples = table.indices.combinations(2).map(l => l.head -> l(1))

        reducedColumns = table.indices.toSet
        val constColumns = pruneConstColumns(table)
        resultCollector ! ConstColumns(constColumns.map(table(_).name))

        log.debug("Found {} constant columns, starting pruning", constColumns.length)
        workers.foreach(actor => actor ! DataRef(table))
        context.become(pruning(table, orderEquivalencies, columnIndexTuples))
      } else {
        log.debug("Initiating work stealing to get work")
        workers.foreach(actor => actor ! DataRef(table))
        requestWorkloads()
        context.become(workStealing(table))
      }

    case DataNotReady | Terminated if sender == dataHolder =>
      // todo: handle loading error of data holder
      log.error("Data Holder has no data")
      throw new DodoException("Loading data failed!")

    case m => log.debug("Unknown message received: {}", m)
  }

  def pruning(table: Array[TypedColumn[Any]], orderEquivalencies: Array[Seq[Int]], columnIndexTuples: Iterator[(Int, Int)]): Receive = {
    case GetTask if columnIndexTuples.isEmpty =>
      log.debug("Caching idle worker {}", sender.path.name)
      idleWorkers :+= sender

    case GetTask if columnIndexTuples.nonEmpty =>
      var nextTuple = columnIndexTuples.next()
      while (!(reducedColumns.contains(nextTuple._1) && reducedColumns.contains(nextTuple._2)) && columnIndexTuples.nonEmpty) {
        nextTuple = columnIndexTuples.next()
      }

      log.debug("Scheduling task to check equivalence to worker {}", sender.path.name)
      sender ! CheckForEquivalency(nextTuple)
      pendingPruningResponses += 1

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
          equivalenceClasses.map { case (key, cols) =>
            table(key).name -> cols.map(table(_).name)
          }
        )
        log.info("Generating first candidates and starting search")
        state.initializeFrom(reducedColumns)

        if (state.workAvailable) {
          log.error("No OCD candidates generated!")
          shutdown()

        } else if (idleWorkers.nonEmpty) {
          // sending work to idleWorkers
          state.sendEqualBatchToEach(idleWorkers, reducedColumns)( (worker, batch) => {
            log.debug("Scheduling {} of {} items to check OCD to idle worker {}", batch.length, state.queueSize, worker.path.name)
          })
          idleWorkers = Seq.empty
        }

        context.become(findingODs(table))
      }

    case m => log.debug("Unknown message received: {}", m)
  }

  def findingODs(table: Array[TypedColumn[Any]]): Receive = {
    case GetTask =>
      val worker = sender
      state.sendBatchTo(worker, reducedColumns) match {
        case scala.util.Success(_) =>
          log.debug("Scheduling task to check OCD to worker {}", worker.path.name)
        case scala.util.Failure(_) =>
          log.debug("Caching worker {} as idle because work queue is empty", worker.path.name)
          idleWorkers :+= worker
          requestWorkloads()
          context.become(workStealing(table))
      }

    case GetWorkLoad =>
      if (sender != self) {
        sender ! WorkLoad(state.queueSize)
        log.info("Asked for workload")
      }

    case AckReceivedTimeout(workThief) =>
      // TODO: refine technique of how to handle message loss
      state.recoverStolenCandidates(workThief) match {
        case scala.util.Success(_) =>
        case scala.util.Failure(f) =>
          log.info(s"Work got lost! $f")
      }

    case WorkToSend(amount: Int) =>
      log.info("Sending work to {}", sender)
      state.sendBatchToThief(sender, amount)
      import context.dispatcher
      context.system.scheduler.scheduleOnce(5 seconds, self, AckReceivedTimeout(sender))

    case StolenWork(stolenQueue) =>
      log.info("Received work from {}", sender)
      state.enqueue(stolenQueue)
      sender ! AckWorkReceived

    case AckWorkReceived =>
      state.ackStolenCandidates(sender)

    case ODsToCheck(newODs) =>
      state.enqueueNewAndAck(newODs, sender)
      if (state.workAvailable && idleWorkers.nonEmpty) {
        sendWorkToIdleWorkers()
      }

    case m => log.debug("Unknown message received: {}", m)
  }

  def workStealing(table: Array[TypedColumn[Any]]): Receive = {
    case GetTask =>
      idleWorkers :+= sender

    case WorkLoad(queueSize: Int) =>
      log.info("Received workload of size {} from {}", queueSize, sender)
      otherWorkloads :+= (queueSize, sender)

    case WorkLoadTimeout =>
      val sortedWorkloads = otherWorkloads.sorted
      val sum = sortedWorkloads.map(_._1).sum
      val averageWl: Int = sum / (sortedWorkloads.size + 1)
      var ownWorkLoad = 0

      log.debug(
        "Work stealing status: averageWl={}, our hasPendingWork ODs={}",
        averageWl,
        state.pendingSize
      )
      if(sum > 0) {
        // there is work to steal, steal some from the busiest nodes
        for ((otherSize, otherRef) <- sortedWorkloads) {
          val amountToSteal = Seq(
            otherSize - averageWl,
            averageWl - ownWorkLoad,
            settings.workers * settings.maxBatchSize
          ).min
          if (amountToSteal > 0) {
            otherRef ! WorkToSend(amountToSteal)
            log.info("Stealing {} elements from {}", amountToSteal, otherRef)
            ownWorkLoad += amountToSteal
          }
        }
      } else if (state.hasNoPendingWork) {
        // we have no work left and got no work from our work stealing attempt, finished?
        // TODO: make sure others don't have anything pending work anymore as well
        log.warning(
          "This node has no more pending candidates and the queue of other nodes is empty as well. Shutting down node."
        )
        shutdown()
      } else {
        // nothing to steal, but we still wait for worker results, wait for them
        log.info("Nothing to steal, but our workers are still busy.")
        context.become(findingODs(table))
      }

    case StolenWork(stolenQueue) =>
      log.info("Received work from {}", sender)
      state.enqueue(stolenQueue)
      sender ! AckWorkReceived
      sendWorkToIdleWorkers()
      context.become(findingODs(table))

    case ODsToCheck(newODs) =>
      state.enqueueNewAndAck(newODs, sender)
      if (state.workAvailable && idleWorkers.nonEmpty) {
        sendWorkToIdleWorkers()
      }

    case GetWorkLoad if sender == self => // ignore

    case m => log.debug("Unknown message received in `workStealing`: {}", m)
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

  def requestWorkloads(): Unit = {
    log.info("Asking for workloads")
    otherWorkloads = Seq.empty
    workStealingMediator ! Publish(workStealingTopic, GetWorkLoad)

    import context.dispatcher
    context.system.scheduler.scheduleOnce(3 second, self, WorkLoadTimeout)
  }

  def sendWorkToIdleWorkers(): Unit = {
    idleWorkers = idleWorkers.filter(worker => {
      state.sendBatchTo(worker, reducedColumns) match {
        case scala.util.Success(_) =>
          log.debug("Scheduling task to check OCD to idle worker {}", worker.path.name)
          false
        case scala.util.Failure(_) => true
      }
    })
  }
}