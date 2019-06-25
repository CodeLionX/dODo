package com.github.codelionx.dodo.actors

import java.io.File

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator._
import com.github.codelionx.dodo.GlobalImplicits.TypedColumnConversions._
import com.github.codelionx.dodo.actors.ClusterListener.{GetNumberOfNodes, NumberOfNodes}
import com.github.codelionx.dodo.actors.DataHolder.{DataNotReady, DataRef, FetchDataFromCluster, LoadDataFromDisk}
import com.github.codelionx.dodo.actors.ResultCollector.{ConstColumns, OrderEquivalencies}
import com.github.codelionx.dodo.actors.Worker._
import com.github.codelionx.dodo.discovery.{CandidateGenerator, DependencyChecking, ODCandidateQueue}
import com.github.codelionx.dodo.types.TypedColumn
import com.github.codelionx.dodo.{DodoException, Settings}

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.language.postfixOps


object ODMaster {

  val name = "odmaster"

  val requestTimeout: FiniteDuration = 5 seconds

  def props(inputFile: Option[File]): Props = Props(new ODMaster(inputFile))

  case class FindODs(dataHolder: ActorRef)

  // reduced columns sync
  case object GetReducedColumns

  case class ReducedColumns(cols: Set[Int])

  // work stealing protocol
  case object GetWorkLoad

  case class WorkLoad(queueSize: Int)

  case object WorkLoadTimeout

  case class WorkToSend(amount: Int)

  case class StolenWork(work: Queue[(Seq[Int], Seq[Int])])

  case object AckWorkReceived

  case class AckReceivedTimeout(workThief: ActorRef)

  // debugging
  val reportingInterval: FiniteDuration = 5 seconds

  private case object ReportReducedColumnStatus

}


class ODMaster(inputFile: Option[File])
  extends Actor
    with ActorLogging
    with DependencyChecking
    with CandidateGenerator {

  import ODMaster._
  import context.dispatcher


  private val settings = Settings(context.system)
  private val nWorkers: Int = settings.workers

  private val clusterListener: ActorRef = context.actorOf(ClusterListener.props, ClusterListener.name)
  private val dataHolder: ActorRef = context.actorOf(DataHolder.props(clusterListener), DataHolder.name)
  val resultCollector: ActorRef = context.actorOf(ResultCollector.props(), ResultCollector.name)
  val workers: Seq[ActorRef] = (0 until nWorkers).map(i =>
    context.actorOf(Worker.props(resultCollector), s"${Worker.name}-$i")
  )

  private val candidateQueue: ODCandidateQueue = ODCandidateQueue.empty(settings.maxBatchSize)

  private var reducedColumns: Set[Int] = Set.empty
  private var pendingPruningResponses = 0

  private var idleWorkers: Seq[ActorRef] = Seq.empty

  private var otherWorkloads: Seq[(Int, ActorRef)] = Seq.empty

  private val masterMediator: ActorRef = DistributedPubSub(context.system).mediator
  private val workStealingTopic = "workStealing"
  private val reducedColumnsTopic = "reducedColumns"

  override def preStart(): Unit = {
    log.info("Starting {}", name)
    Reaper.watchWithDefault(self)
    masterMediator ! Put(self)
    masterMediator ! Subscribe(reducedColumnsTopic, self)
    masterMediator ! Subscribe(workStealingTopic, self)
  }

  override def postStop(): Unit =
    log.info("Stopping {}", name)

  override def receive: Receive = unsubscribed()

  def unsubscribed(receivedPubSubAcks: Int = 0): Receive = {
    case SubscribeAck(Subscribe(`reducedColumnsTopic` | `workStealingTopic`, None, `self`)) =>
      val newReceivedPubSubAcks = receivedPubSubAcks + 1
      if (newReceivedPubSubAcks < 2) {
        context.become(unsubscribed(newReceivedPubSubAcks))
      } else {
        log.debug("subscribed to the reducedColumns and workStealing topic")
        clusterListener ! GetNumberOfNodes
      }

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

        val constColumns = constColumnIndices(table)
        resultCollector ! ConstColumns(constColumns.map(table(_).name))

        log.debug("Found {} constant columns, starting pruning", constColumns.length)
        workers.foreach(actor => actor ! DataRef(table))
        val tempReducedColumns = table.indices.toSet -- constColumns
        context.become(
          pruning(table, tempReducedColumns, orderEquivalencies, columnIndexTuples, first, Cancellable.alreadyCancelled)
        )
      } else {
        log.debug("Requesting reduced columns")

        val cancallable = context.system.scheduler.schedule(
          0 seconds,
          requestTimeout,
          masterMediator,
          Send(self.path.toStringWithoutAddress, GetReducedColumns, localAffinity = false)
        )
        if (log.isDebugEnabled) {
          context.system.scheduler.scheduleOnce(reportingInterval, self, ReportReducedColumnStatus)
        }
        context.become(pruning(table, Set.empty, Array.empty, Iterator.empty, first, cancallable))
      }

    case GetReducedColumns =>
      log.warning("Received request to supply reduced columns, but we have them not ready yet. Ignoring")

    case DataNotReady | Terminated if sender == dataHolder =>
      // todo: handle loading error of data holder
      log.error("Data Holder has no data")
      throw new DodoException("Loading data failed!")

    case m => log.debug("Unknown message received: {}", m)
  }

  def pruning(
               table: Array[TypedColumn[Any]],
               tempReducedColumns: Set[Int],
               orderEquivalencies: Array[Seq[Int]],
               columnIndexTuples: Iterator[(Int, Int)],
               first: Boolean,
               reducedColumnsCancellable: Cancellable
             ): Receive = {
    case GetTask if columnIndexTuples.isEmpty =>
      log.debug("Caching idle worker {}", sender.path.name)
      idleWorkers :+= sender

    case GetTask if columnIndexTuples.nonEmpty =>
      var nextTuple = columnIndexTuples.next()
      while (
        !(tempReducedColumns.contains(nextTuple._1) && tempReducedColumns.contains(nextTuple._2))
          && columnIndexTuples.nonEmpty
      ) {
        nextTuple = columnIndexTuples.next()
      }

      log.debug("Scheduling task to check equivalence to worker {}", sender.path.name)
      sender ! CheckForEquivalency(nextTuple)
      pendingPruningResponses += 1

    case OrderEquivalent(od, isOrderEquiv) =>
      val newReducedColumns = if (isOrderEquiv) {
        orderEquivalencies(od._1) :+= od._2
        tempReducedColumns - od._2
      } else {
        tempReducedColumns
      }
      pendingPruningResponses -= 1
      if (pendingPruningResponses == 0 && columnIndexTuples.isEmpty) {
        val equivalenceClasses = newReducedColumns.foldLeft(Map.empty[Int, Seq[Int]])(
          (map, ind) => map + (ind -> orderEquivalencies(ind))
        )
        resultCollector ! OrderEquivalencies(
          equivalenceClasses.map { case (key, cols) =>
            table(key).name -> cols.map(table(_).name)
          }
        )
        log.info("Pruning done")
        masterMediator ! Publish(reducedColumnsTopic, ReducedColumns(newReducedColumns))
      } else {
        context.become(
          pruning(table, newReducedColumns, orderEquivalencies, columnIndexTuples, first, reducedColumnsCancellable)
        )
      }

    case ReducedColumns(cols) if first =>
      log.info("Received reduced columns, generating first candidates and starting search")
      reducedColumnsCancellable.cancel()
      reducedColumns = cols
      candidateQueue.initializeFrom(reducedColumns)

      if (candidateQueue.noWorkAvailable) {
        log.error("No OCD candidates generated!")
        shutdown()

      } else if (idleWorkers.nonEmpty) {
        // sending work to idleWorkers
        candidateQueue.sendEqualBatchToEach(idleWorkers, reducedColumns)((worker, batch) => {
          log.debug("Scheduling {} items to check OCD to idle worker {}", batch.length, worker.path.name)
        })
        idleWorkers = Seq.empty
      }

      context.become(findingODs(table))

    case ReducedColumns(cols) if !first =>
      log.debug("Received reduced columns, initiating work stealing to get work")
      reducedColumnsCancellable.cancel()
      reducedColumns = cols
      workers.foreach(actor => actor ! DataRef(table))
      requestWorkloads()
      context.become(workStealing(table))

    case ReportReducedColumnStatus =>
      log.debug("Waiting for reduced columns...")
      context.system.scheduler.scheduleOnce(reportingInterval, self, ReportReducedColumnStatus)

    case GetReducedColumns =>
      log.warning("Received request to supply reduced columns, but we have them not ready yet. Ignoring")

    case m => log.debug("Unknown message received: {}", m)
  }

  def findingODs(table: Array[TypedColumn[Any]]): Receive = withWorkStealingHandling {
    case GetTask =>
      val worker = sender
      candidateQueue.sendBatchTo(worker, reducedColumns) match {
        case scala.util.Success(_) =>
          log.debug("Scheduling task to check OCD to worker {}", worker.path.name)
        case scala.util.Failure(_) =>
          log.debug("Caching worker {} as idle because work queue is empty", worker.path.name)
          idleWorkers :+= worker
          requestWorkloads()
          context.become(workStealing(table))
      }

    case GetReducedColumns =>
      log.info(s"Sending reduced columns to ${sender.path}")
      sender ! ReducedColumns(reducedColumns)

    case StolenWork(stolenQueue) =>
      log.info("Received work from {}", sender)
      candidateQueue.enqueue(stolenQueue)
      sender ! AckWorkReceived

    case ODsToCheck(newODs) =>
      candidateQueue.enqueueNewAndAck(newODs, sender)
      sendWorkToIdleWorkers()

    case ReportReducedColumnStatus =>
      log.debug("Master received reduced columns and is already searching OCDs")
  }

  def workStealing(table: Array[TypedColumn[Any]]): Receive = {
    case GetTask =>
      idleWorkers :+= sender

    case GetReducedColumns =>
      log.info(s"Sending reduced columns to ${sender.path}")
      sender ! ReducedColumns(reducedColumns)

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
        candidateQueue.pendingSize
      )
      if (sum > 0) {
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
      } else if (candidateQueue.hasNoPendingWork) {
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
      candidateQueue.enqueue(stolenQueue)
      sender ! AckWorkReceived
      sendWorkToIdleWorkers()
      context.become(findingODs(table))

    case ODsToCheck(newODs) =>
      candidateQueue.enqueueNewAndAck(newODs, sender)
      sendWorkToIdleWorkers()

    case GetWorkLoad if sender == self => // ignore
    case ReportReducedColumnStatus => // ignore

    case m => log.debug("Unknown message received in `workStealing`: {}", m)
  }

  def shutdown(): Unit = {
    val cluster = Cluster(context.system)
    cluster.leave(cluster.selfAddress)
    context.children.foreach(_ ! PoisonPill)
    context.stop(self)
  }

  def withWorkStealingHandling(block: Receive): Receive = block orElse {
    case GetWorkLoad if sender == self => // ignore

    case GetWorkLoad if sender != self =>
      sender ! WorkLoad(candidateQueue.queueSize)
      log.info("Asked for workload")

    case WorkToSend(amount: Int) =>
      log.info("Sending work to {}", sender)
      candidateQueue.sendBatchToThief(sender, amount)
      context.watch(sender)

    case AckWorkReceived =>
      candidateQueue.ackStolenCandidates(sender)
      context.unwatch(sender)

    case Terminated(remoteMaster) =>
      log.warning("Work thief {} did not acknowledge stolen work and died.", remoteMaster.path)
      candidateQueue.recoverStolenCandidates(remoteMaster) match {
        case scala.util.Success(_) =>
          log.info("Stolen work queue was recovered!")
        case scala.util.Failure(f) =>
          log.error("Work got lost! {}", f)
      }
  }

  def requestWorkloads(): Unit = {
    log.info("Asking for workloads")
    otherWorkloads = Seq.empty
    masterMediator ! Publish(workStealingTopic, GetWorkLoad)

    import context.dispatcher
    context.system.scheduler.scheduleOnce(3 second, self, WorkLoadTimeout)
  }

  def sendWorkToIdleWorkers(): Unit =
    if (candidateQueue.workAvailable && idleWorkers.nonEmpty) {
      idleWorkers = idleWorkers.filter(worker => {
        candidateQueue.sendBatchTo(worker, reducedColumns) match {
          case scala.util.Success(_) =>
            log.debug("Scheduling task to check OCD to idle worker {}", worker.path.name)
            false
          case scala.util.Failure(_) => true
        }
      })
    }
}