package com.github.codelionx.dodo.actors.master

import java.io.File

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor._
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator._
import com.github.codelionx.dodo.GlobalImplicits.TypedColumnConversions._
import com.github.codelionx.dodo.Settings
import com.github.codelionx.dodo.actors.ClusterListener.{GetNumberOfNodes, NumberOfNodes}
import com.github.codelionx.dodo.actors.DataHolder.{DataNotReady, DataRef, FetchDataFromCluster, LoadDataFromDisk}
import com.github.codelionx.dodo.actors.ResultCollector.{ConstColumns, OrderEquivalencies}
import com.github.codelionx.dodo.actors.Worker._
import com.github.codelionx.dodo.actors._
import com.github.codelionx.dodo.actors.master.ReducedColumnsProtocol.{GetReducedColumns, ReducedColumns}
import com.github.codelionx.dodo.actors.master.WorkStealingProtocol._
import com.github.codelionx.dodo.discovery.{CandidateGenerator, DependencyChecking}
import com.github.codelionx.dodo.types.TypedColumn

import scala.concurrent.duration._
import scala.language.postfixOps


object ODMaster {

  val name = "odmaster"

  val requestTimeout: FiniteDuration = 5 seconds

  // for debugging
  val reportingInterval: FiniteDuration = 5 seconds

  private[master] val workStealingTopic = "workStealing"

  private[master] val reducedColumnsTopic = "reducedColumns"

  def props(inputFile: Option[File]): Props = Props(new ODMaster(inputFile))

  // messages
  case class FindODs(dataHolder: ActorRef)

  private case object ReportReducedColumnStatus

}


class ODMaster(inputFile: Option[File])
  extends Actor
    with ActorLogging
    with DependencyChecking
    with CandidateGenerator
    with ReducedColumnsProtocol
    with WorkStealingProtocol
    with DowningProtocol {

  import ODMaster._
  import context.dispatcher


  protected val settings: Settings = Settings(context.system)
  protected val nWorkers: Int = settings.workers

  protected val cluster: Cluster = Cluster(context.system)
  protected val clusterListener: ActorRef = context.actorOf(ClusterListener.props, ClusterListener.name)
  protected val dataHolder: ActorRef = context.actorOf(DataHolder.props(clusterListener), DataHolder.name)
  protected val resultCollector: ActorRef = context.actorOf(ResultCollector.props(), ResultCollector.name)
  protected val workers: Seq[ActorRef] = (0 until nWorkers).map(i =>
    context.actorOf(Worker.props(resultCollector), s"${Worker.name}-$i")
  )

  protected val candidateQueue: ODCandidateQueue = ODCandidateQueue.empty(settings.maxBatchSize)
  protected val masterMediator: ActorRef = DistributedPubSub(context.system).mediator

  private var pendingPruningResponses = 0
  private var idleWorkers: Seq[ActorRef] = Seq.empty
  private var table: Array[TypedColumn[Any]] = Array.empty

  override def supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 15 seconds) {
      // if data holder fails, stop the whole system
      case _: Exception if sender == dataHolder =>
        shutdown()
        Stop
      case _: Exception => Restart
    }

  override def preStart(): Unit = {
    log.info("Starting {}", name)
    Reaper.watchWithDefault(self)
    masterMediator ! Put(self)
    masterMediator ! Subscribe(reducedColumnsTopic, self)
    masterMediator ! Subscribe(workStealingTopic, self)
  }

  override def postStop(): Unit = {
    log.info("Stopping {}", name)
    masterMediator ! Unsubscribe(reducedColumnsTopic, self)
    masterMediator ! Unsubscribe(workStealingTopic, self)
  }

  override def receive: Receive = unsubscribed()

  def unsubscribed(receivedPubSubAcks: Int = 0): Receive =
    workStealingHandling(allowStealing = false) orElse
    downingHandling() orElse
    reducedColumnsHandling() orElse {
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

      case m => log.debug("Unknown message received in `unsubscribed`: {}", m)
    }

  def uninitialized(first: Boolean): Receive =
    workStealingHandling(allowStealing = false) orElse
    downingHandling() orElse
    reducedColumnsHandling() orElse {
      case DataRef(tableRef) =>
        table = tableRef
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
            pruning(tempReducedColumns, orderEquivalencies, columnIndexTuples, first, Cancellable.alreadyCancelled)
          )
        } else {
          log.debug("Requesting reduced columns")

          val cancellable = context.system.scheduler.schedule(
            0 seconds,
            requestTimeout,
            masterMediator,
            Send(self.path.toStringWithoutAddress, GetReducedColumns, localAffinity = false)
          )
          if (log.isDebugEnabled) {
            context.system.scheduler.scheduleOnce(reportingInterval, self, ReportReducedColumnStatus)
          }
          context.become(pruning(Set.empty, Array.empty, Iterator.empty, first, cancellable))
        }

      case DataNotReady =>
        log.error("Data Holder has no data. Data loading failed!")
        shutdown()

      case m => log.debug("Unknown message received in `uninitialized`: {}", m)
    }

  def pruning(
               tempReducedColumns: Set[Int],
               orderEquivalencies: Array[Seq[Int]],
               columnIndexTuples: Iterator[(Int, Int)],
               first: Boolean,
               reducedColumnsCancellable: Cancellable
             ): Receive =
    workStealingHandling(allowStealing = false) orElse
    downingHandling(fakeWorkAvailable = first, virulent = !first) orElse
    reducedColumnsHandling() orElse {
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

      case OrderEquivalencyChecked(od, isOrderEquiv) =>
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
            pruning(newReducedColumns, orderEquivalencies, columnIndexTuples, first, reducedColumnsCancellable)
          )
        }

      case ReducedColumns(cols) if first =>
        log.info(
          "Received reduced columns, generating first candidates and starting search. Remaining cols: {}",
          cols.toSeq.sorted.map(table(_).name)
        )
        reducedColumnsCancellable.cancel()
        setReducedColumns(cols)
        candidateQueue.initializeFrom(cols)

        if (candidateQueue.noWorkAvailable) {
          log.error("No OCD candidates generated!")
          shutdown()

        } else if (idleWorkers.nonEmpty) {
          // sending work to idleWorkers
          candidateQueue.sendEqualBatchToEach(idleWorkers, cols)((worker, batch) => {
            log.debug("Scheduling {} items to check OCD to idle worker {}", batch.length, worker.path.name)
          })
          idleWorkers = Seq.empty
        }

        context.become(findingODs())

      case ReducedColumns(cols) if !first =>
        log.info(
          "Received reduced columns, initiating work stealing to get work. Remaining cols: {}",
          cols.toSeq.sorted.map(table(_).name)
        )
        reducedColumnsCancellable.cancel()
        setReducedColumns(cols)
        workers.foreach(actor => actor ! DataRef(table))
        startWorkStealing()

      case ReportReducedColumnStatus =>
        log.debug("Waiting for reduced columns...")
        context.system.scheduler.scheduleOnce(reportingInterval, self, ReportReducedColumnStatus)

      case m => log.debug("Unknown message received in `pruning`: {}", m)
    }

  def findingODs(): Receive =
    workStealingHandling(allowStealing = true) orElse
    downingHandling() orElse
    reducedColumnsHandling() orElse {
      case GetTask =>
        val worker = sender
        candidateQueue.sendBatchTo(worker, getReducedColumns) match {
          case scala.util.Success(_) =>
            log.debug("Scheduling task to check OCD to worker {}", worker.path.name)
          case scala.util.Failure(_) =>
            log.debug("Caching worker {} as idle because work queue is empty", worker.path.name)
            idleWorkers :+= worker
            startWorkStealing()
        }

      case StolenWork(stolenQueue) =>
        log.info("Received {} candidates from {}", stolenQueue.size, sender)
        candidateQueue.enqueue(stolenQueue)
        sender ! AckStolenWork

      case NewODCandidates(newODs) =>
        candidateQueue.enqueueNewAndAck(newODs, sender)
        sendWorkToIdleWorkers()

      case ReportReducedColumnStatus =>
        log.debug("Master received reduced columns and is already searching OCDs")

      case m => log.debug("Unknown message received in `findingODs`: {}", m)
    }

  def workStealing: Receive =
    workStealingHandling(allowStealing = false) orElse
    downingHandling() orElse
    reducedColumnsHandling() orElse
    workStealingImpl orElse {
      case GetTask =>
        idleWorkers :+= sender

      case NewODCandidates(newODs) =>
        candidateQueue.enqueueNewAndAck(newODs, sender)
        sendWorkToIdleWorkers()

      case ReportReducedColumnStatus => // ignore

      case m => log.debug("Unknown message received in `workStealing`: {}", m)
    }

  def downing: Receive =
    workStealingHandling(allowStealing = false) orElse
    downingHandling() orElse
    reducedColumnsHandling() orElse
    downingImpl

  def shutdown(): Unit = {
    log.info("Leaving cluster and shutting down!")
    cluster.leave(cluster.selfAddress)
    context.children.foreach(_ ! PoisonPill)
    context.stop(self)
  }

  def sendWorkToIdleWorkers(): Unit = {
    if (candidateQueue.workAvailable && idleWorkers.nonEmpty) {
      idleWorkers = idleWorkers.filter(worker => {
        candidateQueue.sendBatchTo(worker, getReducedColumns) match {
          case scala.util.Success(_) =>
            log.debug("Scheduling task to check OCD to idle worker {}", worker.path.name)
            false
          case scala.util.Failure(_) => true
        }
      })
    }
  }
}
