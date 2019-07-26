package com.github.codelionx.dodo.actors.master

import java.io.File

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberRemoved, UnreachableMember}
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
import com.github.codelionx.dodo.discovery.{CandidateGenerator, DependencyChecking}
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.language.postfixOps


object ODMaster {

  val name = "odmaster"

  val requestTimeout: FiniteDuration = 5 seconds
  // debugging
  val reportingInterval: FiniteDuration = 5 seconds

  def props(inputFile: Option[File]): Props = Props(new ODMaster(inputFile))

  case class FindODs(dataHolder: ActorRef)

  case class WorkLoad(queueSize: Int, pendingSize: Int)

  case class WorkToSend(amount: Int)

  case class StolenWork(work: Queue[(Seq[Int], Seq[Int])])

  case class AckReceivedTimeout(workThief: ActorRef)

  // work stealing protocol
  case object GetWorkLoad

  case object WorkLoadTimeout

  case object AckWorkReceived

  private case object ReportReducedColumnStatus

}


class ODMaster(inputFile: Option[File])
  extends Actor
    with ActorLogging
    with DependencyChecking
    with CandidateGenerator
    with ReducedColumnsProtocol {

  import ODMaster._
  import context.dispatcher


  private val settings = Settings(context.system)
  private val nWorkers: Int = settings.workers

  val resultCollector: ActorRef = context.actorOf(ResultCollector.props(), ResultCollector.name)
  val workers: Seq[ActorRef] = (0 until nWorkers).map(i =>
    context.actorOf(Worker.props(resultCollector), s"${Worker.name}-$i")
  )

  private val clusterListener: ActorRef = context.actorOf(ClusterListener.props, ClusterListener.name)
  private val cluster = Cluster(context.system)
  private val dataHolder: ActorRef = context.actorOf(DataHolder.props(clusterListener), DataHolder.name)
  private val candidateQueue: ODCandidateQueue = ODCandidateQueue.empty(settings.maxBatchSize)
  private val masterMediator: ActorRef = DistributedPubSub(context.system).mediator
  private val workStealingTopic = "workStealing"
  private val reducedColumnsTopic = "reducedColumns"
  private var pendingPruningResponses = 0
  private var idleWorkers: Seq[ActorRef] = Seq.empty
  private var otherWorkloads: Seq[(Int, Int, ActorRef)] = Seq.empty
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
    reducedColumnsHandling() orElse withWorkStealingHandling {
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
        sender ! AckWorkReceived

      case NewODCandidates(newODs) =>
        candidateQueue.enqueueNewAndAck(newODs, sender)
        sendWorkToIdleWorkers()

      case ReportReducedColumnStatus =>
        log.debug("Master received reduced columns and is already searching OCDs")

      case m => log.debug("Unknown message received in `findingODs`: {}", m)

    }

  def workStealing(pendingResponses: Set[ActorRef]): Receive =
    reducedColumnsHandling() orElse withGetWorkLoadHandling {
      case GetTask =>
        idleWorkers :+= sender

      case WorkLoad(queueSize: Int, pendingSize: Int) =>
        log.debug("Received workload of size {} from {}", queueSize, sender)
        otherWorkloads :+= (queueSize, pendingSize, sender)

      case WorkLoadTimeout if otherWorkloads.isEmpty =>
        startDowningProtocol()

      case WorkLoadTimeout if otherWorkloads.nonEmpty =>
        val sortedWorkloads = otherWorkloads.sorted
        val sum = sortedWorkloads.map(_._1).sum
        val averageWl: Int = sum / (sortedWorkloads.size + 1)
        val pendingSum = sortedWorkloads.map(_._2).sum
        var ownWorkLoad = 0

        log.info(
          "Work stealing status: averageWl={}, others' pendingSum={}, our pending ODs={}",
          averageWl,
          pendingSum,
          candidateQueue.pendingSize
        )
        if (averageWl > 0) {
          // there is work to steal, steal some from the busiest nodes
          val newPendingResponses = sortedWorkloads.flatMap {
            case (otherSize, _, otherRef) =>
              val amountToSteal = Seq(
                otherSize - averageWl,
                averageWl - ownWorkLoad,
                settings.workers * settings.maxBatchSize
              ).min
              if (amountToSteal > 0) {
                otherRef ! WorkToSend(amountToSteal)
                log.info("Stealing {} elements from {}", amountToSteal, otherRef)
                ownWorkLoad += amountToSteal
                context.watch(otherRef)
                Some(otherRef)
              } else {
                None
              }
          }
          context.become(workStealing(newPendingResponses.toSet))
        } else if (candidateQueue.hasNoPendingWork) {
          // we have no work left and got no work from our work stealing attempt
          if (pendingSum > 0) {
            // others will soon have work for us to steal again
            log.info("No work received, will ask again in three seconds")
            context.system.scheduler.scheduleOnce(3 second, self, startWorkStealing())
          }
          // check if ALL nodes are completely out of work
          startDowningProtocol()
        } else {
          // nothing to steal, but we still wait for worker results, wait for them
          log.info("Nothing to steal, but our workers are still busy.")
          context.become(findingODs())
        }

      case Terminated(otherMaster) =>
        context.unwatch(otherMaster)
        updatePendingResponse(pendingResponses, otherMaster)

      case StolenWork(stolenQueue) =>
        log.info("Received {} candidates from {}", stolenQueue.size, sender)
        candidateQueue.enqueue(stolenQueue)
        sender ! AckWorkReceived
        sendWorkToIdleWorkers()
        context.unwatch(sender)
        updatePendingResponse(pendingResponses, sender)

      case NewODCandidates(newODs) =>
        candidateQueue.enqueueNewAndAck(newODs, sender)
        sendWorkToIdleWorkers()

      case WorkToSend(_) =>
        log.info("No work to send")
        sender ! StolenWork(Queue.empty)

      case ReportReducedColumnStatus => // ignore

      case m => log.debug("Unknown message received in `workStealing`: {}", m)
    }

  def downing(pendingMasters: Set[Address]): Receive =
    reducedColumnsHandling() orElse withGetWorkLoadHandling {
      case CurrentClusterState((_, _, nodeAddresses, _, _)) =>
        log.debug("Received current cluster state, {} nodes", nodeAddresses.size)
        if (nodeAddresses.size > 1) {
          masterMediator ! Publish(workStealingTopic, GetWorkLoad)
          context.become(downing(nodeAddresses - cluster.selfAddress))
        } else {
          log.info("Last member in cluster and no work available locally.")
          shutdown()
        }

      case MemberRemoved(node, _) =>
        log.info("Node ({}) left the cluster", node)
        // stop waiting for a workload message from this node
        updatePendingMasters(pendingMasters, node.address)

      case UnreachableMember(node) =>
        log.info("Node ({}) detected unreachable, treated as if down", node)
        // stop waiting for a workload message from this node
        updatePendingMasters(pendingMasters, node.address)

      case WorkLoad(queueSize: Int, pendingSize: Int) =>
        log.info("Received workload of size {} from {}", queueSize, sender.path.address)
        if (queueSize > 0 || pendingSize > 0) {
          cluster.unsubscribe(self)
          startWorkStealing()
        } else {
          updatePendingMasters(pendingMasters, sender.path.address)
        }
    }

  def shutdown(): Unit = {
    log.info("Leaving cluster and shutting down!")
    val cluster = Cluster(context.system)
    cluster.leave(cluster.selfAddress)
    context.children.foreach(_ ! PoisonPill)
    context.stop(self)
  }

  def withGetWorkLoadHandling(block: Receive): Receive = block orElse {
    case GetWorkLoad if sender == self => // ignore

    case GetWorkLoad if sender != self =>
      log.debug("Was asked for workload from {}", sender.path)
      sender ! WorkLoad(candidateQueue.queueSize, candidateQueue.pendingSize)
  }

  def withWorkStealingHandling(block: Receive): Receive = block orElse {
    withGetWorkLoadHandling {
      case WorkToSend(amount: Int) =>
        log.info("Sending work to {}", sender)
        candidateQueue.sendBatchToThief(sender, amount)
        context.watch(sender)

      case AckWorkReceived =>
        candidateQueue.ackStolenCandidates(sender)
        context.unwatch(sender)

      case Terminated(remoteMaster) =>
        context.unwatch(sender)
        log.warning("Work thief {} did not acknowledge stolen work and died.", remoteMaster.path)
        candidateQueue.recoverStolenCandidates(remoteMaster) match {
          case scala.util.Success(_) =>
            log.info("Stolen work queue was recovered!")
          case scala.util.Failure(f) =>
            log.error("Work got lost from remote master {}! {}", remoteMaster, f)
        }
    }
  }

  def startWorkStealing(): Unit = {
    log.info("Asking for workloads")
    otherWorkloads = Seq.empty
    masterMediator ! Publish(workStealingTopic, GetWorkLoad)

    import context.dispatcher
    context.system.scheduler.scheduleOnce(3 second, self, WorkLoadTimeout)
    context.become(workStealing(Set.empty))
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

  def startDowningProtocol(): Unit = {
    log.info("{} started downingProtocol", self.path.name)
    cluster.subscribe(self, classOf[MemberRemoved], classOf[UnreachableMember])
    context.become(downing(Set.empty))
  }

  def updatePendingResponse(pendingResponses: Set[ActorRef], actorToRemove: ActorRef): Unit = {
    val newPendingResponses = pendingResponses - actorToRemove
    if (newPendingResponses.isEmpty) {
      if (candidateQueue.queueSize == 0 && candidateQueue.pendingSize == 0) {
        log.info("Work stealing was unsuccessful")
        startDowningProtocol()
      } else {
        context.become(findingODs())
      }
    } else {
      context.become(workStealing(newPendingResponses))
    }
  }

  def updatePendingMasters(pendingMasters: Set[Address], nodeAddress: Address): Unit = {
    val updatedPendingMasters = pendingMasters - nodeAddress
    if (updatedPendingMasters.isEmpty) {
      log.info("Everybody seems to be finished")
      shutdown()
    } else {
      log.info("Still waiting on work from {}", updatedPendingMasters.map(_.toString).mkString(", "))
      context.become(downing(updatedPendingMasters))
    }
  }
}