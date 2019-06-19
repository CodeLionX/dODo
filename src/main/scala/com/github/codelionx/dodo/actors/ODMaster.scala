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

  case class WorkToSend(amount: Int)

  case class StolenWork(work: Queue[(Seq[Int], Seq[Int])])

  case object AckWorkReceived

  case class AckReceivedTimeout(workThief: ActorRef)

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

  private var odCandidates: Queue[(Seq[Int], Seq[Int])] = Queue.empty
  private var pendingOdCandidates: Map[ActorRef, Queue[(Seq[Int], Seq[Int])]] = Map.empty
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
      log.info("subscribed to the workStealing mediator")
      clusterListener ! GetNumberOfNodes

    case NumberOfNodes(number) =>
      dataHolder ! LoadDataFromDisk(settings.inputFilePath)
      context.become(uninitialized(number <= 1))

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
        workers.foreach(actor => actor ! DataRef(table))
        requestWorkloads()
        context.become(workStealing(table))
      }

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
        odCandidates ++= generateFirstCandidates(reducedColumns)

        if (odCandidates.isEmpty) {
          log.error("No OCD candidates generated!")
          shutdown()

        } else if (idleWorkers.nonEmpty) {
          // sending work to idleWorkers
          val queueLength = odCandidates.length
          val workers = idleWorkers.length
          val batchLength = math.min(
            queueLength / workers,
            settings.maxBatchSize
          )
          idleWorkers.foreach(worker => {
            val (workerODs, newQueue) = odCandidates.splitAt(batchLength)
            log.debug("Scheduling {} of {} items to check OCD to idle worker {}", workerODs.length, queueLength, worker.path.name)
            worker ! CheckForOD(workerODs, reducedColumns)

            odCandidates = newQueue
            pendingOdCandidates += worker -> workerODs
          })
          idleWorkers = Seq.empty
        }

        context.become(findingODs(table))
      }

    case m => log.debug("Unknown message received: {}", m)
  }

  def findingODs(table: Array[TypedColumn[Any]]): Receive = {
    case GetTask =>
      dequeueBatch() match {
        case Some(workerODs) =>
          log.debug("Scheduling task to check OCD to worker {}", sender.path.name)
          sender ! CheckForOD(workerODs, reducedColumns)
          pendingOdCandidates += sender -> workerODs
        case None =>
          log.debug("Caching worker {} as idle because work queue is empty", sender.path.name)
          idleWorkers :+= sender
          requestWorkloads()
          context.become(workStealing(table))
      }

    case GetWorkLoad =>
      if (sender != self) {
        sender ! WorkLoad(odCandidates.length)
        log.info("Asked for workload")
      }

    case AckReceivedTimeout(workThief) =>
      // TODO: refine technique of how to handle message loss
      if (pendingOdCandidates.contains(workThief)) {
        log.info("Work got lost")
        odCandidates ++= pendingOdCandidates(workThief)
        pendingOdCandidates -= workThief
      }

    case WorkToSend(amount: Int) =>
      // in case the length of odCandidates has shrunk since it was send to the WorkThief
      val updatedAmount = math.min(amount, odCandidates.length / 2)
      val (stolenQueue, newQueue) = odCandidates.splitAt(updatedAmount)
      odCandidates = newQueue
      pendingOdCandidates += (sender -> stolenQueue)
      log.info("Sending work to {}", sender)
      sender ! StolenWork(stolenQueue)
      import context.dispatcher
      context.system.scheduler.scheduleOnce(5 seconds, self, AckReceivedTimeout(sender))

    case StolenWork(stolenQueue) =>
      odCandidates ++= stolenQueue
      log.info("Received work from {}", sender)
      sender ! AckWorkReceived

    case AckWorkReceived =>
      pendingOdCandidates -= sender

    case ODsToCheck(newODs) =>
      odCandidates ++= newODs
      pendingOdCandidates -= sender
      if (odCandidates.nonEmpty && idleWorkers.nonEmpty) {
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
      log.info("Received workloadTimeout: averageWl={}", averageWl)
      for ((otherSize, otherRef) <- sortedWorkloads) {
        val amountToSteal = Seq(otherSize - averageWl, averageWl - ownWorkLoad, 1000).min
        if (amountToSteal > 0) {
          otherRef ! WorkToSend(amountToSteal)
          log.info("Stealing {} elements from {}", amountToSteal, otherRef)
          ownWorkLoad += amountToSteal
        }
      }
      if (sum == 0 && pendingOdCandidates.isEmpty) {
        // TODO: make sure others don't have anything pending anymore as well
        shutdown()
      }

    case StolenWork(stolenQueue) =>
      odCandidates ++= stolenQueue
      log.info("Received work from {}", sender)
      sender ! AckWorkReceived
      sendWorkToIdleWorkers()
      context.become(findingODs(table))

    case ODsToCheck(newODs) =>
      odCandidates ++= newODs
      pendingOdCandidates -= sender
      if (odCandidates.nonEmpty && idleWorkers.nonEmpty) {
        sendWorkToIdleWorkers()
      }

    case m => log.debug("Unknown message received: {}", m)
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
      dequeueBatch() match {
        case Some(work) =>
          log.debug("Scheduling task to check OCD to idle worker {}", worker.path.name)
          worker ! CheckForOD(work, reducedColumns)
          pendingOdCandidates += worker -> work
          false
        case None => true
      }
    })
  }

  def dequeueBatch(): Option[Queue[(Seq[Int], Seq[Int])]] = {
    if (odCandidates.isEmpty) {
      None
    } else {
      val queueLength = odCandidates.length
      val batchLength = math.min(
        queueLength,
        settings.maxBatchSize
      )
      val (workerODs, newQueue) = odCandidates.splitAt(batchLength)
      odCandidates = newQueue
      Some(workerODs)
    }
  }
}