package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.github.codelionx.dodo.Settings
import com.github.codelionx.dodo.actors.DataHolder.{DataRef, GetDataRef}
import com.github.codelionx.dodo.actors.ResultCollector.{ConstColumns, OrderEquivalencies}
import com.github.codelionx.dodo.actors.SystemCoordinator.Finished
import com.github.codelionx.dodo.actors.Worker._
import com.github.codelionx.dodo.discovery.{CandidateGenerator, DependencyChecking}
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.immutable.Queue
import scala.language.postfixOps


object ODMaster {

  val name = "odmaster"

  def props(nWorkers: Int, resultCollector: ActorRef, systemCoordinator: ActorRef): Props = Props(new ODMaster(nWorkers, resultCollector, systemCoordinator))

  case class FindODs(dataHolder: ActorRef)

}


class ODMaster(nWorkers: Int, resultCollector: ActorRef, systemCoordinator: ActorRef) extends Actor with ActorLogging with DependencyChecking with CandidateGenerator {

  import ODMaster._


  private val settings = Settings(context.system)
  private val workers: Seq[ActorRef] = (0 until nWorkers).map(i =>
    context.actorOf(Worker.props(resultCollector), s"${Worker.name}-$i")
  )
  private var reducedColumns: Set[Int] = Set.empty
  private var pendingPruningResponses = 0

  private var odsToCheck: Queue[(Seq[Int], Seq[Int])] = Queue.empty
  private var waitingForODStatus: Set[Queue[(Seq[Int], Seq[Int])]] = Set.empty

  private var idleWorkers: Seq[ActorRef] = Seq.empty

  override def preStart(): Unit = {
    log.info("Starting {}", name)
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    log.info("Stopping {}", name)

  override def receive: Receive = uninitialized

  def uninitialized: Receive = {
    case FindODs(dataHolder) =>
      dataHolder ! GetDataRef

    case DataRef(table) =>
      if (table.length <= 1) {
        log.info("No order dependencies due to length of table")
        systemCoordinator ! Finished
        context.stop(self)
      }

      log.debug("Looking for constant columns and generating column tuples for equality checking")
      val orderEquivalencies = Array.fill(table.length) {
        Seq.empty[Int]
      }
      val columnIndexTuples = table.indices.combinations(2).map(l => l.head -> l(1))

      reducedColumns = table.indices.toSet
      val constColumns = pruneConstColumns(table)
      log.debug("Found {} constant columns, starting pruning", constColumns.length)
      resultCollector ! ConstColumns(constColumns.map(table(_).name))
      workers.foreach(actor => actor ! DataRef(table))
      context.become(pruning(table, orderEquivalencies, columnIndexTuples))

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
        odsToCheck ++= generateFirstCandidates(reducedColumns)

        // sending work to idleWorkers
        if (odsToCheck.isEmpty) {
          log.error("No OCD candidates generated!")
          systemCoordinator ! Finished

        } else {
          val queueLength = odsToCheck.length
          val workers = idleWorkers.length
          val batchLength = math.min(
            queueLength / workers,
            settings.maxBatchSize
          )
          idleWorkers.foreach(worker => {
            val (workerODs, newQueue) = odsToCheck.splitAt(batchLength)
            log.debug("Scheduling {} of {} items to check OCD to idle worker {}", workerODs.length, queueLength, worker.path.name)
            worker ! CheckForOD(workerODs, reducedColumns)

            odsToCheck = newQueue
            waitingForODStatus += workerODs
          })
          idleWorkers = Seq.empty
        }

        context.become(findingODs(table))
      }

    case _ => log.debug("Unknown message received")
  }

  def findingODs(table: Array[TypedColumn[Any]]): Receive = {
    case GetTask =>
      dequeueBatch() match {
        case Some(workerODs) =>
          log.debug("Scheduling task to check OCD to worker {}", sender.path.name)
          sender ! CheckForOD(workerODs, reducedColumns)
          waitingForODStatus += workerODs
        case None =>
          log.debug("Caching worker {} as idle because work queue is empty", sender.path.name)
          idleWorkers :+= sender
      }

    case ODsToCheck(originalODs, newODs) =>
      odsToCheck ++= newODs
      waitingForODStatus -= originalODs
      if (odsToCheck.nonEmpty && idleWorkers.nonEmpty) {
        idleWorkers = idleWorkers.filter(worker => {
          dequeueBatch() match {
            case Some(work) =>
              log.debug("Scheduling task to check OCD to idle worker {}", worker.path.name)
              worker ! CheckForOD(work, reducedColumns)
              waitingForODStatus += work
              false
            case None => true
          }
        })
      }
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

  def dequeueBatch(): Option[Queue[(Seq[Int], Seq[Int])]] = {
    if (odsToCheck.isEmpty) {
      None
    } else {
      val queueLength = odsToCheck.length
      val batchLength = math.min(
        queueLength,
        settings.maxBatchSize
      )
      val (workerODs, newQueue) = odsToCheck.splitAt(batchLength)
      odsToCheck = newQueue
      Some(workerODs)
    }
  }
}