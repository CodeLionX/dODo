package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.github.codelionx.dodo.actors.DataHolder.{DataRef, GetDataRef}
import com.github.codelionx.dodo.actors.Worker.{CheckForEquivalency, CheckForOD, GetTask, ODsToCheck, OrderEquivalent}
import com.github.codelionx.dodo.discovery.{CandidateGenerator, Pruning}
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.immutable.Queue
import scala.language.postfixOps


object ODMaster {

  val name = "odmaster"

  def props(nWorkers: Int): Props = Props(new ODMaster(nWorkers))

  case class FindODs(dataHolder: ActorRef)

}


class ODMaster(nWorkers: Int) extends Actor with ActorLogging with Pruning with CandidateGenerator {

  import ODMaster._
  private val workers: Seq[ActorRef] = Seq.fill(nWorkers){context.actorOf(Worker.props(), Worker.name)}
  private var reducedColumns: Set[Int] = Set.empty
  private var pendingPruningResponses = 0

  private var odsToCheck: Queue[(Seq[Int], Seq[Int])] = Queue.empty
  private var waitingForODStatus: Set[(Seq[Int], Seq[Int])] = Set.empty

  override def preStart(): Unit = {
    log.info(s"Starting $name")
    Reaper.watchWithDefault(self)
    // TODO make reaper watch workers
  }

  override def postStop(): Unit =
    log.info(s"Stopping $name")

  override def receive: Receive = uninitialized

  def uninitialized: Receive = {
    case FindODs(dataHolder) =>
      dataHolder ! GetDataRef
    case DataRef(table) =>
      if (table.size <= 1) {
        log.info("No order dependencies due to size of table")
        context.stop(self)
      }
      val orderEquivalencies = Array.fill(table.size){Set.empty[Int]}
      val columnIndexTuples = table.indices.combinations(2).map(l => l.head -> l(1))

      reducedColumns = (0 to table.size - 1).toSet
      pruneConstColumns(table)
      workers.foreach(actor => actor ! DataRef(table))
      context.become(pruning(table, orderEquivalencies, columnIndexTuples))
    case _ => log.info("Unknown message received")
  }

  def pruning(table: Array[TypedColumn[Any]], orderEquivalencies: Array[Set[Int]], columnIndexTuples: Iterator[(Int, Int)]): Receive = {
    case GetTask =>
      if(columnIndexTuples.isEmpty) {
        // TODO: Remember to send task once all pruning answers are in and the state has been changed
      }
      else {
        var nextTuple = columnIndexTuples.next()
        while ((reducedColumns.contains(nextTuple._1) || reducedColumns.contains(nextTuple._2)) && !columnIndexTuples.isEmpty) {
          nextTuple = columnIndexTuples.next()
        }
        sender ! CheckForEquivalency(nextTuple)
        log.info(s"Worker tasked to check OE: $nextTuple")
        pendingPruningResponses += 1
      }
    case OrderEquivalent(od, isOrderEquiv) =>
      if (isOrderEquiv) {
        reducedColumns -= od._2
        orderEquivalencies(od._1) += od._2
        log.info(s"OrderEquivalency found: $od")
      }
      pendingPruningResponses -= 1
      if (pendingPruningResponses == 0 && columnIndexTuples.isEmpty) {
        log.info("Pruning done")
        odsToCheck ++= generateFirstCandidates(reducedColumns)
        context.become(findingODs(table))
      }
    case _ => log.info("Unknown message received")
  }

  def findingODs(table: Array[TypedColumn[Any]]): Receive = {
    case GetTask =>
      if (odsToCheck.nonEmpty) {
        val (odToCheck, newQueue) = odsToCheck.dequeue
        odsToCheck = newQueue
        if (checkOrderDependent((odToCheck._1 ++ odToCheck._2, odToCheck._2 ++ odToCheck._1), table))
        log.info(s"Worker tasked to check OD: $odToCheck")
        sender ! CheckForOD(odToCheck, reducedColumns)
        waitingForODStatus += odToCheck
      }
    case ODsToCheck(originalOD, newODs) =>
      odsToCheck ++= newODs
      waitingForODStatus -= originalOD
      if (waitingForODStatus.isEmpty && odsToCheck.isEmpty) {
        log.info("Found all ODs")
        context.stop(self)
      }
    case _ => log.info("Unknown message received")
  }

  def pruneConstColumns(table: Array[TypedColumn[Any]]) = {
    for (column <- table) {
      if (checkConstant(column)) {
        log.info(s"found const column: ${table.indexOf(column)}")
        reducedColumns -= table.indexOf(column)
      }
    }
  }
}