package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.github.codelionx.dodo.actors.DataHolder.{DataRef, GetDataRef}
import com.github.codelionx.dodo.actors.ResultCollector.{ConstColumns, OrderEquivalencies}
import com.github.codelionx.dodo.actors.Worker.{CheckForEquivalency, CheckForOD, GetTask, ODsToCheck, OrderEquivalent}
import com.github.codelionx.dodo.discovery.{CandidateGenerator, DependencyChecking}
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.immutable.Queue
import scala.language.postfixOps


object ODMaster {

  val name = "odmaster"

  def props(nWorkers: Int, resultCollector: ActorRef): Props = Props(new ODMaster(nWorkers, resultCollector))

  case class FindODs(dataHolder: ActorRef)

}


class ODMaster(nWorkers: Int, resultCollector: ActorRef) extends Actor with ActorLogging with DependencyChecking with CandidateGenerator {

  import ODMaster._
  private val workers: Seq[ActorRef] = Seq.fill(nWorkers){context.actorOf(Worker.props(resultCollector), Worker.name)}
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
      if (table.length <= 1) {
        log.info("No order dependencies due to length of table")
        context.stop(self)
      }
      val orderEquivalencies = Array.fill(table.length){Seq.empty[Int]}
      val columnIndexTuples = table.indices.combinations(2).map(l => l.head -> l(1))

      reducedColumns = table.indices.toSet
      val constColumns = pruneConstColumns(table)
      resultCollector ! ConstColumns(constColumns)
      workers.foreach(actor => actor ! DataRef(table))
      context.become(pruning(table, orderEquivalencies, columnIndexTuples))

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
        var equivalenceClasses: Map[Int, Seq[Int]] = Map.empty
        reducedColumns.foreach(ind => equivalenceClasses += (ind -> orderEquivalencies(ind)))
        resultCollector ! OrderEquivalencies(equivalenceClasses)
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

  def pruneConstColumns(table: Array[TypedColumn[Any]]): Seq[Int] = {
    var constColumns: Seq[Int] = Seq.empty
    for (column <- table) {
      if (checkConstant(column)) {
        reducedColumns -= table.indexOf(column)
        constColumns = constColumns :+ table.indexOf(column)
      }
    }
    constColumns
  }
}