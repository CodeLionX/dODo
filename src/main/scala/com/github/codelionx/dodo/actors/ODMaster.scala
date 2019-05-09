package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.routing.BroadcastPool
import com.github.codelionx.dodo.actors.DataHolder.{DataRef, GetDataRef}
import com.github.codelionx.dodo.actors.Worker.{CheckForEquivalency, CheckForOD, GetTask, ODsToCheck, OrderEquivalent}
import com.github.codelionx.dodo.discovery.{CandidateGenerator, Pruning}
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.mutable
import scala.language.postfixOps


object ODMaster {

  val name = "odmaster"

  def props(nWorkers: Int): Props = Props(new ODMaster(nWorkers))

  case class FindODs(dataHolder: ActorRef)

}


class ODMaster(nWorkers: Int) extends Actor with ActorLogging with Pruning with CandidateGenerator {

  import ODMaster._
  private val broadcastRouter: ActorRef = context.actorOf(BroadcastPool(nWorkers).props(Props[Worker]), "broadcastRouter")
  private var reducedColumns: Set[Int] = Set.empty[Int]
  private var orderEquivalencies: Array[Set[Int]] = Array.empty
  private var lastTuple = (0, 0)
  private var pruningAsked = 0
  private var pruningAnswered = 0

  private val odsToCheck: mutable.Queue[(List[Int], List[Int])] = mutable.Queue.empty[(List[Int], List[Int])]
  private var waitingForODStatus: Set[(List[Int], List[Int])] = Set.empty[(List[Int], List[Int])]

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
      orderEquivalencies = Array.fill(table.size){Set.empty[Int]}
      reducedColumns = (0 to table.size - 1).toSet
      context.become(pruning(table))
      broadcastRouter ! DataRef(table)
      pruneConstColumns(table)
    case _ => log.info("Unknown message received")
  }

  def pruning(table: Array[TypedColumn[Any]]): Receive = {
    case GetTask =>
      lastTuple = getNextTuple(table.size)
      if (lastTuple._1 < table.size - 1) {
        sender ! CheckForEquivalency(lastTuple)
        log.info(s"Worker tasked to check OE: $lastTuple")
        pruningAsked += 1
      } else {
        // TODO: Remember to send task once all pruning answers are in and the state has been changed
      }
    case OrderEquivalent(od, isOrderEquiv) =>
      if (isOrderEquiv) {
        reducedColumns -= od._2
        orderEquivalencies(od._1) += od._2
        log.info(s"OrderEquivalency found: $od")
      }
      pruningAnswered += 1
      val pruningDone = (getNextTuple(table.size)._1 == table.size - 1)
      if (pruningAsked == pruningAnswered && pruningDone) {
        log.info("Pruning done")
        odsToCheck ++= generateFirstCandidates(reducedColumns)
        context.become(findingODs(table))
      }
    case _ => log.info("Unknown message received")
  }

  def findingODs(table: Array[TypedColumn[Any]]): Receive = {
    case GetTask =>
      if (!odsToCheck.isEmpty) {
        val odToCheck = odsToCheck.dequeue()
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

  def getNextTuple(numColumns: Int): (Int, Int) = {
    var leftCol = lastTuple._1
    var rightCol = lastTuple._2
    if (leftCol == numColumns - 1)
      return lastTuple
    do {
      rightCol += 1
      rightCol %= numColumns
      if(rightCol == 0) {
        leftCol += 1
        rightCol = leftCol + 1
      }
    } while ( leftCol < numColumns - 1 &&
      !(reducedColumns.contains(rightCol) && reducedColumns.contains(leftCol)))
    (leftCol, rightCol)
  }
}