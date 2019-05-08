package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.routing.BroadcastPool
import com.github.codelionx.dodo.actors.DataHolder.{DataRef, GetDataRef}
import com.github.codelionx.dodo.actors.Worker.{CheckForEquivalency, CheckForOD, GetTask, ODsToCheck, OrderEquivalent}
import com.github.codelionx.dodo.discovery.Pruning
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.mutable
import scala.language.postfixOps


object ODMaster {

  val name = "odmaster"

  def props(nWorkers: Int): Props = Props(new ODMaster(nWorkers))

  case class FindODs(dataHolder: ActorRef)

}


class ODMaster(nWorkers: Int) extends Actor with ActorLogging with Pruning {

  import ODMaster._
  private val broadcastRouter: ActorRef = context.actorOf(BroadcastPool(nWorkers).props(Props[Worker]), "broadcastRouter")
  private var toBeIgnored: Set[Int] = Set.empty[Int]
  private var lastTuple = (0, 1)
  private var pruningAsked = 0
  private var pruningAnswered = 0

  private val odsToCheck: mutable.Queue[(Int, Int)] = mutable.Queue.empty[(Int, Int)]
  private var waitingForODStatus: Set[(Int, Int)] = Set.empty[(Int, Int)]

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
      context.become(pruning(table))
      broadcastRouter ! DataRef(table)
      pruneConstColumns(table)
    case _ => log.info("Unknown message received")
  }

  def pruning(table: Array[TypedColumn[Any]]): Receive = {
    case GetTask =>
      lastTuple = getNextTuple(table.size)
      if (lastTuple._2 < table.size) {
        sender ! CheckForEquivalency(lastTuple)
        pruningAsked += 1
      }
      // TODO: else make sure worker asks again once pruning is done
    case OrderEquivalent(od, isOrderEquiv) =>
      if (isOrderEquiv) {
        toBeIgnored += od._2
        // TODO: Add datastructure to store OEs in
      }
      pruningAnswered += 1
      if (pruningAsked == pruningAnswered) {
        context.become(findingODs(table))
        // TODO: Generate first level of lattice for ODsToCheck
      }
    case _ => log.info("Unknown message received")
  }

  def findingODs(table: Array[TypedColumn[Any]]): Receive = {
    case GetTask =>
      val odToCheck = odsToCheck.dequeue()
      sender ! CheckForOD(odToCheck)
      waitingForODStatus += odToCheck
    case ODsToCheck(originalOD, newODs) =>
      odsToCheck ++ newODs
      waitingForODStatus -= originalOD
      if (waitingForODStatus.isEmpty && odsToCheck.isEmpty) {
        log.info("Found all ODs")
        context.stop(self)
      }
    case _ => log.info("Unknown message received")
  }

  def pruneConstColumns(table: Array[TypedColumn[Any]]) = {
    // Prune constant columns
    for (column <- table) {
      if (checkConstant(column)) {
        log.info(s"found const column: ${table.indexOf(column)}")
        toBeIgnored += table.indexOf(column)
      }
    }
  }

  def getNextTuple(numColumns: Int): (Int, Int) = {
    var leftCol = lastTuple._1
    var rightCol = lastTuple._2
    while(toBeIgnored.contains(leftCol) && leftCol < numColumns) {
      leftCol += 1
    }
    if (leftCol > rightCol) { rightCol = leftCol}
    do {
      rightCol += 1
    } while (toBeIgnored.contains(rightCol) && rightCol < numColumns)
    var newTupel = (leftCol, rightCol)
  }
}