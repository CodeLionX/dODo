package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.github.codelionx.dodo.actors.DataHolder.DataRef
import com.github.codelionx.dodo.discovery.Pruning
import com.github.codelionx.dodo.types.TypedColumn


object Worker {

  val name = "worker"

  def props(): Props = Props[Worker]

  case object GetTask

  case class CheckForEquivalency(oeToCheck: (Int, Int))

  case class OrderEquivalent(oe: (Int, Int), isOrderEquiv: Boolean)

  case class CheckForOD(odToCheck: (Int, Int))

  case class ODsToCheck(parentOD: (Int, Int), newODs: Array[(Int, Int)])

  case class ODFound(od: (Int, Int))

}


class Worker extends Actor with ActorLogging with Pruning {

  import Worker._


  override def preStart(): Unit = {
    log.info(s"Starting $name")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    log.info(s"Stopping $name")

  override def receive: Receive = uninitialized

  def uninitialized: Receive = {
    case DataRef(table) =>
      context.become(workReady(table))
      sender ! GetTask
    case _ => log.info("Unknown message received")
  }

  def workReady(table: Array[TypedColumn[Any]]): Receive = {
    case CheckForEquivalency(col1Index, col2Index) =>
      sender ! OrderEquivalent(col1Index, col2Index, checkOrderEquivalent(table(col1Index), table(col2Index)))
    case _ => log.info("Unknown message received")
  }
}
