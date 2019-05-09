package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.github.codelionx.dodo.actors.DataHolder.DataRef
import com.github.codelionx.dodo.discovery.{CandidateGenerator, Pruning}
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.immutable.Queue


object Worker {

  val name = "worker"

  def props(): Props = Props[Worker]

  case object GetTask

  case class CheckForEquivalency(oeToCheck: (Int, Int))

  case class OrderEquivalent(oe: (Int, Int), isOrderEquiv: Boolean)

  case class CheckForOD(odToCheck: (List[Int], List[Int]), reducedColumns: Set[Int])

  case class ODsToCheck(parentOD: (List[Int], List[Int]), newODs: Queue[(List[Int], List[Int])])

  case class ODFound(od: (List[Int], List[Int]))

}


class Worker extends Actor with ActorLogging with Pruning with CandidateGenerator{

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
    case CheckForEquivalency(oeToCheck) =>
      sender ! OrderEquivalent(oeToCheck, checkOrderEquivalent(table(oeToCheck._1), table(oeToCheck._2)))
      sender ! GetTask
    case CheckForOD(odCandidate, reducedColumns) =>
      var newCandidates: Queue[(List[Int], List[Int])] = Queue.empty
      if (checkOrderDependent(odCandidate, table)) {
        log.info(s"Found OD: $odCandidate")
        // TODO: Send to ResultCollector
      } else {
        newCandidates ++= generateODCandidates(reducedColumns, odCandidate)
      }
      val mirroredOD = (odCandidate._2, odCandidate._1)
      if(checkOrderDependent(mirroredOD, table)) {
        log.info(s"Found OD: $mirroredOD")
        // TODO: Send to ResultCollector
      } else {
        newCandidates ++= generateODCandidates(reducedColumns, mirroredOD)
      }
      sender ! ODsToCheck(odCandidate, newCandidates)
      sender ! GetTask

    case _ => log.info("Unknown message received")
  }
}
