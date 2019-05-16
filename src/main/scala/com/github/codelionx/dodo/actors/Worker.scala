package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.github.codelionx.dodo.actors.DataHolder.DataRef

import com.github.codelionx.dodo.discovery.{CandidateGenerator, DependencyChecking}

import com.github.codelionx.dodo.actors.ResultCollector.OD

import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.immutable.Queue


object Worker {

  val name = "worker"

  def props(resultCollector: ActorRef): Props = Props(new Worker(resultCollector))

  case object GetTask

  case class CheckForEquivalency(oeToCheck: (Int, Int))

  case class OrderEquivalent(oe: (Int, Int), isOrderEquiv: Boolean)

  case class CheckForOD(odToCheck: (Seq[Int], Seq[Int]), reducedColumns: Set[Int])

  case class ODsToCheck(parentOD: (Seq[Int], Seq[Int]), newODs: Queue[(Seq[Int], Seq[Int])])

  case class ODFound(od: (Seq[Int], Seq[Int]))

}


class Worker(resultCollector: ActorRef) extends Actor with ActorLogging with DependencyChecking with CandidateGenerator{

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
      sender ! GetTask
      context.become(workReady(table))
    case _ => log.info("Unknown message received")
  }

  def workReady(table: Array[TypedColumn[Any]]): Receive = {
    case CheckForEquivalency(oeToCheck) =>
      sender ! OrderEquivalent(oeToCheck, checkOrderEquivalent(table(oeToCheck._1), table(oeToCheck._2)))
      sender ! GetTask

    case CheckForOD(odCandidate, reducedColumns) =>
      val ocdCandidate = (odCandidate._1 ++ odCandidate._2, odCandidate._2 ++ odCandidate._1)
      if (checkOrderDependent(ocdCandidate, table.asInstanceOf[Array[TypedColumn[_]]])) {
        resultCollector ! OD(ocdCandidate)

        var newCandidates: Queue[(Seq[Int], Seq[Int])] = Queue.empty

        if (checkOrderDependent(odCandidate, table.asInstanceOf[Array[TypedColumn[_]]])) {
          resultCollector ! OD(odCandidate)
        } else {
          newCandidates ++= generateODCandidates(reducedColumns, odCandidate)
        }
        val mirroredOD = odCandidate.swap
        if (checkOrderDependent(mirroredOD, table.asInstanceOf[Array[TypedColumn[_]]])) {
          resultCollector ! OD(mirroredOD)
        } else {
          newCandidates ++= generateODCandidates(reducedColumns, odCandidate, leftSide = false)
        }
        sender ! ODsToCheck(odCandidate, newCandidates)
      } else {
        sender ! ODsToCheck(odCandidate, Queue.empty)
      }
      sender ! GetTask

    case _ => log.info("Unknown message received")
  }
}