package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, NotInfluenceReceiveTimeout, Props}
import com.github.codelionx.dodo.Settings
import com.github.codelionx.dodo.actors.DataHolder.DataRef
import com.github.codelionx.dodo.discovery.{CandidateGenerator, DependencyChecking}
import com.github.codelionx.dodo.actors.ResultCollector.Results
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.immutable.Queue


object Worker {

  val name = "worker"

  def props(resultCollector: ActorRef): Props = Props(new Worker(resultCollector))

  case object GetTask

  case class CheckForEquivalency(oeToCheck: (Int, Int))

  case class OrderEquivalent(oe: (Int, Int), isOrderEquiv: Boolean)

  case class CheckForOD(odToCheck: Queue[(Seq[Int], Seq[Int])], reducedColumns: Set[Int])

  case class ODsToCheck(newODs: Queue[(Seq[Int], Seq[Int])])

  case class ODFound(od: (Seq[Int], Seq[Int]))

}


class Worker(resultCollector: ActorRef) extends Actor with ActorLogging with DependencyChecking with CandidateGenerator{

  import Worker._

  private val settings = Settings(context.system)

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

    case CheckForOD(odCandidates, reducedColumns) =>
      var newCandidates: Queue[(Seq[Int], Seq[Int])] = Queue.empty
      var foundODs: Seq[(Seq[String], Seq[String])] = Seq.empty
      var foundOCDs: Seq[(Seq[String], Seq[String])] = Seq.empty
      for (odCandidate <- odCandidates) {
        val ocdCandidate = (odCandidate._1 ++ odCandidate._2, odCandidate._2 ++ odCandidate._1)
        var foundOD = false
        if (checkOrderDependent(ocdCandidate, table.asInstanceOf[Array[TypedColumn[_]]])) {
          if (checkOrderDependent(odCandidate, table.asInstanceOf[Array[TypedColumn[_]]])) {
            foundODs :+= substituteColumnNames(odCandidate, table)
            foundOD = true
          } else {
            newCandidates ++= generateODCandidates(reducedColumns, odCandidate)
          }
          val mirroredOD = odCandidate.swap
          if (checkOrderDependent(mirroredOD, table.asInstanceOf[Array[TypedColumn[_]]])) {
            foundODs :+= substituteColumnNames(mirroredOD, table)
            foundOD = true
          } else {
            newCandidates ++= generateODCandidates(reducedColumns, odCandidate, leftSide = false)
          }
          if (!foundOD || !settings.ocdComparability ) {
            foundOCDs :+= substituteColumnNames(odCandidate, table)
          }
        }
      }
      if (foundODs.nonEmpty || foundOCDs.nonEmpty) {
        resultCollector ! Results(foundODs, foundOCDs)
      }
      sender ! ODsToCheck(newCandidates)
      sender ! GetTask

    case _ => log.info("Unknown message received")
  }

  def substituteColumnNames(dependency: (Seq[Int], Seq[Int]), table: Array[TypedColumn[Any]]): (Seq[String], Seq[String]) = {
    dependency._1.map(table(_).name) -> dependency._2.map(table(_).name)
  }
}
