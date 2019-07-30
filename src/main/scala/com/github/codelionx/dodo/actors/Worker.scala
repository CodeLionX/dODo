package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.github.codelionx.dodo.GlobalImplicits.TypedColumnConversions._
import com.github.codelionx.dodo.Settings
import com.github.codelionx.dodo.actors.DataHolder.DataRef
import com.github.codelionx.dodo.actors.ResultCollector.Results
import com.github.codelionx.dodo.discovery.{CandidateGenerator, DependencyChecking}
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.language.postfixOps


object Worker {

  val name = "worker"

  def props(resultCollector: ActorRef): Props = Props(new Worker(resultCollector))

  case object GetTask

  case class CheckForEquivalency(oeToCheck: (Int, Int))

  case class OrderEquivalencyChecked(oe: (Int, Int), isOrderEquiv: Boolean)

  case class CheckForOD(odToCheck: Queue[(Seq[Int], Seq[Int])], reducedColumns: Set[Int])

  case class ODFound(od: (Seq[Int], Seq[Int]))

  case class NewODCandidates(newODs: Queue[(Seq[Int], Seq[Int])])

  // debugging
  val reportingInterval: FiniteDuration = 5 seconds

  private case object ReportStatus
}


class Worker(resultCollector: ActorRef) extends Actor with ActorLogging with DependencyChecking with CandidateGenerator {

  import Worker._


  private val settings = Settings(context.system)

  private var itemsProcessed = 0L

  override def preStart(): Unit = {
    log.info("Starting {}", name)
    Reaper.watchWithDefault(self)

    if (log.isDebugEnabled) {
      import com.github.codelionx.dodo.GlobalImplicits._
      import context.dispatcher
      log.info("Debugging enabled: performing regular status reporting every {}", reportingInterval.pretty)
      context.system.scheduler.schedule(reportingInterval, reportingInterval, self, ReportStatus)
    }
  }

  override def postStop(): Unit =
    log.info("Stopping {}. Processed {} items", name, itemsProcessed)

  override def receive: Receive = uninitialized

  def uninitialized: Receive = {
    case DataRef(table) =>
      sender ! GetTask
      context.become(workReady(table))

    case ReportStatus =>
      log.debug("Worker uninitialized, waiting for data ref...")

    case _ => log.info("Unknown message received")
  }

  def workReady(table: Array[TypedColumn[Any]]): Receive = {
    case CheckForEquivalency(oeToCheck) =>
      sender ! OrderEquivalencyChecked(oeToCheck, checkOrderEquivalent(table(oeToCheck._1), table(oeToCheck._2)))
      sender ! GetTask
      itemsProcessed += 1

    case CheckForOD(odCandidates, reducedColumns) =>
      var newCandidates: Queue[(Seq[Int], Seq[Int])] = Queue.empty
      var foundODs: Seq[(Seq[String], Seq[String])] = Seq.empty
      var foundOCDs: Seq[(Seq[String], Seq[String])] = Seq.empty
      for (odCandidate <- odCandidates) {
        val ocdCandidate = (odCandidate._1 ++ odCandidate._2, odCandidate._2 ++ odCandidate._1)
        var foundOD = false
        if (checkOrderDependent(ocdCandidate, table)) {
          if (checkOrderDependent(odCandidate, table)) {
            foundODs :+= substituteColumnNames(odCandidate, table)
            foundOD = true
          } else {
            newCandidates ++= generateODCandidates(reducedColumns, odCandidate)
          }
          val mirroredOD = odCandidate.swap
          if (checkOrderDependent(mirroredOD, table)) {
            foundODs :+= substituteColumnNames(mirroredOD, table)
            foundOD = true
          } else {
            newCandidates ++= generateODCandidates(reducedColumns, odCandidate, leftSide = false)
          }
          if (!foundOD || !settings.ocdComparability) {
            foundOCDs :+= substituteColumnNames(odCandidate, table)
          }
        }
      }
      if (foundODs.nonEmpty || foundOCDs.nonEmpty) {
        resultCollector ! Results(foundODs, foundOCDs)
      }

      itemsProcessed += odCandidates.length
      sender ! NewODCandidates(newCandidates)
      sender ! GetTask

    case ReportStatus =>
      val statusMsg = itemsProcessed match {
        case i if i > 1e9 => s"${(i / 1e9).toInt} B"
        case i if i > 1e6 => s"${(i / 1e6).toInt} M"
        case i if i > 1e3 => s"${(i / 1e3).toInt} k"
        case i => i
      }
      log.debug("Processed {} items", statusMsg)

    case _ => log.info("Unknown message received")
  }

  def substituteColumnNames(dependency: (Seq[Int], Seq[Int]), table: Array[TypedColumn[Any]]): (Seq[String], Seq[String]) = {
    dependency._1.map(table(_).name) -> dependency._2.map(table(_).name)
  }
}
