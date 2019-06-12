package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
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

  case class OrderEquivalent(oe: (Int, Int), isOrderEquiv: Boolean)

  case class CheckForOD(odToCheck: Queue[(Seq[Int], Seq[Int])], reducedColumns: Set[Int])

  case class ODsToCheck(parentODs: Queue[(Seq[Int], Seq[Int])], newODs: Queue[(Seq[Int], Seq[Int])])

  case class ODFound(od: (Seq[Int], Seq[Int]))

  case class GetTaskTimeout(master: ActorRef)

  // debugging
  private val reportingInterval = 5 seconds

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
      val timeout = requestNextTask(sender)
      context.become(workReady(table, timeout))

    case ReportStatus =>
      log.debug("Worker uninitialized, waiting for data ref...")

    case _ => log.info("Unknown message received")
  }

  def workReady(table: Array[TypedColumn[Any]], timeoutCancellable: Cancellable): Receive = {
    case CheckForEquivalency(oeToCheck) =>
      timeoutCancellable.cancel()
      sender ! OrderEquivalent(oeToCheck, checkOrderEquivalent(table(oeToCheck._1), table(oeToCheck._2)))
      itemsProcessed += 1
      val timeout = requestNextTask(sender)
      context.become(workReady(table, timeout))

    case CheckForOD(odCandidates, reducedColumns) =>
      timeoutCancellable.cancel()
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
          if (!foundOD || !settings.ocdComparability) {
            foundOCDs :+= substituteColumnNames(odCandidate, table)
          }
        }
      }
      if (foundODs.nonEmpty || foundOCDs.nonEmpty) {
        resultCollector ! Results(foundODs, foundOCDs)
      }
      itemsProcessed += odCandidates.length
      sender ! ODsToCheck(odCandidates, newCandidates)
      val timeout = requestNextTask(sender)
      context.become(workReady(table, timeout))

    case GetTaskTimeout(master) =>
      log.warning("No task received from master, trying again")
      val timeout = requestNextTask(master)
      context.become(workReady(table, timeout))

    case ReportStatus =>
      val statusMsg = itemsProcessed match {
        case i if i > 1000 * 1000 * 1000 => s"${i / 1000 / 1000 / 1000}M"
        case i if i > 1000 * 1000 => s"${i / 1000 / 1000}M"
        case i if i > 1000 => s"${i / 1000}k"
        case i => i
      }
      log.debug("Processed {} items", statusMsg)

    case _ => log.info("Unknown message received")
  }

  def substituteColumnNames(dependency: (Seq[Int], Seq[Int]), table: Array[TypedColumn[Any]]): (Seq[String], Seq[String]) = {
    dependency._1.map(table(_).name) -> dependency._2.map(table(_).name)
  }

  def requestNextTask(master: ActorRef): Cancellable = {
    master ! GetTask
    import context.dispatcher
    context.system.scheduler.scheduleOnce(5 seconds, self, GetTaskTimeout(master))
  }
}
