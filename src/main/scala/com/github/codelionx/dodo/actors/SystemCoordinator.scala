package com.github.codelionx.dodo.actors

import java.time.LocalDateTime

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import com.github.codelionx.dodo.Settings
import com.github.codelionx.dodo.actors.DataHolder.{DataLoaded, LoadData}
import com.github.codelionx.dodo.actors.ODMaster.FindODs
import com.github.codelionx.dodo.discovery.DependencyChecking

import scala.concurrent.duration.Duration


object SystemCoordinator {

  val name = "systemcoordinator"

  def props(): Props = Props[SystemCoordinator]

  case object Initialize

  case object Finished

  case object Shutdown

}

class SystemCoordinator extends Actor with ActorLogging with DependencyChecking {

  import SystemCoordinator._
  import com.github.codelionx.dodo.GlobalImplicits._

  private val settings = Settings(context.system)

  val nWorkers = settings.workers
  def resultCollector: ActorRef = context.actorOf(ResultCollector.props(), ResultCollector.name)
  val dataHolder: ActorRef = context.actorOf(DataHolder.props(), DataHolder.name)
  val odMaster: ActorRef = context.actorOf(ODMaster.props(nWorkers, resultCollector, self), ODMaster.name)

  var startTime: LocalDateTime = _
  var startNanos: Long = _


  override def preStart(): Unit = {
    log.info(s"Starting $name")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    log.info(s"Stopping $name")

  override def receive: Receive = {
    case Initialize =>
      log.info("Preparing for OD discovery: loading data")
      startTime = LocalDateTime.now()
      startNanos = System.nanoTime()
      dataHolder ! LoadData(settings.inputFilePath)

    case DataLoaded =>
      log.info("Starting master and passing ref to data holder")
      outputDurationFor("Data loading and parsing")
      startTime = LocalDateTime.now()
      startNanos = System.nanoTime()

      log.info(s"Session is in the hand of ${ODMaster.name}")
      odMaster ! FindODs(dataHolder)

    case Finished =>
      log.info("OD Discovery finished, shutting down")
      outputDurationFor("Order Dependency Discovery with Pruning")

      // stops this actor and all direct childs
      context.stop(self)

    case Shutdown =>
      log.info("Shutdown message received, stopping all actors and system!")
      context.children.foreach(_ ! PoisonPill)
      context.stop(self)
      context.system.terminate()

    // TODO
    case _ => log.info("Unknown message received")
  }

  def outputDurationFor(taskDescription: String): Unit = {
    val endNanos = System.nanoTime()
    val endTime = LocalDateTime.now()
    val duration = Duration.fromNanos(endNanos - startNanos)
    println(
      s"""|
          |================================================
          |$taskDescription
          |------------------------------------------------
          |Started with timestamp: $startTime
          |Finished with timestamp: $endTime
          |================================================
          |Duration: ${duration.pretty}
          |""".stripMargin
    )
  }
}
