package com.github.codelionx.dodo.actors

import java.time.LocalDateTime

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.github.codelionx.dodo.actors.DataHolder.{DataLoaded, LoadData}
import com.github.codelionx.dodo.actors.ODMaster.FindODs
import com.github.codelionx.dodo.discovery.DependencyChecking

import scala.concurrent.duration.Duration


object SystemCoordinator {

  val name = "systemcoordinator"

  def props(dataSource: String): Props = Props(new SystemCoordinator(dataSource))

  case object Initialize

  case object Finished

}

class SystemCoordinator(dataSource: String) extends Actor with ActorLogging with DependencyChecking {

  import SystemCoordinator._


  val nWorkers = 1
  val resultCollector: ActorRef = context.actorOf(ResultCollector.props("data/results.txt"), ResultCollector.name)
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
      dataHolder ! LoadData(dataSource)

    case DataLoaded =>
      log.info("Starting master and passing ref to data holder")
      log.info(s"Session is in the hand of ${ODMaster.name}")
      startTime = LocalDateTime.now()
      startNanos = System.nanoTime()
      odMaster ! FindODs(dataHolder)

    case Finished =>
      log.info("OD Discovery finished, shutting down")

      val endNanos = System.nanoTime()
      val endTime = LocalDateTime.now()
      val duration = Duration.fromNanos(endNanos - startNanos)
      println(
        s"""|
            |Started OD discovery with timestamp: $startTime
            |Finished OD discovery with timestamp: $endTime
            |================================================
            |Duration: ${duration.toMillis}
            |================================================
            |""".stripMargin
      )

      // stops this actor and all direct childs
      context.stop(self)

    // TODO
    case _ => log.info("Unknown message received")
  }
}
