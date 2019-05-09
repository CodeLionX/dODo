package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.github.codelionx.dodo.actors.DataHolder.{DataRef, GetDataRef, LoadData}
import com.github.codelionx.dodo.discovery.Pruning


object SystemCoordinator {

  val name = "systemcoordinator"

  def props(dataSource: String): Props = Props(new SystemCoordinator(dataSource))

  case object Initialize

}

class SystemCoordinator(dataSource: String) extends Actor with ActorLogging with Pruning {

  import SystemCoordinator._
  import com.github.codelionx.dodo.types.Implicits._


  val dataHolder: ActorRef = context.actorOf(DataHolder.props(), DataHolder.name)

  // TODO: setup workerManager to start extracting ODs once data is read
  // TODO: create result Collector

  // TODO: setup and handle workers

  override def preStart(): Unit = {
    log.info(s"Starting $name")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    log.info(s"Stopping $name")

  override def receive: Receive = {
    case Initialize =>
      log.info("Preparing for OD discovery")
      log.info("  Loading and parsing data")
      dataHolder ! LoadData(dataSource)

      log.info("  Creating initial search space")
      log.info("  ...")

      // test if data passing works
      // ---
      log.info("Testing data passing ...")
      dataHolder ! GetDataRef

    case DataRef(data) =>
      log.info("... data passing successful:")
      println(data.prettyPrint)

      for (column <- data) {
        if (checkConstant(column))
          log.info(s"found const column: ${data.indexOf(column)}")
        for (col2 <- data) {
          if (checkOrderEquivalent(column, col2))
            log.info(s"Found Order Equivalence: ${data.indexOf(column)} <-> ${data.indexOf(col2)}")
        }
      }

      log.info("shutting down")
      context.stop(self)
    // ---

    // TODO
    case _ => log.info("Unknown message received")
  }
}
