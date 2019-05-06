package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, Props}


object SystemCoordinator {

  val name = "systemcoordinator"

  def props(dataSource: String): Props = Props(new SystemCoordinator(dataSource))

}

class SystemCoordinator(dataSource: String) extends Actor with ActorLogging {

  import SystemCoordinator._

  // TODO: get dataHolder to read data from dataSource
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
    // TODO
    case _ => log.info("Unknown message received")
  }
}
