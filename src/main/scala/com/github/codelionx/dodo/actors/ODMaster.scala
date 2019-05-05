package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, Props}

import scala.language.postfixOps


object ODMaster {

  val name = "odmaster"

  def props(nWorkers: Int): Props = Props(new ODMaster(nWorkers))

}


class ODMaster(nWorkers: Int) extends Actor with ActorLogging {

  import ODMaster._

  // TODO: setup and handle workers

  override def preStart(): Unit = {
    log.info(s"Starting $name")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    log.info(s"Stopping $name")

  override def receive: Receive = {
    // TODO: data is loaded, find ODs now
    case _ => log.info("Unknown message received")
  }

}