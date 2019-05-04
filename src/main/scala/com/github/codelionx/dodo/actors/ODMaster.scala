package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, OneForOneStrategy, Props, SupervisorStrategy}

import scala.language.postfixOps

object ODMaster {

  val odMasterName = "odmaster"

  def props(nWorkers: Int): Props = Props(new ODMaster()(nWorkers))

}


class ODMaster(nWorkers: Int) extends Actor with ActorLogging {
  import ODMaster._

  // TODO: setup and handle workers

  override def preStart(): Unit = {
    log.info(s"Starting $odMasterName")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    log.info(s"Stopping $odMasterName")

  override def receive: Receive = {
    // TODO: data is loaded, find ODs now
    case _ => log.info("Unknown message received")
  }

  def ready(sessionActor: ActorRef): Receive = {
    case m => log.info(s"$odMasterName received a message: $m")
  }
}