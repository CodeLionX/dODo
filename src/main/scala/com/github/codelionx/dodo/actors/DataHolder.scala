package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, Props}


object DataHolder {

  val name = "dataholder"

  def props(): Props = Props[DataHolder]

}

class DataHolder extends Actor with ActorLogging {

  import DataHolder._


  override def preStart(): Unit = {
    log.info(s"Starting $name")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    log.info(s"Stopping $name")

  override def receive: Receive = {
    // TODO: get dataSource -> load data
    // TODO: Other nodes need data -> send it
    case _ => log.info("Unknown message received")
  }

}
