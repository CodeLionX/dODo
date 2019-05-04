package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.github.codelionx.dodo.parsing.CSVParser

object DataHolder {

  val dataHolderName = "dataholder"

  def props(): Props = Props(new DataHolder())

}

class DataHolder extends Actor with ActorLogging{
  import DataHolder._

  override def preStart(): Unit = {
    log.info(s"Starting $dataHolderName")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    log.info(s"Stopping $dataHolderName")

  override def receive: Receive = {
    // TODO: get dataSource -> load data
    // TODO: Other nodes need data -> send it
    case _ => log.info("Unknown message received")
  }

  def ready(sessionActor: ActorRef): Receive = {
    case m => log.info(s"$dataHolderName received a message: $m")
  }

}
