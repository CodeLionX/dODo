package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, Props}


object ResultCollector {

  val resultCollectorName = "resultcollector"

  def props(): Props = Props[ResultCollector]

}


class ResultCollector extends Actor with ActorLogging {

  import ResultCollector._


  override def preStart(): Unit = {
    log.info(s"Starting $resultCollectorName")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    log.info(s"Stopping $resultCollectorName")

  override def receive: Receive = {
    // TODO: extract ODs from OCDs?
    // TODO: write ODs into file
    case _ => log.info("Unknown message received")
  }
}
