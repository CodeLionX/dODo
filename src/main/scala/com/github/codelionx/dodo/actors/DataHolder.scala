package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, Props}
import com.github.codelionx.dodo.parsing.CSVParser
import com.github.codelionx.dodo.types.TypedColumn


object DataHolder {

  val name = "dataholder"

  def props(): Props = Props[DataHolder]

  case class LoadData(localFilename: String) extends Serializable

  // following messages should only be used within the actor system
  /**
    * Message to request the data reference. It is returned as [[com.github.codelionx.dodo.actors.DataHolder.DataRef]]
    */
  case object GetDataRef

  case class DataRef(relation: Array[TypedColumn[Any]])

}

class DataHolder extends Actor with ActorLogging {

  import DataHolder._


  override def preStart(): Unit = {
    log.info(s"Starting $name")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    log.info(s"Stopping $name")

  override def receive: Receive = uninitialized

  def uninitialized: Receive = {
    case LoadData(localFilename) =>
      val data = CSVParser.parse(localFilename)
      log.info(s"Loaded data from $localFilename. $name is ready")
      context.become(dataReady(data))

    case _ => log.info("Unknown message received")
  }

  def dataReady(relation: Array[TypedColumn[Any]]): Receive = {
    case GetDataRef =>
      log.info(s"Serving data to ${sender.path}")
      sender ! DataRef(relation)

    case _ => log.info("Unknown message received")
  }
}
