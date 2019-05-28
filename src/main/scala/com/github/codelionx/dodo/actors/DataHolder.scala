package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp.{IncomingConnection, ServerBinding}
import akka.stream.scaladsl.{Source, Tcp}
import com.github.codelionx.dodo.Settings
import com.github.codelionx.dodo.Settings.DefaultValues
import com.github.codelionx.dodo.actors.DataStreamServant.HandleConnection
import com.github.codelionx.dodo.parsing.CSVParser
import com.github.codelionx.dodo.types.TypedColumn

import scala.concurrent.Future
import scala.language.postfixOps


object DataHolder {

  val name = "dataholder"

  def props(): Props = Props[DataHolder]

  case class LoadData(localFilename: String) extends Serializable

  case object DataLoaded

  /**
    * Message to request the data reference. It is returned as [[com.github.codelionx.dodo.actors.DataHolder.DataRef]]
    */
  case object GetDataRef

  case object DataNotReady

  case class DataRef(relation: Array[TypedColumn[Any]]) extends Serializable

}

class DataHolder extends Actor with ActorLogging {

  import DataHolder._


  implicit val mat: ActorMaterializer = ActorMaterializer()(context)

  private val settings = Settings(context.system)
  private val source: Source[IncomingConnection, Future[ServerBinding]] =
    Tcp()(context.system).bind(DefaultValues.HOST, DefaultValues.PORT + 1000)

  override def preStart(): Unit = {
    log.info(s"Starting $name")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    log.info(s"Stopping $name")

  override def receive: Receive = uninitialized

  def uninitialized: Receive = {

    case LoadData(localFilename) =>
      val data = CSVParser(settings.parsing).parse(localFilename)
      log.info(s"Loaded data from $localFilename. $name is ready")
      sender ! DataLoaded

      source.runForeach { connection =>
        log.info(s"Received connection from ${connection.remoteAddress}")
        context.actorOf(DataStreamServant.props(data)) ! HandleConnection(connection)
      }

      context.become(dataReady(data))

    case GetDataRef =>
      log.warning(s"Request to serve data from uninitialized $name")
      sender ! DataNotReady

    case msg => log.info(s"Unknown message received: $msg")
  }

  def dataReady(relation: Array[TypedColumn[Any]]): Receive = {
    case LoadData(_) =>
      sender ! DataLoaded

    case GetDataRef =>
      log.info(s"Serving data to ${sender.path}")
      sender ! DataRef(relation)

    case msg => log.info(s"Unknown message received: $msg")
  }
}
