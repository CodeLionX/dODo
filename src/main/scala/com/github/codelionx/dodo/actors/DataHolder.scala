package com.github.codelionx.dodo.actors

import java.net.InetSocketAddress

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp.{IncomingConnection, OutgoingConnection, ServerBinding}
import akka.stream.scaladsl.{Source, Tcp}
import com.github.codelionx.dodo.GlobalImplicits._
import com.github.codelionx.dodo.Settings
import com.github.codelionx.dodo.Settings.DefaultValues
import com.github.codelionx.dodo.parsing.CSVParser
import com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol._
import com.github.codelionx.dodo.sidechannel.{ActorStreamConnector, DataStreamServant}
import com.github.codelionx.dodo.types.TypedColumn

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random


object DataHolder {

  val name = "dataholder"

  def props(): Props = Props[DataHolder]

  case class LoadDataFromDisk(localFilename: String)

  case class FetchDataFrom(address: InetSocketAddress)

  case object DataLoaded

  /**
    * Message to request the data reference. It is returned as [[com.github.codelionx.dodo.actors.DataHolder.DataRef]]
    */
  case object GetDataRef

  case object DataNotReady

  case class DataRef(relation: Array[TypedColumn[Any]])

}

class DataHolder extends Actor with ActorLogging {

  import DataHolder._


  implicit val mat: ActorMaterializer = ActorMaterializer()(context)

  private val r: Random = new Random()
  private val settings = Settings(context.system)
  private val tcpConnection: Source[IncomingConnection, Future[ServerBinding]] =
    Tcp()(context.system).bind("0.0.0.0", DefaultValues.PORT + 1000)

  private def acceptConnections(data: Array[TypedColumn[Any]]): Unit = {
    tcpConnection.runForeach { connection =>
      log.info(s"Received connection from ${connection.remoteAddress}")
      context.actorOf(DataStreamServant.props(data, connection), s"streamServant-${r.nextInt()}")
    }
  }

  private def openRemoteConnection(address: InetSocketAddress): Future[OutgoingConnection] = {
    implicit val mat: ActorMaterializer = ActorMaterializer()

    val remoteConnection = Tcp()(context.system).outgoingConnection(address)
    val actorConnector = ActorStreamConnector.withSingleSource[DataOverStream, GetDataOverStream.type](
      self,
      GetDataOverStream,
      classOf[DataOverStream]
    )(context.system)

    remoteConnection
      .reduce(_ ++ _)
      .join(actorConnector)
      .run()
  }

  override def preStart(): Unit =
    Reaper.watchWithDefault(self)

  override def receive: Receive = uninitialized

  def uninitialized: Receive = {

    case LoadDataFromDisk(localFilename) =>
      val data = CSVParser(settings.parsing).parse(localFilename)
      log.info(s"Loaded data from $localFilename. $name is ready")
      acceptConnections(data)
      sender ! DataLoaded
      context.become(dataReady(data))

    case FetchDataFrom(address) =>
      openRemoteConnection(address)
      context.become(handleStreamResult(sender, address))

    case GetDataRef =>
      log.warning(s"Request to serve data from uninitialized $name")
      sender ! DataNotReady

    case msg => log.info(s"Unknown message received: $msg")
  }

  def handleStreamResult(originalSender: ActorRef, address: InetSocketAddress): Receive = {

    case StreamInit =>
      sender ! StreamACK

    case DataOverStream(data) =>
      log.info("Received data over stream")
      println(data.prettyPrintAsTable())
      sender ! StreamACK
      context.become(receivedData(data, originalSender))

    case Failure(cause) =>
      log.error(s"Error processing fetch data request: $cause. Trying again.")
      context.system.scheduler.scheduleOnce(
        2 second,
        self,
        FetchDataFrom(address)
      )(scala.concurrent.ExecutionContext.Implicits.global)
      context.become(uninitialized)

    case GetDataRef =>
      log.warning(s"Request to serve data from uninitialized $name")
      sender ! DataNotReady
  }

  def receivedData(data: Array[TypedColumn[Any]], originalSender: ActorRef): Receive = {

    case StreamComplete =>
      log.info("Stream completed!")
      sender ! StreamACK
      originalSender ! DataLoaded
      context.become(dataReady(data))

    case GetDataRef =>
      log.warning(s"Request to serve data from uninitialized $name")
      sender ! DataNotReady
  }

  def dataReady(relation: Array[TypedColumn[Any]]): Receive = {
    case LoadDataFromDisk(_) =>
      sender ! DataLoaded

    case FetchDataFrom(_) =>
      sender ! DataLoaded

    case GetDataRef =>
      log.info(s"Serving data to ${sender.path}")
      sender ! DataRef(relation)
  }
}
