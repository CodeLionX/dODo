package com.github.codelionx.dodo.actors

import java.net.InetSocketAddress

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp
import akka.stream.scaladsl.Tcp.{IncomingConnection, OutgoingConnection}
import com.github.codelionx.dodo.Settings
import com.github.codelionx.dodo.Settings.DefaultValues
import com.github.codelionx.dodo.parsing.CSVParser
import com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol._
import com.github.codelionx.dodo.sidechannel.{ActorStreamConnector, DataStreamServant}
import com.github.codelionx.dodo.types.TypedColumn

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.language.postfixOps
import scala.util.{Random, Try}


object DataHolder {

  val name = "dataholder"

  def props(publicHostname: String): Props = Props(new DataHolder(publicHostname))

  // load data commands and Ack messages
  case class LoadDataFromDisk(localFilename: String)

  case class FetchDataFrom(otherDataHolder: ActorSelection)

  // sidechannel address request-response pair
  case object GetSidechannelAddress extends Serializable

  case class SidechannelAddress(address: InetSocketAddress) extends Serializable

  // data ref messages
  /**
    * Message to request the data reference. It is returned as [[com.github.codelionx.dodo.actors.DataHolder.DataRef]]
    */
  case object GetDataRef

  case object DataNotReady

  case class DataRef(relation: Array[TypedColumn[Any]])

}

class DataHolder(publicHostname: String) extends Actor with ActorLogging {

  import DataHolder._


  private val r: Random = new Random()
  private val settings = Settings(context.system)

  private def openSidechannel(data: Array[TypedColumn[Any]]): Int = {
    implicit val mat: ActorMaterializer = ActorMaterializer()(context)
    implicit val system: ActorSystem = context.system

    val handler: IncomingConnection => Unit = connection => {
      log.info(s"Received connection from ${connection.remoteAddress}")
      // handle incoming requests in own actor
      context.actorOf(DataStreamServant.props(data, connection), s"streamServant-${r.nextInt()}")
    }

    @tailrec
    def tryBindTo(port: Int): Int = {
      val future = Tcp().bind("0.0.0.0", port).runForeach(handler)
      Try {
        Await.result(future, 2 seconds)
      } match {
        case scala.util.Success(_) => port
        case scala.util.Failure(f) => f match {
          case _: TimeoutException | _: InterruptedException =>
            // no specific error in reasonable amount of time: assuming a successful binding
            port
          case error =>
            log.warning(s"Binding to port $port failed (${error.getMessage}). Trying again on ${port + 1}")
            tryBindTo(port + 1)
        }
      }
    }

    val port = tryBindTo(DefaultValues.PORT + 1000)
    log.info(s"Accepting incoming connections to $port")
    port
  }

  private def openRemoteConnection(address: InetSocketAddress): Future[OutgoingConnection] = {
    implicit val mat: ActorMaterializer = ActorMaterializer()(context)
    implicit val system: ActorSystem = context.system

    val remoteConnection = Tcp().outgoingConnection(address)
    val actorConnector = ActorStreamConnector.withSingleSource[DataOverStream, GetDataOverStream.type](
      targetActorRef = self,
      singleSourceElem = GetDataOverStream,
      deserializationClassHint = classOf[DataOverStream]
    )

    remoteConnection
      .reduce(_ ++ _)
      .join(actorConnector)
      .run()
  }

  override def preStart(): Unit = Reaper.watchWithDefault(self)

  override def receive: Receive = uninitialized

  def uninitialized: Receive = withCommonNotReady {

    case LoadDataFromDisk(localFilename) =>
      val data = CSVParser(settings.parsing).parse(localFilename)
      val port = openSidechannel(data)

      log.info(s"Loaded data from $localFilename. $name is ready")
      sender ! DataRef(data)
      context.become(dataReady(data, port))

    case FetchDataFrom(otherDataHolder) =>
      otherDataHolder ! GetSidechannelAddress
      context.become(handleFetchDataResult(sender))
  }

  def handleFetchDataResult(originalSender: ActorRef): Receive = withCommonNotReady {

    case SidechannelAddress(socketAddress) =>
      openRemoteConnection(socketAddress)
      context.become(handleStreamResult(originalSender, socketAddress))

    case DataNotReady =>
      log.error(s"Other data holder (${sender.path}) is no ready yet.")
      throw new RuntimeException("Fetch data from another data holder failover logic is not implemented yet!")
  }

  def handleStreamResult(originalSender: ActorRef, address: InetSocketAddress): Receive = withCommonNotReady {

    case StreamInit =>
      sender ! StreamACK

    case DataOverStream(data) =>
      log.info("Received data over stream")
      sender ! StreamACK
      context.become(receivedData(originalSender, data))

    case Failure(cause) =>
      log.error(s"Error processing fetch data request: $cause. Trying again.")
      import context.dispatcher
      context.system.scheduler.scheduleOnce(
        2 second,
        self,
        SidechannelAddress(address)
      )
      context.become(handleFetchDataResult(originalSender))
  }

  def receivedData(originalSender: ActorRef, data: Array[TypedColumn[Any]]): Receive = withCommonNotReady {

    case StreamComplete =>
      log.info("Stream completed!")
      sender ! StreamACK

      val port = openSidechannel(data)
      originalSender ! DataRef(data)
      context.become(dataReady(data, port))
  }

  def dataReady(relation: Array[TypedColumn[Any]], boundPort: Int): Receive = {

    case LoadDataFromDisk(_) =>
      sender ! DataRef(relation)

    case FetchDataFrom(_) =>
      sender ! DataRef(relation)

    case GetSidechannelAddress =>
      sender ! SidechannelAddress(InetSocketAddress.createUnresolved(publicHostname, boundPort))

    case GetDataRef =>
      log.info(s"Serving data to ${sender.path}")
      sender ! DataRef(relation)
  }

  def withCommonNotReady(block: Receive): Receive = {
    val commonNotReady: Receive = {
      case GetDataRef | GetSidechannelAddress =>
        log.warning(s"Request to serve data from uninitialized $name")
        sender ! DataNotReady
    }

    block orElse commonNotReady
  }
}
