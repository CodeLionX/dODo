package com.github.codelionx.dodo.actors

import java.io.File
import java.net.InetSocketAddress

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp
import akka.stream.scaladsl.Tcp.{IncomingConnection, OutgoingConnection}
import com.github.codelionx.dodo.{DodoException, Settings}
import com.github.codelionx.dodo.actors.ClusterListener.{GetLeftNeighbor, GetRightNeighbor, LeftNeighbor, RightNeighbor}
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

  def props(clusterListener: ActorRef): Props = Props(new DataHolder(clusterListener))

  // sidechannel address request-response pair
  case object GetSidechannelAddress extends Serializable

  case class SidechannelAddress(address: InetSocketAddress) extends Serializable

  // load data commands
  case class LoadDataFromDisk(localFile: File)

  case object FetchDataFromCluster

  // data ref request message
  /**
    * Message to request the data reference. It is returned as [[com.github.codelionx.dodo.actors.DataHolder.DataRef]]
    */
  case object GetDataRef

  // response messages
  case object DataNotReady

  case class DataRef(relation: Array[TypedColumn[Any]])

  // exceptions
  case class FetchDataException(cause: Throwable = null) extends DodoException(
    "Neither left nor right neighbor could provide data. No dataset available!",
    cause
  )

}

class DataHolder(clusterListener: ActorRef) extends Actor with ActorLogging {

  import DataHolder._


  private val userGuardian = "user"

  private val r: Random = new Random()
  private val settings = Settings(context.system)

  private def openSidechannel(data: Array[TypedColumn[Any]]): Int = {
    implicit val mat: ActorMaterializer = ActorMaterializer()(context)
    implicit val system: ActorSystem = context.system

    val handler: IncomingConnection => Unit = connection => {
      log.info("Received connection from {}", connection.remoteAddress)
      // handle incoming requests in own actor
      context.actorOf(DataStreamServant.props(data, connection), s"streamServant-${r.nextInt()}")
    }

    @tailrec
    def tryBindTo(host: String, port: Int): Int = {
      val future = Tcp().bind(host, port).runForeach(handler)
      Try {
        Await.result(future, 2 seconds)
      } match {
        case scala.util.Success(_) => port
        case scala.util.Failure(f) => f match {
          case _: TimeoutException | _: InterruptedException =>
            // no specific error in reasonable amount of time: assuming a successful binding
            port
          case error =>
            log.warning("Binding to port {} failed ({}). Trying again on {}", port, error.getMessage, port + 1)
            tryBindTo(host, port + 1)
        }
      }
    }

    val sidechannelSettings = settings.sideChannel
    val port = tryBindTo(sidechannelSettings.hostname, sidechannelSettings.startingPort)
    log.info("Accepting incoming connections to {}:{}", sidechannelSettings.hostname, port)
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

    case LoadDataFromDisk(localFile) =>
      val data = CSVParser(settings.parsing).parse(localFile)
      val port = openSidechannel(data)

      log.info("Loaded data from {}. {} is ready", localFile.getAbsolutePath, name)
      sender ! DataRef(data)

      context.become(dataReady(data, port))

    case FetchDataFromCluster =>
      log.debug("Request to load data from cluster: searching left neighbour")
      clusterListener ! GetLeftNeighbor
      context.become(handleLeftNeighborResults(sender))
  }

  def handleLeftNeighborResults(originalSender: ActorRef): Receive = withCommonNotReady {
    case LeftNeighbor(address) =>
      val otherDataHolder = context.actorSelection(address / userGuardian / ODMaster.name / name)
      log.info("Asking left neighbour ({}) for sidechannel address", otherDataHolder)
      otherDataHolder ! GetSidechannelAddress
      context.become(handleFetchDataResult(originalSender, true))

    case akka.actor.Status.Failure(f) =>
      log.warning("Could not get left neighbor address, because {}. Trying with right neighbor", f)
      clusterListener ! GetRightNeighbor
      context.become(handleRightNeighborResults(sender))
  }

  // this is just the failover solution if the left neighbor does not send the data
  def handleRightNeighborResults(originalSender: ActorRef): Receive = withCommonNotReady {
    case RightNeighbor(address) =>
      val otherDataHolder = context.actorSelection(address / userGuardian / ODMaster.name / name)
      log.info("Asking right neighbour ({}) for sidechannel address", otherDataHolder)
      otherDataHolder ! GetSidechannelAddress
      context.become(handleFetchDataResult(originalSender, false))

    case akka.actor.Status.Failure(f) =>
      log.error("Could not get right neighbor address, because {}", f)
      throw FetchDataException(f)
  }

  def handleFetchDataResult(originalSender: ActorRef, isLeftNB: Boolean): Receive = withCommonNotReady {
    case SidechannelAddress(socketAddress) =>
      openRemoteConnection(socketAddress)
      context.become(handleStreamResult(originalSender, isLeftNB, socketAddress))

    case DataNotReady if isLeftNB =>
      log.warning("Left data holder ({}) is no ready yet. Trying with right neighbor", sender.path)
      clusterListener ! GetRightNeighbor
      context.become(handleRightNeighborResults(sender))

    case DataNotReady if !isLeftNB =>
      log.error("Right data holder ({}) is no ready yet, as well.", sender.path)
      throw FetchDataException()

  }

  def handleStreamResult(originalSender: ActorRef, isLeftNB: Boolean, address: InetSocketAddress): Receive = withCommonNotReady {

    case StreamInit =>
      sender ! StreamACK

    case DataOverStream(data) =>
      log.info("Received data over stream from {}", address)
      sender ! StreamACK
      context.become(receivedData(originalSender, data))

    case Failure(cause) =>
      log.error("Error processing fetch data request: {}. Trying again.", cause)
      import context.dispatcher
      context.system.scheduler.scheduleOnce(
        2 second,
        self,
        SidechannelAddress(address)
      )
      context.become(handleFetchDataResult(originalSender, isLeftNB))
  }

  def receivedData(originalSender: ActorRef, data: Array[TypedColumn[Any]]): Receive = withCommonNotReady {

    case StreamComplete =>
      log.info("Stream completed! {} is ready.", name)
      sender ! StreamACK

      val port = openSidechannel(data)
      originalSender ! DataRef(data)
      context.become(dataReady(data, port))
  }

  def dataReady(relation: Array[TypedColumn[Any]], boundPort: Int): Receive = {

    case LoadDataFromDisk(_) =>
      sender ! DataRef(relation)

    case FetchDataFromCluster =>
      sender ! DataRef(relation)

    case GetSidechannelAddress =>
      sender ! SidechannelAddress(InetSocketAddress.createUnresolved(settings.sideChannel.hostname, boundPort))

    case GetDataRef =>
      log.info("Serving data to {}", sender.path)
      sender ! DataRef(relation)
  }

  def withCommonNotReady(block: Receive): Receive = {
    val commonNotReady: Receive = {
      case GetDataRef | GetSidechannelAddress =>
        log.warning("Request to serve data from uninitialized {}", name)
        sender ! DataNotReady
    }

    block orElse commonNotReady
  }
}
