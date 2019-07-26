package com.github.codelionx.dodo.actors

import java.io.File

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import akka.stream.SourceRef
import akka.util.ByteString
import com.github.codelionx.dodo.actors.ClusterListener.{GetLeftNeighbor, GetRightNeighbor, LeftNeighbor, RightNeighbor}
import com.github.codelionx.dodo.actors.master.ODMaster
import com.github.codelionx.dodo.parsing.CSVParser
import com.github.codelionx.dodo.sidechannel.ActorStreamConnector
import com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol._
import com.github.codelionx.dodo.types.TypedColumn
import com.github.codelionx.dodo.{DodoException, Settings}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try


object DataHolder {

  val name = "dataholder"

  def props(clusterListener: ActorRef): Props = Props(new DataHolder(clusterListener))

  // sidechannel address request-response pair
  case object GetSidechannelRef extends Serializable

  case class SidechannelRef(data: SourceRef[ByteString])

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
  private val settings = Settings(context.system)

  override def preStart(): Unit = Reaper.watchWithDefault(self)

  override def postStop(): Unit = log.info("DataHolder stopped")

  override def receive: Receive = uninitialized

  def uninitialized: Receive = withCommonNotReady {

    case LoadDataFromDisk(localFile) =>
      Try{
        CSVParser(settings.parsing).parse(localFile)
      } match {
        case scala.util.Success(data) =>
          log.info("Loaded data from {}. {} is ready", localFile.getAbsolutePath, name)
          sender ! DataRef(data)
          context.become(dataReady(data))

        case scala.util.Failure(f) =>
          log.error("Could not load data from {}, because {}", localFile.getAbsoluteFile, f)
          sender ! DataNotReady
      }

    case FetchDataFromCluster =>
      log.debug("Request to load data from cluster: searching left neighbour")
      clusterListener ! GetLeftNeighbor
      context.become(handleLeftNeighborResults(sender))
  }

  def handleLeftNeighborResults(originalSender: ActorRef): Receive = withCommonNotReady {
    case LeftNeighbor(address) =>
      val otherDataHolder = context.actorSelection(address / userGuardian / ODMaster.name / name)
      log.info("Asking left neighbour ({}) for sidechannel ref", otherDataHolder)
      otherDataHolder ! GetSidechannelRef
      context.become(handleFetchDataResult(originalSender, isLeftNB = true))

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
      otherDataHolder ! GetSidechannelRef
      context.become(handleFetchDataResult(originalSender, isLeftNB = false))

    case akka.actor.Status.Failure(f) =>
      log.error("Could not get right neighbor address, because {}", f)
      throw FetchDataException(f)
  }

  def handleFetchDataResult(originalSender: ActorRef, isLeftNB: Boolean): Receive = withCommonNotReady {
    case SidechannelRef(sourceRef) =>
      ActorStreamConnector.consumeSourceRefVia(sourceRef, self)
      context.become(handleStreamResult(originalSender, isLeftNB))

    case DataNotReady if isLeftNB =>
      log.warning("Left data holder ({}) is no ready yet. Trying with right neighbor", sender.path)
      clusterListener ! GetRightNeighbor
      context.become(handleRightNeighborResults(sender))

    case DataNotReady if !isLeftNB =>
      log.error("Right data holder ({}) is no ready yet, as well.", sender.path)
      throw FetchDataException()

  }

  def handleStreamResult(originalSender: ActorRef, isLeftNB: Boolean): Receive = withCommonNotReady {

    case StreamInit =>
      sender ! StreamACK

    case dataMessage: DataOverStream =>
      log.info("Received data over stream.")
      sender ! StreamACK
      context.become(receivedData(originalSender, dataMessage.data))

    case Failure(cause) =>
      log.error("Error processing fetch data request: {}. Trying again.", cause)

      import context.dispatcher
      context.system.scheduler.scheduleOnce(
        2 second,
        self,
        FetchDataFromCluster
      )
      context.become(uninitialized)
  }

  def receivedData(originalSender: ActorRef, data: Array[TypedColumn[Any]]): Receive = withCommonNotReady {

    case StreamComplete =>
      log.info("Stream completed! {} is ready.", name)
      sender ! StreamACK
      originalSender ! DataRef(data)
      context.become(dataReady(data))
  }

  def dataReady(relation: Array[TypedColumn[Any]]): Receive = {

    case LoadDataFromDisk(_) =>
      sender ! DataRef(relation)

    case FetchDataFromCluster =>
      sender ! DataRef(relation)

    case GetSidechannelRef =>
      log.info("Serving data via sidechannel to {}", sender.path)
      val dataSource = ActorStreamConnector.prepareSourceRef(relation)
      import context.dispatcher
      dataSource pipeTo sender

    case GetDataRef =>
      log.info("Serving data to {}", sender.path)
      sender ! DataRef(relation)
  }

  def withCommonNotReady(block: Receive): Receive = {
    val commonNotReady: Receive = {
      case GetDataRef | GetSidechannelRef =>
        log.warning("Request to serve data from uninitialized {}", name)
        sender ! DataNotReady
    }

    block orElse commonNotReady
  }
}
