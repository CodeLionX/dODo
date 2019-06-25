package com.github.codelionx.dodo.sidechannel


import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import akka.stream.QueueOfferResult.{Dropped, Enqueued, QueueClosed}
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.stream.scaladsl.Tcp.IncomingConnection
import com.github.codelionx.dodo.DodoException
import com.github.codelionx.dodo.actors.Reaper
import com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol._
import com.github.codelionx.dodo.types.TypedColumn

import scala.concurrent.duration._
import scala.language.postfixOps


object DataStreamServant {

  final val MAXIMUM_NUMBER_OF_RETRIES = 3

  def props(data: Array[TypedColumn[Any]], connection: IncomingConnection): Props =
    Props(new DataStreamServant(data, connection))

}


class DataStreamServant(data: Array[TypedColumn[Any]], connection: IncomingConnection) extends Actor with ActorLogging {

  import DataStreamServant._

  ////////////// write test message in bytes into file `out`
  //  import akka.serialization.SerializationExtension
  //  import java.io.FileOutputStream
  //  val writer = new FileOutputStream("out")
  //  val serialization = SerializationExtension(context.system)
  //  writer.write(serialization.serialize(GetDataOverStream).get)
  //  writer.close()
  //////////////


  implicit private val mat: ActorMaterializer = ActorMaterializer()(context)
  implicit private val system: ActorSystem = context.system
  import context.dispatcher

  private val dataMsg = DataOverStream(data)
  private val actorConnector = ActorStreamConnector.withQueueSource[GetDataOverStream.type, DataOverStream](
    targetActorRef = self,
    deserializationClassHint = GetDataOverStream.getClass
  )

  val sourceQueue: SourceQueueWithComplete[DataOverStream] = connection.handleWith(actorConnector)
  log.debug("DataStreamServant for connection from {} ready", connection.remoteAddress)

  override def preStart(): Unit = Reaper.watchWithDefault(self)

  override def postStop(): Unit =
    log.debug("DataStreamServant for connection from {} stopped", connection.remoteAddress)

  override def receive: Receive = {

    case StreamInit =>
      sender ! StreamACK

    case GetDataOverStream =>
      log.debug("Received request for data over stream")
      sourceQueue.offer(dataMsg) pipeTo self
      sender ! StreamACK
      context.become(handleEnqueing())

    case Failure(cause) =>
      log.error("Error processing incoming request: {}, {}", cause, cause.getCause)
      sourceQueue.fail(cause)
      context.stop(self)

    case StreamComplete =>
      sender ! StreamACK
      context.stop(self)
  }

  def handleEnqueing(retries: Int = MAXIMUM_NUMBER_OF_RETRIES): Receive = {

    case Enqueued =>
      sourceQueue.complete()
      context.become(waitingForClose)

    case QueueClosed =>
      log.warning("Stream closed prematurely")
      context.stop(self)

    case Failure(cause) =>
      log.error("Streaming failed to deliver data, because {}", cause)
      context.stop(self)

    case Dropped if retries > 0 =>
      log.warning(
        "Enqueued data message was dropped, trying again ({} / {} retries)",
        3 - retries + 1,
        MAXIMUM_NUMBER_OF_RETRIES
      )

      context.system.scheduler.scheduleOnce(1 second) {
        sourceQueue.offer(dataMsg) pipeTo self
        context.become(handleEnqueing(retries - 1))
      }

    case Dropped =>
      sourceQueue.fail(new DodoException("all $MAXIMUM_NUMBER_OF_RETRIES retries failed!"))
      log.error("Could not send data after {} retries", MAXIMUM_NUMBER_OF_RETRIES)
      context.stop(self)

    case StreamComplete =>
      log.info("Stream closed.")
      sender ! StreamACK
      context.stop(self)
  }

  def waitingForClose: Receive = {

    case StreamComplete =>
      log.info("Data was streamed!")
      sender ! StreamACK
      context.stop(self)
  }
}
