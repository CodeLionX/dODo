package com.github.codelionx.dodo.sidechannel


import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import akka.stream.QueueOfferResult.{Dropped, Enqueued, QueueClosed}
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.stream.scaladsl.Tcp.IncomingConnection
import com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol._
import com.github.codelionx.dodo.types.TypedColumn

import scala.concurrent.duration._
import scala.language.postfixOps


object DataStreamServant {

  private final val MAXIMUM_NUMBER_OF_RETRIES = 3

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


  implicit private val mat: ActorMaterializer = ActorMaterializer()

  private val actorConnector = ActorStreamConnector.withQueueSource[GetDataOverStream.type, DataOverStream](
    self,
    GetDataOverStream.getClass
  )(context.system)

  val sourceQueue: SourceQueueWithComplete[DataOverStream] = connection.handleWith(actorConnector)
  log.info(s"DataStreamServant for connection from ${connection.remoteAddress} ready")

  private def offerData(source: SourceQueueWithComplete[DataOverStream], data: DataOverStream): Unit = {
    import context.dispatcher
    source.offer(data) pipeTo self
  }

  override def receive: Receive = {

    case StreamInit =>
      sender ! StreamACK

    case GetDataOverStream =>
      log.debug("Received request for data over stream")
      offerData(sourceQueue, DataOverStream(data))
      sender ! StreamACK
      context.become(handleEnqueing())

    case Failure(cause) =>
      log.error(s"Error processing incoming request: $cause, ${cause.getCause}")
      sourceQueue.fail(cause)
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
      log.error(s"Streaming failed to deliver data, because $cause")
      context.stop(self)

    case Dropped if retries > 0 =>
      log.warning(s"Enqueued data message was dropped, trying again" +
        s"(${3 - retries + 1} / $MAXIMUM_NUMBER_OF_RETRIES retries)")

      context.system.scheduler.scheduleOnce(1 second) {
        offerData(sourceQueue, DataOverStream(data))
        context.become(handleEnqueing(retries - 1))
      }(scala.concurrent.ExecutionContext.Implicits.global)

    case Dropped =>
      sourceQueue.fail(new RuntimeException(s"all $MAXIMUM_NUMBER_OF_RETRIES retries failed!"))
      log.error(s"Could not send data after $MAXIMUM_NUMBER_OF_RETRIES retries")
      context.stop(self)
  }

  def waitingForClose: Receive = {

    case StreamComplete =>
      log.info("Data was streamed! Stopping servant")
      sender ! StreamACK
      context.stop(self)
  }
}
