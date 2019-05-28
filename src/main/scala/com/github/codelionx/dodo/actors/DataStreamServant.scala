package com.github.codelionx.dodo.actors

import akka.actor.Status.Failure
import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.pipe
import akka.serialization.SerializationExtension
import akka.stream.QueueOfferResult.{Dropped, Enqueued, QueueClosed}
import akka.stream.scaladsl.Tcp.IncomingConnection
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.{ActorMaterializer, OverflowStrategy, QueueOfferResult}
import akka.util.ByteString
import com.github.codelionx.dodo.actors.DataStreamServant._
import com.github.codelionx.dodo.types.TypedColumn

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps


object DataStreamServant {

  def props(data: Array[TypedColumn[Any]]): Props = Props(new DataStreamServant(data))

  case class HandleConnection(con: IncomingConnection)

  case object GetDataOverStream

  case class DataRefOverStream(data: Array[TypedColumn[Any]])

  case object StreamInit

  case object StreamACK

  case object StreamComplete

}


class DataStreamServant(data: Array[TypedColumn[Any]]) extends Actor with ActorLogging {

  implicit private val mat: ActorMaterializer = ActorMaterializer()
  private val serialization = SerializationExtension(context.system)

  ////////////// write test message in bytes into file `out`
  //  val writer = new FileOutputStream("out")
  //  writer.write(serialization.serialize(GetDataOverStream).get)
  //  writer.close()
  //////////////

  private val actorConnector = Flow.fromSinkAndSourceMat(
    Sink.actorRefWithAck[GetDataOverStream.type](self, StreamInit, StreamACK, StreamComplete),
    Source.queue[DataRefOverStream](1, OverflowStrategy.backpressure)
      .map(serialization.serialize)
      .map {
        case scala.util.Success(msg) => ByteString.fromArray(msg)
        case scala.util.Failure(f) => throw f
      }
  )(Keep.right)

  private val handler = Flow[ByteString]
    .map(bytes => serialization.deserialize(bytes.toArray, GetDataOverStream.getClass))
    .map {
      case scala.util.Success(msg) => msg
      case scala.util.Failure(f) => throw f
    }
    .viaMat(actorConnector)(Keep.right)

  override def receive: Receive = {
    case HandleConnection(connection) =>
      val sourceQueue = connection.handleWith(handler)
      context.become(streamRequest(sourceQueue))
  }

  def streamRequest(source: SourceQueueWithComplete[DataStreamServant.DataRefOverStream]): Receive = {

    case StreamInit =>
      log.info("Stream initialized")
      sender ! StreamACK

    case GetDataOverStream =>
      log.info("Received request for data over stream")
      val offerFuture: Future[QueueOfferResult] = source.offer(DataRefOverStream(data))
      sender ! StreamACK

      import context.dispatcher
      offerFuture pipeTo self
      context.become(handleEnqueing(source))

    case Failure(f) =>
      log.error(s"Error processing incoming request: $f")
      source.fail(f)

    case msg =>
      log.error(s"Received unknown request: $msg")
      source.fail(new RuntimeException("Unknown request"))
      context.become(waitingForClose())
  }

  def handleEnqueing(source: SourceQueueWithComplete[DataStreamServant.DataRefOverStream]): Receive = {

    case Enqueued =>
      log.info("Data was send")
      source.complete()
      context.become(waitingForClose())

    case QueueClosed =>
      log.info("Stream closed")
      context.stop(self)

    case Failure(cause) =>
      log.error(s"Streaming failed to deliver data, because $cause")
      source.fail(cause)
    //      context.stop(self)

    case Dropped =>
      log.info("Data was dropped, trying again")
      context.system.scheduler.scheduleOnce(1 second) {
        val offerFuture: Future[QueueOfferResult] = source.offer(DataRefOverStream(data))
        import context.dispatcher
        offerFuture pipeTo self
        context.become(handleEnqueing(source))
      }(scala.concurrent.ExecutionContext.Implicits.global)

  }

  def waitingForClose(): Receive = {

    case StreamComplete =>
      log.info("Stream completed! Stopping servant")
      sender ! StreamACK
      context.stop(self)
  }
}
