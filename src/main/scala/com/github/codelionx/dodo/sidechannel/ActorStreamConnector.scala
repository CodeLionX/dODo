package com.github.codelionx.dodo.sidechannel

import akka.actor.{ActorContext, ActorRef, ActorSystem}
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.{Framing, Sink, Source, StreamRefs}
import akka.stream.{ActorMaterializer, SourceRef}
import akka.util.ByteString
import com.github.codelionx.dodo.DodoException
import com.github.codelionx.dodo.actors.DataHolder.SidechannelRef
import com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol._

import scala.concurrent.Future


object ActorStreamConnector {

  private val frameLength = 50000

  /**
    * Creates a [[akka.stream.SourceRef]] containing the `data` (wrapped in a
    * [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.OverStream]] message,
    * serialized, chunked, and framed) and wraps it inside
    * a [[com.github.codelionx.dodo.actors.DataHolder.SidechannelRef]]. The returned future can be piped to another
    * actor reference (possibly on another node). See [[akka.pattern.pipe]].
    *
    * @see [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.OverStreamCompanion]] for enhancing this
    *     functionality
    */
  def prepareSourceRef[T: OverStreamCompanion](data: T)(implicit context: ActorContext): Future[SidechannelRef] = {
    val system: ActorSystem = context.system
    val serialization = SerializationExtension(system)
    import system.dispatcher
    implicit val mat: ActorMaterializer = ActorMaterializer()(context)

    Source.single(data)
      .map(data => OverStreamCompanion(data))
      .map(serialization.serialize)
      .map {
        case scala.util.Success(msg) => ByteString.fromArray(msg)
        case scala.util.Failure(cause) => throw new DodoException("Serialization of message failed", cause)
      }
      .flatMapConcat(bytes =>
        Source.fromIterator(() => bytes.grouped(frameLength))
      )
      .via(Framing.simpleFramingProtocolEncoder(frameLength))
      .runWith(StreamRefs.sourceRef())
      .map(SidechannelRef)
  }

  /**
    * Takes a [[akka.stream.SourceRef]] and processes it by sending it to the supplied `actorRef`.
    * The data is decoded, aggregated and deserialized to the supplied
    * [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.OverStream]] message. See
    * [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol]] for all messages that the supplied actor
    * must handle.
    */
  def consumeSourceRefOfClassVia[T](source: SourceRef[ByteString], msgClass: Class[_ <: OverStream[T]], actorRef: ActorRef)(implicit context: ActorContext): Unit = {
    val system: ActorSystem = context.system
    val serialization = SerializationExtension(system)
    implicit val mat: ActorMaterializer = ActorMaterializer()(context)

    source
      .via(Framing.simpleFramingProtocolDecoder(frameLength))
      .reduce(_ ++ _)
      .map(bytes => serialization.deserialize(bytes.toArray, msgClass))
      .map {
        case scala.util.Success(msg) => msg
        case scala.util.Failure(cause) => throw new DodoException("Deserialization of message failed", cause)
      }
      .runWith(Sink.actorRefWithAck(actorRef, StreamInit, StreamACK, StreamComplete))
  }

}
