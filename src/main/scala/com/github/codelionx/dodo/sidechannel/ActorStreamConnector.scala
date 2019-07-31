package com.github.codelionx.dodo.sidechannel

import akka.actor.{ActorContext, ActorRef, ActorSystem}
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.{Framing, Sink, Source, StreamRefs}
import akka.stream.{ActorMaterializer, SourceRef}
import akka.util.ByteString
import com.github.codelionx.dodo.DodoException
import com.github.codelionx.dodo.actors.DataHolder.SidechannelRef
import com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.{DataOverStream, StateOverStream, StreamACK, StreamComplete, StreamInit}
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.immutable.Queue
import scala.concurrent.Future


object ActorStreamConnector {

  private val frameLength = 50000

  /**
    * Creates a [[akka.stream.SourceRef]] containing the `data` (wrapped in a
    * [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.DataOverStream]] message,
    * serialized, chunked, and framed) and wraps it inside
    * a [[com.github.codelionx.dodo.actors.DataHolder.SidechannelRef]]. The returned future can be piped to another
    * actor reference (possibly on another node). See [[akka.pattern.pipe]].
    */
  def prepareSourceRef(data: Array[TypedColumn[Any]])(implicit context: ActorContext): Future[SidechannelRef] = {
    val system: ActorSystem = context.system
    val serialization = SerializationExtension(system)
    import system.dispatcher
    implicit val mat: ActorMaterializer = ActorMaterializer()(context)

    Source.single(data)
      .map(DataOverStream)
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
    * The data is decoded, aggregated and deserialized to the
    * [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.DataOverStream]] message. See
    * [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol]] for all messages that the supplied actor
    * must handle.
    */
  def consumeSourceRefVia(source: SourceRef[ByteString], actorRef: ActorRef)(implicit context: ActorContext): Unit = {
    val system: ActorSystem = context.system
    val serialization = SerializationExtension(system)
    implicit val mat: ActorMaterializer = ActorMaterializer()(context)

    source
      .via(Framing.simpleFramingProtocolDecoder(frameLength))
      .reduce(_ ++ _)
      .map(bytes => serialization.deserialize(bytes.toArray, classOf[DataOverStream]))
      .map {
        case scala.util.Success(msg) => msg
        case scala.util.Failure(cause) => throw new DodoException("Deserialization of message failed", cause)
      }
      .runWith(Sink.actorRefWithAck(actorRef, StreamInit, StreamACK, StreamComplete))
  }

  /**
    * Creates a [[akka.stream.SourceRef]] containing the `data` (wrapped in a
    * [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.StateOverStream]] message,
    * serialized, chunked, and framed) and wraps it inside
    * a [[com.github.codelionx.dodo.actors.DataHolder.SidechannelRef]]. The returned future can be piped to another
    * actor reference (possibly on another node). See [[akka.pattern.pipe]].
    */
  def prepareStateRef(data: (ActorRef, Queue[(Seq[Int], Seq[Int])], Int))(implicit context: ActorContext): Future[SidechannelRef] = {
    val system: ActorSystem = context.system
    val serialization = SerializationExtension(system)
    import system.dispatcher
    implicit val mat: ActorMaterializer = ActorMaterializer()(context)

    Source.single(data)
      .map(StateOverStream)
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
    * The data is decoded, aggregated and deserialized to the
    * [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.StateOverStream]] message. See
    * [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol]] for all messages that the supplied actor
    * must handle.
    */
  def consumeStateRefVia(source: SourceRef[ByteString], actorRef: ActorRef)(implicit context: ActorContext): Unit = {
    val system: ActorSystem = context.system
    val serialization = SerializationExtension(system)
    implicit val mat: ActorMaterializer = ActorMaterializer()(context)

    source
      .via(Framing.simpleFramingProtocolDecoder(frameLength))
      .reduce(_ ++ _)
      .map(bytes => serialization.deserialize(bytes.toArray, classOf[StateOverStream]))
      .map {
        case scala.util.Success(msg) => msg
        case scala.util.Failure(cause) => throw new DodoException("Deserialization of message failed", cause)
      }
      .runWith(Sink.actorRefWithAck(actorRef, StreamInit, StreamACK, StreamComplete))
  }


}
