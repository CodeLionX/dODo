package com.github.codelionx.dodo.sidechannel

import akka.actor.{ActorContext, ActorRef, ActorSystem}
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.{Framing, Sink, Source, StreamRefs}
import akka.stream.{ActorMaterializer, SourceRef}
import akka.util.ByteString
import com.github.codelionx.dodo.DodoException
import com.github.codelionx.dodo.actors.DataHolder.SidechannelRef
import com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.{DataOverStream, StreamACK, StreamComplete, StreamInit}
import com.github.codelionx.dodo.types.TypedColumn

import scala.concurrent.Future


object ActorStreamConnector {

  private val frameLength = 50000

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


}
