package com.github.codelionx.dodo.sidechannel

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.serialization.{Serialization, SerializationExtension}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, SourceQueueWithComplete}
import akka.util.ByteString
import com.github.codelionx.dodo.DodoException
import com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.{StreamACK, StreamComplete, StreamInit}

import scala.util.{Failure, Success}


object ActorStreamConnector {

  /**
    * Builds a flow for receiving stream elements to the `targetActorRef` with a
    * [[akka.stream.scaladsl.Source#queue]] as outgoing stream source. The
    * incoming messages get deserialized to type `IN` and sent to the actor.
    * The outgoing messages of type `OUT` can be enqueued after materialization
    * of the flow (`.run*().offer(elem)`) and get serialized to [[akka.util.ByteString]].
    */
  def withQueueSource[IN <: AnyRef, OUT <: AnyRef](
                                                    targetActorRef: ActorRef,
                                                    deserializationClassHint: Class[_ <: IN]
                                                  )(
                                                    implicit system: ActorSystem
                                                  ): Flow[ByteString, ByteString, SourceQueueWithComplete[OUT]] = {
    val serialization = SerializationExtension(system)
    buildFlow(
      Source.queue[OUT](1, OverflowStrategy.backpressure),
      serialization,
      targetActorRef,
      deserializationClassHint
    )
  }

  /**
    * Builds a flow for receiving stream elements to the `targetActorRef` with a
    * [[akka.stream.scaladsl.Source#single]] as outgoing stream source. The
    * incoming messages get deserialized to type `IN` and sent to the actor.
    * The outgoing message `singleSourceElem` is directly deserialized to a
    * [[akka.util.ByteString]] and sent out.
    */
  def withSingleSource[IN <: AnyRef, OUT <: AnyRef](
                                                     targetActorRef: ActorRef,
                                                     singleSourceElem: OUT,
                                                     deserializationClassHint: Class[_ <: IN]
                                                   )(
                                                     implicit system: ActorSystem
                                                   ): Flow[ByteString, ByteString, NotUsed] = {
    val serialization = SerializationExtension(system)
    buildFlow(
      Source.single(singleSourceElem),
      serialization,
      targetActorRef,
      deserializationClassHint
    )
  }

  private def buildFlow[IN <: AnyRef, OUT <: AnyRef, MAT](
                                                           source: Source[OUT, MAT],
                                                           serialization: Serialization,
                                                           targetActorRef: ActorRef,
                                                           deserializationClassHint: Class[_ <: IN]
                                                         ): Flow[ByteString, ByteString, MAT] = {
    val sinkSourceFlow = Flow.fromSinkAndSourceMat(
      Sink.actorRefWithAck[IN](targetActorRef, StreamInit, StreamACK, StreamComplete),
      source
    )(Keep.right)

    Flow[ByteString]
      // breaks completion of serving stream? but the current version works without it!
      // .reduce(_ ++ _)
      .map(bytes => serialization.deserialize(bytes.toArray, deserializationClassHint))
      .map {
        case Success(msg) => msg
        case Failure(cause) => throw new DodoException("Deserialization of message failed", cause)
      }
      .viaMat(sinkSourceFlow)(Keep.right)
      .map(serialization.serialize)
      .map {
        case Success(msg) => ByteString.fromArray(msg)
        case Failure(cause) => throw new DodoException("Serialization of message failed", cause)
      }
  }
}
