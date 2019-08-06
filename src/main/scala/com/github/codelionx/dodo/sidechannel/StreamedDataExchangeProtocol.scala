package com.github.codelionx.dodo.sidechannel

import akka.actor.ActorRef
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.immutable.Queue


object StreamedDataExchangeProtocol {

  /**
    * Type class for constructing [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.OverStream]]
    * messages containing different data types.
    *
    * @see [[com.github.codelionx.dodo.sidechannel.ActorStreamConnector]]
    * @tparam T data type that is wrapped by the [[OverStream]] message
    */
  trait OverStreamCompanion[T] {

    def apply(data: T): OverStream[T]

  }

  /**
    * Companion implementing type class functionality for
    * [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.OverStreamCompanion]].
    *
    * @see [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.OverStreamCompanion]]
    */
  object OverStreamCompanion {

    def apply[T: OverStreamCompanion](data: T): OverStream[T] = implicitly[OverStreamCompanion[T]].apply(data)

    // implicit instances for the type class
    implicit def dataOverStream: OverStreamCompanion[Array[TypedColumn[Any]]] = DataOverStream

    implicit def stateOverStream: OverStreamCompanion[(ActorRef, Queue[(Seq[Int], Seq[Int])], Int)] = StateOverStream
  }

  trait OverStream[T] extends Serializable {

    def data: T
  }

  object OverStream {

    /**
      * Syntactic sugar to allow `OverStream.apply()` instead of using `OverStreamCompanion.apply()` to construct
      * [[OverStream]] messages.
      */
    def apply[T: OverStreamCompanion](data: T): OverStream[T] = OverStreamCompanion(data)
  }

  // dataset message for sidechannel
  object DataOverStream extends OverStreamCompanion[Array[TypedColumn[Any]]] {

    override def apply(data: Array[TypedColumn[Any]]): DataOverStream = new DataOverStream(data)
  }

  /**
    * Message that wraps the relational data. Sent via the stream sidechannel.
    */
  class DataOverStream(val data: Array[TypedColumn[Any]])
    extends OverStream[Array[TypedColumn[Any]]]

  // state message for sidechannel
  object StateOverStream extends OverStreamCompanion[(ActorRef, Queue[(Seq[Int], Seq[Int])], Int)] {

    override def apply(data: (ActorRef, Queue[(Seq[Int], Seq[Int])], Int)): StateOverStream = new StateOverStream(data)
  }

  /**
    * Message that wraps the current state and version number. Sent via the stream sidechannel.
    */
  class StateOverStream(val data: (ActorRef, Queue[(Seq[Int], Seq[Int])], Int))
    extends OverStream[(ActorRef, Queue[(Seq[Int], Seq[Int])], Int)]

  /**
    * Indicates stream initialization.
    *
    * <b>Only used internally!</b>
    */
  case object StreamInit

  /**
    * Indicates stream completion.
    *
    * <b>Only used internally!</b>
    */
  case object StreamComplete

  /**
    * Response to
    * [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.StreamInit]]
    * or
    * [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.StreamComplete]]
    * to indicate readiness (used for back-pressure-mechanism).
    *
    * <b>Only used internally!</b>
    */
  case object StreamACK

}
