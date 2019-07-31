package com.github.codelionx.dodo.sidechannel

import akka.actor.ActorRef
import com.github.codelionx.dodo.types.TypedColumn

import scala.collection.immutable.Queue




object StreamedDataExchangeProtocol {

  /**
    * Message that wrappes the relational data. Sent via the stream sidechannel.
    */
  case class DataOverStream(data: Array[TypedColumn[Any]]) extends Serializable

  /**
    * Message that wrappes the current state and version number. Sent via the stream sidechannel.
    */
  case class StateOverStream(data: (ActorRef, Queue[(Seq[Int], Seq[Int])], Int)) extends Serializable

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
