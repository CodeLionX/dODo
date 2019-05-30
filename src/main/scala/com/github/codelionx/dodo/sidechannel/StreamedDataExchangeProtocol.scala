package com.github.codelionx.dodo.sidechannel

import com.github.codelionx.dodo.types.TypedColumn


object StreamedDataExchangeProtocol {

  /**
    * Request the relational data. Sent via the stream sidechannel.
    */
  case object GetDataOverStream

  /**
    * Response to [[com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.GetDataOverStream]]
    * that contains the relational data. Sent via the stream sidechannel.
    */
  case class DataOverStream(data: Array[TypedColumn[Any]])

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
