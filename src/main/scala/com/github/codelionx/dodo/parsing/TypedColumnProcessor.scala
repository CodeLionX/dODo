package com.github.codelionx.dodo.parsing

import com.github.codelionx.dodo.types.{TypedColumn, TypedColumnBuilder}
import com.univocity.parsers.common.ParsingContext
import com.univocity.parsers.common.processor.RowProcessor


object TypedColumnProcessor {

  val DEFAULT_N_INFERRING_ROWS = 20

  def apply(nInferringRows: Int = DEFAULT_N_INFERRING_ROWS): TypedColumnProcessor = new TypedColumnProcessor(nInferringRows)

}

class TypedColumnProcessor private(numberOfTypeInferringRows: Int) extends RowProcessor {

  private object State extends Enumeration {
    type State = Value
    val TypeInferring, EmptyingBuffer, Parsing = Value
  }

  private var state: State.State = State.TypeInferring
  private var columns: Array[TypedColumnBuilder[_ <: Any]] = _
  private val untypedRowBuffer: Array[Array[String]] = Array.ofDim(numberOfTypeInferringRows)
  private var inferrer: TypeInferrer = _
  private var columnsIndex: Int = 0
  private var untypedRowBufferIndex: Int = 0

  private def runTypeInferring(row: Array[String]): Unit = {
    // lazy initialization of type inferrer to use the row size
    if (inferrer == null) {
      inferrer = new IterativeTypeInferrer(row.length)
    }

    inferrer.inferTypesFromRow(row)
    untypedRowBuffer(untypedRowBufferIndex) = row
    untypedRowBufferIndex += 1
  }

  private def runEmptyingBuffer(): Unit = {
    // type inferring finished, continue with reading the buffer again and parse it to the right types
    if (untypedRowBuffer.nonEmpty) {
      val types = inferrer.columnTypes

      // initialize column arrays
      columns = Array.ofDim(types.length)
      types.indices.foreach(i =>
        columns(i) = types(i).createTypedColumnBuilder
      )

      // fill column arrays with buffered data
      untypedRowBuffer.foreach(bufferedRow => {
        for (j <- bufferedRow.indices) {
          columns(j).append(bufferedRow(j))
        }
        columnsIndex += 1
      })
    }
  }

  private def parseRow(row: Array[String]): Unit = {
    for (j <- row.indices) {
      columns(j).append(row(j))
    }
    columnsIndex += 1
  }

  def columnarData: Array[TypedColumn[_ <: Any]] = columns.map(_.toTypedColumn)

  // from RowProcessor

  override def processStarted(context: ParsingContext): Unit = {}

  override def rowProcessed(row: Array[String], context: ParsingContext): Unit = {
    state match {
      case State.TypeInferring =>
        runTypeInferring(row)
        if (untypedRowBufferIndex >= numberOfTypeInferringRows)
          state = State.EmptyingBuffer

      case State.EmptyingBuffer =>
        runEmptyingBuffer()
        parseRow(row)
        state = State.Parsing

      case State.Parsing =>
        parseRow(row)
    }
  }

  override def processEnded(context: ParsingContext): Unit = {}
}