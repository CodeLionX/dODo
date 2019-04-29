package com.github.codelionx.dodo.parsing

import com.github.codelionx.dodo.types.{TypedColumn, TypedColumnBuilder}
import com.univocity.parsers.common.ParsingContext
import com.univocity.parsers.common.processor.AbstractRowProcessor


object TypedColumnProcessor {

  val DEFAULT_N_INFERRING_ROWS = 20

  def apply(nInferringRows: Int = DEFAULT_N_INFERRING_ROWS): TypedColumnProcessor = new TypedColumnProcessor(nInferringRows)

}

/**
  * A [[com.univocity.parsers.common.processor.RowProcessor]] that infers the data types of the read CSV rows and
  * stores them in [[com.github.codelionx.dodo.types.TypedColumn]]s. The type inference is based on the first `numberOfTypeInferringRows` rows read.
  * After wards all following values will be parsed to the determined data type.
  *
  * @see [[com.github.codelionx.dodo.parsing.TypeInferrer]] for information how the data types are inferred
  * @param numberOfTypeInferringRows rows used for inferring and refining the data types for the columns
  */
class TypedColumnProcessor private(numberOfTypeInferringRows: Int) extends AbstractRowProcessor {

  private object State extends Enumeration {
    type State = Value
    val TypeInferring, EmptyingBuffer, Parsing = Value
  }

  private var state: State.State = State.TypeInferring
  private var columns: Array[TypedColumnBuilder[Any]] = _
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

  /**
    * Returns the columnar data parsed from the CSV file as an array of [[com.github.codelionx.dodo.types.TypedColumn]]s.
    */
  def columnarData: Array[TypedColumn[Any]] = columns.map(_.toTypedColumn)

  // from RowProcessor
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
}