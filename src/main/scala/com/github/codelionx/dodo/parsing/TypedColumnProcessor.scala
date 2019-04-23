package com.github.codelionx.dodo.parsing

import com.github.codelionx.dodo.types.TypedColumnBuilder
import com.univocity.parsers.common.ParsingContext
import com.univocity.parsers.common.processor.RowProcessor

class TypedColumnProcessor(numberOfTypeInferingRows: Int = 20) extends RowProcessor {

  object State extends Enumeration {
    type State = Value
    val TypeInferring, EmptyingBuffer, Parsing = Value
  }

  var state: State.State = State.TypeInferring
  var columns: Array[TypedColumnBuilder[_ <: Any]] = _
  val untypedRowBuffer: Array[Array[String]] = Array.ofDim(numberOfTypeInferingRows)
  var inferrer: TypeInferrer = _
  var columnsIndex: Int = 0
  var untypedRowBufferIndex: Int = 0

  override def processStarted(context: ParsingContext): Unit = {}

  override def rowProcessed(row: Array[String], context: ParsingContext): Unit = {
    state match {
      case State.TypeInferring =>
        // lazy initialization of type inferrer to use the row size
        if (inferrer == null) {
          inferrer = new IterativeTypeInferrer(row.length)
        }

        inferrer.refreshTypesFromRow(row)
        untypedRowBuffer(untypedRowBufferIndex) = row
        untypedRowBufferIndex += 1

        if (untypedRowBufferIndex >= numberOfTypeInferingRows)
          state = State.EmptyingBuffer

      case State.EmptyingBuffer =>
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
        state = State.Parsing

      case State.Parsing =>
        for (j <- row.indices) {
          columns(j).append(row(j))
        }
        columnsIndex += 1
    }
  }

  override def processEnded(context: ParsingContext): Unit = {
    println(inferrer.columnTypes.mkString(" - "))
    println("Buffered and reparsed columns:")
    println(s"Current row index: $columnsIndex")
    println("  " +
      columns.zip(inferrer.columnTypes).map {
        case (xs, t) => t.toString + ": " + xs.toArray.mkString(", ")
      }.mkString("\n  ")
    )
  }
}