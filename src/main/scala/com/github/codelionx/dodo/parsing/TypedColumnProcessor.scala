package com.github.codelionx.dodo.parsing

import com.github.codelionx.dodo.Settings.ParsingSettings
import com.github.codelionx.dodo.types.{TypedColumn, TypedColumnBuilder}
import com.univocity.parsers.common.ParsingContext
import com.univocity.parsers.common.processor.AbstractRowProcessor


object TypedColumnProcessor {

  /**
    * Creates a new TypedColumnProcessor.
    *
    * @see [[com.github.codelionx.dodo.parsing.TypeInferrer]] for information how the data types are inferred
    * @param settings parsing settings
    * @return a new [[com.github.codelionx.dodo.parsing.TypedColumnProcessor]]
    */
  def apply(settings: ParsingSettings): TypedColumnProcessor = new TypedColumnProcessor(settings)

}

/**
  * A [[com.univocity.parsers.common.processor.RowProcessor]] that infers the data types of the read CSV rows and
  * stores them in [[com.github.codelionx.dodo.types.TypedColumn]]s. The type inference is based on the first `numberOfTypeInferringRows` rows read.
  * After wards all following values will be parsed to the determined data type.
  *
  * @see [[com.github.codelionx.dodo.parsing.TypeInferrer]] for information how the data types are inferred
  * @param settings parsing settings
  */
class TypedColumnProcessor private(settings: ParsingSettings) extends AbstractRowProcessor {

  private object State extends Enumeration {

    type State = Value
    val TypeInferring, Parsing, Finished = Value
  }

  private var state: State.State = State.TypeInferring
  private var columns: Array[TypedColumnBuilder[Any]] = _
  private val untypedRowBuffer: Array[Array[String]] = Array.ofDim(settings.nInferringRows)
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

  private def runEmptyingBuffer(context: ParsingContext): Unit = {
    // type inferring finished, continue with reading the buffer again and parse it to the right types
    if (untypedRowBuffer.nonEmpty) {
      val types = inferrer.columnTypes
      val headers: Array[String] =
        if(settings.parseHeader)
          context.headers()
        else
          generateSyntheticColumnNames(types.length)

      // initialize column arrays
      columns = Array.ofDim(types.length)
      types.indices.foreach(i => {
        columns(i) = types(i)
          .createTypedColumnBuilder
          .withName(headers(i))
      })

      // fill column arrays with buffered data
      untypedRowBuffer.foreach(bufferedRow => {
        if (bufferedRow != null) {
          for (j <- bufferedRow.indices) {
            columns(j).append(bufferedRow(j))
          }
          columnsIndex += 1
        }
      })

    }
  }

  private def parseRow(row: Array[String]): Unit = {
    for (j <- row.indices) {
      columns(j).append(row(j))
    }
    columnsIndex += 1
  }

  private def generateSyntheticColumnNames(length: Int): Array[String] = {
    // seems very fast, so no optimization necessary
    val columnIndexToName = (i: Int) => {
      var name = ""
      var number = i
      while(number > 0) {
        val remainder = number % 26
        if(remainder == 0) {
          name += "Z"
          number = (number / 26) - 1
        } else {
          name += ('A' + remainder - 1).toChar
          number /= 26
        }
      }
      name.reverse
    }
    (1 to length).map(columnIndexToName).toArray
  }

  /**
    * Returns the columnar data parsed from the CSV file as an array of [[com.github.codelionx.dodo.types.TypedColumn]]s.
    */
  def columnarData: Array[TypedColumn[Any]] =
    if (state == State.Finished) columns.map(_.toTypedColumn)
    else throw new IllegalAccessException("The parsing process has not finished yet, but proc.columnarData was accessed")

  // from RowProcessor
  override def rowProcessed(row: Array[String], context: ParsingContext): Unit = {
    state match {
      case State.TypeInferring =>
        runTypeInferring(row)
        if (untypedRowBufferIndex >= settings.nInferringRows) {
          runEmptyingBuffer(context)
          state = State.Parsing
        }

      case State.Parsing =>
        parseRow(row)
    }
  }

  override def processEnded(context: ParsingContext): Unit = {
    /* If number of type inferring rows is greater than number of records, we have to parse the buffer at the end of
     * the parsing process with the type information collected till now.
     * This is reflected in this method:
     *  - the parsing process has ended (no more records)
     *  - but we are still in the TypeInferring state
     */
    if (state == State.TypeInferring) {
      runEmptyingBuffer(context)
    }
    state = State.Finished
  }
}