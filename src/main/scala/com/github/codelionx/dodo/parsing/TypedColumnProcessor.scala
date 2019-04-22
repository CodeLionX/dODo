package com.github.codelionx.dodo.parsing

import com.univocity.parsers.common.ParsingContext
import com.univocity.parsers.common.processor.RowProcessor

class TypedColumnProcessor(numberOfTypeInferingRows: Int = 20) extends RowProcessor {

//  val columns: Array[]
  val untypedRowBuffer: Array[Array[String]] = Array.ofDim(numberOfTypeInferingRows)
  var inferrer: TypeInferrer = _
  var nTypedRows: Int = 0

  override def processStarted(context: ParsingContext): Unit = {}

  override def rowProcessed(row: Array[String], context: ParsingContext): Unit = {
    if(nTypedRows < numberOfTypeInferingRows) {
      // lazy initialization of type inferrer to use the row size
      if (inferrer == null) {
        inferrer = new IterativeTypeInferrer(row.length)
      }

      inferrer.refreshTypesFromRow(row)
      untypedRowBuffer(nTypedRows) = row
      nTypedRows += 1
    } else {

      if (untypedRowBuffer.nonEmpty) {
        val types = inferrer.columnTypes
        untypedRowBuffer.map( row => {
          types.indices.map( i =>
            types(i).parse(row(i))
          )
        })
      }
    }
  }

  override def processEnded(context: ParsingContext): Unit = {
    println(inferrer.columnTypes.mkString(" - "))
  }
}