package com.github.codelionx.dodo.parsing

import com.github.codelionx.dodo.types.typedColumns
import com.github.codelionx.dodo.types.typedColumns.{TypedColumn, TypedColumnBuffer}
import com.univocity.parsers.common.ParsingContext
import com.univocity.parsers.common.processor.RowProcessor

import scala.collection.mutable.ArrayBuffer

class TypedColumnProcessor(numberOfTypeInferingRows: Int = 20) extends RowProcessor {

  var columns: Array[TypedColumnBuffer] = _
  val untypedRowBuffer: Array[Array[String]] = Array.ofDim(numberOfTypeInferingRows)
  var inferrer: TypeInferrer = _
  var columnsIndex: Int = 0
  var untypedRowBufferIndex: Int = 0

  override def processStarted(context: ParsingContext): Unit = {}

  override def rowProcessed(row: Array[String], context: ParsingContext): Unit = {
    if(untypedRowBufferIndex < numberOfTypeInferingRows) {
      // lazy initialization of type inferrer to use the row size
      if (inferrer == null) {
        inferrer = new IterativeTypeInferrer(row.length)
      }

      inferrer.refreshTypesFromRow(row)
      untypedRowBuffer(untypedRowBufferIndex) = row
      untypedRowBufferIndex += 1

    } else {
      // type inferring finished, continue with reading the buffer again and parse it to the right types
      if (untypedRowBuffer.nonEmpty) {
        val types = inferrer.columnTypes

        // initialize column arrays
        columns =  Array.ofDim(types.length)
        types.indices.foreach(i => {
          val dataType = types(i)
          columns(i) = typedColumns.bufferFromDataType(dataType)
        })

        // fill column arrays with buffered data
        untypedRowBuffer.foreach( row => {
          val values = types.indices.map( i =>
            types(i).parse(row(i))
          )
          for(j <- values.indices) {
            val tBuffer = columns(columnsIndex)
            tBuffer.append(values(j))
          }
          columnsIndex += 1
        })

        println("Buffered and reparsed columns:")
        println(s"Current row index: $columnsIndex")
        println(columns.zip(inferrer.columnTypes).map{
          case (xs, t) => t.toString + xs.backingArray.mkString(", ")
        }.mkString("\n  ")
        )
      }
    }
  }

  override def processEnded(context: ParsingContext): Unit = {
    println(inferrer.columnTypes.mkString(" - "))
  }
}