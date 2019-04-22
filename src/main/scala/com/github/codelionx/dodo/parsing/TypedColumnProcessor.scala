package com.github.codelionx.dodo.parsing

import com.univocity.parsers.common.ParsingContext
import com.univocity.parsers.common.processor.RowProcessor

class TypedColumnProcessor extends RowProcessor {

  override def processStarted(context: ParsingContext): Unit = {}

  override def rowProcessed(row: Array[String], context: ParsingContext): Unit = {
    val types = row.map(TypeInferrer.inferType)
    println(types.mkString(" - "))
  }

  override def processEnded(context: ParsingContext): Unit = {}
}