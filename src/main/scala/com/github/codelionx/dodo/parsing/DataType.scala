package com.github.codelionx.dodo.parsing

// potential optimization point by using java enum (4x less code)

sealed trait DataType

case object DoubleType extends DataType
case object LongType extends DataType
case object DateType extends DataType
case object StringType extends DataType
