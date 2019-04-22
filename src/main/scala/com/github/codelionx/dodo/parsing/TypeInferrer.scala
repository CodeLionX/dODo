package com.github.codelionx.dodo.parsing

import com.github.codelionx.dodo.types._


object TypeInferrer {

  def inferType(value: String): DataType = {
    if (NullType.isNull(value))
      return NullType

    val dateChecker = DateType.isDateChecker(value)
    if (dateChecker.isDate) {
      dateChecker.dateType

    } else {
      if (LongType.isLong(value))
        LongType

      else if (DoubleType.isDouble(value))
        DoubleType

      else
        StringType

    }
  }

}

trait TypeInferrer {

  def refreshTypesFromRow(row: Array[String]): Unit

  def columnTypes: Seq[DataType]

}

class IterativeTypeInferrer(numberOfColumns: Int) extends TypeInferrer {

  private val types: Array[DataType] = Array.apply( (0 until numberOfColumns).map(_ => NullType): _* )

  /**
    * Columns in a row must arrive in the same order every time!
    *
    * @param row
    */
  override def refreshTypesFromRow(row: Array[String]): Unit = {
    val newTypes = row.map(TypeInferrer.inferType)

    for(i <- row.indices) {
      if(types(i) != StringType && types(i) < newTypes(i)) {
        types(i) = newTypes(i)
      }
    }
  }

  override def columnTypes: Seq[DataType] = types
}