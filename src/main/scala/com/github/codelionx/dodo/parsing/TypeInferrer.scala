package com.github.codelionx.dodo.parsing

import com.github.codelionx.dodo.types._


object TypeInferrer {

  /**
    * Infers the type of the value and returns it as an instance of [[DataType]].
    *
    * @note The value is not transformed! You can do this by using [[DataType.parse]].
    */
  def inferType(value: String): DataType[_ <: Any] = {
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

/**
  * Infers the type from incoming rows (parsed from CSV) for each column.
  */
trait TypeInferrer {

  def inferTypesFromRow(row: Array[String]): Unit

  /**
    * Returns the inferred column types as instance of [[DataType]].
    */
  def columnTypes: Seq[DataType[Any]]

}

/**
  * Type inferrer that iteratively refines the column types each time called. The type order (which type is more
  * specific than another one) is defined in [[com.github.codelionx.dodo.types.DateType$.ordering]].
  *
  * @param numberOfColumns used to initialize the column data type representation
  */
class IterativeTypeInferrer(numberOfColumns: Int) extends TypeInferrer {

  private val types: Array[DataType[Any]] =
    Array.apply((0 until numberOfColumns).map(_ => NullType.asInstanceOf[DataType[Any]]): _*)

  /**
    * @note Columns in a row must arrive in the same order every time!
    */
  override def inferTypesFromRow(row: Array[String]): Unit = {
    val newTypes = row.map(TypeInferrer.inferType)

    for (i <- row.indices) {
      if (types(i) != newTypes(i) && types(i) < newTypes(i)) {
        types(i) = newTypes(i).asInstanceOf[DataType[Any]]
      }
    }
  }

  override def columnTypes: Seq[DataType[Any]] = types
}