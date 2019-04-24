package com.github.codelionx.dodo.types


/**
  * Represents a column of a dataset associated with a specific type. The cell data is stored in the correct
  * (primitive) type.
  *
  * @tparam T the data type
  */
trait TypedColumn[T <: Any] {

  def dataType: DataType[T]

  def toArray: Array[T]

  def apply(index: Int): T

}
