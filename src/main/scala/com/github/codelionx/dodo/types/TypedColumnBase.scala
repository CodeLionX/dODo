package com.github.codelionx.dodo.types

import scala.reflect.ClassTag


/**
  * Represents a column of a dataset associated with a specific type. The cell data is stored in the correct
  * (primitive) type.
  *
  * @tparam T the data type
  */
abstract class TypedColumnBase[T <: Any](implicit ev: ClassTag[T]) {

  protected val tag: ClassTag[T] = ev

  /**
    * Returns the [[com.github.codelionx.dodo.types.DataType]] associated with this column.
    */
  def dataType: DataType[T]

  /**
    * Returns the backing array of this `TypedColumn`.
    */
  def array: Array[Option[T]]

  /**
    * Column name
    */
  def name: String
}