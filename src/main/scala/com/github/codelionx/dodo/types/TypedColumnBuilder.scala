package com.github.codelionx.dodo.types

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


object TypedColumnBuilder {

  /**
    * Creates a new [[TypedColumnBuilder]] from the `dataType`.
    *
    * @param dataType specifies the column's type
    */
  def apply[T <: Any : ClassTag](dataType: DataType[T]): TypedColumnBuilder[T] = new TypedColumnBuilder(dataType)

}

/**
  * Creates a [[com.github.codelionx.dodo.types.TypedColumn]] instance by iteratively calling
  * [[TypedColumnBuilder#append]] with a cell values as [[String]].
  *
  * @param dataType defines the column's data type and how the cells are parsed
  */
final class TypedColumnBuilder[T <: Any : ClassTag] private(dataType: DataType[T]) {

  private val buffer: ArrayBuffer[T] = ArrayBuffer.empty

  /**
    * Returns the [[com.github.codelionx.dodo.types.TypedColumn]] instance with all the parsed cell data.
    */
  def toTypedColumn: TypedColumn[T] = new TypedColumnImpl(dataType, buffer.toArray)

  def toArray: Array[T] = buffer.toArray

  /**
    * Parses and adds the elements to this column in order.
    */
  def append(elems: String*): Unit = buffer.append(elems.map(dataType.parse): _*)

  private case class TypedColumnImpl(dataType: DataType[T], arr: Array[T]) extends TypedColumn[T] {

    def toArray: Array[T] = arr

    def apply(index: Int): T = arr(index)
  }

}
