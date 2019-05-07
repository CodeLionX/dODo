package com.github.codelionx.dodo.types

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


object TypedColumnBuilder {

  /**
    * Creates a new [[com.github.codelionx.dodo.types.TypedColumnBuilder]] from the `dataType`.
    *
    * @param dataType specifies the column's type
    */
  def apply[T <: Any : ClassTag](dataType: DataType[T]): TypedColumnBuilder[T] = new TypedColumnBuilder(dataType)

}

/**
  * Creates a [[com.github.codelionx.dodo.types.TypedColumn]] instance by iteratively calling
  * [[com.github.codelionx.dodo.types.TypedColumnBuilder#append]] with a cell values as [[String]].
  *
  * @param dataType defines the column's data type and how the cells are parsed
  */
final class TypedColumnBuilder[T <: Any : ClassTag] private(dataType: DataType[T]) {

  private val buffer: ArrayBuffer[T] = ArrayBuffer.empty

  def clear(): Unit = buffer.clear()

  /**
    * Returns the [[com.github.codelionx.dodo.types.TypedColumn]] instance with all the parsed cell data.
    */
  def toTypedColumn: TypedColumn[T] = TypedColumnImpl(dataType, buffer.toArray)

  def toArray: Array[T] = buffer.toArray

  /**
    * Parses and adds the elements to this column in order.
    */
  def append(elems: String*): Unit = buffer.append(elems.map(dataType.parse): _*)

  /**
    * Add a single element to this builder.
    */
  def +=(elem: T): TypedColumnBuilder.this.type = {
    buffer += elem
    thi
  }

  private case class TypedColumnImpl(dataType: DataType[T], array: Array[T]) extends TypedColumn[T]

}
