package com.github.codelionx.dodo.types

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


object TypedColumnBuilder {

  def apply[T <: Any : ClassTag](dataType: DataType[T]): TypedColumnBuilder[T] = new TypedColumnBuilder(dataType)

}

final class TypedColumnBuilder[T <: Any : ClassTag] private(dataType: DataType[T]) {

  private val buffer: ArrayBuffer[T] = ArrayBuffer.empty

  def toTypedColumn: TypedColumn[T] = new TypedColumnImpl[T](dataType, buffer.toArray)

  def toArray: Array[T] = buffer.toArray

  def append(elems: String*): Unit = buffer.append(elems.map(dataType.parse): _*)

  private class TypedColumnImpl(val dataType: DataType[T], arr: Array[T]) extends TypedColumn[T] {

    def toArray: Array[T] = arr

    def apply(index: Int): T = arr(index)
  }
}
