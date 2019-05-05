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

  /**
    * Returns the [[com.github.codelionx.dodo.types.TypedColumn]] instance with all the parsed cell data.
    */
  def toTypedColumn: TypedColumn[T] = TypedColumnImpl(dataType, buffer.toArray)

  def toArray: Array[T] = buffer.toArray

  /**
    * Parses and adds the elements to this column in order.
    */
  def append(elems: String*): Unit = buffer.append(elems.map(dataType.parse): _*)

  private case class TypedColumnImpl(dataType: DataType[T], arr: Array[T]) extends TypedColumn[T] {

    override def toArray: Array[T] = arr

    override def apply(i: Int): T = arr.apply(i)

    override def update(i: Int, x: T): Unit = arr.update(i, x)

    override def toString: String =
      s"""|Column of $dataType:
          |-------------------------------
          |${arr.mkString(",")}
          |""".stripMargin
  }

}
