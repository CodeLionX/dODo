package com.github.codelionx.dodo.types

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


object TypedColumnBuilder {

  private val DEFAULT_COLUMN_NAME = "UNKNOWN"

  /**
    * Creates a new [[com.github.codelionx.dodo.types.TypedColumnBuilder]] from the `dataType`.
    *
    * @param dataType specifies the column's type
    */
  def apply[T <: Any : ClassTag](dataType: DataType[T]): TypedColumnBuilder[T] = apply(dataType, DEFAULT_COLUMN_NAME)

  /**
    * Creates a new [[com.github.codelionx.dodo.types.TypedColumnBuilder]] from the `dataType` and `name`.
    *
    * @param dataType specifies the column's type
    * @param name specifie sthe column's name
    */
  def apply[T <: Any : ClassTag](dataType: DataType[T], name: String): TypedColumnBuilder[T] = new TypedColumnBuilder(dataType, name)

  /**
    * Creates a new [[com.github.codelionx.dodo.types.TypedColumn]] from the supplied `elems` parsing them to the
    * supplied `dataType`.
    *
    * @param dataType specifies the column's type
    * @param elems content of the column, will be parsed to the `dataType`
    */
  def withType[T <: Any](dataType: DataType[T])(elems: String*)(implicit ev: ClassTag[T]): TypedColumn[T] =
    withType(dataType, DEFAULT_COLUMN_NAME)(elems: _*)

  /**
    * Creates a new [[com.github.codelionx.dodo.types.TypedColumn]] from the supplied `elems` parsing them to the
    * supplied `dataType`.
    *
    * @param dataType specifies the column's type
    * @param name specifies name of the column
    * @param elems content of the column, will be parsed to the `dataType`
    */
  def withType[T <: Any](dataType: DataType[T], name: String)(elems: String*)(implicit ev: ClassTag[T]): TypedColumn[T] = {
    val builder = new TypedColumnBuilder(dataType, name)
    builder.append(elems: _*)
    builder.toTypedColumn
  }

  /**
    * Creates a new [[com.github.codelionx.dodo.types.TypedColumn]] from the supplied `elems`.
    *
    * @param elems content of the column
    */
  def from[T <: Any](elems: T*)(implicit ev: ClassTag[T]): TypedColumn[T] =
    from(DEFAULT_COLUMN_NAME)(elems: _*)

  /**
    * Creates a new [[com.github.codelionx.dodo.types.TypedColumn]] from the supplied `elems` with the name `name`.
    *
    * @param name specifies name of the column
    * @param elems content of the column
    */
  def from[T <: Any](name: String)(elems: T*)(implicit ev: ClassTag[T]): TypedColumn[T] = {
    val tpe = DataType.of[T]
    val builder = new TypedColumnBuilder[T](tpe, name)
    elems.foreach{ elem =>
      builder += elem
    }
    builder.toTypedColumn
  }

}

/**
  * Creates a [[com.github.codelionx.dodo.types.TypedColumn]] instance by iteratively calling
  * [[com.github.codelionx.dodo.types.TypedColumnBuilder#append]] with a cell values as [[String]].
  *
  * @param dataType defines the column's data type and how the cells are parsed
  */
final class TypedColumnBuilder[T <: Any : ClassTag] private(dataType: DataType[T], var name: String) {

  private val buffer: ArrayBuffer[Option[T]] = ArrayBuffer.empty

  def clear(): Unit = buffer.clear()

  /**
    * Returns the [[com.github.codelionx.dodo.types.TypedColumn]] instance with all the parsed cell data.
    */
  def toTypedColumn: TypedColumn[T] = TypedColumnImpl(dataType, name, buffer.toArray)

  def toArray: Array[Option[T]] = buffer.toArray

  /**
    * Parses and adds the elements to this column in order.
    */
  def append(elems: String*): Unit = buffer.append(elems.map(dataType.parse): _*)

  /**
    * Add a single element to this builder.
    */
  def +=(elem: T): TypedColumnBuilder.this.type = this += Some(elem)

  /**
    * Add a single element to this builder.
    */
  def +=(elem: Option[T]): TypedColumnBuilder.this.type = {
    buffer += elem
    this
  }

  /**
    * Set name of this column.
    *
    * Allows method chaining.
    */
  def withName(name: String): TypedColumnBuilder.this.type = {
    this.name = name
    this
  }

  private case class TypedColumnImpl(dataType: DataType[T], name: String, array: Array[Option[T]]) extends TypedColumn[T]

}
