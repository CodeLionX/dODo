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
}

trait TypedColumn[T <: Any] {

  def toArray: Array[T]

  def apply(index: Int): T
}

private class TypedColumnImpl[T <: Any : ClassTag](val dataType: DataType[T], arr: Array[T]) extends TypedColumn[T] {

  def toArray: Array[T] = arr

  def apply(index: Int): T = arr(index)
}


//  trait TypedColumnBuffer extends TypedColumn {
//    type T <: Any
//
//    def toTypedColumn: TypedColumn
//
//    def apply(index: Int): T
//
//    def append[U <: T](elems: U*): Unit
//  }
//
//  final class StringColumnBuffer extends TypedColumnBuffer {
//    override type T = String
//
//    private val buffer: ArrayBuffer[String] = ArrayBuffer.empty
//
//    override def toTypedColumn: TypedColumn = new StringColumn(buffer.toArray)
//
//    override def backingArray: Array[String] = buffer.toArray
//
//    override def apply(index: Int): String = buffer(index)
//
//    override def append[U <: T](elems: U*): Unit = buffer.append(elems: _*)
//  }
//
//  def bufferFromDataType[V <: Any: ClassTag](t: DataType[V]): TypedColumnBuilder[V] = t match {
//    case NullType => StringColumnBuffer.empty
//    case StringType => StringColumnBuffer.empty
//    case _ => new TypedColumnBuilder[V]
//    case LongType => ArrayBuffer.empty[Long]
//    case DoubleType => ArrayBuffer.empty[Double]
//    case LocalDateType(_) => ArrayBuffer.empty[LocalDateTime]
//    case ZonedDateType(_) => ArrayBuffer.empty[ZonedDateTime]
//  }
//
//  def fromDataType(t: DataType): TypedColumn = t match {
//    case NullType => StringColumn.empty
//    case StringType => StringColumn.empty
//    case LongType => LongColumn.empty
//    case DoubleType => DoubleColumn.empty
//    case LocalDateType(_) => LocalDateColumn.empty
//    case ZonedDateType(_) => ZonedDateColumn.empty
//  }
//
//  final class StringColumn private (arr: Array[String]) extends TypedColumn {
//    override type T = String
//
//    override def backingArray: Array[String] = arr
//
//    override def apply(index: Int): String = arr(index)
//  }
//
//  final class StringColumnBuffer extends TypedColumnBuffer {
//    override type T = String
//
//    private val buffer: ArrayBuffer[String] = ArrayBuffer.empty
////    private val dataType: DataType = StringType
//
//    override def toTypedColumn: TypedColumn = new StringColumn(buffer.toArray)
//
//    override def backingArray: Array[String] = buffer.toArray
//
//    override def apply(index: Int): String = buffer(index)
//
//    override def append[U <: T](elems: U*): Unit = buffer.append(elems: _*)
//
////    def append(elems: )
//  }
//
//  object StringColumn {
//    val empty: StringColumn = new StringColumn(Array.empty)
//  }
//
//  object StringColumnBuffer {
//    val empty: StringColumnBuffer = new StringColumnBuffer
//  }
//
//  type LongColumn = Array[Long]
//
//  object LongColumn {
//    val empty: LongColumn = Array.empty[Long]
//  }
//
//  type DoubleColumn = Array[Double]
//
//  object DoubleColumn {
//    val empty: DoubleColumn = Array.empty[Double]
//  }
//
//  type LocalDateColumn = Array[LocalDateTime]
//
//  object LocalDateColumn {
//    def empty: LocalDateColumn = Array.empty[LocalDateTime]
//  }
//
//  type ZonedDateColumn = Array[ZonedDateTime]
//
//  object ZonedDateColumn {
//    def empty: ZonedDateColumn = Array.empty[ZonedDateTime]
//  }
//}