package com.github.codelionx.dodo.types

import scala.collection.mutable
import scala.reflect.ClassTag

object TypedColumn {
  implicit class OrderedTypedColumn[T](col: TypedColumn[T]) {
    def ordering: Ordering[TypedColumn[T]] = col.dataType.ordering
  }

//  implicit def ordering[T, A <: TypedColumn[T]](implicit ev: O)
}

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
  def array: Array[T]
}

/**
  * Represents a column of a dataset associated with a specific type that provides a rich interface to manipulate and
  * access the column's data.
  *
  * @tparam T the data type
  */
trait TypedColumn[T <: Any]
  extends TypedColumnBase[T]
    with TypedColumnArrayLike[T]
    with TypedColumnSeqLike[T, TypedColumn[T]] {

  override protected def newBuilder: mutable.Builder[T, TypedColumn[T]] = new BuilderAdapter

  override def toString: String =
    s"""|Column of $dataType:
        |-------------------------------
        |${mkString(",")}
        |""".stripMargin

  /**
    * Adapter class to bridge between our buffer-based [[com.github.codelionx.dodo.types.TypedColumnBuilder]] and the
    * [[scala.collection.mutable.Builder]] interface
    */
  private final class BuilderAdapter extends mutable.ReusableBuilder[T, TypedColumn[T]] {

    private val internalBuilder = TypedColumnBuilder[T](dataType)(tag)

    override def clear(): Unit = internalBuilder.clear()

    override def result(): TypedColumn[T] = internalBuilder.toTypedColumn

    override def +=(elem: T): BuilderAdapter.this.type = {
      internalBuilder += elem
      this
    }
  }

}
