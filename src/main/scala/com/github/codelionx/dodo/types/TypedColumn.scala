package com.github.codelionx.dodo.types

import java.util.Objects

import com.github.codelionx.dodo.types.TypedColumn.BuilderAdapter

import scala.collection.mutable


object TypedColumn {

  /**
    * Adapter class to bridge between our buffer-based [[com.github.codelionx.dodo.types.TypedColumnBuilder]] and the
    * [[scala.collection.mutable.Builder]] interface
    */
  private final class BuilderAdapter[T](ref: TypedColumn[T]) extends mutable.ReusableBuilder[T, TypedColumn[T]] {

    private val internalBuilder = TypedColumnBuilder[T](ref.dataType, ref.name)(ref.tag)

    override def clear(): Unit = internalBuilder.clear()

    override def result(): TypedColumn[T] = internalBuilder.toTypedColumn

    override def +=(elem: T): BuilderAdapter.this.type = {
      internalBuilder += elem
      this
    }
  }

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
    with TypedColumnSeqLike[T, TypedColumn[T]]
    with TypedColumnSorting[T, TypedColumn[T]]
    with Serializable {

  override protected def newBuilder: mutable.Builder[T, TypedColumn[T]] = new BuilderAdapter(this)


  // overrides of [[java.lang.Object]]

  override def hashCode(): Int = Objects.hash(dataType, name, array.toSeq)

  override def canEqual(o: Any): Boolean = o.isInstanceOf[TypedColumn[T]]

  override def equals(o: Any): Boolean = o match {
    case o: TypedColumn[T] => o.canEqual(this) && this.hashCode() == o.hashCode()
    case _ => false
  }

  override def toString: String =
    s"""|Column of $dataType:
        |-------------------------------
        |${mkString(",")}
        |""".stripMargin

}
