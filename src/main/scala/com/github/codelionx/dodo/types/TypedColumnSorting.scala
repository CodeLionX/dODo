package com.github.codelionx.dodo.types

import scala.collection.mutable


trait TypedColumnSorting[T, +Repr] {
  self: TypedColumnBase[T] =>

  /**
    * Sorts this column zipped with the array indices according to the ordering defined in the data type.
    */
  def sortedWithOwnIndices: Array[(T, Int)] = {
    implicit val tOrdering: Ordering[T] = dataType.ordering
    val withIndices = array.zipWithIndex
    withIndices.sorted
  }

  def sortedIndices: Array[Int] = {
    sortedWithOwnIndices.map(_._2)
  }

  /**
    * Sorts this column based on the ordering defined in the data type.
    */
  def sorted: Repr = {
    implicit val tOrdering: Ordering[T] = dataType.ordering
    val sorted = array.sorted
    val builder = newBuilder

    if (sorted.length == 1)
      builder ++= sorted
    else {
      builder.sizeHint(sorted.length)
      val i = 0
      while (i < sorted.length) {
        builder += sorted(i)
      }
    }

    builder.result()
  }

  protected def newBuilder: mutable.Builder[T, Repr]
}
