package com.github.codelionx.dodo.discovery

import com.github.codelionx.dodo.types.{NullType, TypedColumn}


trait Pruning extends IndexedOrdering {

  /**
    * Returns `true` if the column is constant (has only one distinct value).
    */
  def checkConstant(column: TypedColumn[_ <: Any]): Boolean = {
    column.dataType == NullType || column.distinct.length == 1
  }

  /**
    * Returns `true` if `col1` and `col2` are order equivalent. Means ordering by `col1` orders `col2` and ordering
    * by `col2` orders `col1`.
    */
  def checkOrderEquivalent(col1: TypedColumn[_ <: Any], col2: TypedColumn[_ <: Any]): Boolean = {
    val sortedCol1 = col1.sortedIndices
    val sortedCol2 = col2.sortedIndices
    sortedCol1 sameElements sortedCol2
  }

  def checkOrderDependent(od: (Seq[Int], Seq[Int]), table: Array[TypedColumn[_ <: Any]]): Boolean = {
    val (x, y) = od
    val index = orderedIndicesOf(table, x)

    for (i <- 0 to index.length - 2) {
      for (colIndex <- y) {
        if (!checkOrdering(index(i), index(i + 1), table(colIndex))) {
          return false
        }
      }
    }
    true
  }

  private def checkOrdering(index1: Int, index2: Int, col: TypedColumn[_ <: Any]): Boolean = {
    val ordering = col.dataType.ordering.asInstanceOf[Ordering[Any]]

    val value1 = col(index1)
    val value2 = col(index2)

    if (ordering.lteq(value1, value2))
      true
    else
      false
  }
}
