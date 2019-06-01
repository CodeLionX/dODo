package com.github.codelionx.dodo.discovery

import com.github.codelionx.dodo.types.{NullType, TypedColumn}


trait DependencyChecking extends IndexedOrdering {

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
    if (sortedCol1 sameElements sortedCol2) {
      for (i <- 0 to sortedCol1.length - 2) {
        if ((col1(sortedCol1(i)) == col1(sortedCol1(i + 1)))
          != (col2(sortedCol2(i)) == col2(sortedCol2(i + 1)))) {
          return false
        }
      }
      true
    } else {
      false
    }
  }

  /**
    * Checks if the left-hand-side list of attributes of the `od` sorts the right-hand-side list of attributes for the
    * supplied `table`.
    *
    * @return `true` if the order dependency holds, `false` otherwise
    */
  def checkOrderDependent(od: (Seq[Int], Seq[Int]), table: Array[TypedColumn[_ <: Any]]): Boolean = {
    val (x, y) = od
    val index = orderedIndicesOf(table, x)

    for (i <- 0 to index.length - 2) {
      val index1 = index(i)
      val index2 = index(i + 1)
      val compRight = checkTupleOrdering(y, table, index1, index2)
      if (compRight > 0) {
        // swap
        return false
      } else {
        val comp2 = checkTupleOrdering(x, table, index1, index2)
        if (comp2 == 0 && compRight != 0) {
          // split
          return false
        }
      }
    }
    true
  }

  @inline
  private def checkTupleOrdering(y: Seq[Int], table: Array[TypedColumn[_ <: Any]], index1: Int, index2: Int): Int = {
    for (columnIndex <- y) {
      val column = table(columnIndex)
      val ordering = column.dataType.ordering.asInstanceOf[Ordering[Option[Any]]]

      val value1 = column(index1)
      val value2 = column(index2)

      val comp = ordering.compare(value1, value2)

      if (comp != 0) {
        return comp
      }
    }
    0
  }
}
