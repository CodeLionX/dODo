package com.github.codelionx.dodo.discovery

import com.github.codelionx.dodo.types.{NullType, TypedColumn}


trait Pruning {

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

  def checkOrderDependent (od: (List[Int], List[Int]), table: Array[TypedColumn[Any]]): Boolean = {
    true
  }
}
