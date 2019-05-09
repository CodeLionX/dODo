package com.github.codelionx.dodo.discovery

import com.github.codelionx.dodo.types.{NullType, TypedColumn}


trait Pruning {

  def checkConstant (column: TypedColumn[Any]): Boolean = {
    column.dataType == NullType || column.distinct.length == 1
  }

  def checkOrderEquivalent (col1: TypedColumn[Any], col2: TypedColumn[Any]): Boolean = {
    val sortedCol1 = col1.sortedIndices
    val sortedCol2 = col2.sortedIndices
    sortedCol1 sameElements sortedCol2
  }
}
