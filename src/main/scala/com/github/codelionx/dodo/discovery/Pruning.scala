package com.github.codelionx.dodo.discovery

import com.github.codelionx.dodo.types.{NullType, TypedColumn}


trait Pruning {

  def checkConstant (column: TypedColumn[Any]): Boolean = {
    column.dataType == NullType || column.distinct.length == 1
  }


  def checkOrderEquivalent (col1: TypedColumn[Any], col2: TypedColumn[Any]): Boolean = {
//    val indexCol1 = col1.zipWithIndex.toSeq
//    val sortedCol1 = indexCol1.sorted.map(_._2)
//
//    // potential for later optimization
//    // iterate over col2 instead of sorting
//    val indexCol2 = col2.zipWithIndex.toSeq
//    val sortedCol2 = indexCol2.sorted.map(_._2)
//
//    sortedCol1.equals(sortedCol2)
    false
  }


  def checkOrderDependent (od: (List[Int], List[Int]), table: Array[TypedColumn[Any]]): Boolean = {
    true
  }
}
