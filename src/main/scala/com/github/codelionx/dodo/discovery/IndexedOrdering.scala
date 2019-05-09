package com.github.codelionx.dodo.discovery

import com.github.codelionx.dodo.types.{DataType, TypedColumn}


object OrderingIndexer {

  def orderedIndices(dataset: Array[TypedColumn[_ <: Any]]): Seq[Int] =
    if (dataset.length == 1)
      dataset.head.sortedWithOwnIndices.map(_._2)
    else
      recSorting(dataset, dataset.head.indices, 0)

  private def recSorting(dataset: Array[TypedColumn[_ <: Any]], sortIndices: Seq[Int], colIndex: Int): Seq[Int] = {
    // get new values from dataset of the current slice
    val indexedCol = Array.ofDim[(Any, Int)](sortIndices.length)
    val column = dataset(colIndex)
    for (i <- sortIndices.indices) {
      val index = sortIndices(i)
      indexedCol(i) = column(index) -> index
    }

    // sort slice according to this column
    val sortedIndexedCol = sort(indexedCol, column.dataType)

    if (colIndex < dataset.length) {
      // check for constants
      var start = 0
      for (i <- 1 until sortedIndexedCol.length) {
        if (!sortedIndexedCol(start)._1.equals(sortedIndexedCol(i)._1)) {
          if (start != i - 1) {
            val indexRange = start until i
            val currentView = sortedIndexedCol.view(start, i).map(_._2)
            val updatedSlice = recSorting(dataset, currentView, colIndex + 1)
            for (j <- updatedSlice.indices) {
              val index = indexRange(j)
              sortedIndexedCol(index) = sortedIndexedCol(index)._1 -> updatedSlice(j)
            }
          }
          start = i
        }
      }
    }
    sortedIndexedCol.map(_._2)
  }

  @inline
  private def sort(colIndices: Array[(Any, Int)], dataType: DataType[_]): Array[(Any, Int)] = {
    // work around scala implicit search for an Ordering[Any] ;)
    implicit val tOrdering: Ordering[Any] = dataType.ordering.asInstanceOf[Ordering[Any]]
    colIndices.sorted
  }
}
