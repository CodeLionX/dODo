package com.github.codelionx.dodo.discovery

import com.github.codelionx.dodo.types.{DataType, TypedColumn}

import scala.collection.mutable


trait IndexedOrdering {

  /**
    * Returns the original indices of the dataset in the order when sorting by the supplied columns lexicographically.
    */
  def orderedIndicesOf(dataset: Array[TypedColumn[_ <: Any]], sortingColumns: Seq[Int]): Seq[Int] = {
    if (sortingColumns.length <= 0 || dataset.length <= 0)
      Seq.empty
    else if (sortingColumns.length == 1)
      dataset(sortingColumns.head).sortedIndices
    else if (sortingColumns.max >= dataset.length)
      throw new IllegalArgumentException(
        "Can not sort by column indices that are greater than the number of columns in the dataset"
      )
    else {
      val bufferView = mutable.Buffer.empty[TypedColumn[_ <: Any]]
      for (column <- sortingColumns) {
        bufferView += dataset(column)
      }
      recSorting(bufferView.toArray, dataset.head.indices, 0)
    }
  }

  /**
    * Returns the original indices of the dataset in the order when sorting it lexicographically on all columns.
    */
  def orderedIndices(dataset: Array[TypedColumn[_ <: Any]]): Seq[Int] =
    if (dataset.length <= 0)
      Seq.empty
    else if (dataset.length == 1)
      dataset.head.sortedIndices
    else
      recSorting(dataset, dataset.head.indices, 0)

  private def recSorting(dataset: Array[TypedColumn[_ <: Any]], sortIndices: Seq[Int], colIndex: Int): Seq[Int] = {
    val column = dataset(colIndex)
    val slice = extractIndexedSlice(column, sortIndices)
    val sortedSlice = sort(slice, column.dataType)

    if (colIndex < dataset.length - 1) {
      val ranges = constantRanges(sortedSlice)

      // sort those constant ranges by next column
      for (indexRange <- ranges) {
        val currentView = sortedSlice.view(indexRange.start, indexRange.end).map(_._2)
        val updatedSlice = recSorting(dataset, currentView, colIndex + 1)
        for (j <- updatedSlice.indices) {
          val index = indexRange(j)
          sortedSlice(index) = sortedSlice(index)._1 -> updatedSlice(j)
        }
      }
    }
    sortedSlice.map(_._2)
  }

  @inline
  private def extractIndexedSlice(column: TypedColumn[_ <: Any], sortIndices: Seq[Int]): Array[(Any, Int)] = {
    if (column.length == sortIndices.length)
      return column.zipWithIndex.toArray

    val indexedSlice = Array.ofDim[(Any, Int)](sortIndices.length)
    for (i <- sortIndices.indices) {
      val index = sortIndices(i)
      indexedSlice(i) = column(index) -> index
    }
    indexedSlice
  }

  @inline
  private def sort(slice: Array[(Any, Int)], dataType: DataType[_]): Array[(Any, Int)] = {
    // work around scala implicit search for an Ordering[Any] ;)
    implicit val tOrdering: Ordering[Any] = dataType.ordering.asInstanceOf[Ordering[Any]]
    slice.sorted
  }

  @inline
  private def constantRanges(sortedSlice: Array[(Any, Int)]): Seq[Range] = {
    val ranges = mutable.Buffer.empty[Range]
    var start = 0
    for (i <- 1 until sortedSlice.length) {
      if (!(sortedSlice(start)._1 equals sortedSlice(i)._1)) {
        if (start != i - 1) {
          ranges += start until i
        }
        start = i
      }
    }
    ranges
  }

}
