package com.github.codelionx.dodo.discovery

import com.github.codelionx.dodo.types.{LongType, StringType, TypedColumn, TypedColumnBuilder}
import org.scalatest.{Matchers, WordSpec}


class IndexedOrderingSpec extends WordSpec with Matchers {

  object IndexedOrderingTester extends IndexedOrdering

  "The IndexedOrdering" should {

    "order a dataset" in {
      val dataset: Array[TypedColumn[_ <: Any]] = Array(
        TypedColumnBuilder.from("C", "A", "C", "Z", "C"),
        TypedColumnBuilder.from(8L, 20L, 5L, 24L, 5L),
        TypedColumnBuilder.from("e", "d", "b", "c", "a")
      )
      IndexedOrderingTester.orderedIndices(dataset) shouldEqual Seq(1, 4, 2, 0, 3)
    }

    "order a single column in a dataset" in {
      val dataset: Array[TypedColumn[_ <: Any]] = Array(
        TypedColumnBuilder.from(12.5, 0.01, 123.1, 14.2, 0.45)
      )
      IndexedOrderingTester.orderedIndices(dataset) shouldEqual Seq(1, 4, 0, 3, 2)
    }
  }
}
