package com.github.codelionx.dodo.discovery

import com.github.codelionx.dodo.types.{LongType, StringType, TypedColumn, TypedColumnBuilder}
import org.scalatest.{Matchers, WordSpec}


class OrderingIndexerSpec extends WordSpec with Matchers {

  "The OrderingIndexer" should {

    "order a dataset" in {
      val dataset: Array[TypedColumn[_ <: Any]] = Array(
        TypedColumnBuilder.from("C", "A", "C", "Z", "C"),
        TypedColumnBuilder.from(8L, 20L, 5L, 24L, 5L),
        TypedColumnBuilder.from("e", "d", "b", "c", "a")
      )
      OrderingIndexer.orderedIndices(dataset) shouldEqual Seq(1, 4, 2, 0, 3)
    }
  }
}
