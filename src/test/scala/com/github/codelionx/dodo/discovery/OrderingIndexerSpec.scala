package com.github.codelionx.dodo.discovery

import com.github.codelionx.dodo.types.{LongType, StringType, TypedColumn, TypedColumnBuilder}
import org.scalatest.{Matchers, WordSpec}


class OrderingIndexerSpec extends WordSpec with Matchers {

  "The OrderingIndexer" should {
    "order a dataset" in {
      val dataset: Array[TypedColumn[_ <: Any]] = Array(
        (TypedColumnBuilder(StringType)
          += "C" += "A" += "C" += "Z" += "C"
          ).toTypedColumn,
        (TypedColumnBuilder(LongType)
          += 8 += 20 += 5 += 24 += 5
          ).toTypedColumn,
        (TypedColumnBuilder(StringType)
          += "e" += "d" += "b" += "c" += "a"
          ).toTypedColumn
      )
      OrderingIndexer.orderedIndices(dataset) shouldEqual Seq(1, 4, 2, 0, 3)
    }
  }
}
