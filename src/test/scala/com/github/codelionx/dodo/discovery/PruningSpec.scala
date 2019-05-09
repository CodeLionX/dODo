package com.github.codelionx.dodo.discovery

import java.time.ZonedDateTime

import com.github.codelionx.dodo.types.{DateType, TypedColumn, TypedColumnBuilder}
import org.scalatest.{Matchers, WordSpec}


class PruningSpec extends WordSpec with Matchers {

  object PruningTester extends Pruning

  "The Pruning" should {

    "find a constant column of long type" in {
      val variableColumn = TypedColumnBuilder.from[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
      val constantColumn = TypedColumnBuilder.from[Long](1, 1, 1, 1, 1, 1, 1, 1, 1)

      PruningTester.checkConstant(variableColumn) shouldBe false
      PruningTester.checkConstant(constantColumn) shouldBe true
    }

    "find a constant column of double type" in {
      val variableColumn = TypedColumnBuilder.from(.1, .2, .3, .4, .5, .6, .7, .8, .9)
      val constantColumn = TypedColumnBuilder.from(.1, .1, .1, .1, .1, .1, .1, .1, .1)
      val nearlyConstantColumn = TypedColumnBuilder.from(.000000001, .000000002)

      PruningTester.checkConstant(variableColumn) shouldBe false
      PruningTester.checkConstant(constantColumn) shouldBe true
      PruningTester.checkConstant(nearlyConstantColumn) shouldBe false
    }

    "find a constant column of string type" in {
      val variableColumn = TypedColumnBuilder.from("a", "a", "b", "a", "c")
      val constantColumn = TypedColumnBuilder.from("a", "a", "a", "a", "a")

      PruningTester.checkConstant(variableColumn) shouldBe false
      PruningTester.checkConstant(constantColumn) shouldBe true
    }

    "find a constant column of ZonedDateTime type" in {
      val now = ZonedDateTime.now()
      val variableColumn = TypedColumnBuilder.from(now, ZonedDateTime.from(now), now.minusDays(1), now.plusNanos(12345))
      val constantColumn = TypedColumnBuilder.from(now, ZonedDateTime.from(now), now)

      PruningTester.checkConstant(variableColumn) shouldBe false
      PruningTester.checkConstant(constantColumn) shouldBe true
    }

    "find a constant column of mixed ZonedDateTime formats" in {
      val date1 = "2019-05-09T12:22:04+02:00"
      val date2 = "Thu, 9 May 2019 12:22:04 +0200"
      val parsed1 = DateType.dateChecker(date1).dateType.parse(date1).asInstanceOf[ZonedDateTime]
      val parsed2 = DateType.dateChecker(date2).dateType.parse(date2).asInstanceOf[ZonedDateTime]

      val mixedFormatColumn = TypedColumnBuilder.from[ZonedDateTime](parsed1, parsed2)
      PruningTester.checkConstant(mixedFormatColumn) shouldBe true
    }

    "check for order equivalence" in {
      val col1 = TypedColumnBuilder.from("C", "A", "B", "Z", "C")

      val col2 = TypedColumnBuilder.from(10L, 5L, 8L, 24L, 11L)
      val col3 = TypedColumnBuilder.from(.11, .5, .8, .24, .10)
      val col4 = TypedColumnBuilder.from("x", "a", "c", "d", "a")

      PruningTester.checkOrderEquivalent(col1, col2) shouldEqual true
      PruningTester.checkOrderEquivalent(col1, col3) shouldEqual false
      PruningTester.checkOrderEquivalent(col1, col4) shouldEqual false
    }
  }
}
