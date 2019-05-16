package com.github.codelionx.dodo.discovery

import java.time.ZonedDateTime

import com.github.codelionx.dodo.types.{DateType, TypedColumn, TypedColumnBuilder}
import org.scalatest.{Matchers, WordSpec}


class DependencyCheckingSpec extends WordSpec with Matchers {

  object DependencyCheckingTester extends DependencyChecking

  "The DependencyChecking" should {

    "find a constant column of long type" in {
      val variableColumn = TypedColumnBuilder.from[Long](1, 2, 3, 4, 5, 6, 7, 8, 9)
      val constantColumn = TypedColumnBuilder.from[Long](1, 1, 1, 1, 1, 1, 1, 1, 1)

      DependencyCheckingTester.checkConstant(variableColumn) shouldBe false
      DependencyCheckingTester.checkConstant(constantColumn) shouldBe true
    }

    "find a constant column of double type" in {
      val variableColumn = TypedColumnBuilder.from(.1, .2, .3, .4, .5, .6, .7, .8, .9)
      val constantColumn = TypedColumnBuilder.from(.1, .1, .1, .1, .1, .1, .1, .1, .1)
      val nearlyConstantColumn = TypedColumnBuilder.from(.000000001, .000000002)

      DependencyCheckingTester.checkConstant(variableColumn) shouldBe false
      DependencyCheckingTester.checkConstant(constantColumn) shouldBe true
      DependencyCheckingTester.checkConstant(nearlyConstantColumn) shouldBe false
    }

    "find a constant column of string type" in {
      val variableColumn = TypedColumnBuilder.from("a", "a", "b", "a", "c")
      val constantColumn = TypedColumnBuilder.from("a", "a", "a", "a", "a")

      DependencyCheckingTester.checkConstant(variableColumn) shouldBe false
      DependencyCheckingTester.checkConstant(constantColumn) shouldBe true
    }

    "find a constant column of ZonedDateTime type" in {
      val now = ZonedDateTime.now()
      val variableColumn = TypedColumnBuilder.from(now, ZonedDateTime.from(now), now.minusDays(1), now.plusNanos(12345))
      val constantColumn = TypedColumnBuilder.from(now, ZonedDateTime.from(now), now)

      DependencyCheckingTester.checkConstant(variableColumn) shouldBe false
      DependencyCheckingTester.checkConstant(constantColumn) shouldBe true
    }

    "find a constant column of mixed ZonedDateTime formats" in {
      val date1 = "2019-05-09T12:22:04+02:00"
      val date2 = "Thu, 9 May 2019 12:22:04 +0200"
      val parsed1 = DateType.dateChecker(date1).dateType.parse(date1).asInstanceOf[ZonedDateTime]
      val parsed2 = DateType.dateChecker(date2).dateType.parse(date2).asInstanceOf[ZonedDateTime]

      val mixedFormatColumn = TypedColumnBuilder.from[ZonedDateTime](parsed1, parsed2)
      DependencyCheckingTester.checkConstant(mixedFormatColumn) shouldBe true
    }

    "check for order equivalence" in {
      val col1 = TypedColumnBuilder.from("C", "A", "B", "Z", "C")

      val col2 = TypedColumnBuilder.from(10L, 5L, 8L, 24L, 11L)
      val col3 = TypedColumnBuilder.from(.11, .5, .8, .24, .10)
      val col4 = TypedColumnBuilder.from("x", "a", "c", "d", "a")

      DependencyCheckingTester.checkOrderEquivalent(col1, col2) shouldEqual true
      DependencyCheckingTester.checkOrderEquivalent(col1, col3) shouldEqual false
      DependencyCheckingTester.checkOrderEquivalent(col1, col4) shouldEqual false
    }

    "check for order dependencies of lists with only one element" in {
      val dataset: Array[TypedColumn[_ <: Any]] = Array(
        TypedColumnBuilder.from("C", "A", "C", "Z", "C"),
        TypedColumnBuilder.from(1.1, 0.2, 3.01, 23.1, 2.24),
        TypedColumnBuilder.from(80L, 20L, 105L, 294L, 102L)
      )

      DependencyCheckingTester.checkOrderDependent(Seq(0) -> Seq(1), dataset) shouldEqual false
      DependencyCheckingTester.checkOrderDependent(Seq(1) -> Seq(0), dataset) shouldEqual true
      DependencyCheckingTester.checkOrderDependent(Seq(0) -> Seq(2), dataset) shouldEqual false
      DependencyCheckingTester.checkOrderDependent(Seq(1) -> Seq(2), dataset) shouldEqual true
      DependencyCheckingTester.checkOrderDependent(Seq(2) -> Seq(1), dataset) shouldEqual true
    }

    "check for order dependencies of longer lists" in {
      val dataset: Array[TypedColumn[_ <: Any]] = Array(
        TypedColumnBuilder.from("C", "A", "C", "Z", "C"),
        TypedColumnBuilder.from(1.1, 0.2, 3.01, 23.1, 2.24),
        TypedColumnBuilder.from(80L, 20L, 105L, 294L, 102L)
      )

      DependencyCheckingTester.checkOrderDependent(Seq(0) -> Seq(1), dataset) shouldEqual false
      DependencyCheckingTester.checkOrderDependent(Seq(1) -> Seq(0), dataset) shouldEqual true
      DependencyCheckingTester.checkOrderDependent(Seq(0) -> Seq(2), dataset) shouldEqual false
      DependencyCheckingTester.checkOrderDependent(Seq(1) -> Seq(2), dataset) shouldEqual true
      DependencyCheckingTester.checkOrderDependent(Seq(2) -> Seq(1), dataset) shouldEqual true
    }

    "check for a -> b, a -/> c, but a -> bc" in {
      val dataset: Array[TypedColumn[_ <: Any]] = Array(
        TypedColumnBuilder.from("C", "A", "C", "Z", "C"),
        TypedColumnBuilder.from(1.1, 2.2, 1.3, 23.1, 1.2),
        TypedColumnBuilder.from(801L, 120L, 105L, 294L, 102L)
      )

      DependencyCheckingTester.checkOrderDependent(Seq(0) -> Seq(1), dataset) shouldEqual true
      DependencyCheckingTester.checkOrderDependent(Seq(0) -> Seq(2), dataset) shouldEqual false
      DependencyCheckingTester.checkOrderDependent(Seq(0) -> Seq(1, 2), dataset) shouldEqual true
    }

  }
}