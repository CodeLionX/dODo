package com.github.codelionx.dodo.types

import org.scalatest.{Matchers, WordSpec}


class TypedColumnBuilderSpec extends WordSpec with Matchers {

  "The TypedColumnBuilder factory methods" should {

    "produce the same typed columns" in {
      val columnValues = Seq(25.1, 0.4, 1.2, 123.00001)
      val tpe = DoubleType

      val builder = TypedColumnBuilder(tpe)
      builder.append(columnValues.map(_.toString): _*)
      val longBuilderResult = builder.toTypedColumn
      
      val explicitTypedResult = TypedColumnBuilder.withType(tpe)(columnValues.map(_.toString): _*)
      val implicitTypedResult = TypedColumnBuilder.from(columnValues: _*)

      // check equality
      longBuilderResult shouldEqual explicitTypedResult
      longBuilderResult shouldEqual implicitTypedResult
      explicitTypedResult shouldEqual implicitTypedResult

      // check contents
      longBuilderResult.dataType shouldEqual DoubleType
      longBuilderResult.array.toSeq shouldEqual columnValues
    }
  }

}
