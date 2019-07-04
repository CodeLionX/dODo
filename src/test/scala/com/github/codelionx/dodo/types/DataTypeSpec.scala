package com.github.codelionx.dodo.types

import java.time.{LocalDate, LocalDateTime, ZonedDateTime}

import org.scalatest.{Matchers, WordSpec}


class DataTypeSpec extends WordSpec with Matchers {

  "The DataType companion object" should {

    "support ZonedDateTime in the factory method" in {
      val dt = DataType.of[ZonedDateTime]
      dt shouldEqual ZonedDateTimeType(DateFormat.DEFAULT)
    }

    "support LocalDateTime in the factory method" in {
      val dt = DataType.of[LocalDateTime]
      dt shouldEqual LocalDateTimeType(DateFormat.DEFAULT)
    }

    "support LocalDate in the factory method" in {
      val dt = DataType.of[LocalDate]
      dt shouldEqual LocalDateType(DateFormat.DEFAULT)
    }

    "support Double in the factory method" in {
      val dt = DataType.of[Double]
      dt shouldEqual DoubleType
    }

    "support Long in the factory method" in {
      val dt = DataType.of[Long]
      dt shouldEqual LongType
    }

    "support String in the factory method" in {
      val dt = DataType.of[String]
      dt shouldEqual StringType
    }

    "support Null in the factory method" in {
      val dt = DataType.of[Null]
      dt shouldEqual NullType
    }

    "throw an error if the type is not supported" in {
      an[IllegalArgumentException] shouldBe thrownBy {
        DataType.of[Int]
      }
      an[IllegalArgumentException] shouldBe thrownBy {
        DataType.of[Float]
      }
      an[IllegalArgumentException] shouldBe thrownBy {
        DataType.of[Char]
      }
      an[IllegalArgumentException] shouldBe thrownBy {
        DataType.of[Boolean]
      }
      an[IllegalArgumentException] shouldBe thrownBy {
        DataType.of[Any]
      }
      case class Test(a: Int)
      an[IllegalArgumentException] shouldBe thrownBy {
        DataType.of[Test]
      }
    }
  }

  "The StringType" should {
    val dt = DataType.of[String]

    "parse nulls correctly" when {

      def shouldParseToNone(value: String): Unit = {
        s"the value is `${if(value == null) "literal<null>" else value}`" in {
          val nullValue = dt.parse(value)
          nullValue shouldBe None
        }
      }

      Seq("null", "?", "", null).foreach(value =>
        shouldParseToNone(value)
      )
    }

    "parse correct strings to Some" in {
      val value = "`!value1<$2²#+."
      val parsed = dt.parse(value)

      parsed shouldBe Some(value)
    }
  }
}
