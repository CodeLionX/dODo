package com.github.codelionx.dodo.parsing

import java.time.format.DateTimeFormatter

import com.github.codelionx.dodo.types._
import org.scalatest.{Matchers, WordSpec}


class TypeInferrerSpec extends WordSpec with Matchers {

  "A TypeInferrer" should {
    "infer doubles" in {
      TypeInferrer.inferType("1.2").shouldEqual(DoubleType)
      TypeInferrer.inferType("123.").shouldEqual(DoubleType)
      TypeInferrer.inferType(".123").shouldEqual(DoubleType)
    }

    "infer longs" in {
      TypeInferrer.inferType("123876").shouldEqual(LongType)
      TypeInferrer.inferType(Long.MaxValue.toString).shouldEqual(LongType)
    }

    "infer datetimes without time zone" in {
      TypeInferrer.inferType("2011-12-03T10:15:30").shouldEqual(LocalDateType(DateTimeFormatter.ISO_DATE_TIME))

    }

    "infer datetimes with time zone" in {
      TypeInferrer.inferType("2019-12-27T22:15:30+02:00").shouldEqual(ZonedDateType(DateTimeFormatter.ISO_DATE_TIME))
      TypeInferrer.inferType("Tue, 3 Jun 2008 11:05:30 GMT").shouldEqual(ZonedDateType(DateTimeFormatter.RFC_1123_DATE_TIME))
      TypeInferrer.inferType("10 Jun 2019 08:05:30 GMT").shouldEqual(ZonedDateType(DateTimeFormatter.RFC_1123_DATE_TIME))
    }

    "infer dates" in {
      // dates are always local (without zone)
      TypeInferrer.inferType("2011-12-03").shouldEqual(LocalDateType(DateTimeFormatter.ISO_DATE))
      TypeInferrer.inferType("2011-12-03+01:00").shouldEqual(LocalDateType(DateTimeFormatter.ISO_DATE))
      TypeInferrer.inferType("20111203").shouldEqual(LocalDateType(DateTimeFormatter.BASIC_ISO_DATE))
    }

    "infer strings" in {
      TypeInferrer.inferType("some string").shouldEqual(StringType)
      TypeInferrer.inferType("Monday, 2011/03/12").shouldEqual(StringType)
    }

    "infer NULLS" in {
      TypeInferrer.inferType(null).shouldEqual(NullType)
      TypeInferrer.inferType("").shouldEqual(NullType)
      TypeInferrer.inferType("null").shouldEqual(NullType)
      TypeInferrer.inferType("?").shouldEqual(NullType)
    }
  }
}
