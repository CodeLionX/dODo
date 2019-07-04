package com.github.codelionx.dodo.parsing

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

    "infer datetimes with time zone" in {
      TypeInferrer.inferType("2019-12-27T22:15:30+02:00").shouldEqual(ZonedDateTimeType(IsoDateTimeFormat))
      TypeInferrer.inferType("Tue, 3 Jun 2008 11:05:30 GMT").shouldEqual(ZonedDateTimeType(RFC_1123_Format))
      TypeInferrer.inferType("10 Jun 2019 08:05:30 GMT").shouldEqual(ZonedDateTimeType(RFC_1123_Format))
    }

    "infer datetimes without time zone" in {
      TypeInferrer.inferType("2011-12-03T10:15:30").shouldEqual(LocalDateTimeType(IsoDateTimeFormat))
    }

    "infer dates" in {
      // dates are always local (without zone)
      TypeInferrer.inferType("2011-12-03").shouldEqual(LocalDateType(IsoDateFormat))
      TypeInferrer.inferType("2011-12-03+01:00").shouldEqual(LocalDateType(IsoDateFormat))
      TypeInferrer.inferType("01/28/1997").shouldEqual(LocalDateType(CustomUSDateFormat))
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

    // bugfix guides
    "not infer 0-padded number with 6 digits as date" in {
      TypeInferrer.inferType("000023").shouldEqual(LongType)
    }
  }
}
