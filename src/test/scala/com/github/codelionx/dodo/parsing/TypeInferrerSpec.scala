package com.github.codelionx.dodo.parsing

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

    "infer dates" in {
      TypeInferrer.inferType("2011-12-03T10:15:30").shouldEqual(DateType)
      TypeInferrer.inferType("2011-12-03").shouldEqual(DateType)
    }

    "infer strings" in {
      TypeInferrer.inferType("some string").shouldEqual(StringType)
      TypeInferrer.inferType("Monday, 2011/03/12").shouldEqual(StringType)
    }
  }
}
