package com.github.codelionx.dodo.parsing

import com.github.codelionx.dodo.Settings
import com.github.codelionx.dodo.types.TypedColumn
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpec}


class CSVParserSpec extends WordSpec with Matchers {

  val noHeader: Boolean = false
  val hasHeader: Boolean = true

  val testFilePath: String = "data/ocdd"
  val testFileDatasets: Seq[(String, Boolean)] = Seq(
    // without header
    "abalone.csv" -> noHeader,
    "adult.csv" -> noHeader,
    "balance-scale.csv" -> noHeader,
    "breast-cancer-wisconsin.csv" -> noHeader,
    "bridges.csv" -> noHeader,
    "chess.csv" -> noHeader,
    "echo1.csv" -> noHeader,
    "echocardiogram.csv" -> noHeader,
    "hepatitis.csv" -> noHeader,
    "horse.csv" -> noHeader,
    "horseShort.csv" -> noHeader,
    "iris.csv" -> noHeader,
    "iris2.csv" -> noHeader,
    "letter.csv" -> noHeader,
    "nursery.csv" -> noHeader,
    "plista_1k.csv" -> noHeader,
    "test.csv" -> noHeader,
    // with header
    "ac_cb.csv" -> hasHeader,
    "ac_no_cb.csv" -> hasHeader,
    "ch1.csv" -> hasHeader,
    "ch2.csv" -> hasHeader,
    "flight_1k.csv" -> hasHeader,
    "ncvoter_1001r_19c.csv" -> hasHeader,
    "TPCH_region.csv" -> hasHeader,
    "TPCH_supplier.csv" -> hasHeader,
    "uniprot_1001r_223c.csv" -> hasHeader,
    "WDC_age.csv" -> hasHeader,
    "WDC_appearances.csv" -> hasHeader,
    "WDC_astrology.csv" -> hasHeader,
    "WDC_astronomical.csv" -> hasHeader,
    "WDC_game.csv" -> hasHeader,
    "WDC_kepler.csv" -> hasHeader,
    "WDC_planets.csv" -> hasHeader,
    "WDC_planetz.csv" -> hasHeader,
    "WDC_satellites.csv" -> hasHeader,
    "WDC_science.csv" -> hasHeader,
    "WDC_symbols.csv" -> hasHeader
  )

  val parseAllTestFilesIn: AfterWord = afterWord("parse testfile")

  def parseFile(filename: String, hasHeader: Boolean): Unit = {
    filename in {
      val filepath = s"$testFilePath/$filename"
      val settings = new Settings.ParsingSettings(ConfigFactory.parseString(
        s"""com.github.codelionx.dodo {
           |  parsing {
           |    inferring-rows = 20
           |    has-header = $hasHeader
           |  }
           |}
       """.stripMargin), "com.github.codelionx.dodo")

      var data: Array[TypedColumn[Any]] = Array.empty
      noException shouldBe thrownBy {
        data = CSVParser(settings).parse(filepath)
      }

      if (!hasHeader) {
        data.foreach(_.name should fullyMatch regex "[A-Z]+".r)
      }
    }
  }

  "The CSVParser" should parseAllTestFilesIn {
    testFileDatasets.foreach { case (filename, withHeader) =>
      parseFile(filename, withHeader)

    }
  }
}
