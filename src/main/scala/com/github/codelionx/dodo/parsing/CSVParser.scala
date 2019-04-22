package com.github.codelionx.dodo.parsing

import java.io.File

import com.univocity.parsers.csv.CsvParserSettings
import com.univocity.parsers.csv.CsvParser

import scala.io.Codec
import scala.collection.JavaConverters._

object CSVParser {

  private implicit val fileCodec: Codec = Codec.UTF8

  private val settings = {
    val settings = new CsvParserSettings
    settings.detectFormatAutomatically()
    settings.setHeaderExtractionEnabled(false)
    settings
  }

  def read(file: String): Unit = {
    val parser = new CsvParser(settings)
    parser.parseAllRecords(new File(file))
      .asScala
      .map( rec => rec.getValues)
      .map(_.mkString(" "))
      .foreach(println)
    println(parser.getRecordMetadata.typeOf(4))
  }

  def useProcessor(file: String): Unit = {
    val s = settings.clone()
    s.setProcessor(new TypedColumnProcessor)
    val parser = new CsvParser(s)
    parser.parse(new File(file))
  }

  def main(args: Array[String]): Unit = {
//    CSVParser.read("data/iris.csv")
    CSVParser.useProcessor("data/iris.csv")
  }
}