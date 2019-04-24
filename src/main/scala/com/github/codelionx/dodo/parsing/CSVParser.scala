package com.github.codelionx.dodo.parsing

import java.io.File

import com.univocity.parsers.csv.CsvParserSettings
import com.univocity.parsers.csv.CsvParser

import scala.io.{Codec, StdIn}
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
    val p = TypedColumnProcessor()
    s.setProcessor(p)
    val parser = new CsvParser(s)
    parser.parse(new File(file))

    val data = p.columnarData
    println("Column Types:")
    println(s"  ${data.map(_.dataType).mkString(" ")}")
    println("Parsed columns:")
    println(s"  Number of rows: ${data(0).toArray.length}")
  }

  def main(args: Array[String]): Unit = {
//    println("Press <enter> to start parsing the input")
//    StdIn.readLine()
//    println("Beginning ...")
//    CSVParser.read("data/iris.csv")
    CSVParser.useProcessor("data/flights_20_500k.csv")
//    println("... finished. Press <enter> to end session.")
//    StdIn.readLine()
  }
}