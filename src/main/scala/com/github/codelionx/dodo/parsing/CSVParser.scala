package com.github.codelionx.dodo.parsing

import java.io.File

import com.github.codelionx.dodo.Settings.ParsingSettings
import com.github.codelionx.dodo.types.TypedColumn
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}

import scala.io.Codec


/**
  * CSV Parser for reading input files and converting them to a list of
  * [[com.github.codelionx.dodo.types.TypedColumn]]s. Use the method
  * [[com.github.codelionx.dodo.parsing.CSVParser#parse]] to read input from a file.
  */
object CSVParser {

  def apply(settings: ParsingSettings): CSVParser = new CSVParser(settings)

}

class CSVParser(settings: ParsingSettings) {

  private implicit val fileCodec: Codec = Codec.UTF8

  private val parserSettings = {
    val s = new CsvParserSettings
    s.detectFormatAutomatically()
    s.setHeaderExtractionEnabled(settings.parseHeader)
    s
  }

  /**
    * Reads a CSV file and parses it to a list of [[com.github.codelionx.dodo.types.TypedColumn]]s.
    *
    * @param file file name, can contain relative or absolute paths, see [[java.io.File]] for more infos
    * @return the list of [[com.github.codelionx.dodo.types.TypedColumn]]s containing all data of the file
    */
  def parse(file: String): Array[TypedColumn[Any]] = parse(new File(file))

  /**
    * Reads a CSV file and parses it to a list of [[com.github.codelionx.dodo.types.TypedColumn]]s.
    *
    * @param file [[java.io.File]] pointing to the dataset
    * @return the list of [[com.github.codelionx.dodo.types.TypedColumn]]s containing all data of the file
    */
  def parse(file: File): Array[TypedColumn[Any]] = {
    val p = TypedColumnProcessor(settings)
    val s = parserSettings.clone()
    s.setProcessor(p)
    val parser = new CsvParser(s)

    // parse and return result
    parser.parse(file)
    p.columnarData
  }
}