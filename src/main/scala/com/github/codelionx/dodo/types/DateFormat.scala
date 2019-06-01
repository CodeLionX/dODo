package com.github.codelionx.dodo.types

import java.time.format.DateTimeFormatter


/**
  * Can be used to forward and backward map the marker objects to
  * [[java.time.format.DateTimeFormatter]]s.
  *
  * @see [[com.github.codelionx.dodo.types.DateFormat]]
  */
object DateFormat {

  /**
    * Supported datetime formats.
    */
  val datetimeFormats: Seq[DateFormat] = Seq(IsoDateTimeFormat, RFC_1123_Format)
  /**
    * Supported date formats (without time info).
    */
  val dateFormats: Seq[DateFormat] = Seq(IsoDateFormat, BasicFormat, IsoLocalDateFormat)

  /**
    * Default date time format.
    */
  val DEFAULT: DateFormat = BasicFormat

  /**
    * Backward mapping from a [[java.time.format.DateTimeFormatter]] to the DateFormat.
    *
    * @return a [[com.github.codelionx.dodo.types.DateFormat]] marker object that represents the supplied `formatter`.
    */
  def apply(formatter: DateTimeFormatter): DateFormat = formatter match {
    case DateTimeFormatter.BASIC_ISO_DATE => BasicFormat
    case DateTimeFormatter.ISO_DATE_TIME => IsoDateTimeFormat
    case DateTimeFormatter.ISO_DATE => IsoDateFormat
    case DateTimeFormatter.ISO_LOCAL_DATE => IsoLocalDateFormat
    case DateTimeFormatter.RFC_1123_DATE_TIME => RFC_1123_Format
    case f => throw new IllegalArgumentException(s"The specified formatter is not supported: $f")
  }

  /**
    * Forward mapping from the DateFormat to a [[java.time.format.DateTimeFormatter]].
    *
    * @return the corresponding formatter
    */
  def toFormatter(dateFormat: DateFormat): DateTimeFormatter = dateFormat match {
    case BasicFormat => DateTimeFormatter.BASIC_ISO_DATE
    case IsoDateTimeFormat => DateTimeFormatter.ISO_DATE_TIME
    case IsoDateFormat => DateTimeFormatter.ISO_DATE
    case IsoLocalDateFormat => DateTimeFormatter.ISO_LOCAL_DATE
    case RFC_1123_Format => DateTimeFormatter.RFC_1123_DATE_TIME
  }

  implicit class ConvertableDateFormat(dateFormat: DateFormat) {

    /**
      * Converts this marker object to the corresponding [[java.time.format.DateTimeFormatter]]
      *
      * @note This is not explicitly implemented in the marker objects as the `DateTimeFormatter`s are not serializable.
      */
    def toFormatter: DateTimeFormatter = DateFormat.toFormatter(dateFormat)
  }
}


/**
  * Represents a serializable date and time format (a.k.a. marker objects) that can be used to get the real
  * date formatters using an implicit method or the companion object.
  *
  * @see [[com.github.codelionx.dodo.types.DateFormat$]]
  */
sealed trait DateFormat extends Serializable

case object BasicFormat extends DateFormat
case object IsoDateTimeFormat extends DateFormat
case object IsoDateFormat extends DateFormat
case object IsoLocalDateFormat extends DateFormat
case object RFC_1123_Format extends DateFormat
