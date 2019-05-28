package com.github.codelionx.dodo.types

import java.time.format.DateTimeFormatter


object DateFormat {
  // supported date formats
  val datetimeFormats: Seq[DateFormat] = Seq(IsoDateTimeFormat, RFC_1123_Format)
  val dateFormats: Seq[DateFormat] = Seq(IsoDateFormat, BasicFormat, IsoLocalDateFormat)

  // default format
  val DEFAULT: DateFormat = BasicFormat

  def apply(formatter: DateTimeFormatter): DateFormat = formatter match {
    case DateTimeFormatter.BASIC_ISO_DATE => BasicFormat
    case DateTimeFormatter.ISO_DATE_TIME => IsoDateTimeFormat
    case DateTimeFormatter.ISO_DATE => IsoDateFormat
    case DateTimeFormatter.ISO_LOCAL_DATE => IsoLocalDateFormat
    case DateTimeFormatter.RFC_1123_DATE_TIME => RFC_1123_Format
  }

  def toFormatter(dateFormat: DateFormat): DateTimeFormatter = dateFormat match {
    case BasicFormat => DateTimeFormatter.BASIC_ISO_DATE
    case IsoDateTimeFormat => DateTimeFormatter.ISO_DATE_TIME
    case IsoDateFormat => DateTimeFormatter.ISO_DATE
    case IsoLocalDateFormat => DateTimeFormatter.ISO_LOCAL_DATE
    case RFC_1123_Format => DateTimeFormatter.RFC_1123_DATE_TIME
  }

  implicit class ConvertableDateFormat(dateFormat: DateFormat) {
    def toFormatter: DateTimeFormatter = DateFormat.toFormatter(dateFormat)
  }
}

sealed trait DateFormat extends Serializable

case object BasicFormat extends DateFormat
case object IsoDateTimeFormat extends DateFormat
case object IsoDateFormat extends DateFormat
case object IsoLocalDateFormat extends DateFormat
case object RFC_1123_Format extends DateFormat
