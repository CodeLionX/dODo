package com.github.codelionx.dodo.parsing

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZonedDateTime}

import scala.util.{Failure, Success, Try}


object TypeInferrer {

  // supported data types
  private val types: Seq[DataType] = Seq(DoubleType, LongType, DoubleType, StringType)

  // supported date types
  private val datetimeFormats = Seq(DateTimeFormatter.ISO_ZONED_DATE_TIME, DateTimeFormatter.ISO_OFFSET_DATE_TIME, DateTimeFormatter.ISO_DATE_TIME, DateTimeFormatter.RFC_1123_DATE_TIME)
  private val dateFormats = Seq(DateTimeFormatter.ISO_OFFSET_DATE, DateTimeFormatter.BASIC_ISO_DATE, DateTimeFormatter.ISO_DATE, DateTimeFormatter.ISO_LOCAL_DATE)

  def inferType(value: String): DataType = {
    if (isLong(value))
      LongType
    else if (isDouble(value))
      DoubleType
    else if (isDate(value))
      DateType
    else
      StringType
  }

  def isDouble(value: String): Boolean = Try {
    value.toDouble
  } match {
    case Success(_) => true
    case Failure(_) => false
  }

  def isLong(value: String): Boolean = Try {
    value.toLong
  } match {
    case Success(_) => true
    case Failure(_) => false
  }

  def isDate(value: String): Boolean = {
    for (format <- datetimeFormats) {
      Try {
        ZonedDateTime.parse(value, format)
      } recoverWith {
        case _: Throwable => Try {
          LocalDateTime.parse(value, format)
        }
      } match {
        case Success(_) => return true
        case Failure(_) =>
      }
    }
    for (format <- dateFormats) {
      Try {
        LocalDate.parse(value, format)
      } match {
        case Success(_) => return true
        case Failure(_) =>
      }
    }
    false
  }
}
