package com.github.codelionx.dodo

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor

import scala.util.{Failure, Success, Try}


package object types {

  private def orderMapping(in: DataType) = in match {
    case NullType => 0
    case StringType => 1
    case LongType => 2
    case DoubleType => 3
    case LocalDateType(_) => 4
    case ZonedDateType(_) => 5
  }

  object DataType {

    implicit def ordering[A <: DataType]: Ordering[A] =
      Ordering.by(orderMapping)
  }

  sealed trait DataType extends Ordered[DataType] {
    type T <: Any

    def parse(value: String): T

    override def compare(that: DataType): Int = orderMapping(this).compare(orderMapping(that))
  }

  final case object LongType extends DataType {
    override type T = Long

    def isLong(value: String): Boolean = Try {
      value.toLong
    } match {
      case Success(_) => true
      case Failure(_) => false
    }

    def parse(value: String): Long = Try {
      value.toLong
    }.getOrElse(0L)
  }

  final case object DoubleType extends DataType {
    override type T = Double

    def isDouble(value: String): Boolean = Try {
      value.toDouble
    } match {
      case Success(_) => true
      case Failure(_) => false
    }

    def parse(value: String): Double = Try {
      value.toDouble
    }.getOrElse(.0)
  }

  object DateType {

    // supported date formats
    private val datetimeFormats = Seq(DateTimeFormatter.ISO_DATE_TIME, DateTimeFormatter.RFC_1123_DATE_TIME)
    private val dateFormats = Seq(DateTimeFormatter.ISO_DATE, DateTimeFormatter.BASIC_ISO_DATE, DateTimeFormatter.ISO_LOCAL_DATE)

    def isDateChecker(value: String) = new DateParser(value)

    class DateParser {
      private var format: DateTimeFormatter = DateTimeFormatter.BASIC_ISO_DATE
      private var isZoned: Boolean = false
      private var success: Boolean = false

      def this(value: String) = {
        this()
        checkForDateTime(value)
        checkForDate(value)
      }

      def isDate: Boolean = success

      def dateType: DataType =
        if (isZoned)
          ZonedDateType(format)
        else
          LocalDateType(format)

      private def checkForDateTime(value: String): Unit = {
        for (format <- datetimeFormats) {
          Try {
            ZonedDateTime.parse(value, format)
            this.format = format
            this.isZoned = true
          } recoverWith {
            case _: Throwable =>
              Try {
                LocalDateTime.parse(value, format)
                this.format = format
                this.isZoned = false
              }
          } match {
            case Success(_) =>
              this.success = true
              return
            case Failure(_) =>
          }
        }
      }

      private def checkForDate(value: String): Unit = {
        for (format <- dateFormats) {
          Try {
            LocalDate.parse(value, format)
            this.format = format
            this.isZoned = false
          } match {
            case Success(_) =>
              this.success = true
              return
            case Failure(_) =>
          }
        }
      }
    }

  }

  final case class ZonedDateType(format: DateTimeFormatter) extends DataType {
    override type T = ZonedDateTime

    def parse(value: String): ZonedDateTime = Try {
      format.parse[ZonedDateTime](value, (temp: TemporalAccessor) => ZonedDateTime.from(temp))
    }.getOrElse(ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault()))
  }

  final case class LocalDateType(format: DateTimeFormatter) extends DataType {
    override type T = LocalDateTime

    override def parse(value: String): LocalDateTime = Try {
      format.parse[LocalDateTime](value, (temp: TemporalAccessor) => LocalDateTime.from(temp))
    }.getOrElse(LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault()))
  }

  final case object StringType extends DataType {
    override type T = String

    override def parse(value: String): String = value
  }

  final case object NullType extends DataType {
    override type T = Null

    def isNull(value: String): Boolean = value == null || value.isEmpty || value.equalsIgnoreCase("null")

    override def parse(value: String): Null = null
  }

}