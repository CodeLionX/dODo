package com.github.codelionx.dodo

import java.time._
import java.time.format.DateTimeFormatter

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}


package object types {

  private def orderMapping(in: DataType[_]) = in match {
    case NullType => 0
    case StringType => 1
    case LongType => 2
    case DoubleType => 3
    case LocalDateType(_) => 4
    case ZonedDateType(_) => 5
  }

  object DataType {

    implicit def ordering[A <: DataType[_]]: Ordering[A] =
      Ordering.by(orderMapping)
  }

  sealed trait DataType[T <: Any] extends Ordered[DataType[_]] {

    val tpe: ClassTag[T]

    def createTypedColumnBuilder: TypedColumnBuilder[T]

    override def compare(that: DataType[_]): Int = orderMapping(this).compare(orderMapping(that))
  }

  final case object LongType extends DataType[Long] {

    override val tpe: ClassTag[Long] = ClassTag.Long

    def isLong(value: String): Boolean = Try {
      value.toLong
    } match {
      case Success(_) => true
      case Failure(_) => false
    }

    override def createTypedColumnBuilder: TypedColumnBuilder[Long] = TypedColumnBuilder(this)
  }

  final case object DoubleType extends DataType[Double] {

    override val tpe: ClassTag[Double] = ClassTag.Double

    def isDouble(value: String): Boolean = Try {
      value.toDouble
    } match {
      case Success(_) => true
      case Failure(_) => false
    }

    override def createTypedColumnBuilder: TypedColumnBuilder[Double] = TypedColumnBuilder(this)
  }

  object DateType {

    // supported date formats
    private val datetimeFormats = Seq(DateTimeFormatter.ISO_DATE_TIME, DateTimeFormatter.RFC_1123_DATE_TIME)
    private val dateFormats = Seq(DateTimeFormatter.ISO_DATE, DateTimeFormatter.BASIC_ISO_DATE, DateTimeFormatter.ISO_LOCAL_DATE)

    def dateChecker(value: String) = new DateChecker(value)

    class DateChecker {
      private var format: DateTimeFormatter = DateTimeFormatter.BASIC_ISO_DATE
      private var isZoned: Boolean = false
      private var success: Boolean = false

      def this(value: String) = {
        this()
        checkForDateTime(value)
        checkForDate(value)
      }

      def isDate: Boolean = success

      def dateType: DataType[_ <: Any] =
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

  final case class ZonedDateType(format: DateTimeFormatter) extends DataType[ZonedDateTime] {

    override val tpe: ClassTag[ZonedDateTime] = ClassTag(classOf[ZonedDateTime])

    override def createTypedColumnBuilder: TypedColumnBuilder[ZonedDateTime] = TypedColumnBuilder(this)
  }

  final case class LocalDateType(format: DateTimeFormatter) extends DataType[LocalDateTime] {

    override val tpe: ClassTag[LocalDateTime] = ClassTag(classOf[LocalDateTime])

    override def createTypedColumnBuilder: TypedColumnBuilder[LocalDateTime] = TypedColumnBuilder(this)
  }

  final case object StringType extends DataType[String] {

    override val tpe: ClassTag[String] = ClassTag(classOf[String])

    override def createTypedColumnBuilder: TypedColumnBuilder[String] = TypedColumnBuilder(this)
  }

  final case object NullType extends DataType[Null] {

    override val tpe: ClassTag[Null] = ClassTag.Null

    def isNull(value: String): Boolean = value == null || value.isEmpty || value.equalsIgnoreCase("null")

    override def createTypedColumnBuilder: TypedColumnBuilder[Null] = TypedColumnBuilder(this)
  }

}