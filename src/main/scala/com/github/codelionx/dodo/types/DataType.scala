package com.github.codelionx.dodo.types

import java.time._
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAccessor

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}


object DataType {

  private def orderMapping(in: DataType[_]): Int = in match {
    case NullType => 0
    case StringType => 1
    case LongType => 2
    case DoubleType => 3
    case LocalDateType(_) => 4
    case ZonedDateType(_) => 5
  }

  /**
    * Import `DataType._` to use this implicit DataType ordering.
    */
  implicit def ordering[A <: DataType[_]]: Ordering[A] =
    Ordering.by(orderMapping)
}

/**
  * Represents a data type supported by this application.
  *
  * @see [[com.github.codelionx.dodo.types.StringType]],
  *      [[com.github.codelionx.dodo.types.LongType]],
  *      [[com.github.codelionx.dodo.types.DoubleType]],
  *      [[com.github.codelionx.dodo.types.NullType]],
  *      [[com.github.codelionx.dodo.types.ZonedDateType]],
  *      [[com.github.codelionx.dodo.types.LocalDateType]]
  * @tparam T underlying (primitive) type
  */
sealed trait DataType[T <: Any] extends Ordered[DataType[_]] {

  /**
    * Returns the underlying type's [[scala.reflect.ClassTag]]. Can be used for pattern matching or using the type
    * on runtime.
    *
    * @example {{{
    * val String_ = classOf[String]
    * val dataType: DataType[_] = _
    * dataType.tpe.runtimeClass match {
    *   case String_ => // do sth with the string
    * }
    * }}}
    */
  def tpe: ClassTag[T]

  /**
    * Parses the `value` to the underlying (primitive) type or uses the default value.
    */
  def parse(value: String): T

  /**
    * Creates a [[com.github.codelionx.dodo.types.TypedColumnBuilder]] that can be used to create a column of this type.
    */
  def createTypedColumnBuilder: TypedColumnBuilder[T]

  override def compare(that: DataType[_]): Int = DataType.orderMapping(this).compare(DataType.orderMapping(that))
}

/**
  * Represents a primitive [[scala.Long]].
  */
case object LongType extends DataType[Long] {

  override val tpe: ClassTag[Long] = ClassTag.Long

  /**
    * Checks if the value is a [[scala.Long]] value.
    */
  def isLong(value: String): Boolean = Try {
    value.toLong
  } match {
    case Success(_) => true
    case Failure(_) => false
  }

  def parse(value: String): Long = Try {
    value.toLong
  }.getOrElse(0L)

  override def createTypedColumnBuilder: TypedColumnBuilder[Long] = TypedColumnBuilder(this)
}

/**
  * Represents a primitive [[scala.Double]].
  */
case object DoubleType extends DataType[Double] {

  override val tpe: ClassTag[Double] = ClassTag.Double

  /**
    * Checks if the value is a [[scala.Double]] value.
    */
  def isDouble(value: String): Boolean = Try {
    value.toDouble
  } match {
    case Success(_) => true
    case Failure(_) => false
  }

  def parse(value: String): Double = Try {
    value.toDouble
  }.getOrElse(.0)

  override def createTypedColumnBuilder: TypedColumnBuilder[Double] = TypedColumnBuilder(this)
}

/**
  * Encapsulates zoned and unzoned date time types and their different formats.
  */
object DateType {

  // supported date formats
  private val datetimeFormats = Seq(DateTimeFormatter.ISO_DATE_TIME, DateTimeFormatter.RFC_1123_DATE_TIME)
  private val dateFormats = Seq(DateTimeFormatter.ISO_DATE, DateTimeFormatter.BASIC_ISO_DATE, DateTimeFormatter.ISO_LOCAL_DATE)

  /**
    * Checks the value for different datetime and date formats.
    *
    * @return a [[com.github.codelionx.dodo.types.DateType.DateChecker]] that provides access if the check was successful and if yes to the correct
    *         data type
    */
  def dateChecker(value: String) = new DateChecker(value)

  /**
    * Checks a string if it is a valid date and gives access to the result and the data type.
    */
  class DateChecker {

    private var format: DateTimeFormatter = DateTimeFormatter.BASIC_ISO_DATE
    private var isZoned: Boolean = false
    private var success: Boolean = false

    private[DateType] def this(value: String) = {
      this()
      checkForDateTime(value)
      checkForDate(value)
    }

    /**
      * Returns `true` if the value is a date
      */
    def isDate: Boolean = success

    /**
      * If the value is a valid date, returns either a [[com.github.codelionx.dodo.types.ZonedDateType]] or a
      * [[com.github.codelionx.dodo.types.LocalDateType]]. If the value is no
      * valid date, an exception is thrown.
      *
      * @note only call if [[com.github.codelionx.dodo.types.DateType.DateChecker#isDate]] is `true`
      */
    def dateType: DataType[_ <: Any] =
      if (!success)
        throw new IllegalAccessException("The value is not a valid date.")
      else if (isZoned)
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

/**
  * Represents a [[java.time.ZonedDateTime]].
  */
final case class ZonedDateType(format: DateTimeFormatter) extends DataType[ZonedDateTime] {

  override val tpe: ClassTag[ZonedDateTime] = ClassTag(classOf[ZonedDateTime])

  def parse(value: String): ZonedDateTime = Try {
    format.parse[ZonedDateTime](value, (temp: TemporalAccessor) => ZonedDateTime.from(temp))
  }.getOrElse(ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault()))

  override def createTypedColumnBuilder: TypedColumnBuilder[ZonedDateTime] = TypedColumnBuilder(this)
}

/**
  * Represents a [[java.time.LocalDateTime]].
  */
final case class LocalDateType(format: DateTimeFormatter) extends DataType[LocalDateTime] {

  override val tpe: ClassTag[LocalDateTime] = ClassTag(classOf[LocalDateTime])

  override def parse(value: String): LocalDateTime = Try {
    format.parse[LocalDateTime](value, (temp: TemporalAccessor) => LocalDateTime.from(temp))
  }.getOrElse(LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault()))

  override def createTypedColumnBuilder: TypedColumnBuilder[LocalDateTime] = TypedColumnBuilder(this)
}

/**
  * Represents a [[scala.Predef.String]].
  */
case object StringType extends DataType[String] {

  override val tpe: ClassTag[String] = ClassTag(classOf[String])

  override def parse(value: String): String = value

  override def createTypedColumnBuilder: TypedColumnBuilder[String] = TypedColumnBuilder(this)
}

/**
  * Represents a [[Null]].
  */
case object NullType extends DataType[Null] {

  override val tpe: ClassTag[Null] = ClassTag.Null

  /**
    * Checks if the value is a `null` value.
    */
  def isNull(value: String): Boolean = value == null || value.isEmpty || value.equalsIgnoreCase("null")

  override def parse(value: String): Null = null

  override def createTypedColumnBuilder: TypedColumnBuilder[Null] = TypedColumnBuilder(this)
}
