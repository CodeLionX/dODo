package com.github.codelionx.dodo.types

import java.time._
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
    case LocalDateTimeType(_) => 5
    case ZonedDateTimeType(_) => 6
  }

  /**
    * Import `DataType._` to use this implicit DataType ordering.
    */
  implicit def ordering[A <: DataType[_]]: Ordering[A] =
    Ordering.by(orderMapping)

  /**
    * Creates the corresponding `DataType` instance to the supplied type parameter `T`.
    *
    * @note The date and time types use the default format.
    */
  def of[T <: Any](implicit ev: ClassTag[T]): DataType[T] = {
    val ZonedClass = classOf[ZonedDateTime]
    val LocalDateTimeClass = classOf[LocalDateTime]
    val LocalDateClass = classOf[LocalDate]
    val DoubleClass = classOf[Double]
    val LongClass = classOf[Long]
    val StringClass = classOf[String]
    val NullClass = classOf[Null]

    val tpe = ev.runtimeClass match {
      case ZonedClass => ZonedDateTimeType(DateFormat.DEFAULT)
      case LocalDateTimeClass => LocalDateTimeType(DateFormat.DEFAULT)
      case LocalDateClass => LocalDateType(DateFormat.DEFAULT)
      case DoubleClass => DoubleType
      case LongClass => LongType
      case StringClass => StringType
      case NullClass => NullType
      case _ => throw new IllegalArgumentException(s"$ev is not supported as DataType!")
    }
    tpe.asInstanceOf[DataType[T]]
  }
}

/**
  * Represents a data type supported by this application.
  *
  * @see [[com.github.codelionx.dodo.types.StringType]],
  *      [[com.github.codelionx.dodo.types.LongType]],
  *      [[com.github.codelionx.dodo.types.DoubleType]],
  *      [[com.github.codelionx.dodo.types.NullType]],
  *      [[com.github.codelionx.dodo.types.ZonedDateTimeType]],
  *      [[com.github.codelionx.dodo.types.LocalDateTimeType]],
  *      [[com.github.codelionx.dodo.types.LocalDateType]]
  * @tparam T underlying (primitive) type
  */
sealed trait DataType[T <: Any] extends Ordered[DataType[_]] with Serializable {

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
    * Parses the `value` to the underlying (primitive) type. It returns [[None]] for `null` values or parsing errors.
    */
  def parse(value: String): Option[T]

  def ordering: Ordering[Option[T]]

  /**
    * Creates a [[com.github.codelionx.dodo.types.TypedColumnBuilder]] that can be used to create a column of this type.
    */
  def createTypedColumnBuilder: TypedColumnBuilder[T]

  override def compare(that: DataType[_]): Int = DataType.orderMapping(this).compare(DataType.orderMapping(that))
}

/**
  * Encapsulates zoned and unzoned date time types and their different formats.
  */
object DateType {

  /**
    * Checks the value for different datetime and date formats.
    *
    * @return a [[com.github.codelionx.dodo.types.DateType.DateChecker]] that provides access if the check was successful and if yes to the correct
    *         data type
    */
  def dateChecker(value: String): DateChecker = new DateChecker(value)

  /**
    * Checks a string if it is a valid date and gives access to the result and the data type.
    */
  class DateChecker {

    private var format: DateFormat = DateFormat.DEFAULT
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
      * If the value is a valid date, returns either a [[com.github.codelionx.dodo.types.ZonedDateTimeType]] or a
      * [[com.github.codelionx.dodo.types.LocalDateType]]. If the value is no
      * valid date, an exception is thrown.
      *
      * @note only call if [[com.github.codelionx.dodo.types.DateType.DateChecker#isDate]] is `true`
      */
    def dateType: DataType[_ <: Any] =
      if (!success)
        throw new IllegalAccessException("The value is not a valid date.")
      else if (isZoned)
        ZonedDateTimeType(format)
      else {
        // if it is just a date format without time information
        if(DateFormat.dateFormats.contains(format))
          LocalDateType(format)
        else
          LocalDateTimeType(format)
      }

    private def checkForDateTime(value: String): Unit = {
      for (format <- DateFormat.datetimeFormats) {
        Try {
          ZonedDateTime.parse(value, format.toFormatter)
          this.format = format
          this.isZoned = true
        } recoverWith {
          case _: Throwable =>
            Try {
              LocalDateTime.parse(value, format.toFormatter)
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
      for (format <- DateFormat.dateFormats) {
        Try {
          LocalDate.parse(value, format.toFormatter)
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
final case class ZonedDateTimeType(format: DateFormat) extends DataType[ZonedDateTime] {

  @transient
  private lazy val formatter = format.toFormatter

  override val tpe: ClassTag[ZonedDateTime] = ClassTag(classOf[ZonedDateTime])

  override val ordering: Ordering[Option[ZonedDateTime]] = Ordering.Option(Ordering.by(_.toEpochSecond))

  def parse(value: String): Option[ZonedDateTime] = Try {
    formatter.parse[ZonedDateTime](value, (temp: TemporalAccessor) => ZonedDateTime.from(temp))
  }.toOption

  override def createTypedColumnBuilder: TypedColumnBuilder[ZonedDateTime] = TypedColumnBuilder(this)
}


/**
  * Represents a [[java.time.LocalDateTime]].
  */
final case class LocalDateTimeType(format: DateFormat) extends DataType[LocalDateTime] {

  @transient
  private lazy val formatter = format.toFormatter

  override val tpe: ClassTag[LocalDateTime] = ClassTag(classOf[LocalDateTime])

  override val ordering: Ordering[Option[LocalDateTime]] = Ordering.Option(Ordering.by(_.toEpochSecond(ZoneOffset.UTC)))

  override def parse(value: String): Option[LocalDateTime] = Try {
    formatter.parse[LocalDateTime](value, (temp: TemporalAccessor) => LocalDateTime.from(temp))
  }.toOption

  override def createTypedColumnBuilder: TypedColumnBuilder[LocalDateTime] = TypedColumnBuilder(this)
}

/**
  * Represents a [[java.time.LocalDate]].
  */
final case class LocalDateType(format: DateFormat) extends DataType[LocalDate] {

  @transient
  private lazy val formatter = format.toFormatter

  override val tpe: ClassTag[LocalDate] = ClassTag(classOf[LocalDate])

  override val ordering: Ordering[Option[LocalDate]] = Ordering.Option(Ordering.by(
    _.atStartOfDay.toEpochSecond(ZoneOffset.UTC)
  ))

  override def parse(value: String): Option[LocalDate] = Try {
    formatter.parse[LocalDate](value, (temp: TemporalAccessor) => LocalDate.from(temp))
  }.toOption

  override def createTypedColumnBuilder: TypedColumnBuilder[LocalDate] = TypedColumnBuilder(this)
}

/**
  * Represents a primitive [[scala.Long]].
  */
case object LongType extends DataType[Long] {

  override val tpe: ClassTag[Long] = ClassTag.Long

  override val ordering: Ordering[Option[Long]] = Ordering.Option[Long]

  /**
    * Checks if the value is a [[scala.Long]] value.
    */
  def isLong(value: String): Boolean = Try {
    value.toLong
  } match {
    case Success(_) => true
    case Failure(_) => false
  }

  def parse(value: String): Option[Long] = Try {
    value.toLong
  }.toOption

  override def createTypedColumnBuilder: TypedColumnBuilder[Long] = TypedColumnBuilder(this)
}

/**
  * Represents a primitive [[scala.Double]].
  */
case object DoubleType extends DataType[Double] {

  override val tpe: ClassTag[Double] = ClassTag.Double

  override val ordering: Ordering[Option[Double]] = Ordering.Option[Double]

  /**
    * Checks if the value is a [[scala.Double]] value.
    */
  def isDouble(value: String): Boolean = Try {
    value.toDouble
  } match {
    case Success(_) => true
    case Failure(_) => false
  }

  def parse(value: String): Option[Double] = Try {
    value.toDouble
  }.toOption

  override def createTypedColumnBuilder: TypedColumnBuilder[Double] = TypedColumnBuilder(this)
}

/**
  * Represents a [[scala.Predef.String]].
  */
case object StringType extends DataType[String] {

  override val tpe: ClassTag[String] = ClassTag(classOf[String])

  override val ordering: Ordering[Option[String]] = Ordering.Option[String]

  override def parse(value: String): Option[String] =
    if(NullType.isNull(value)) None
    else Some(value)

  override def createTypedColumnBuilder: TypedColumnBuilder[String] = TypedColumnBuilder(this)
}

/**
  * Represents a [[Null]].
  */
case object NullType extends DataType[Null] {

  override val tpe: ClassTag[Null] = ClassTag.Null

  override val ordering: Ordering[Option[Null]] = (_: Any, _: Any) => 0

  /**
    * Checks if the value is a `null` value.
    */
  def isNull(value: String): Boolean =
    value == null || value.isEmpty || value.equalsIgnoreCase("null") || value.equalsIgnoreCase("?")

  override def parse(value: String): Option[Null] = None

  override def createTypedColumnBuilder: TypedColumnBuilder[Null] = TypedColumnBuilder(this)
}
