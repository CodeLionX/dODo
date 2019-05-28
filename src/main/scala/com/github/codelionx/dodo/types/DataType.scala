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

  /**
    * Creates the corresponding `DataType` instance to the supplied type parameter `T`.
    *
    * @note The date and time types use the default format.
    */
  def of[T <: Any](implicit ev: ClassTag[T]): DataType[T] = {
    val ZonedClass = classOf[ZonedDateTime]
    val LocalClass = classOf[LocalDateTime]
    val DoubleClass = classOf[Double]
    val LongClass = classOf[Long]
    val StringClass = classOf[String]
    val NullClass = classOf[Null]

    val tpe = ev.runtimeClass match {
      case ZonedClass => ZonedDateType(DateType.DEFAULT_FORMAT)
      case LocalClass => LocalDateType(DateType.DEFAULT_FORMAT)
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

  // supported date formats
  private val datetimeFormats = Seq(DateTimeFormatter.ISO_DATE_TIME, DateTimeFormatter.RFC_1123_DATE_TIME)
  private val dateFormats = Seq(DateTimeFormatter.ISO_DATE, DateTimeFormatter.BASIC_ISO_DATE, DateTimeFormatter.ISO_LOCAL_DATE)

  // default format
  val DEFAULT_FORMAT: DateTimeFormatter = DateTimeFormatter.BASIC_ISO_DATE

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

    private var format: DateTimeFormatter = DEFAULT_FORMAT
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

  override val ordering: Ordering[Option[ZonedDateTime]] = Ordering.Option(Ordering.by(_.toEpochSecond))

  def parse(value: String): Option[ZonedDateTime] = Try {
    format.parse[ZonedDateTime](value, (temp: TemporalAccessor) => ZonedDateTime.from(temp))
  }.toOption

  override def createTypedColumnBuilder: TypedColumnBuilder[ZonedDateTime] = TypedColumnBuilder(this)
}

/**
  * Represents a [[java.time.LocalDateTime]].
  */
final case class LocalDateType(format: DateTimeFormatter) extends DataType[LocalDateTime] {

  override val tpe: ClassTag[LocalDateTime] = ClassTag(classOf[LocalDateTime])

  override val ordering: Ordering[Option[LocalDateTime]] = Ordering.Option(Ordering.by(_.toEpochSecond(ZoneOffset.UTC)))

  override def parse(value: String): Option[LocalDateTime] = Try {
    format.parse[LocalDateTime](value, (temp: TemporalAccessor) => LocalDateTime.from(temp))
  }.toOption

  override def createTypedColumnBuilder: TypedColumnBuilder[LocalDateTime] = TypedColumnBuilder(this)
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
