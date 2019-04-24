package com.github.codelionx.dodo.parsing

import java.time.temporal.TemporalAccessor
import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}

import com.github.codelionx.dodo.types._

import scala.util.Try


/**
  * Type class that adds a `parse()` method to [[com.github.codelionx.dodo.types.DataType]]s and details how to
  * parse the different data types.
  *
  * @see [[https://scalac.io/typeclasses-in-scala]] for how to write type classes
  * @tparam V output type, the string should be parsed to
  */
trait DataTypeParse[V] {

  def parse(dataType: DataType[V])(in: String): V

}

object DataTypeParse {

  /**
    * Pulls in the correct instance for the supplied type by an implicit.
    *
    * @param p implicit implementation of the [[DataTypeParse]] trait
    */
  def apply[V](implicit p: DataTypeParse[V]): DataTypeParse[V] = p

  // could be used to parse with `DataTypeParse.parse(dType)("something")`
  def parse[V: DataTypeParse](dataType: DataType[V])(in: String): V = DataTypeParse[V].parse(dataType)(in)

  /**
    * Ops for this type class. Specifies, where this type class can be applied to (on [[DataType]]s).
    */
  implicit class DataTypeParseOps[V: DataTypeParse](dataType: DataType[V]) {
    def parse(value: String): V = DataTypeParse[V].parse(dataType)(value)
  }

  // implementations:

  implicit val stringCanParse: DataTypeParse[String] = new DataTypeParse[String] {
    override def parse(dataType: DataType[String])(in: String): String = in
  }

  implicit val nullCanParse: DataTypeParse[Null] = new DataTypeParse[Null] {
    override def parse(dataType: DataType[Null])(in: String): Null = null
  }

  implicit val longCanParse: DataTypeParse[Long] = new DataTypeParse[Long] {
    override def parse(dataType: DataType[Long])(in: String): Long = Try {
      in.toLong
    }.getOrElse(0L)
  }

  implicit val doubleCanParse: DataTypeParse[Double] = new DataTypeParse[Double] {
    override def parse(dataType: DataType[Double])(in: String): Double = Try {
      in.toDouble
    }.getOrElse(.0)
  }

  implicit val zonedDateTimeCanParse: DataTypeParse[ZonedDateTime] = new DataTypeParse[ZonedDateTime] {
    override def parse(dataType: DataType[ZonedDateTime])(in: String): ZonedDateTime = Try {
      dataType.asInstanceOf[ZonedDateType].format.parse[ZonedDateTime](in, (temp: TemporalAccessor) => ZonedDateTime.from(temp))
    }.getOrElse(ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault()))
  }

  implicit val localDateTimeCanParse: DataTypeParse[LocalDateTime] = new DataTypeParse[LocalDateTime] {
    override def parse(dataType: DataType[LocalDateTime])(in: String): LocalDateTime = Try {
      dataType.asInstanceOf[LocalDateType].format.parse[LocalDateTime](in, (temp: TemporalAccessor) => LocalDateTime.from(temp))
    }.getOrElse(LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault()))
  }
}


object Main {

  def main(args: Array[String]): Unit = {
    import DataTypeParse._

    val lType = LongType
    println(lType.parse("1234567788"))
    val sType = StringType
    println(sType.parse("bla"))
  }
}