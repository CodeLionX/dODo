package com.github.codelionx.dodo.parsing

import com.github.codelionx.dodo.types.{DataType, LongType, StringType}

import scala.util.Try


/**
  * Type class 
  *
  * @see https://scalac.io/typeclasses-in-scala for how to write type classes
  * @tparam V
  */
trait Parse[V] {
  def parse(in: String): V
}

object Parse {

  def apply[V](implicit p: Parse[V]): Parse[V] = p

//  def parse[V : Parse](dataType: DataType[V])(in: String): V = Parse[V].parse(dataType)(in)

  implicit val longCanParse: Parse[Long] = (in: String) => Try {
    in.toLong
  }.getOrElse(0L)

  implicit val stringCanParse: Parse[String] = identity[String]

  implicit class ParseOps[V : Parse](dataType: DataType[V]) {
    def parse(value: String): V = Parse[V].parse(value)
  }
}


object Main {

  def main(args: Array[String]): Unit = {
    import Parse._

    val lType = LongType
    println(lType.parse("1234567788"))
    val sType = StringType
    println(sType.parse("bla"))
  }
}