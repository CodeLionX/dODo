package com.github.codelionx.dodo.types

import scala.util.Try


trait Parse[V] {
  def parse(dataType: DataType[V])(in: String): V
}

object Parse {

  def apply[V](implicit p: Parse[V]): Parse[V] = p

  def parse[V : Parse](dataType: DataType[V])(in: String): V = Parse[V].parse(dataType)(in)

  implicit val longCanParse: Parse[Long] = new Parse[Long] {
    override def parse(dataType: DataType[Long])(in: String): Long = Try {
      in.toLong
    }.getOrElse(0L)
  }

  implicit class ParseOps[V : Parse](dataType: DataType[V]) {
    def parse(value: String): V = Parse[V].parse(dataType)(value)
  }
}


object Main {
  def main(args: Array[String]): Unit = {
    val sType = StringType
    println(sType.parse("bla"))
  }
}