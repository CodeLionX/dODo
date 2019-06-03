package com.github.codelionx.dodo

import com.github.codelionx.dodo.types.TypedColumn

import scala.concurrent.duration._


object GlobalImplicits {

  implicit class RichTypedColumnArray[T](val ta: Array[TypedColumn[T]]) extends AnyVal {

    def prettyPrint: String =
      s"""|Relation
          |===============================
          |${ta.map(col => col.toString).mkString("\n")}
          |""".stripMargin

    def prettyPrintAsTable(limit: Int = 20): String = {
      val headerNames = ta.map(col => s"${col.name}[${col.dataType}]")
      val header = headerNames.mkString("  ")
      val lengthMapping = headerNames.map(_.length)
      val data = ta(0).indices.take(limit).map(index =>
        ta.map(col =>
          // noinspection ScalaMalformedFormatString
          s"%${lengthMapping(ta.indexOf(col))}s".format(col(index))
        ).mkString("  ")
      ).mkString("\n")
      val bigSep = "=" * header.length
      val medSep = "-" * header.length

      s"""|Relation
          |$bigSep
          |$header
          |$medSep
          |$data
          |$medSep
          | ($limit of ${ta(0).length} rows shown)
          |""".stripMargin
    }
  }

  /**
    * Taken from <a href="https://alvinalexander.com/java/jwarehouse/akka-2.3/akka-testkit/src/test/scala/akka/testkit/metrics/reporter/PrettyDuration.scala.shtml">
    *   PrettyDuration.scala - alvinalexander.com
    * </a>
    * Provides implicit methods to pretty print durations.
    */
  implicit class PrettyPrintableDuration(val duration: Duration) extends AnyVal {

    def pretty: String = pretty(includeNanos = false)

    /**
      * Selects most appropriate TimeUnit for given duration and formats it accordingly
      */
    def pretty(includeNanos: Boolean, precision: Int = 4): String = {
      require(precision > 0, "precision must be > 0")

      duration match {
        case d: FiniteDuration =>
          val nanos = d.toNanos
          val unit = chooseUnit(nanos)
          val value = nanos.toDouble / NANOSECONDS.convert(1, unit)
          val postfix = if (includeNanos) s" ($nanos ns)" else ""
          // noinspection ScalaMalformedFormatString
          s"%.${precision}g %s%s".format(value, abbreviate(unit), postfix)

        case d: Duration.Infinite if d == Duration.MinusInf =>
          s"-∞ (minus infinity)"

        case _ =>
          s"∞ (infinity)"
      }
    }

    private def chooseUnit(nanos: Long): TimeUnit = {
      val d: Duration = nanos.nanos

      if (d.toDays > 0) DAYS
      else if (d.toHours > 0) HOURS
      else if (d.toMinutes > 0) MINUTES
      else if (d.toSeconds > 0) SECONDS
      else if (d.toMillis > 0) MILLISECONDS
      else if (d.toMicros > 0) MICROSECONDS
      else NANOSECONDS
    }

    private def abbreviate(unit: TimeUnit): String = unit match {
      case NANOSECONDS => "ns"
      case MICROSECONDS => "µs"
      case MILLISECONDS => "ms"
      case SECONDS => "s"
      case MINUTES => "min"
      case HOURS => "h"
      case DAYS => "d"
    }
  }

}
