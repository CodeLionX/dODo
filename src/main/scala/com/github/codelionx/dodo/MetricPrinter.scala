package com.github.codelionx.dodo

import java.time.{Instant, LocalDateTime, ZoneId}

import com.github.codelionx.dodo.MetricPrinter.ResultKey.ResultKey
import com.github.codelionx.dodo.MetricPrinter.TimingKey.TimingKey


object MetricPrinter {

  object TimingKey extends Enumeration {
    type TimingKey = Value
    val START, END, PRUNING_START, PRUNING_END, RECEIVED_REDUCED_COLUMNS = Value
  }

  object ResultKey extends Enumeration {
    type ResultKey = Value
    val CCs, OECs, ODs, OCDs, PROCCESSED_ITEMS = Value
  }

  def printHeader(): Unit =
    println("MetricType,Metric,Description,Value")

  def printTiming(name: TimingKey, millis: Long = System.currentTimeMillis()): Unit =
    println(s"Timing,${name},${LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of("UTC"))},${millis}")

  def printResult(name: ResultKey, value: Long, description: String = ""): Unit =
    println(s"Result,${name},${description},${value}")
}
