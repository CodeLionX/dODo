package com.github.codelionx.dodo.actors

import java.io.{BufferedWriter, File, FileWriter}

import akka.actor.{Actor, ActorLogging, Props}
import com.github.codelionx.dodo.MetricPrinter.ResultKey
import com.github.codelionx.dodo.{MetricPrinter, Settings}


object ResultCollector {

  val name = "resultcollector"

  def props(): Props = Props[ResultCollector]

  case class ConstColumns(ccs: Seq[String])

  case class OrderEquivalencies(oe: Map[String, Seq[String]])

  case class Results(ods: Seq[(Seq[String], Seq[String])], ocds: Seq[(Seq[String], Seq[String])])

}


class ResultCollector extends Actor with ActorLogging {

  import ResultCollector._


  private val settings = Settings(context.system)

  private var odsFound = 0L
  private var ocdsFound = 0L

  // FileWriter
  val bw = new BufferedWriter(new FileWriter(new File(settings.outputFilePath)))

  override def preStart(): Unit =
    Reaper.watchWithDefault(self)

  override def postStop(): Unit = {
    bw.close()
    log.info("{} ODs found", odsFound + ocdsFound)
    MetricPrinter.printResult(ResultKey.ODs, odsFound, description = "just ODs")
    MetricPrinter.printResult(ResultKey.OCDs, ocdsFound, description = "just OCDs")
  }

  override def receive: Receive = {
    case ConstColumns(ccs) =>
      write("Constant columns: " + prettyList(ccs))
      MetricPrinter.printResult(ResultKey.CCs, ccs.size, description = "Constant Columns")

    case OrderEquivalencies(oes) =>
      write(
        s"Order equivalent: \n${
          oes
            .filter(oe => oe._2.nonEmpty)
            .map(oe => oe._1.toString + " ↔ " + prettyList(oe._2) + "\n")
            .mkString
        }".stripMargin
      )
      MetricPrinter.printResult(ResultKey.OECs, oes.size, description = "Order Equivalent Columns")

    case Results(ods, ocds) =>
      for (od <- ods) {
        val left = prettyList(od._1)
        val right = prettyList(od._2)
        write(s"OD: $left ↦ $right")
        odsFound += 1
      }
      for (ocd <- ocds) {
        val left = prettyList(ocd._1)
        val right = prettyList(ocd._2)
        if (settings.ocdComparability) {
          ocdsFound += 1
        }
        write(s"OCD: $left ~ $right")
      }

    case _ => log.info("Unknown message received")
  }

  def write(message: String): Unit = {
    bw.write(message + "\n")
    if (settings.outputToConsole)
      log.info(message)
  }

  def prettyList(l: Seq[String]): String = {
    val newString: StringBuilder = new StringBuilder()
    if (l.nonEmpty) {
      newString.append(l.head)
      l.tail.foreach(elem => newString.append(", ").append(elem))
    }
    newString.toString()
  }
}
