package com.github.codelionx.dodo.actors

import java.io.{BufferedWriter, File, FileWriter}

import akka.actor.{Actor, ActorLogging, Props}
import com.github.codelionx.dodo.Settings


object ResultCollector {

  val name = "resultcollector"

  def props(): Props = Props[ResultCollector]

  case class ConstColumns(ccs: Seq[Int])

  case class OrderEquivalencies(oe: Map[Int, Seq[Int]])

  case class OD(od: (Seq[Int], Seq[Int]))

  case class OCD(ocd: (Seq[Int], Seq[Int]))

}


class ResultCollector extends Actor with ActorLogging {

  import ResultCollector._


  private val settings = Settings(context.system)

  private var odsFound = 0

  // FileWriter
  val bw = new BufferedWriter(new FileWriter(new File(settings.outputFilePath)))

  override def preStart(): Unit = {
    log.info(s"Starting $name")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit = {
    bw.close()
    log.info(s"$odsFound ODs found")
    log.info(s"Stopping $name")
  }

  override def receive: Receive = {
    case ConstColumns(ccs) =>
      write("Constant columns: " + prettyList(ccs))

    case OrderEquivalencies(oes) =>
      write("Order Equivalent:")
      oes.foreach(oe =>
        if (oe._2.nonEmpty) {
          write(oe._1.toString + ", " + prettyList(oe._2))
        }
      )

    case OD(od) =>
      val left = prettyList(od._1)
      val right = prettyList(od._2)
      write(s"OD: $left => $right")
      odsFound += 1
      // TODO: extract order equivalent ods

    case OCD(ocd) =>
      val left = prettyList(ocd._1)
      val right = prettyList(ocd._2)
      if (settings.ocdComparability) {
        odsFound += 1
      }
      write(s"OCD: $left ~ $right")

    case _ => log.info("Unknown message received")
  }

  def write(message: String): Unit = {
    bw.write(message + "\n")
    log.info(message)
  }

  def prettyList(l: Seq[Int]): String = {
    val newString: StringBuilder = new StringBuilder()
    if (l.nonEmpty) {
      newString.append(l.head)
      l.tail.foreach(elem => newString.append(", ").append(elem))
    }
    newString.toString()
  }
}
