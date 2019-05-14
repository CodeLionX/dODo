package com.github.codelionx.dodo.actors

import java.io.{BufferedWriter, File, FileWriter}

import akka.actor.{Actor, ActorLogging, ActorRef, Props}


object ResultCollector {

  val name = "resultcollector"

  def props(filename: String): Props = Props(new ResultCollector(filename))

  case class ConstColumns(ccs: Seq[Int])

  case class OrderEquivalencies(oe: Map[Int, Seq[Int]])

  case class OD(od: (Seq[Int], Seq[Int]))
}


class ResultCollector(filename: String) extends Actor with ActorLogging {

  import ResultCollector._

  private var orderEquivalencies : Map[Int, Seq[Int]] = Map.empty
  // FileWriter
  val file = new File(filename)
  val bw = new BufferedWriter(new FileWriter(file))

  override def preStart(): Unit = {
    log.info(s"Starting $name")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    bw.close()
    log.info(s"Stopping $name")

  override def receive: Receive = {
    case ConstColumns(ccs) =>
      bw.write("Constant columns: \n")
      bw.write(ccs.toString() + "\n")
    case OrderEquivalencies(oes) =>
      orderEquivalencies = oes
      bw.write("Order Equivalent: \n")
      oes.foreach(oe => bw.write(oe.toString() + "\n"))
    case OD(od) =>
      val left = od._1
      val right = od._2
      bw.write(s"OD: $left => $right \n")
      // TODO: extract order equivalent ods
    case _ => log.info("Unknown message received")
  }
}
