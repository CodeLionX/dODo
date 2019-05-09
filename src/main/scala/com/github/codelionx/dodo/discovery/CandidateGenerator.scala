package com.github.codelionx.dodo.discovery

import scala.collection.immutable.Queue


trait CandidateGenerator {
  def generateFirstCandidates(columns: Set[Int]): Queue[(List[Int], List[Int])] = {
    val fixedOrderCols = columns.toList
    val candidates = fixedOrderCols.flatMap(col1 => fixedOrderCols.drop(fixedOrderCols.indexOf(col1) + 1).map(col2 => (List(col1), List(col2))))
    Queue(candidates: _*)
  }

  def generateODCandidates(columns: Set[Int], od: (List[Int], List[Int])): Queue[(List[Int], List[Int])] = {
    val newCandidates = columns.map(col => (od._1 :+ col, od._2))
    Queue(newCandidates.toList: _*)
  }
}
