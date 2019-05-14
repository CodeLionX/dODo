package com.github.codelionx.dodo.discovery

import scala.collection.immutable.Queue


trait CandidateGenerator {
  def generateFirstCandidates(columns: Set[Int]): Queue[(Seq[Int], Seq[Int])] = {
    val candidates = columns.toSeq
      .combinations(2)
      .map(l => Seq(l.head) -> Seq(l(1))) // list to tuple: only safe in this special case (we know length == 2)
      .toSeq
    Queue(candidates: _*)
  }

  def generateODCandidates(columns: Set[Int], od: (Seq[Int], Seq[Int])): Queue[(Seq[Int], Seq[Int])] = {
    val aPlus = columns -- od._1.toSet -- od._2.toSet
    val newCandidates = aPlus.map(col => (od._1 :+ col, od._2))
    Queue(newCandidates.toList: _*)
  }
}
