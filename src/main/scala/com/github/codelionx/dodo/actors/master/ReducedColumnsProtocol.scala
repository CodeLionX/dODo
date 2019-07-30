package com.github.codelionx.dodo.actors.master


object ReducedColumnsProtocol {

  /**
    * Marker trait for all messages that are involved in the reduced columns protocol.
    */
  trait ReducedColumnsMessage

  case object GetReducedColumns extends ReducedColumnsMessage

  case class ReducedColumns(cols: Set[Int]) extends ReducedColumnsMessage

}

trait ReducedColumnsProtocol {
  this: ODMaster =>

  import ReducedColumnsProtocol._


  private var reducedColumns: Set[Int] = Set.empty

  def setReducedColumns(columns: Set[Int]): Unit = {
    if (reducedColumns.nonEmpty) {
      throw new IllegalArgumentException("Reduced columns are already set!")
    }
    reducedColumns = columns
  }

  def getReducedColumns: Set[Int] = reducedColumns

  def reducedColumnsHandling(): Receive = {
    case GetReducedColumns if sender == self => // ignore

    case GetReducedColumns if reducedColumns.nonEmpty =>
      log.info("Sending reduced columns to {}", sender.path)
      sender ! ReducedColumns(reducedColumns)

    case GetReducedColumns =>
      log.warning(
        "Received request to supply reduced columns to {}, but they are not ready yet. Ignoring",
        sender.path
      )
  }
}
