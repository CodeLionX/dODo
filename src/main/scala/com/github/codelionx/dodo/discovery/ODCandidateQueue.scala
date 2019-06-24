package com.github.codelionx.dodo.discovery

import akka.actor.ActorRef
import com.github.codelionx.dodo.DodoException
import com.github.codelionx.dodo.actors.ODMaster.StolenWork
import com.github.codelionx.dodo.actors.Worker.CheckForOD

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.util.{Failure, Success, Try}


object ODCandidateQueue {

  def empty(maxBatchSize: Int)(implicit parentActor: ActorRef): ODCandidateQueue =
    new ODCandidateQueue(mutable.Queue.empty, maxBatchSize, parentActor)

  type ODCandidate = (Seq[Int], Seq[Int])

  case class ODCandidateQueueException(msg: String) extends DodoException(msg)

  implicit class OptionOps[A](opt: Option[A]) {

    def toTry(msg: String): Try[A] = {
      opt
        .map(Success(_))
        .getOrElse(Failure(ODCandidateQueueException(msg)))
    }
  }

}

class ODCandidateQueue private(
                                private val odCandidates: mutable.Queue[ODCandidate],
                                maxBatchSize: Int,
                                parentActor: ActorRef
                              ) extends CandidateGenerator {

  import ODCandidateQueue._

  implicit val sender: ActorRef = parentActor

  private val pendingOdCandidates: mutable.Map[ActorRef, Queue[ODCandidate]] = mutable.Map.empty
  private val toBeStolenOdCandidates: mutable.Map[ActorRef, Queue[ODCandidate]] = mutable.Map.empty

  def initializeFrom(reducedColumnSet: Set[Int]): Unit = {
    val initialODs = generateFirstCandidates(reducedColumnSet)
    odCandidates ++= initialODs
  }

  def workAvailable: Boolean = odCandidates.nonEmpty

  def hasPendingWork: Boolean = pendingOdCandidates.nonEmpty

  def hasNoPendingWork: Boolean = !hasPendingWork

  def queueSize: Int = odCandidates.size

  def queueLength: Int = queueSize

  def pendingSize: Int = pendingOdCandidates.size

  def pendingLength: Int = pendingSize

  private val noop: (ActorRef, Seq[ODCandidate]) => Unit = (_, _) => ()

  def enqueue(work: Seq[ODCandidate]): Unit =
    odCandidates ++= work

  def enqueueNewAndAck(work: Seq[ODCandidate], worker: ActorRef): Unit = {
    odCandidates ++= work
    pendingOdCandidates -= worker
  }

  def sendEqualBatchToEach(
                            workers: Seq[ActorRef], reducedColumns: Set[Int]
                          )(
                            cb: (ActorRef, Seq[ODCandidate]) => Unit = noop
                          ): Unit = {
    val batchLength = math.min(
      queueLength / workers.size,
      maxBatchSize
    )
    workers.foreach(worker => {
      val batch = dequeueBatch(batchLength)
      worker ! CheckForOD(batch, reducedColumns)
      pendingOdCandidates += worker -> batch
    })
  }

  def sendBatchTo(worker: ActorRef, reducedColumns: Set[Int]): Try[(ActorRef, Queue[ODCandidate])] = {
    if (odCandidates.isEmpty) {
      Failure(ODCandidateQueueException(s"Can not send work to worker ${worker.path.name}. Work queue is empty."))
    } else {
      val batchLength = math.min(queueLength, maxBatchSize)
      val batch = dequeueBatch(batchLength)
      val pair = worker -> batch
      worker ! CheckForOD(batch, reducedColumns)
      pendingOdCandidates += pair
      Success(pair)
    }
  }

  def sendBatchToThief(thief: ActorRef, amount: Int): Unit = {
    // in case the length of odCandidates has shrunk since it was send to the WorkThief
    // always send halve of our work queue as maximum
    val updatedAmount = math.min(amount, queueLength / 2)
    val batch = dequeueBatch(updatedAmount)
    toBeStolenOdCandidates += (thief -> batch)
    thief ! StolenWork(batch)
  }

  def recoverStolenCandidates(thief: ActorRef): Try[Unit] = {
    toBeStolenOdCandidates
      .remove(thief)
      .toTry(s"No candidates for thief ${thief.path} cached!")
      .map(stolenCandidates =>
        // store candidates back in our own queue
        odCandidates ++= stolenCandidates
      )
  }

  def ackStolenCandidates(thief: ActorRef): Unit =
    toBeStolenOdCandidates.remove(thief)


  private def dequeueBatch(size: Int): Queue[ODCandidate] = Queue(
    (0 until size).flatMap { _ =>
      Try {
        odCandidates.dequeue()
      }.toOption
    }: _*
  )
}
