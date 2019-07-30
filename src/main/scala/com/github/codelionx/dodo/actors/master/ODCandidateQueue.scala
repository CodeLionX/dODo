package com.github.codelionx.dodo.actors.master

import akka.actor.ActorRef
import com.github.codelionx.dodo.DodoException
import com.github.codelionx.dodo.actors.Worker.CheckForOD
import com.github.codelionx.dodo.actors.master
import com.github.codelionx.dodo.actors.master.WorkStealingProtocol.StolenWork
import com.github.codelionx.dodo.discovery.CandidateGenerator

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.util.{Failure, Success, Try}


object ODCandidateQueue {

  /**
    * Creates a new empty candidate queue.
    *
    * @param maxBatchSize configured batch size, used to communicate to workers
    * @param parentActor  used as sender of the work messages send to the workers
    */
  def empty(maxBatchSize: Int)(implicit parentActor: ActorRef): ODCandidateQueue =
    new ODCandidateQueue(mutable.Queue.empty, maxBatchSize, parentActor)

  // type alias for easier function signatures
  type ODCandidate = (Seq[Int], Seq[Int])

  implicit class OptionOps[A](opt: Option[A]) {

    /**
      * Converts an option to a try using a
      * [[master.ODCandidateQueue.ODCandidateQueueException]]
      * as exception type with the supplied message `msg` on failure.
      */
    def toTry(msg: String): Try[A] = {
      opt
        .map(Success(_))
        .getOrElse(Failure(ODCandidateQueueException(msg)))
    }
  }

  case class ODCandidateQueueException(msg: String) extends DodoException(msg)

}

class ODCandidateQueue private(
                                private val odCandidates: mutable.Queue[ODCandidateQueue.ODCandidate],
                                maxBatchSize: Int,
                                parentActor: ActorRef
                              ) extends CandidateGenerator {

  import ODCandidateQueue._


  // make parent actor available as implicit sender
  implicit val sender: ActorRef = parentActor

  // states capturing pending candidates
  private val pendingOdCandidates: mutable.Map[ActorRef, Queue[ODCandidate]] = mutable.Map.empty
  private val toBeStolenOdCandidates: mutable.Map[ActorRef, Queue[ODCandidate]] = mutable.Map.empty

  // callback-helper used in `sendEqualBatchToEach`
  private val noop: (ActorRef, Seq[ODCandidate]) => Unit = (_, _) => ()

  /**
    * Generates the first candidates taking `reducedColumnSet` into account.
    */
  def initializeFrom(reducedColumnSet: Set[Int]): Unit = {
    val initialODs = generateFirstCandidates(reducedColumnSet)
    odCandidates ++= initialODs
  }

  def workAvailable: Boolean = odCandidates.nonEmpty

  def noWorkAvailable: Boolean = !workAvailable

  def hasPendingWork: Boolean = pendingOdCandidates.nonEmpty

  def hasNoPendingWork: Boolean = !hasPendingWork

  def queueSize: Int = odCandidates.size

  def queueLength: Int = queueSize

  def pendingSize: Int = pendingOdCandidates.foldLeft(0)((sum, elem) => sum + elem._2.size) + toBeStolenOdCandidates.foldLeft(0)((sum, elem) => sum + elem._2.size)

  def pendingLength: Int = pendingSize

  def enqueue(work: Seq[ODCandidate]): Unit =
    odCandidates ++= work

  /**
    * Enqueues new work to the work queue, but also removes the pending work for the worker `worker`.
    */
  def enqueueNewAndAck(work: Seq[ODCandidate], worker: ActorRef): Unit = {
    odCandidates ++= work
    pendingOdCandidates -= worker
  }

  /**
    * Distributes all available work to the provided `workers` equally.
    * For each worker-batch combination the callback `cb` will be executed.
    *
    * After calling this method, the work queue will be empty, so `noWorkAvailable` and `hasPendingWork` will be
    * `true`.
    */
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
      cb(worker, batch)
    })
  }

  /**
    * Sends a batch of work to a single worker `worker` and moves the candidates to the pending queue.
    *
    * @return a [[scala.util.Success]] if batch was send and a [[scala.util.Failure]] if the batch was not send
    */
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

  /**
    * Sends a batch of `amount` candidates to the work thief `thief` and moves the send candidates in a pending state.
    *
    * Use [[ODCandidateQueue#ackStolenCandidates]] to completely remove candidates.
    * Use [[ODCandidateQueue#recoverStolenCandidates]] to move the pending stolen
    * candidates back into this work queue.
    */
  def sendBatchToThief(thief: ActorRef, amount: Int): Unit = {
    // in case the length of odCandidates has shrunk since it was send to the WorkThief
    // always send halve of our work queue as maximum
    val updatedAmount = math.min(amount, queueLength / 2)
    val batch = dequeueBatch(updatedAmount)
    toBeStolenOdCandidates += (thief -> batch)
    thief ! StolenWork(batch)
  }

  /**
    * Moves the candidates stolen by `thief` via
    * [[ODCandidateQueue#sendBatchToThief]] back into this work queue if they are
    * found in the pending stolen candidates queue.
    */
  def recoverStolenCandidates(thief: ActorRef): Try[Unit] = {
    toBeStolenOdCandidates
      .remove(thief)
      .toTry(s"No candidates for thief ${thief.path} cached!")
      .map(stolenCandidates =>
        // store candidates back in our own queue
        odCandidates ++= stolenCandidates
      )
  }

  /**
    * Completely removes the stolen candidates from this queue.
    *
    * @see [[ODCandidateQueue#sendBatchToThief]]
    */
  def ackStolenCandidates(thief: ActorRef): Unit =
    toBeStolenOdCandidates.remove(thief)


  private def dequeueBatch(size: Int): Queue[ODCandidate] = Queue(
    (0 until size).flatMap { _ =>
      Try {
        odCandidates.dequeue()
      }.toOption
    }: _*
  )


  /**
    * merges the pending and working queues to send to other masters during state replication
    */
  def shareableState(): Queue[ODCandidate] = {
    val merged = (pendingOdCandidates.map(_._2).flatten ++ toBeStolenOdCandidates.map(_._2).flatten ++ odCandidates).toList
    scala.collection.immutable.Queue(merged: _*)
  }
}
