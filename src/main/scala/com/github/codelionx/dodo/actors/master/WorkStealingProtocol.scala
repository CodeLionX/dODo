package com.github.codelionx.dodo.actors.master

import akka.actor.{ActorRef, Terminated}
import akka.cluster.pubsub.DistributedPubSubMediator.Publish

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.language.postfixOps


object WorkStealingProtocol {

  /**
    * Marker trait for all messages that are involved in the work stealing protocol.
    */
  trait WorkStealingMessage

  case object GetWorkLoad extends WorkStealingMessage

  case object WorkLoadTimeout extends WorkStealingMessage

  case class WorkLoad(queueSize: Int, pendingSize: Int) extends WorkStealingMessage


  case class StealWork(amount: Int) extends WorkStealingMessage

  case class StolenWork(work: Queue[(Seq[Int], Seq[Int])]) extends WorkStealingMessage

  case object AckStolenWork extends WorkStealingMessage

  case class AckStolenWorkTimeout(workThief: ActorRef) extends WorkStealingMessage

}

trait WorkStealingProtocol {
  this: ODMaster =>

  import WorkStealingProtocol._


  private var otherWorkloads: Seq[(Int, Int, ActorRef)] = Seq.empty
  private var pendingResponses: Set[ActorRef] = Set.empty

  def workStealingHandling(allowStealing: Boolean): Receive = {
    case GetWorkLoad if sender == self => // ignore

    case GetWorkLoad if sender != self =>
      log.debug("Sending workload to {}", sender.path)
      sender ! WorkLoad(candidateQueue.queueSize, candidateQueue.pendingSize)

    case StealWork(_) if !allowStealing =>
      log.info("No work to send")
      sender ! StolenWork(Queue.empty)

    case StealWork(amount: Int) if allowStealing =>
      log.info("Sending work to {}", sender.path)
      candidateQueue.sendBatchToThief(sender, amount)
      context.watch(sender)

    case AckStolenWork if allowStealing =>
      candidateQueue.ackStolenCandidates(sender)
      context.unwatch(sender)

    case Terminated(remoteMaster) if allowStealing =>
      context.unwatch(sender)
      log.warning("Work thief {} did not acknowledge stolen work and died.", remoteMaster.path)
      candidateQueue.recoverStolenCandidates(remoteMaster) match {
        case scala.util.Success(_) =>
          log.info("Stolen work queue was recovered!")
        case scala.util.Failure(f) =>
          log.error("Work got lost from remote master {}! {}", remoteMaster, f)
      }
  }

  def startWorkStealing(): Unit = {
    log.info("Starting work stealing protocol")
    log.debug("Asking for workloads")
    otherWorkloads = Seq.empty
    pendingResponses = Set.empty
    masterMediator ! Publish(ODMaster.workStealingTopic, GetWorkLoad)

    import context.dispatcher
    context.system.scheduler.scheduleOnce(3 second, self, WorkLoadTimeout)
    context.become(workStealing)
  }

  def workStealingImpl: Receive = {
    case WorkLoad(queueSize: Int, pendingSize: Int) =>
      log.debug("Received workload of size {} from {}", queueSize, sender)
      otherWorkloads :+= (queueSize, pendingSize, sender)

    case WorkLoadTimeout if otherWorkloads.isEmpty =>
      // make sure, we really don't have any work left
      if(candidateQueue.noWorkAvailable && candidateQueue.hasNoPendingWork) {
        log.debug("Got no workload from the other nodes and we still have no work.")
        startDowningProtocol()
      } else {
        log.info("Got no workload from the other nodes, but we have work again. Exiting work stealing protocol")
        context.become(findingODs())
      }

    case WorkLoadTimeout if otherWorkloads.nonEmpty =>
      val sortedWorkloads = otherWorkloads.sorted
      val sum = sortedWorkloads.map(_._1).sum
      val averageWl: Int = sum / (sortedWorkloads.size + 1)
      val pendingSum = sortedWorkloads.map(_._2).sum
      var ownWorkLoad = 0

      log.debug(
        "Work stealing status: averageWl={}, others' pendingSum={}, our pending ODs={}",
        averageWl,
        pendingSum,
        candidateQueue.pendingSize
      )
      if (averageWl > 0) {
        // there is work to steal, steal some from the busiest nodes
        pendingResponses = sortedWorkloads.flatMap {
          case (otherSize, _, otherRef) =>
            val amountToSteal = Seq(
              otherSize - averageWl,
              averageWl - ownWorkLoad,
              settings.workers * settings.maxBatchSize
            ).min
            if (amountToSteal > 0) {
              otherRef ! StealWork(amountToSteal)
              log.info("Stealing {} elements from {}", amountToSteal, otherRef)
              ownWorkLoad += amountToSteal
              context.watch(otherRef)
              Some(otherRef)
            } else {
              None
            }
        }.toSet
      } else if (candidateQueue.noWorkAvailable && candidateQueue.hasNoPendingWork) {
        // we have no work left and got no work from our work stealing attempt
        if (pendingSum > 0) {
          // others will soon have work for us to steal again
          log.info("No work received, will ask again in three seconds")
          import context.dispatcher
          context.system.scheduler.scheduleOnce(3 second, self, startWorkStealing())
        }
        // check if ALL nodes are completely out of work
        log.debug("All nodes are out of work")
        startDowningProtocol()
      } else {
        // nothing to steal, but we still wait for worker results, wait for them
        log.info("Nothing to steal, but our workers are still busy. Exiting work stealing protocol")
        context.become(findingODs())
      }

    case Terminated(otherMaster) =>
      log.warning("{} terminated while we were trying to steal work from it!", otherMaster.path)
      context.unwatch(otherMaster)
      updatePendingResponse(otherMaster)

    case StolenWork(stolenQueue) =>
      log.info("Received {} candidates from {}", stolenQueue.size, sender)
      candidateQueue.enqueue(stolenQueue)
      sender ! AckStolenWork
      sendWorkToIdleWorkers()
      context.unwatch(sender)
      updatePendingResponse(sender)
  }

  private def updatePendingResponse(actorToRemove: ActorRef): Unit = {
    pendingResponses -= actorToRemove
    if (pendingResponses.isEmpty) {
      if (candidateQueue.noWorkAvailable && candidateQueue.hasNoPendingWork) {
        log.info("Work stealing failed")
        startDowningProtocol()
      } else {
        log.info("Work stealing was successful")
        context.become(findingODs())
      }
    }
  }
}
