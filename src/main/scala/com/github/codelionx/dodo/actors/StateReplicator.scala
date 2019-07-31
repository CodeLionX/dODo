package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.github.codelionx.dodo.actors.ClusterListener.{LeftNeighborDown, LeftNeighborRef, RightNeighborDown, RightNeighborRef}
import com.github.codelionx.dodo.actors.Worker.NewODCandidates

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

object StateReplicator {

  val name = "statereplicator"

  def props(master: ActorRef): Props = Props( new StateReplicator(master))

  // messages
  case object GetState

  case class CurrentState(state: Queue[(Seq[Int], Seq[Int])])

  case class ReplicateState(queue: Queue[(Seq[Int], Seq[Int])], versionNr: Int)

  case class StateVersion(failedNode: ActorRef, versionNr: Int)

  val replicateStateInterval: FiniteDuration = 5 seconds
}

class StateReplicator(master: ActorRef) extends Actor with ActorLogging {
  import StateReplicator._

  private var stateVersion: Int = 0
  private var neighbourStates: mutable.Map[ActorRef, (Queue[(Seq[Int], Seq[Int])], Int)] = mutable.Map.empty
  private var leftNode: ActorRef = Actor.noSender
  private var rightNode: ActorRef = Actor.noSender

  override def preStart(): Unit = {
    log.info("Starting {}", name)
    Reaper.watchWithDefault(self)
  }

  override def receive: Receive = uninitialized(false, false)

  def uninitialized(foundRightNeighbour: Boolean, foundLeftNeighbour: Boolean): Receive = {
    case LeftNeighborRef(leftNeighbour) =>
      updateLeftNeighbour(leftNeighbour)
      if (foundRightNeighbour) {
        startReplication()
      } else {
        context.become(uninitialized(foundRightNeighbour, true))
      }

    case RightNeighborRef(rightNeighbour) =>
      updateRightNeighbour(rightNeighbour)
      if (foundLeftNeighbour) {
        startReplication()
      } else {
        context.become(uninitialized(true, foundLeftNeighbour))
      }
  }

  def initialized(): Receive = {
    case CurrentState(state) =>
      log.info("Replicating current state to both neighbours")
      replicateState(state)

    case ReplicateState(state, versionNr) =>
      if (neighbourStates.contains(sender) && neighbourStates(sender)._2 < versionNr) {
        neighbourStates(sender) = (state, versionNr)
      } else {
        neighbourStates += sender -> (state, versionNr)
      }
      log.info("Received state with version Nr {} from {}", versionNr, sender)

    case LeftNeighborRef(newNeighbour) =>
      updateLeftNeighbour(newNeighbour)

    case RightNeighborRef(newNeighbour) =>
      updateRightNeighbour(newNeighbour)

    case LeftNeighborDown(newNeighbour) =>
      log.info("Left neighbour down. Comparing version with {}. My current version is {}", newNeighbour.path, neighbourStates(leftNode)._2)
      newNeighbour ! StateVersion(leftNode, neighbourStates(leftNode)._2)
      updateLeftNeighbour(newNeighbour)

    case RightNeighborDown(newNeighbour) =>
      log.info("Left neighbour down. Comparing version with {}. My current version is {}", newNeighbour.path, neighbourStates(leftNode)._2)
      newNeighbour ! StateVersion(rightNode, neighbourStates(rightNode)._2)
      updateRightNeighbour(newNeighbour)

    case StateVersion(failedNode, versionNr) =>
      log.info("{} has version {} of {}'s state. I have {}", sender, versionNr, failedNode, neighbourStates(failedNode)._2)
      if (neighbourStates(failedNode)._2 > versionNr) {
        master ! NewODCandidates(neighbourStates(failedNode)._1)
      }
      neighbourStates -= failedNode
  }

  def replicateState(currentState: Queue[(Seq[Int], Seq[Int])]): Unit = {
    sendState(leftNode, currentState)
    sendState(rightNode, currentState)
  }

  def sendState(receiver: ActorRef, currentState: Queue[(Seq[Int], Seq[Int])]): Unit = {
    receiver ! ReplicateState(currentState, stateVersion)
    log.info("Sending {} my current state", receiver)
    stateVersion += 1
  }

  def updateLeftNeighbour(newNeighbour: ActorRef): Unit = {
    // figure out on which side this new neighbour is
    log.info("Setting {} as my left neighbour", newNeighbour)
    neighbourStates -= leftNode
    leftNode = newNeighbour
  }

  def updateRightNeighbour(newNeighbour: ActorRef): Unit = {
    // figure out on which side this new neighbour is
    log.info("Setting {} as my right neighbour", newNeighbour)
    neighbourStates -= rightNode
    rightNode = newNeighbour
  }

  def startReplication(): Unit = {
    import com.github.codelionx.dodo.GlobalImplicits._
    import context.dispatcher
    log.info("Found both neighbours. Replicating state every {}", replicateStateInterval.pretty)
    context.system.scheduler.schedule(0 seconds, replicateStateInterval, master, GetState)
    context.become(initialized())
  }
}
