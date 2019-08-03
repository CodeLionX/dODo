package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import com.github.codelionx.dodo.actors.ClusterListener.{LeftNeighborDown, LeftNeighborRef, RightNeighborDown, RightNeighborRef}
import com.github.codelionx.dodo.actors.DataHolder.SidechannelRef
import com.github.codelionx.dodo.actors.Worker.NewODCandidates
import com.github.codelionx.dodo.sidechannel.ActorStreamConnector
import com.github.codelionx.dodo.sidechannel.StreamedDataExchangeProtocol.{StateOverStream, StreamACK, StreamComplete, StreamInit}

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

  val replicateStateInterval: FiniteDuration = 10 seconds
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
      leftNode = leftNeighbour
      if (foundRightNeighbour) {
        startReplication()
      } else {
        context.become(uninitialized(foundRightNeighbour, true))
      }

    case RightNeighborRef(rightNeighbour) =>
      rightNode = rightNeighbour
      if (foundLeftNeighbour) {
        startReplication()
      } else {
        context.become(uninitialized(true, foundLeftNeighbour))
      }
  }

  def initialized(): Receive = {
    case CurrentState(state) =>
      replicateStateViaStream(state)

    case ReplicateState(state, versionNr) =>
      updateNeighboursState(sender, state, versionNr)

    case LeftNeighborRef(newNeighbour) =>
      updateLeftNeighbour(newNeighbour)

    case RightNeighborRef(newNeighbour) =>
      updateRightNeighbour(newNeighbour)

    case LeftNeighborDown(newNeighbour) =>
      log.info("Left neighbour {} down. Comparing version with {}.", leftNode.path, newNeighbour.path)
      if (neighbourStates.contains(leftNode)) {
        newNeighbour ! StateVersion(leftNode, neighbourStates(leftNode)._2)
        log.info("My current state for {} is {}", leftNode.path, neighbourStates(leftNode)._2)
      } else {
        newNeighbour ! StateVersion(leftNode, -1)
        log.info("I do not have a state for {}", leftNode.path)
      }
      leftNode = newNeighbour

    case RightNeighborDown(newNeighbour) =>
      log.info("Right neighbour {} down. Comparing version with {}.", rightNode.path, newNeighbour.path)
      if (neighbourStates.contains(rightNode)) {
        newNeighbour ! StateVersion(rightNode, neighbourStates(rightNode)._2)
        log.info("My current state for {} is {}", rightNode.path, neighbourStates(rightNode)._2)
      } else {
        newNeighbour ! StateVersion(rightNode, -1)
        log.info("I do not have a state for {}", rightNode.path)
      }
      rightNode = newNeighbour

    case StateVersion(failedNode, versionNr) =>
      log.info("{} has version {} of {}'s state.", sender.path, versionNr, failedNode.path)
      if (neighbourStates.contains(failedNode)) {
        if (neighbourStates(failedNode)._2 > versionNr) {
          log.info("Using my version of {}'s state", failedNode.path)
          master ! NewODCandidates(neighbourStates(failedNode)._1)
        }
        neighbourStates -= failedNode
      }

    case SidechannelRef(sourceRef) =>
      log.debug("Receiving state over sidechannel from {}", sender)
      ActorStreamConnector.consumeSourceRefOfClassVia(sourceRef, classOf[StateOverStream], self)

    case stateMessage: StateOverStream =>
      log.debug("Received data over stream from {}.", stateMessage.data._1.path)
      updateNeighboursState(stateMessage.data._1, stateMessage.data._2, stateMessage.data._3)
      sender ! StreamACK

    case StreamComplete =>
      log.debug("Stream completed!", name)
      sender ! StreamACK

    case StreamInit =>
      sender ! StreamACK
  }

  def replicateStateViaStream(currentState: Queue[(Seq[Int], Seq[Int])]): Unit = {
    sendStateViaStream(leftNode, currentState)
    sendStateViaStream(rightNode, currentState)
  }

  def sendStateViaStream(receiver: ActorRef, currentState: Queue[(Seq[Int], Seq[Int])]): Unit = {
    log.info("Sending state via sidechannel to {}", receiver)
    val versionedState = (self, currentState, stateVersion)
    val state = ActorStreamConnector.prepareSourceRef(versionedState)
    import context.dispatcher
    state pipeTo receiver
    stateVersion += 1
  }

  def updateLeftNeighbour(newNeighbour: ActorRef): Unit = {
    log.info("Setting {} as my left neighbour", newNeighbour.path)
    if (neighbourStates.contains(leftNode)) {
      neighbourStates -= leftNode
    }
    leftNode = newNeighbour
  }

  def updateRightNeighbour(newNeighbour: ActorRef): Unit = {
    log.info("Setting {} as my right neighbour", newNeighbour.path)
    if (neighbourStates.contains(rightNode)) {
      neighbourStates -= rightNode
    }
    rightNode = newNeighbour
  }

  def updateNeighboursState(neighbour: ActorRef, state: Queue[(Seq[Int], Seq[Int])], versionNr: Int): Unit = {
    if (neighbourStates.contains(neighbour) && neighbourStates(neighbour)._2 < versionNr) {
      neighbourStates(neighbour) = (state, versionNr)
    } else {
      neighbourStates += neighbour -> (state, versionNr)
    }
    log.info("Received state with version Nr {} from {}", versionNr, neighbour.path)
    log.debug("Currently holding states of {}", neighbourStates.keys)
  }

  def startReplication(): Unit = {
    import com.github.codelionx.dodo.GlobalImplicits._
    import context.dispatcher
    log.info("Found both neighbours. Replicating state every {}", replicateStateInterval.pretty)
    context.system.scheduler.schedule(0 seconds, replicateStateInterval, master, GetState)
    context.become(initialized())
  }
}
