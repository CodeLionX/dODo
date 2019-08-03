package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import com.github.codelionx.dodo.Settings
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

  case class StateVersion(failedNode: ActorRef, versionNr: Int)
}

class StateReplicator(master: ActorRef) extends Actor with ActorLogging {
  import StateReplicator._

  private val replicateStateInterval: FiniteDuration = Settings(context.system).stateReplicationInterval
  private val neighborStates: mutable.Map[ActorRef, (Queue[(Seq[Int], Seq[Int])], Int)] = mutable.Map.empty
  private var stateVersion: Int = 0
  private var leftReplicator: ActorRef = Actor.noSender
  private var rightReplicator: ActorRef = Actor.noSender

  override def preStart(): Unit = {
    log.info("Starting {}", name)
    Reaper.watchWithDefault(self)
  }

  override def receive: Receive = uninitialized(foundRightNeighbor = false, foundLeftNeighbor = false)

  def uninitialized(foundRightNeighbor: Boolean, foundLeftNeighbor: Boolean): Receive =
    stateRecoveryHandling orElse
    stateReceptionHandling orElse {
      case LeftNeighborRef(leftNeighbor) =>
        leftReplicator = leftNeighbor
        if (foundRightNeighbor) {
          startReplication()
        } else {
          context.become(uninitialized(foundRightNeighbor, foundLeftNeighbor = true))
        }

      case RightNeighborRef(rightNeighbor) =>
        rightReplicator = rightNeighbor
        if (foundLeftNeighbor) {
          startReplication()
        } else {
          context.become(uninitialized(foundRightNeighbor = true, foundLeftNeighbor))
        }

      case akka.actor.Status.Failure(error) =>
        log.error("Could not find neighbor, because", error)
    }

  def initialized(): Receive =
    stateRecoveryHandling orElse
    stateReceptionHandling orElse {
      case CurrentState(state) =>
        sendStateViaStream(leftReplicator, state)
        sendStateViaStream(rightReplicator, state)

      case LeftNeighborRef(newNeighbour) =>
        updateLeftNeighbor(newNeighbour)

      case RightNeighborRef(newNeighbour) =>
        updateRightNeighbor(newNeighbour)

      case LeftNeighborDown(newNeighbour) =>
        sendStateVersionTo(leftReplicator, newNeighbour)
        leftReplicator = newNeighbour

      case RightNeighborDown(newNeighbour) =>
        sendStateVersionTo(rightReplicator, newNeighbour)
        rightReplicator = newNeighbour
    }

  def stateRecoveryHandling: Receive = {
    case StateVersion(failedNode, versionNr) =>
      log.info("{} has version {} of {}'s state.", sender.path, versionNr, failedNode.path)
      if (neighborStates.contains(failedNode)) {
        if (neighborStates(failedNode)._2 > versionNr) {
          log.info("Using my version of {}'s state", failedNode.path)
          master ! NewODCandidates(neighborStates(failedNode)._1)
        }
        neighborStates -= failedNode
      }
  }

  def stateReceptionHandling: Receive = {
    case SidechannelRef(sourceRef) =>
      log.debug("Receiving state over sidechannel from {}", sender)
      ActorStreamConnector.consumeSourceRefOfClassVia(sourceRef, classOf[StateOverStream], self)

    case StreamInit =>
      sender ! StreamACK

    case stateMessage: StateOverStream =>
      val (owner, state, version) = stateMessage.data
      log.debug("Received data over stream from {}.", owner.path)
      updateNeighborState(owner, state, version)
      sender ! StreamACK

    case StreamComplete =>
      log.debug("{} completed stream.", name)
      sender ! StreamACK
  }

  def sendStateVersionTo(lostReplicator: ActorRef, newNeighbor: ActorRef): Unit = {
    log.info(
      "{} neighbor {} down. Comparing version with {}.",
      if(lostReplicator == leftReplicator) "Left" else "Right",
      lostReplicator.path,
      newNeighbor.path
    )
    if (neighborStates.contains(lostReplicator)) {
      newNeighbor ! StateVersion(lostReplicator, neighborStates(lostReplicator)._2)
      log.info("My current state for {} is {}", lostReplicator.path, neighborStates(lostReplicator)._2)
    } else {
      newNeighbor ! StateVersion(lostReplicator, -1)
      log.info("I do not have a state for {}", lostReplicator.path)
    }
  }

  def sendStateViaStream(receiver: ActorRef, currentState: Queue[(Seq[Int], Seq[Int])]): Unit = {
    log.info("Sending state via sidechannel to {}", receiver)
    val versionedState = (self, currentState, stateVersion)
    val state = ActorStreamConnector.prepareSourceRef(versionedState)
    import context.dispatcher
    state pipeTo receiver
    stateVersion += 1
  }

  def updateLeftNeighbor(newNeighbour: ActorRef): Unit = {
    log.info("Setting {} as my left neighbor", newNeighbour.path)
    if (neighborStates.contains(leftReplicator)) {
      neighborStates -= leftReplicator
    }
    leftReplicator = newNeighbour
  }

  def updateRightNeighbor(newNeighbour: ActorRef): Unit = {
    log.info("Setting {} as my right neighbor", newNeighbour.path)
    if (neighborStates.contains(rightReplicator)) {
      neighborStates -= rightReplicator
    }
    rightReplicator = newNeighbour
  }

  def updateNeighborState(neighbor: ActorRef, state: Queue[(Seq[Int], Seq[Int])], versionNr: Int): Unit = {
    if (neighborStates.contains(neighbor) && neighborStates(neighbor)._2 < versionNr) {
      neighborStates(neighbor) = (state, versionNr)
    } else {
      neighborStates += neighbor -> (state, versionNr)
    }
    log.info("Received state with version Nr {} from {}", versionNr, neighbor.path)
    log.debug("Currently holding states of {}", neighborStates.keys)
  }

  def startReplication(): Unit = {
    import com.github.codelionx.dodo.GlobalImplicits._
    import context.dispatcher
    log.info("Found both neighbours. Replicating state every {}", replicateStateInterval.pretty)
    context.system.scheduler.schedule(0 seconds, replicateStateInterval, master, GetState)
    context.become(initialized())
  }
}
