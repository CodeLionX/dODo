package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSelection, Address, Props, RootActorPath, Terminated}
import akka.cluster.Cluster
import com.github.codelionx.dodo.actors.ClusterListener.{GetLeftNeighbor, GetRightNeighbor, LeftNeighbor, RightNeighbor}
import com.github.codelionx.dodo.actors.Worker.ODsToCheck

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

object StateReplicator {

  val name = "statereplicator"

  def props(master: ActorRef, clusterListener: ActorRef): Props = Props( new StateReplicator(master, clusterListener))

  // messages
  case object GetState

  case class CurrentState(state: Queue[(Seq[Int], Seq[Int])])

  case object NewNeighbourIntroduction

  case class ReplicateState(queue: Queue[(Seq[Int], Seq[Int])], versionNr: Int)

  case class StateVersion(failedNode: ActorRef, versionNr: Int)

  val replicateStateInterval: FiniteDuration = 5 seconds

  private case object UpdateState
}

class StateReplicator(master: ActorRef, clusterListener: ActorRef) extends Actor with ActorLogging {
  import StateReplicator._

  private var stateVersion: Int = 0
  private var neighbourStates: mutable.Map[ActorRef, (Queue[(Seq[Int], Seq[Int])], Int)] = mutable.Map.empty
  private var leftNode: ActorRef = _
  private var rightNode: ActorRef = _
  private var currentState: Queue[(Seq[Int], Seq[Int])] = Queue.empty

  private val userGuardian = "user"
  private val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    log.info("Starting {}", name)
    Reaper.watchWithDefault(self)
    log.info("Replicating state every {} seconds", replicateStateInterval)
    updateNeighbours()
  }

  override def receive: Receive = uninitialized(cluster.selfAddress, cluster.selfAddress)

  def uninitialized(rightAddress: Address, leftAddress: Address): Receive = {
    case akka.actor.Status.Failure(error) =>
      log.info("Cannot sync state because of {}", error)

    case RightNeighbor(address) =>
      if(address.address != rightAddress) {
        val newNeighbour = stateReplicatorFromAddress(address)
        newNeighbour ! NewNeighbourIntroduction
        // this should only happen when we are looking for the other neighbour of a recently failed node
        log.info("Telling {} I am their new neighbour", newNeighbour)
        context.become(uninitialized(address.address, leftAddress))
      }

    case LeftNeighbor(address) =>
      if(address.address != leftAddress) {
        val newNeighbour = stateReplicatorFromAddress(address)
        newNeighbour ! NewNeighbourIntroduction
        // this should only happen when we are looking for the other neighbour of a recently failed node
        log.info("Telling {} I am their new neighbour", newNeighbour)
        context.become(uninitialized(rightAddress, address.address))
      }

    case ReplicateState(queue, versionNr) =>
      log.info("Received current state from {}", sender.path)
      neighbourStates += sender -> (queue, versionNr)
      addNeighbour(sender, leftAddress, rightAddress)

    case NewNeighbourIntroduction =>
      log.debug("{} has joined the cluster", sender.path)
      neighbourStates += sender -> (Queue.empty, -1)
      sender ! ReplicateState(Queue.empty, -1)
      updateNeighbours()
      addNeighbour(sender, leftAddress, rightAddress)

  }

  def initialized(): Receive = {
    case UpdateState =>
      master ! GetState

    case CurrentState(state) =>
      log.info("Replicating current state to both neighbours")
      currentState = state
      replicateState()

    case NewNeighbourIntroduction =>
      log.debug("{} is a new neighbour", sender.path)
      neighbourStates += sender -> (Queue.empty, -1)
      // figure out on which side this new neighbour is and send state
      updateNeighbours()

    case ReplicateState(queue, versionNr) =>
      if (neighbourStates.contains(sender)) {
        if (neighbourStates(sender)._2 < versionNr) {
          neighbourStates += sender -> (queue, versionNr)
          log.info("Received current state from {}", sender)
        }
      } else {
        updateNeighbours()
        neighbourStates += sender -> (queue, versionNr)
      }

    case Terminated(otherMaster) =>
      log.info("Detected {} has terminated", otherMaster)
      context.unwatch(otherMaster)
      if (otherMaster == rightNode) {
        clusterListener ! GetRightNeighbor
      } else {
        clusterListener ! GetLeftNeighbor
      }

    case StateVersion(failedNode, versionNr) =>
      log.info("{} has version {} of {}'s state", sender, versionNr, failedNode)
      neighbourStates += sender -> (Queue.empty, -1)
      if (neighbourStates(failedNode)._2 > versionNr) {
        master ! ODsToCheck(neighbourStates(failedNode)._1)
      }
      neighbourStates -= failedNode
      // this will also replicate the new state to the updated neighbours
      updateNeighbours()

    case RightNeighbor(address) =>
      if (address.address != rightNode.path.address) {
        log.info("We have a new neighbour")
        val rightMaster = neighbourStates.keys.find(_.path.address == address.address)
        // check if we are updating because of an Introduction or a node-Failure
        if (rightMaster.isDefined && rightMaster.get != leftNode) {
          neighbourStates -= rightNode
          context.unwatch(rightNode)
          rightNode = rightMaster.get
          context.watch(rightNode)
          replicateState()
          log.info("{} set as right neighbour", rightMaster.get)
        } else {
          // it seems the previous neighbour failed
          val newNeighbour = stateReplicatorFromAddress(address)
          newNeighbour ! StateVersion(rightNode, neighbourStates(rightNode)._2)
          // this should only happen when we are looking for the other neighbour of a recently failed node
          log.info("Syncing for {}'s state with {}", rightNode, newNeighbour)
        }
      }

    case LeftNeighbor(address) =>
      if (address.address != leftNode.path.address) {
        val leftMaster = neighbourStates.keys.find(_.path.address == address.address)
        if (leftMaster.isDefined) {
          neighbourStates -= leftNode
          context.unwatch(leftNode)
          leftNode = leftMaster.get
          context.watch(leftNode)
          replicateState()
          log.info("{} set as left neighbour", leftMaster.get)
        } else {
          val newNeighbour = stateReplicatorFromAddress(address)
          newNeighbour ! StateVersion(leftNode, neighbourStates(leftNode)._2)
          // this should only happen when we are looking for the other neighbour of a recently failed node
          log.info("Syncing for {}'s state with {}", leftNode, newNeighbour)
        }
      }

    case akka.actor.Status.Failure(_) =>
      if(neighbourStates.size > 1) {
        // only the one that has not failed will answer
        rightNode ! StateVersion(leftNode, neighbourStates(leftNode)._2)
        leftNode ! StateVersion(rightNode, neighbourStates(rightNode)._2)
      } else {
        log.info("Not enough neighbours anymore")
        context.become(uninitialized(leftNode.path.address, rightNode.path.address))
      }
  }

  def stateReplicatorFromAddress(address: RootActorPath): ActorSelection = {
    context.actorSelection(address / userGuardian / ODMaster.name / name)
  }

  def replicateState(): Unit = {
    if (leftNode != null) {
      sendState(leftNode)
    }
    if (rightNode != null) {
      sendState(rightNode)
    }
  }

  def sendState(receiver: ActorRef): Unit = {
    receiver ! ReplicateState(currentState, stateVersion)
    log.info("Sending {} my current state", receiver)
    stateVersion += 1
  }

  def updateNeighbours(): Unit = {
    clusterListener ! GetLeftNeighbor
    clusterListener ! GetRightNeighbor
  }

  def addNeighbour(potentialNeighbour: ActorRef, leftAddress: Address, rightAddress: Address): Unit = {
    // figure out on which side this new neighbour is

    if (potentialNeighbour.path.address == rightAddress) {
      log.info("Setting {} as my right neighbour", potentialNeighbour)
      rightNode = potentialNeighbour
      context.watch(potentialNeighbour)
    }
    if (potentialNeighbour.path.address == leftAddress) {
      log.info("Setting {} as my left neighbour", potentialNeighbour)
      leftNode = potentialNeighbour
      context.watch(potentialNeighbour)
    }
    if (neighbourStates.contains(leftNode) && neighbourStates.contains(rightNode)) {
      log.info("Found both neighbours")
      import context.dispatcher
      context.system.scheduler.schedule(0 seconds, replicateStateInterval, master, UpdateState)
      context.become(initialized())
    }
  }
}
