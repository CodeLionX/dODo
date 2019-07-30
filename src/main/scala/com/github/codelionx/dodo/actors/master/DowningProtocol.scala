package com.github.codelionx.dodo.actors.master

import akka.actor.Address
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberRemoved, UnreachableMember}
import akka.cluster.pubsub.DistributedPubSubMediator.Publish


object DowningProtocol {

  /**
    * Marker trait for all messages that are involved in the downing protocol.
    */
  trait DowningMessage

  case object GetWorkloadDowning extends DowningMessage

  case class WorkloadDowning(stillWorkAvailable: Boolean) extends DowningMessage

}

trait DowningProtocol {
  this: ODMaster =>

  import DowningProtocol._


  private var pendingMasters: Set[Address] = Set.empty

  /**
    * Handles downing messages from other masters.
    *
    * @param fakeWorkAvailable if `true` always responds to the other masters that we still have work available
    * @param virulent          if `true` a received
    *                          [[com.github.codelionx.dodo.actors.master.DowningProtocol.GetWorkloadDowning]] message will
    *                          move this actor in the downing state as well.
    */
  def downingHandling(fakeWorkAvailable: Boolean = false, virulent: Boolean = false): Receive = {
    case GetWorkloadDowning if sender == self => //ignore

    case GetWorkloadDowning if sender != self && fakeWorkAvailable =>
      log.info("Faking downing protocol workload response to have work available as response to {}", sender.path)
      sender ! WorkloadDowning(stillWorkAvailable = true)

      if (virulent) {
        log.warning("Received downing request prematurely... changing into downing protocol as well!")
        startDowningProtocol()
      }

    case GetWorkloadDowning if sender != self =>
      val workAvailable = candidateQueue.workAvailable && candidateQueue.hasPendingWork
      log.info(
        "Responding to downing workload with {} to {}",
        if (workAvailable) "work available" else "no work available",
        sender.path
      )
      sender ! WorkloadDowning(workAvailable)

      if (virulent && !workAvailable) {
        log.warning("Received downing request prematurely... changing into downing protocol as well!")
        startDowningProtocol()
      }
  }

  def startDowningProtocol(): Unit = {
    log.info("{} started downingProtocol", self.path.name)
    pendingMasters = Set.empty
    cluster.subscribe(self, classOf[MemberRemoved], classOf[UnreachableMember])

    context.become(downing)
  }

  def downingImpl: Receive = {
    case CurrentClusterState((_, _, nodeAddresses, _, _)) =>
      log.debug("Received current cluster state, {} nodes", nodeAddresses.size)
      if (nodeAddresses.size > 1) {
        masterMediator ! Publish(ODMaster.workStealingTopic, GetWorkloadDowning)
        pendingMasters = nodeAddresses - cluster.selfAddress
      } else {
        log.info("Last member in cluster and no work available locally.")
        cluster.unsubscribe(self)
        shutdown()
      }

    case MemberRemoved(node, _) =>
      log.info("Node ({}) left the cluster", node)
      // stop waiting for a workload message from this node
      removeFromPendingMasters(node.address)

    case UnreachableMember(node) =>
      log.info("Node ({}) detected unreachable, treated as if down", node)
      // stop waiting for a workload message from this node
      removeFromPendingMasters(node.address)

    case WorkloadDowning(stillWorkAvailable) =>
      log.info("{} has {} available", sender.path.address, if (stillWorkAvailable) "work" else "no work")
      if (stillWorkAvailable) {
        cluster.unsubscribe(self)
        startWorkStealing()
      } else {
        removeFromPendingMasters(sender.path.address)
      }
  }

  private def removeFromPendingMasters(nodeAddress: Address): Unit = {
    pendingMasters -= nodeAddress
    if (pendingMasters.isEmpty) {
      log.info("Everybody seems to be finished")
      cluster.unsubscribe(self)
      shutdown()
    } else {
      log.info("Still waiting on work from {}", pendingMasters.map(_.toString).mkString(", "))
    }
  }
}
