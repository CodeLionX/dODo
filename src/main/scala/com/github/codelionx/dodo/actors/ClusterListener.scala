package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props, RootActorPath}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import com.github.codelionx.dodo.DodoException
import com.github.codelionx.dodo.actors.master.ODMaster

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try


object ClusterListener {

  val name = "clistener"

  def props(master: ActorRef, stateReplicator: ActorRef): Props = Props(new ClusterListener(master, stateReplicator))

  case object GetLeftNeighbor
  case class LeftNeighbor(address: RootActorPath)
  case class LeftNeighborRef(neighbor: ActorRef)
  case class LeftNeighborDown(newNeighbor: ActorRef)

  case object GetRightNeighbor
  case class RightNeighbor(address: RootActorPath)
  case class RightNeighborRef(neighbor: ActorRef)
  case class RightNeighborDown(newNeighbor: ActorRef)

  case object GetNumberOfNodes
  case class NumberOfNodes(number: Int)

  case class RegisterActorRefs(master: ActorRef, stateReplicator: ActorRef)

  case class ClusterStateException(msg: String) extends DodoException(msg)

  case class MemberActors(member: Member, clusterListener: ActorRef, master: ActorRef, stateReplicator: ActorRef)
}


class ClusterListener(master: ActorRef, stateReplicator: ActorRef) extends Actor with ActorLogging {

  import ClusterListener._

  implicit private val memberOrder: Ordering[Member] = Member.ordering
  implicit private val memberActorOrder: Ordering[MemberActors] = Ordering.by(_.member)

  private val cluster = Cluster(context.system)
  private val selfMember = cluster.selfMember
  private val userGuardian = "user"

  override def preStart(): Unit = {
    Reaper.watchWithDefault(self)
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberUp], classOf[MemberRemoved],
      classOf[UnreachableMember], classOf[ReachableMember]
    )
  }

  override def postStop(): Unit =
    cluster.unsubscribe(self)

  override def receive: Receive = internalReceive(Nil, Nil)

  def internalReceive(members: Seq[MemberActors], pendingNodes: Seq[Member]): Receive = {
    case MemberUp(`selfMember`) =>
      log.debug("We joined the cluster")
      val myself = MemberActors(selfMember, self, master, stateReplicator)
      context.become(internalReceive((members :+ myself).sorted, pendingNodes))

    case MemberUp(node) =>
      log.debug("New node ({}) joined the cluster", node)
      val newNeighbour = context.actorSelection(RootActorPath(node.address) / userGuardian / ODMaster.name / name)
      log.debug("Sending my refs to {}", newNeighbour)
      newNeighbour ! RegisterActorRefs(master, stateReplicator)
      context.become(internalReceive(members, pendingNodes :+ node))

    case MemberRemoved(node, _) =>
      log.debug("Node ({}) left the cluster", node)
      val newMembers = updateNeighborsOnMemberRemoved(members, node)
      context.become(internalReceive(newMembers, pendingNodes.filterNot(_ == node)))

    case UnreachableMember(node) =>
      log.debug("Node ({}) detected unreachable", node)

    case ReachableMember(node) =>
      log.debug("Node ({}) detected reachable again", node)

    case GetNumberOfNodes if selfUp(members) =>
      sender ! NumberOfNodes(members.length + pendingNodes.length)

    case GetNumberOfNodes if !selfUp(members) =>
      log.debug(
        "Requeueing request {} with 1 second timeout, we are not part of the cluster yet!",
        GetNumberOfNodes
      )
      context.system.scheduler.scheduleOnce(1 second, self, GetNumberOfNodes)(context.dispatcher, sender)

    case m @ (GetLeftNeighbor | GetRightNeighbor) if pendingNodes.nonEmpty =>
      log.debug(
        "Requeueing request {} with 1 second timeout, {} nodes still have pending actor identifications",
        m,
        pendingNodes
      )
      context.system.scheduler.scheduleOnce(1 second, self, m)(context.dispatcher, sender)

    case GetLeftNeighbor if members.length >= 2 =>
      getLeftNeighbor(members)
        .map(member => sender ! LeftNeighbor(RootActorPath(member.member.address)))
        .recover(sendError())

    case GetRightNeighbor if members.length >= 2 =>
      getRightNeighbor(members)
        .map(member => sender ! RightNeighbor(RootActorPath(member.member.address)))
        .recover(sendError())

    case GetLeftNeighbor | GetRightNeighbor if members.length < 2 =>
      log.warning("Cluster size too small for neighbor operations: {}", members.length)
      sendError().apply(ClusterStateException(s"Cluster size is too small: only ${members.length} of 2 members"))

    case m @ RegisterActorRefs(otherMaster, otherSR) =>
      log.debug("Received ActorRefs from {}", sender.path)
      pendingNodes.find(_.address == sender.path.address) match {
        case Some(node) =>
          val newMember = MemberActors(node, sender, otherMaster, otherSR)
          val newMembers = (members :+ newMember).sorted
          val newPendingNodes = pendingNodes.filterNot(_ == node)
          updateNeighborsNew(newMembers)
          context.become(internalReceive(newMembers, newPendingNodes))
        case None =>
          log.warning("Received actor refs of a node that we don't know!")
          log.debug("Requeueing request {} with 1 second timeout", m)
          context.system.scheduler.scheduleOnce(1 second, self, m)(context.dispatcher, sender)
      }

    case m => log.debug("Received unknown message: {}", m)
  }

  private def selfUp(members: Seq[MemberActors]): Boolean =
    members.map(_.member).contains(selfMember)

  private def getLeftNeighbor(members: Seq[MemberActors]): Try[MemberActors] = Try {
    members.map(_.member).indexOf(selfMember) match {
      case -1 => throwSelfNotFound
      case  0 => members.last
      case  i => members(i - 1)
    }
  }

  private def getRightNeighbor(members: Seq[MemberActors]): Try[MemberActors] = Try {
    val end = members.length - 1
    members.map(_.member).indexOf(selfMember) match {
      case  -1   => throwSelfNotFound
      case `end` => members.head
      case   i   => members(i + 1)
    }
  }

  private def wasRightNeighbor(selfIndex: Int, otherIndex: Int, lastIndex: Int): Boolean =
    (selfIndex + 1 == otherIndex) || (selfIndex == lastIndex && otherIndex == 0)

  private def wasLeftNeighbor(selfIndex: Int, otherIndex: Int, lastIndex: Int): Boolean =
    (selfIndex == otherIndex + 1) || (otherIndex == lastIndex && selfIndex == 0)

  private def updateNeighborsNew(members: Seq[MemberActors]): Unit = {
    log.debug("Sending right and left neighbor to state replicator")
    log.debug("Ring: ({} ->)", members.map(_.member.address).mkString(" -> "))
    getRightNeighbor(members)
      .map(member => stateReplicator ! RightNeighborRef(member.stateReplicator))
    getLeftNeighbor(members)
      .map(member => stateReplicator ! LeftNeighborRef(member.stateReplicator))
  }

  private def updateNeighborsOnMemberRemoved(members: Seq[MemberActors], removedMember: Member): Seq[MemberActors] = {
    val selfIndex = members.map(_.member).indexOf(selfMember)
    val removedIndex = members.map(_.member).indexOf(removedMember)
    val newMembers = members.filterNot(_.member == removedMember).sorted
    if (wasRightNeighbor(selfIndex, removedIndex, members.length-1)) {
      getRightNeighbor(newMembers)
        .map(member => stateReplicator ! RightNeighborDown(member.stateReplicator))
    } else if (wasLeftNeighbor(selfIndex, removedIndex, members.length-1)) {
      getLeftNeighbor(newMembers)
        .map(member => stateReplicator ! LeftNeighborDown(member.stateReplicator))
    }
    newMembers
  }

  private def throwSelfNotFound: Nothing = {
    log.warning("Could not find self node in the member list: our node is not up yet??")
    throw ClusterStateException("Could not find self node in the member list")
  }

  private def sendError(receiver: ActorRef = sender): PartialFunction[Throwable, Unit] = {
    case error => receiver ! akka.actor.Status.Failure(error)
  }
}
