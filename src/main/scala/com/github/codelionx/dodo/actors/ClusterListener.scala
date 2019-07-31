package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props, RootActorPath}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import com.github.codelionx.dodo.DodoException
import com.github.codelionx.dodo.actors.master.ODMaster

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

  implicit private val memberOrder: Ordering[Member] = Member.ageOrdering
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
      context.become(internalReceive(members :+ myself, pendingNodes))

    case MemberUp(node) =>
      log.debug("New node ({}) joined the cluster", node)
      val newNeighbour = context.actorSelection(RootActorPath(node.address) / userGuardian / ODMaster.name / name)
      newNeighbour ! RegisterActorRefs(master, stateReplicator)
      context.become(internalReceive(members, pendingNodes :+ node))

    case MemberRemoved(node, _) =>
      log.debug("Node ({}) left the cluster", node)
      updateNeighborsRemoved(members, node)
      context.become(internalReceive(members.filterNot(_.member == node), pendingNodes.filterNot(_ == node)))

    case UnreachableMember(node) =>
      updateNeighborsRemoved(members, node)
      log.debug("Node ({}) detected unreachable", node)

    case ReachableMember(node) =>
      log.debug("Node ({}) detected reachable again", node)

    case GetNumberOfNodes =>
      sender ! NumberOfNodes(members.length)

    case GetLeftNeighbor if members.length >= 2 =>
      getLeftNeighbor(members)
        .map(member => sender ! LeftNeighbor(RootActorPath(member.member.address)))
        .recover(sendError)

    case GetRightNeighbor if members.length >= 2 =>
      getRightNeighbor(members)
        .map(member => sender ! RightNeighbor(RootActorPath(member.member.address)))
        .recover(sendError)

    case GetLeftNeighbor | GetRightNeighbor if members.length < 2 =>
      log.warning("Cluster size too small for neighbor operations: {}", members.length)
      sendError.apply(ClusterStateException(s"Cluster size is too small: only ${members.length} of 2 members"))

    case m @ RegisterActorRefs(otherMaster, otherSR) =>
      log.debug("Received ActorRefs from {}", sender.path)
      pendingNodes.find(_.address == sender.path.address) match {
        case Some(node) =>
          val newMember = MemberActors(node, sender, otherMaster, otherSR)
          val newMembers = (members :+ newMember).sorted
          val newPendingNodes = pendingNodes.filterNot(_ == node)
          updateNeighborsNew(newMembers, newMember)
          context.become(internalReceive(newMembers, newPendingNodes))
        case None =>
          log.warning("Received actor refs of a node that we don't know! {}", m)
      }
  }

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

  private def isRightNeighbor(selfIndex: Int, otherIndex: Int, lastIndex: Int): Boolean = {
    val isNeighbor = (selfIndex + 1 == otherIndex) || (selfIndex == lastIndex && otherIndex == 0)
    isNeighbor
  }

  private def isLeftNeighbor(selfIndex: Int, otherIndex: Int, lastIndex: Int): Boolean = {
    val isNeighbor = (selfIndex == otherIndex + 1) || (otherIndex == lastIndex && selfIndex == 0)
    isNeighbor
  }

  private def updateNeighborsNew(members: Seq[MemberActors], newMember: MemberActors): Unit = {
    val selfIndex = members.map(_.member).indexOf(selfMember)
    if (isRightNeighbor(selfIndex, members.indexOf(newMember), members.length-1)) {
      stateReplicator ! RightNeighborRef(newMember.stateReplicator)
    } else if (isLeftNeighbor(selfIndex, members.indexOf(newMember), members.length-1)) {
      stateReplicator ! LeftNeighborRef(newMember.stateReplicator)
    }
  }

  private def updateNeighborsRemoved(members: Seq[MemberActors], removedMember: Member): Unit = {
    val selfIndex = members.map(_.member).indexOf(selfMember)
    val removedIndex = members.map(_.member).indexOf(removedMember)
    val newMembers = members.filterNot(_.member == removedMember)
    if (isRightNeighbor(selfIndex, removedIndex, members.length-1)) {
      getRightNeighbor(newMembers)
        .map(member => stateReplicator ! RightNeighborDown(member.stateReplicator))
        .recover(sendError)
    } else if (isLeftNeighbor(selfIndex, removedIndex, members.length-1)) {
      getLeftNeighbor(newMembers)
        .map(member => stateReplicator ! LeftNeighborDown(member.stateReplicator))
        .recover(sendError)
    }
  }

  private def throwSelfNotFound = {
    log.error("Could not find self node in the member list: our node is not up yet??")
    throw ClusterStateException("Could not find self node in the member list")
  }

  private def sendError: PartialFunction[Throwable, Unit] = {
    case error => sender ! akka.actor.Status.Failure(error)
  }
}
