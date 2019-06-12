package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, Props, RootActorPath}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}

import scala.util.Try


object ClusterListener {

  val name = "clistener"

  def props: Props = Props[ClusterListener]

  case object GetLeftNeighbor
  case class LeftNeighbor(address: RootActorPath)

  case object GetRightNeighbor
  case class RightNeighbor(address: RootActorPath)

  class ClusterStateException(message: String, cause: Throwable = null) extends RuntimeException(message, cause) {
    def this(cause: Throwable) = this(cause.getMessage, cause)
  }

}


class ClusterListener extends Actor with ActorLogging {

  import ClusterListener._


  implicit private val memberOrder: Ordering[Member] = Member.ageOrdering

  private val cluster = Cluster(context.system)
  private val selfMember = cluster.selfMember

  override def preStart(): Unit = {
    Reaper.watchWithDefault(self)
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberUp], classOf[MemberRemoved],
      classOf[UnreachableMember], classOf[ReachableMember]
    )
  }

  override def postStop(): Unit =
    cluster.unsubscribe(self)

  override def receive: Receive = internalReceive(Nil)

  def internalReceive(members: Seq[Member]): Receive = {
    case MemberUp(node) =>
      log.debug("New node ({}) joined the cluster", node)
      context.become(internalReceive((members :+ node).sorted))

    case MemberRemoved(node, _) =>
      log.debug("Node ({}) left the cluster", node)
      context.become(internalReceive(members.filterNot(_ == node)))

    case UnreachableMember(node) =>
      log.info("Node ({}) detected unreachable", node)

    case ReachableMember(node) =>
      log.info("Node ({}) detected reachable again", node)

    case GetLeftNeighbor if members.length >= 2 =>
      getLeftNeighbor(members)
        .map(member => sender ! LeftNeighbor(RootActorPath(member.address)))
        .recover(sendError)

    case GetRightNeighbor if members.length >= 2 =>
      getRightNeighbor(members)
        .map(member => sender ! RightNeighbor(RootActorPath(member.address)))
        .recover(sendError)

    case GetLeftNeighbor | GetRightNeighbor if members.length < 2 =>
      log.warning("Cluster size too small for neighbor operations: {}", members.length)
      sendError.apply(new ClusterStateException(s"Cluster size is too small: only ${members.length} of 2 members"))
  }

  private def getLeftNeighbor(members: Seq[Member]): Try[Member] = Try {
    members.indexOf(selfMember) match {
      case -1 => throwSelfNotFound
      case  0 => members.last
      case  i => members(i - 1)
    }
  }

  private def getRightNeighbor(members: Seq[Member]): Try[Member] = Try {
    val end = members.length - 1
    members.indexOf(selfMember) match {
      case  -1   => throwSelfNotFound
      case `end` => members.head
      case   i   => members(i + 1)
    }
  }

  private def throwSelfNotFound = {
    log.error("Could not find self node in the member list: our node is not up yet??")
    throw new ClusterStateException("Could not find self node in the member list")
  }

  private def sendError: PartialFunction[Throwable, Unit] = {
    case error => sender ! akka.actor.Status.Failure(error)
  }
}
