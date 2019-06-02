package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member, UniqueAddress}
import com.github.codelionx.dodo.actors.ClusterListener.{GetLeftNeighbour, LeftNeighbour}


object ClusterListener {

  val name = "clistener"

  def props: Props = Props[ClusterListener]

  case object GetLeftNeighbour

  case class LeftNeighbour(address: UniqueAddress)

}


class ClusterListener extends Actor with ActorLogging {

  implicit val memberOrder: Ordering[Member] = Member.ageOrdering

  private val cluster = Cluster(context.system)

  cluster.selfMember

  override def preStart(): Unit =
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberUp], classOf[MemberRemoved],
      classOf[UnreachableMember], classOf[ReachableMember]
    )

  override def postStop(): Unit =
    cluster.unsubscribe(self)

  override def receive: Receive = internalReceive(Nil)

  def internalReceive(members: Seq[Member]): Receive = {
    case MemberUp(node) =>
      context.become(internalReceive((members :+ node).sorted))

    case MemberRemoved(node, _) =>
      context.become(internalReceive(members.filterNot(_ == node)))

    case UnreachableMember(node) =>
      log.info(s"$node detected unreachable")

    case ReachableMember(node) =>
      log.info(s"$node detected reachable again")

    case GetLeftNeighbour =>
      val selfMember = cluster.selfMember
      val selfPosition = members.indexOf(selfMember)

      val leftMember = selfPosition match {
        case -1 =>
          log.error("our node is not up yet??")
          throw new RuntimeException("Could not find self node in the member list!")
        case 0 =>
          members.last
        case i =>
          members(i - 1)
      }

      sender ! LeftNeighbour(leftMember.uniqueAddress)

  }
}
