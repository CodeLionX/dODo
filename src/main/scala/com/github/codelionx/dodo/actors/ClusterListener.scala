package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}


object ClusterListener {

  def props: Props = Props[ClusterListener]
}


class ClusterListener extends Actor with ActorLogging {

  implicit val memberOrder: Ordering[Member] = Member.ageOrdering

  private val cluster = Cluster(context.system)

  override def preStart(): Unit =
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberUp], classOf[MemberRemoved],
      classOf[UnreachableMember], classOf[ReachableMember]
    )

  override def postStop(): Unit =
    cluster.unsubscribe(self)

  override def receive: Receive = internalReceive(Nil)

  def internalReceive(members: List[Member]): Receive = {
    case MemberUp(node) =>
      context.become(internalReceive((node :: members).sorted))

    case MemberRemoved(node, _) =>
      context.become(internalReceive(members.filterNot(_ == node)))

    case UnreachableMember(node) =>
      log.info(s"$node detected unreachable")

    case ReachableMember(node) =>
      log.info(s"$node detected reachable again")
  }
}
