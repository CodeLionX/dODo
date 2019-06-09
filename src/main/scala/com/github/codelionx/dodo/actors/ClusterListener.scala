package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, Props, RootActorPath}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import com.github.codelionx.dodo.actors.ClusterListener.{GetLeftNeighbour, LeftNeighbour}

import scala.util.Try


object ClusterListener {

  val name = "clistener"

  def props: Props = Props[ClusterListener]

  case object GetLeftNeighbour

  case class LeftNeighbour(address: RootActorPath)

}


class ClusterListener extends Actor with ActorLogging {

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
      context.become(internalReceive((members :+ node).sorted))

    case MemberRemoved(node, _) =>
      context.become(internalReceive(members.filterNot(_ == node)))

    case UnreachableMember(node) =>
      log.info(s"$node detected unreachable")

    case ReachableMember(node) =>
      log.info(s"$node detected reachable again")

    case GetLeftNeighbour =>
      getLeftNeighbour(members)
        .map(member =>
          sender ! LeftNeighbour(RootActorPath(member.address))
        )
        .recover(sendError)
  }

  private def sendError: PartialFunction[Throwable, Unit] = {
    case error => sender ! akka.actor.Status.Failure(error)
  }

  private def getLeftNeighbour(members: Seq[Member]): Try[Member] = Try {
    members.indexOf(selfMember) match {
      case -1 =>
        log.error("our node is not up yet??")
        log.info(s"current member list: ${members.mkString(", ")}")
        throw new RuntimeException("Could not find self node in the member list!")
      case 0 =>
        members.last
      case i =>
        members(i - 1)
    }
  }
}
