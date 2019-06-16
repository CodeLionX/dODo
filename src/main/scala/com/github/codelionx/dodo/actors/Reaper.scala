package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props, Terminated}

import scala.collection.mutable.ArrayBuffer


object Reaper {

  val name = "reaper"

  def props: Props = Props[Reaper]

  def watchWithDefault(actor: ActorRef)(implicit context: ActorContext): Unit = {
    val reaper = context.system.actorSelection(s"/user/$name")
    reaper ! WatchMe(actor)
  }

  // should only be send within actor systems
  case class WatchMe(ref: ActorRef)

}


class Reaper extends Actor with ActorLogging {

  import Reaper._


  val watched: ArrayBuffer[ActorRef] = ArrayBuffer.empty[ActorRef]

  override def preStart(): Unit =
    log.info("Started {} at {}", name, self.path)

  override def postStop(): Unit =
    log.info("Stopped {}", name)

  override def receive: Receive = {
    case WatchMe(ref) =>
      context.watch(ref)
      watched += ref

    case Terminated(ref) =>
      watched -= ref
      if (watched.isEmpty) terminateSystem()

    case unexpected =>
      log.error("Unknown message: {}", unexpected)
  }

  def terminateSystem(): Unit = {
    log.info("Terminating system as all actors died.")
    context.system.terminate()
  }
}
