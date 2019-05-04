package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props, Terminated}

import scala.collection.mutable.ArrayBuffer

//reaper-companion
//reaper-messages
object Reaper {

  val reaperName = "reaper"

  def props: Props = Props[Reaper]

  def watchWithDefault(actor: ActorRef)(implicit context: ActorContext): Unit = {
    val reaper = context.system.actorSelection(s"/user/$reaperName")
    reaper ! WatchMe(actor)
  }

  // should only be send within actor systems
  case class WatchMe(ref: ActorRef)

}


//Reaper actor
class Reaper extends Actor with ActorLogging{
  import Reaper._

  val watched: ArrayBuffer[ActorRef] = ArrayBuffer.empty[ActorRef]

  override def preStart(): Unit = {
    super.preStart()
    log.info("Started reaper at {}", self.path)
  }

  def receive: Receive = {
    case WatchMe(ref) =>
      context.watch(ref)
      watched+=ref

    case Terminated(ref) =>
      watched-=ref
      if(watched.isEmpty) terminateSystem()

    case unexpected =>
      log.error(s"ERROR: Unknown message: $unexpected")
  }

  def terminateSystem(): Unit = {
    context.system.terminate()
  }

  override def postStop(): Unit = {
    super.postStop()
    log.info("Stopped {}", self.path)
  }

}
