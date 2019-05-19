package com.github.codelionx.dodo.actors

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.github.codelionx.dodo.Settings
import com.github.codelionx.dodo.actors.DataHolder.{DataLoaded, LoadData}
import com.github.codelionx.dodo.actors.ODMaster.FindODs
import com.github.codelionx.dodo.discovery.DependencyChecking


object SystemCoordinator {

  val name = "systemcoordinator"

  def props(): Props = Props[SystemCoordinator]

  case object Initialize

}

class SystemCoordinator extends Actor with ActorLogging with DependencyChecking {

  import SystemCoordinator._

  private val settings = Settings(context.system)

  val nWorkers = settings.workers
  val resultCollector: ActorRef = context.actorOf(ResultCollector.props(), ResultCollector.name)
  val dataHolder: ActorRef = context.actorOf(DataHolder.props(), DataHolder.name)
  val odMaster: ActorRef = context.actorOf(ODMaster.props(nWorkers, resultCollector), ODMaster.name)


  override def preStart(): Unit = {
    log.info(s"Starting $name")
    Reaper.watchWithDefault(self)
  }

  override def postStop(): Unit =
    log.info(s"Stopping $name")

  override def receive: Receive = {
    case Initialize =>
      log.info("Preparing for OD discovery")
      log.info("  Loading and parsing data")
      dataHolder ! LoadData(settings.inputFilePath)

//      log.info("  Creating initial search space")
//      log.info("  ...")

      // test if data passing works
      // ---
//      log.info("Testing data passing ...")
//      dataHolder ! GetDataRef

    case DataLoaded =>
      log.info("  Starting master and passing ref to data holder")
      log.info(s"Session is in the hand of ${ODMaster.name}")
      odMaster ! FindODs(dataHolder)

//    case DataRef(data) =>
//      log.info("... data passing successful:")
//      println(data.prettyPrint)

      //log.info("shutting down")
      //context.stop(self)
    // ---

    // TODO
    case _ => log.info("Unknown message received")
  }
}
