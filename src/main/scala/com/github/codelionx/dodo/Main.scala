package com.github.codelionx.dodo

import com.github.codelionx.dodo.actors.Reaper
import com.github.codelionx.dodo.actors.master.ODMaster
import com.github.codelionx.dodo.cli.MainCommand

import scala.language.postfixOps


object Main extends MainCommand {

  private final val actorSystemName = "dodo-system"

  override def run(): Unit = {

    val system = ActorSystem.actorSystem(
      actorSystemName,
      ActorSystem.configuration(actorSystemName, host, port, seedHost, seedPort, hasHeader)
    )

    system.actorOf(Reaper.props, Reaper.name)
    system.actorOf(ODMaster.props(inputFile), ODMaster.name)

  }
}
