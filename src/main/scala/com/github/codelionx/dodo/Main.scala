package com.github.codelionx.dodo

import akka.cluster.Cluster
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

    val cluster = Cluster(system)

    cluster.registerOnMemberUp {

      system.actorOf(Reaper.props, Reaper.name)

      val master = system.actorOf(ODMaster.props(inputFile), ODMaster.name)

    }

  }
}
