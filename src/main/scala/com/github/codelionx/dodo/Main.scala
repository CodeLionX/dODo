package com.github.codelionx.dodo

import akka.cluster.Cluster
import com.github.codelionx.dodo.actors.SystemCoordinator.Initialize
import com.github.codelionx.dodo.actors.{Reaper, SystemCoordinator}

import scala.language.postfixOps


object Main {

  val actorSystemName = "dodo-system"

  def main(args: Array[String]): Unit = {

    val system = ActorSystem.actorSystem(actorSystemName, ActorSystem.defaultConfiguration)

    val cluster = Cluster(system)

    cluster.registerOnMemberUp {

      system.actorOf(Reaper.props, Reaper.name)

      val systemCoordinator = system.actorOf(SystemCoordinator.props(), SystemCoordinator.name)
      systemCoordinator ! Initialize

    }

  }
}
