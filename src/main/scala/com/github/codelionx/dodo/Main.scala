package com.github.codelionx.dodo

import akka.cluster.Cluster
import com.github.codelionx.dodo.Settings.DefaultValues
import com.github.codelionx.dodo.actors.SystemCoordinator.Initialize
import com.github.codelionx.dodo.actors.{Reaper, SystemCoordinator}


object Main {

  val actorSystemName = "dodo-system"

  def main(args: Array[String]): Unit = {

    val system = ActorSystem.actorSystem(actorSystemName, ActorSystem.configuration(
      actorSystemName = actorSystemName,
      actorSystemRole = DefaultValues.NodeRole.Leader,
      host = DefaultValues.HOST,
      port = DefaultValues.PORT,
      masterHost = DefaultValues.HOST,
      masterPort = DefaultValues.PORT
    ))

    val cluster = Cluster(system)

    cluster.registerOnMemberUp {
      println("Cluster up")

      system.actorOf(Reaper.props, Reaper.name)
      val systemCoordinator = system.actorOf(SystemCoordinator.props("data/iris.csv"), SystemCoordinator.name)

      systemCoordinator ! Initialize

      // intentionally stopping systemCoordinator to test reaper functionality
      //      system.stop(systemCoordinator)
      // or:
      //      systemCoordinator ! PoisonPill
    }
  }
}
