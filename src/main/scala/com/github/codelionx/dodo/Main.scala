package com.github.codelionx.dodo

import akka.actor.PoisonPill
import akka.cluster.Cluster
import com.github.codelionx.dodo.actors.SystemCoordinator.Initialize
import com.github.codelionx.dodo.actors.{Reaper, SystemCoordinator}


object Main {

  val actorSystemName = "dodo-system"

  def main(args: Array[String]): Unit = {
    val masterRole = "master"
    val slaveRole = "slave"
    val host = "localhost"
    val port = 7877

    val system = ActorSystem.actorSystem(actorSystemName, ActorSystem.configuration(
      actorSystemName = actorSystemName,
      actorSystemRole = masterRole,
      host = host,
      port = port,
      masterHost = host,
      masterPort = port
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
