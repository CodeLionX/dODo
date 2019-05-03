package com.github.codelionx.dodo

import akka.cluster.Cluster


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

      // shutting down
      println("nothing to do -- shutting down")
      // system.terminate()
      // or
      cluster.down(cluster.selfAddress)
    }
  }
}
