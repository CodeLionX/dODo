package com.github.codelionx.dodo

import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Await

import scala.language.postfixOps
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object ActorSystem {

  def configuration(actorSystemName: String, actorSystemRole: String, host: String, port: Int, masterHost: String, masterPort: Int): Config = {
    ConfigFactory.parseString(
      s"""akka.remote.artery.canonical.hostname = "$host"
         |akka.remote.artery.canonical.port = "$port"
         |akka.cluster.roles = [$actorSystemRole]
         |akka.cluster.seed-nodes = [
         |  "akka://$actorSystemName@$masterHost:$masterPort"
         |]
       """.stripMargin)
      .withFallback(ConfigFactory.load("application"))
  }

  def actorSystem(actorSystemName: String, config: Config): akka.actor.ActorSystem = {
    val system = akka.actor.ActorSystem(actorSystemName, config)

    // Register a callback that terminates the ActorSystem when it is detached from the cluster
    Cluster(system).registerOnMemberRemoved {
      system.terminate()

      new Thread() {
        override def run(): Unit = {
          Await.ready(system.terminate(), 10 seconds).recover {
            case _: Exception => System.exit(-1)
          }
        }
      }.start()
    }

    system
  }
}
