package com.github.codelionx.dodo

import akka.cluster.Cluster
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps


object ActorSystem {

  def defaultConfiguration: Config = ConfigFactory.load()

  def configuration(
                     actorSystemName: String,
                     host: Option[String],
                     port: Option[Int],
                     seedHost: Option[String],
                     seedPort: Option[Int]
                   ): Config = {
    val hostnameString = host match {
      case Some(name) => s"hostname = $name"
      case None => ""
    }
    val portString = port match {
      case Some(p) => s"port = $p"
      case None => ""
    }
    val seedNodeString = (seedHost, seedPort) match {
      case (Some(sh), Some(sp)) =>
        s"""akka.cluster.seed-nodes = ["akka://$actorSystemName@$sh:$sp"]"""
      case (None, Some(sp)) =>
        s"""akka.cluster.seed-nodes = ["akka://$actorSystemName@"$${akka.remote.artery.canonical.hostname}":$sp"]"""
      case (Some(sh), None) =>
        s"""akka.cluster.seed-nodes = ["akka://$actorSystemName@$sh:"$${akka.remote.artery.canonical.port}]"""
      case (None, None) => ""
    }
    ConfigFactory.parseString(
      s"""akka.remote.artery.canonical {
         |  $hostnameString
         |  $portString
         |}
         |$seedNodeString
       """.stripMargin)
      .withFallback(ConfigFactory.defaultApplication())
      .withFallback(ConfigFactory.defaultReference())
      .withFallback(ConfigFactory.defaultOverrides())
      .resolve()
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
