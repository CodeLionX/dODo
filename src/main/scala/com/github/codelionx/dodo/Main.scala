package com.github.codelionx.dodo

import akka.cluster.Cluster
import com.github.codelionx.dodo.actors.{ODMaster, Reaper}

import scala.language.postfixOps


object Main {

  val actorSystemName = "dodo-system"

  def main(args: Array[String]): Unit = {

    val system = ActorSystem.actorSystem(actorSystemName, ActorSystem.defaultConfiguration)

    val cluster = Cluster(system)

    cluster.registerOnMemberUp {

      system.actorOf(Reaper.props, Reaper.name)

      val master = system.actorOf(ODMaster.props(), ODMaster.name)

    }

  }
}
