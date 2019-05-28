package com.github.codelionx.dodo

import akka.cluster.Cluster
import com.github.codelionx.dodo.Settings.DefaultValues
import com.github.codelionx.dodo.actors.DataHolder.LoadData
import com.github.codelionx.dodo.actors.SystemCoordinator.{Initialize, Shutdown}
import com.github.codelionx.dodo.actors.{DataHolder, Reaper, SystemCoordinator}

import scala.concurrent.duration._
import scala.language.postfixOps


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

      system.actorOf(Reaper.props, Reaper.name)
//      val systemCoordinator = system.actorOf(SystemCoordinator.props(), SystemCoordinator.name)
//
//      systemCoordinator ! Initialize
      val dataHolder = system.actorOf(DataHolder.props())
      val settings = Settings(system)
      dataHolder ! LoadData(settings.inputFilePath)
    }

  }
}
