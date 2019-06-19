package com.github.codelionx.dodo

import java.io.File

import akka.cluster.Cluster
import com.github.codelionx.dodo.actors.SystemCoordinator.Initialize
import com.github.codelionx.dodo.actors.{Reaper, SystemCoordinator}
import org.backuity.clist
import org.backuity.clist.CliMain

import scala.language.postfixOps


object Main extends CliMain[Unit](
  name = "dODo - Distributed Order dependency Discovery Optimization",
  description = "discover order dependencies through order compatibility dependencies in CSV files"
) {

  private final val actorSystemName = "dodo-system"

  var host: Option[String] = clist.opt[Option[String]](
    description = "this machine's hostname or IP to bind against",
    default = None
  )

  var port: Option[Int] = clist.opt[Option[Int]](
    description = "port to bind against",
    default = None
  )

  var seedHost: Option[String] = clist.opt[Option[String]](
    description = "hostname or IP address of the cluster seed node",
    default = None
  )

  var seedPort: Option[Int] = clist.opt[Option[Int]](
    description = "port of the cluster seed node",
    default = None
  )

  var inputFile: Option[File] = clist.opt[Option[File]](
    description = "path to the input CSV file",
    default = None
  )

  def run(): Unit = {

    val system = ActorSystem.actorSystem(
      actorSystemName,
      ActorSystem.configuration(actorSystemName, host, port, seedHost, seedPort)
    )

    val cluster = Cluster(system)

    cluster.registerOnMemberUp {

      system.actorOf(Reaper.props, Reaper.name)

      val systemCoordinator = system.actorOf(SystemCoordinator.props(), SystemCoordinator.name)
      systemCoordinator ! Initialize

    }

  }
}
