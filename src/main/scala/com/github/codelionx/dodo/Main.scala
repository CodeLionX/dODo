package com.github.codelionx.dodo

import java.io.File

import akka.cluster.Cluster
import com.github.codelionx.dodo.actors.{ODMaster, Reaper}
import org.backuity.clist
import org.backuity.clist.{Cli, Command}

import scala.language.postfixOps


object Main extends Command(
  name = "dODo",
  description = "Distributed Order dependency Discovery Optimization - " +
    "discover order dependencies through order compatibility dependencies in CSV files"
) with ApplicationManifestHelper {

  def main(args: Array[String]): Unit = {
    Cli.parse(args)
      .withProgramName(appName.getOrElse("dodo"))
      .version(appVersion.getOrElse("0.0.0"), "--version")
      .withHelpCommand("--help")
      .withCommand(this) { _ => run() }
  }

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
    description = "path to the input CSV file. Required for the seed node! "
      + "If not specified, this node tries to fetch the data from another node "
      + "(potentially the seed node).",
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

      val master = system.actorOf(ODMaster.props(), ODMaster.name)

    }

  }
}
