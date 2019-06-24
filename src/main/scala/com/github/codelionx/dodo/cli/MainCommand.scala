package com.github.codelionx.dodo.cli

import java.io.File

import org.backuity.clist
import org.backuity.clist.{Cli, Command}


/**
  * CLI command definition with all arguments and options. Implements the `main` function. Add your own logic by
  * overriding the [[com.github.codelionx.dodo.cli.MainCommand#run]] function.
  */
abstract class MainCommand extends Command(
  name = "dODo",
  description = "Distributed Order dependency Discovery Optimization - " +
    "discover order dependencies through order compatibility dependencies in CSV files"
) with ApplicationManifestHelper {

  /**
    * Is executed when command is run.
    */
  def run(): Unit

  final def main(args: Array[String]): Unit = {
    Cli.parse(args)
      .withProgramName(appName.getOrElse("dodo"))
      .version(appVersion.getOrElse("0.0.0"), "--version")
      .withHelpCommand("--help")
      .withCommand(this) { _ => run() }
  }

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

  var hasHeader: Option[Boolean] = clist.opt[Option[Boolean]](
    description = "override the setting if the input CSV file has a header",
    default = None
  )
}