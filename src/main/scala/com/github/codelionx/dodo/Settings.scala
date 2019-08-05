package com.github.codelionx.dodo

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import com.github.codelionx.dodo.Settings.{ParsingSettings, SideChannelSettings}
import com.typesafe.config.Config

import scala.concurrent.duration.{Duration, FiniteDuration}


object Settings extends ExtensionId[Settings] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): Settings = new Settings(system.settings.config)

  override def lookup(): ExtensionId[_ <: Extension] = Settings

  /**
    * Provides default values as constants
    */
  final object DefaultValues {

    final object NodeRole {

      final val Leader = "leader"
      final val Follower = "follower"

    }
  }

  class ParsingSettings(config: Config, namespace: String) {

    private val subnamespace = s"$namespace.parsing"

    val nInferringRows: Int = config.getInt(s"$subnamespace.inferring-rows")

    val parseHeader: Boolean = config.getBoolean(s"$subnamespace.has-header")
  }

  class SideChannelSettings(config: Config, namespace: String) {

    private val subnamespace = s"$namespace.side-channel"

    val hostname: String = config.getString(s"$subnamespace.hostname")

    val startingPort: Int = config.getInt(s"$subnamespace.port")
  }
}


class Settings(config: Config) extends Extension {

  private val namespace = "com.github.codelionx.dodo"

  val inputFilePath: String = config.getString(s"$namespace.input-file")

  val outputFilePath: String = config.getString(s"$namespace.output-file")

  val outputToConsole: Boolean = config.getBoolean(s"$namespace.output-to-console")

  val workers: Int = config.getInt(s"$namespace.workers")

  val maxBatchSize: Int = config.getInt(s"$namespace.max-batch-size")

  val ocdComparability: Boolean = config.getBoolean(s"$namespace.ocd-comparability")

  val stateReplicationInterval: FiniteDuration = Duration.fromNanos(
    config.getDuration(s"$namespace.replication-interval").toNanos
  )

  val parsing: ParsingSettings = new ParsingSettings(config, namespace)

  val sideChannel: SideChannelSettings = new SideChannelSettings(config, namespace)
}
