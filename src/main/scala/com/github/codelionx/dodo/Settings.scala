package com.github.codelionx.dodo

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import com.github.codelionx.dodo.Settings.ParsingSettings
import com.typesafe.config.Config


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

    final val HOST = "localhost"
    final val PORT = 7877
  }

  class ParsingSettings(config: Config, namespace: String) {

    private val subnamespace = s"$namespace.parsing"

    val nInferringRows: Int = config.getInt(s"$subnamespace.inferring-rows")

    val parseHeader: Boolean = config.getBoolean(s"$subnamespace.has-header")
  }
}


class Settings(config: Config) extends Extension {

  private val namespace = "com.github.codelionx.dodo"

  val inputFilePath: String = config.getString(s"$namespace.input-file")

  val outputFilePath: String = config.getString(s"$namespace.output-file")

  val workers: Int = config.getInt(s"$namespace.workers")

  val maxBatchSize: Int = config.getInt(s"$namespace.max-batch-size")

  val ocdComparability: Boolean = config.getBoolean(s"$namespace.ocd-comparability")

  val parsing: ParsingSettings = new ParsingSettings(config, namespace)

}
