package com.github.codelionx.dodo

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
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

}


class Settings(config: Config) extends Extension {

  private val namespace = "com.github.codelionx.dodo"

  // add config values here
  //val linearizationPartitionSize: Int = config.getInt(s"$namespace.linearization-partition-size")

}