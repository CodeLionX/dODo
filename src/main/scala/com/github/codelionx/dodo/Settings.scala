package com.github.codelionx.dodo

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import com.typesafe.config.Config


class SettingsImpl(config: Config) extends Extension {

  private val namespace = "com.github.codelionx.dodo"

  //val linearizationPartitionSize: Int = config.getInt(s"$namespace.linearization-partition-size")

}


object Settings extends ExtensionId[SettingsImpl] with ExtensionIdProvider {

  override def createExtension(system: ExtendedActorSystem): SettingsImpl = new SettingsImpl(system.settings.config)

  override def lookup(): ExtensionId[_ <: Extension] = Settings

}