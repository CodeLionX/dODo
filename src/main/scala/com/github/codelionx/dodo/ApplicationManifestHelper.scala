package com.github.codelionx.dodo


/**
  * Provides access to the application name and version written to the `MANIFEST.MF` during the build process.
  * The values can be set in the `build.sbt` file.
  */
trait ApplicationManifestHelper {

  private val appPackage = getClass.getPackage

  /**
    * Returns the application name specified in `META_INF/MANIFEST.MF`-file as
    * `Implementation-Title`.
    */
  val appName: Option[String] = Option(appPackage.getImplementationTitle)


  /**
    * Returns the application version specified in `META_INF/MANIFEST.MF`-file as
    * `Implementation-Version`.
    */
  val appVersion: Option[String] = Option(appPackage.getImplementationVersion)

}
