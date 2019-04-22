scalaVersion := "2.12.8"

lazy val akkaVersion = "2.5.22"
lazy val univocityVersion = "2.8.1"


organization := "com.github.codelionx"
name := "dodo"
version := "0.0.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  // csv parsing
  "com.univocity" % "univocity-parsers" % univocityVersion,
  // test
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
)

// set main class for assembly
//mainClass in assembly := Some("com.github.codelionx.dodo.App")

// skip tests during assembly
//test in assembly := {}