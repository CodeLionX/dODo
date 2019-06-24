scalaVersion := "2.12.8"

lazy val akkaVersion = "2.5.22"
lazy val univocityVersion = "2.8.1"
lazy val clistVersion = "3.5.1"


organization := "com.github.codelionx"
name := "dodo"
version := "0.0.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  // csv parsing
  "com.univocity" % "univocity-parsers" % univocityVersion,
  // logging
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  // test
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  // serialization
  "com.twitter" %% s"chill-akka" % "0.9.3",
  //"com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.2",
  // cli
  "org.backuity.clist" %% "clist-core" % clistVersion,
  "org.backuity.clist" %% "clist-macros" % clistVersion % "provided",
)

// set main class for assembly
mainClass in assembly := Some("com.github.codelionx.dodo.Main")

// skip tests during assembly
test in assembly := {}