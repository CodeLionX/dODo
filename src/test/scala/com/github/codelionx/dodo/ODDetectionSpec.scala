package com.github.codelionx.dodo

import akka.actor.{ActorRef, Props, ActorSystem => AkkaActorSystem}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.github.codelionx.dodo.actors.{ODMaster, Worker}
import com.github.codelionx.dodo.actors.ResultCollector.{ConstColumns, OrderEquivalencies, Results}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.language.postfixOps


object ODDetectionSpec {
  val config: Config = ConfigFactory.parseString(
    """akka.remote.artery.canonical.hostname = "127.0.0.1"
       |akka.remote.artery.canonical.port = "7880"
       |akka.cluster.seed-nodes = [
       |  "akka://ODDetectionSpec@127.0.0.1:7880"
       |]
     """.stripMargin)
    .withFallback(ConfigFactory.load())
}
class ODDetectionSpec extends TestKit(AkkaActorSystem("ODDetectionSpec", ODDetectionSpec.config))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {

  "The system" must {

    "find the right results" in {
      val probe = TestProbe()
      val master = system.actorOf(Props(new ODMaster() {
        override val resultCollector: ActorRef = probe.ref
        override val workers: Seq[ActorRef] = Seq(context.actorOf(Worker.props(probe.ref), s"${Worker.name}"))
      }))

      // Constant columns
      probe.expectMsg(ConstColumns(Seq("F")))

      // Order equivalencies
      probe.expectMsg(OrderEquivalencies(Map(
        "A" -> Seq(),
        "B" -> Seq(),
        "C" -> Seq(),
        "D" -> Seq("E")
      )))

      // OCDs & ODs
      var ocds: Set[(Seq[String], Seq[String])] = Set.empty
      var ods: Set[(Seq[String], Seq[String])] = Set.empty
      probe.receiveWhile(max = 500 millis) {
        case Results(od, ocd) =>
          ods ++= od.toSet
          ocds ++= ocd.toSet
      }
      val expectedODs = Set(
        (Seq("A"), Seq("B")),
        (Seq("B", "D"), Seq("A")),
        (Seq("A", "C"), Seq("D")),
        (Seq("D", "B"), Seq("A")),
        (Seq("D", "A"), Seq("B")),
        (Seq("C", "A"), Seq("D")),
        (Seq("B", "C", "D"), Seq("A")),
        (Seq("A", "C"), Seq("B", "D")),
        (Seq("A", "C"), Seq("D", "B")),
        (Seq("A", "B", "C"), Seq("D")),
        (Seq("B", "A", "C"), Seq("D")),
        (Seq("B", "C", "A"), Seq("D")),
        (Seq("C", "B", "A"), Seq("D"))
      )

      val expectedOCDs = Set(
        (Seq("A"), Seq("D")),
        (Seq("B"), Seq("D")),
        (Seq("C"), Seq("D")),
        (Seq("A"), Seq("B", "C")),
        (Seq("A", "B"), Seq("D")),
        (Seq("B", "A"), Seq("D")),
        (Seq("B", "C"), Seq("D")),
        (Seq("C", "B"), Seq("D")),
        (Seq("A", "D"), Seq("B", "C")),
        (Seq("B", "C"), Seq("D", "A"))
      )

      ocds shouldEqual expectedOCDs
      ods shouldEqual expectedODs
    }
  }
}
