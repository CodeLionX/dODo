package com.github.codelionx.dodo.discovery

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.github.codelionx.dodo.actors.ResultCollector.{ConstColumns, OCD, OD, OrderEquivalencies}
import com.github.codelionx.dodo.actors.SystemCoordinator.Initialize
import com.github.codelionx.dodo.actors.SystemCoordinator
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import scala.concurrent.duration._

class ODDetectionSpec extends TestKit(ActorSystem("ODDetectionSpec"))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll {
  "The system" must {
    "find the right results" in {
      val probe = TestProbe()
      val systemCoord = system.actorOf(Props(new SystemCoordinator() {
        override def resultCollector: ActorRef = probe.ref
        override def localFileName: String = "testData/test.csv"
      }))
      systemCoord ! Initialize
      // Constant columns
      probe.expectMsg(ConstColumns(Seq("F")))
      // Order equivalencies
      probe.expectMsg(OrderEquivalencies(Map(
        "A" -> Seq(),
        "B" -> Seq(),
        "C" -> Seq(),
        "D" -> Seq("E"))))
      // OCDs
      var ocds: Set[(Seq[String], Seq[String])] = Set.empty
      var ods: Set[(Seq[String], Seq[String])] = Set.empty
      probe.receiveWhile(500 millis) {
        case OCD(ocd) => ocds += ocd
        case OD(od) => ods += od
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

      val emptySet: Set[(Seq[String], Seq[String])] = Set.empty
      ocds -- expectedOCDs shouldBe emptySet
      ods -- expectedODs shouldBe emptySet
    }
  }
}
