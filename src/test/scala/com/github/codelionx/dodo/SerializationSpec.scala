package com.github.codelionx.dodo

import java.time.{LocalDateTime, ZonedDateTime}

import akka.actor.{ActorSystem => AkkaActorSystem}
import akka.serialization.SerializationExtension
import akka.testkit.{ImplicitSender, TestKit}
import com.github.codelionx.dodo.types._
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.TryValues._
import org.scalatest.{Matchers, WordSpecLike}

import scala.reflect.ClassTag


object SerializationSpec {
  val config: Config = ConfigFactory.parseString(
    s"""akka.remote.artery.canonical.hostname = "127.0.0.1"
       |akka.remote.artery.canonical.port = "7990"
       |akka.cluster.seed-nodes = [
       |  "akka://SerializationSpec@127.0.0.1:7990"
       |]
       """.stripMargin)
    .withFallback(ConfigFactory.load())
}


class SerializationSpec extends TestKit(AkkaActorSystem("SerializationSpec", SerializationSpec.config))
  with ImplicitSender
  with WordSpecLike
  with Matchers {

  private val serialization = SerializationExtension(system)

  "DataType instances" should {

    "be serializable" when {

      def testDTserializability[T <: DataType[_]](dt: T)(implicit ev: ClassTag[T]): Unit = {
        val serialized = serialization.serialize(dt).success.value
        val deserialized = serialization.deserialize(serialized, ev.runtimeClass).success.value

        deserialized shouldEqual dt
      }

      "of ZonedDateTime type" in {
        val dt = DataType.of[ZonedDateTime]
        testDTserializability(dt)
      }

      "of LocalDateTime type" in {
        val dt = DataType.of[LocalDateTime]
        testDTserializability(dt)
      }

      "of Long type" in {
        val dt = DataType.of[Long]
        testDTserializability(dt)
      }

      "of Double type" in {
        val dt = DataType.of[Double]
        testDTserializability(dt)
      }

      "of string type" in {
        val dt = DataType.of[String]
        testDTserializability(dt)
      }

      "of null type" in {
        val dt = DataType.of[Null]
        testDTserializability(dt)
      }
    }
  }

  "TypedColumns" should {

    "be serializable" in {
      val column = TypedColumnBuilder.withType(LocalDateType(IsoDateTimeFormat), "Timestamp")(
        "2019-12-27T22:15:30+02:00", "2019-12-27T01:49:38+02:00",
        "2019-12-27T22:15:30+08:00", "2017-01-09T22:15:30+02:01"
      )

      val serialized = serialization.serialize(column).success.value
      val deserialized = serialization.deserialize(serialized, classOf[TypedColumn[LocalDateTime]]).success.value

      deserialized shouldEqual column
    }
  }

  "Datasets" should {

    "be serializable" in {
      val data = Array(
        TypedColumnBuilder.withType(LocalDateType(IsoDateTimeFormat), "Timestamp")(
        "2019-12-27T22:15:30+02:00", "2019-12-27T01:49:38+02:00",
          "2019-12-27T22:15:30+08:00", "2017-01-09T22:15:30+02:01"),
        TypedColumnBuilder.from("Age")(16L, 25L, 1L, 32345L),
        TypedColumnBuilder.from("Name")("Sabine", "2#3~\\45²", "Wrong", null),
        TypedColumnBuilder.from("Speed")(13.6, 123.0000000001, 9128347.2, .010203040506)
      )

      val serialized = serialization.serialize(data).success.value
      val deserialized = serialization.deserialize(serialized, classOf[Array[TypedColumn[Any]]]).success.value

      deserialized shouldEqual data
    }
  }
}
