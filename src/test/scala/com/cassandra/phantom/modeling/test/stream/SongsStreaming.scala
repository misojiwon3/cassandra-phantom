package com.cassandra.phantom.modeling.test.stream

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import com.cassandra.phantom.modeling.connector.Connector
import com.cassandra.phantom.modeling.database.ProductionDatabase
import com.cassandra.phantom.modeling.entity.Song
import com.cassandra.phantom.modeling.service.SongsService
import com.datastax.driver.core.utils.UUIDs
import com.outworkers.phantom.streams._
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Created by thiago on 4/13/16.
  *
  * This is a very simple example, actually taken from the akka streams
  * quick start guide example on how to play with streams.
  *
  * The point here is how to get a stream from the database using phantom-dsl.
  *
  * http://doc.akka.io/docs/akka/2.4.4/scala/stream/stream-quickstart.html
  *
  */
object SongsStreaming extends ProductionDatabase with Connector.connector.Connector {

  def main(args: Array[String]) {
    val log = LoggerFactory.getLogger(getClass)

    println("#######################################################")
    database.truncate(10.seconds)

    val insert = Future.sequence(List(
      SongsService.saveOrUpdate(Song(UUIDs.timeBased(), "Pr1ison Song", "Toxicity", "System of a Down")),
      SongsService.saveOrUpdate(Song(UUIDs.timeBased(), "Ae1rials", "Toxicity", "System of a Down")),
      SongsService.saveOrUpdate(Song(UUIDs.timeBased(), "To1xicity", "Toxicity", "System of a Down"))
    ))

    val f = for {
      f2 <- insert
    } yield f2

    Await.result(f, 10.seconds)

    implicit val system = ActorSystem("QuickStart")
    implicit val materializer = ActorMaterializer()

    Source
      .fromPublisher(database.songsModel.publisher())
      .via(Flow[Song].map(song => s"Title: ${song.title} - Album: ${song.album} - Artist: ${song.artist}"))
      .to(Sink.foreach(println))
      .run()
  }
}
