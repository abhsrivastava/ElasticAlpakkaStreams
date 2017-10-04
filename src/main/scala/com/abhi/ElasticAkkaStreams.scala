package com.abhi

/**
  * Created by ASrivastava on 9/30/17.
  */

import java.nio.file.Paths
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.alpakka.elasticsearch.IncomingMessage
import akka.stream.alpakka.elasticsearch.scaladsl._
import akka.stream.scaladsl._
import akka.stream.{ActorMaterializer, ClosedShape}
import akka.util.ByteString
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import spray.json._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object ElasticAkkaStreams extends App {
   implicit val actorSystem = ActorSystem()
   implicit val actorMaterializer = ActorMaterializer()
   implicit val client = RestClient.builder(new HttpHost("abhisheks-mini", 9200)).build()
   import DefaultJsonProtocol._
   implicit val format = jsonFormat2(CountryCapital)
   val sinkSettings = ElasticsearchSinkSettings(bufferSize = 100000, retryInterval = 5000, maxRetry = 100)
   val resource = getClass.getResource("/countrycapital.csv")
   val path = Paths.get(resource.toURI)
   val source = FileIO.fromPath(path)
   val flow1 = CsvParsing.lineScanner()
   val flow2 = Flow[List[ByteString]].map(list => list.map(_.utf8String))
   val flow3 = Flow[List[String]].map(list => CountryCapital(list(0), list(1)))
   val flow4 = Flow[CountryCapital].map{ cc => cc.toJson.asJsObject}
   val flow5 = Flow[JsObject].map(IncomingMessage[JsObject](Some(UUID.randomUUID.toString), _))
   val sink = ElasticsearchSink("myindex", "mytype", sinkSettings)
   val graph = RunnableGraph.fromGraph(GraphDSL.create(sink){implicit builder =>
      s =>
         import GraphDSL.Implicits._
         source ~> flow1 ~> flow2 ~> flow3 ~> flow4 ~> flow5 ~> s.in
         ClosedShape
   })
   val future = graph.run()
   future.onComplete{ _ =>
      client.close()
      actorSystem.terminate()
   }
   Await.result(actorSystem.whenTerminated, Duration.Inf)
}

case class CountryCapital(country: String, capital: String)
