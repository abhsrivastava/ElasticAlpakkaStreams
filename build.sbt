name := "ElasticAkkaStreams"

version := "1.0"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
   "com.lightbend.akka" %% "akka-stream-alpakka-elasticsearch" % "0.13",
   "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "0.13",
   "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.13"
)

mainClass in run := Some("com.abhi.ElasticAkkaStreams")