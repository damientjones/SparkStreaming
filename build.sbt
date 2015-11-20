name := "SparkStreaming"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.5.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.11" % "1.5.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.5.0"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.5.0-M2"