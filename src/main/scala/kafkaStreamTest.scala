/**
 * Created by damien.jones on 11/19/2015.
 * All logic for streaming job must be inside the closure
 * All other logic will be executed only once
 * Broadcast variables can be created and re-broadcast as demonstrated below
 * Driver executes logic inside foreachRDD, workers execute logic inside of foreach
 */

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafkaStreamTest {

  def main {
    val conf = new SparkConf().setAppName("simpleStreaming").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(1))
    val sc = ssc.sparkContext
    // setup initial broadcast variable
    var x = sc.broadcast("hello")
    // Define which topics to read from
    val topics = Map("test"->1)

    // inputs are: streaming context, ip address:port for kafka, groupid of kafka consumer (put in whatever name you want), map of topics to subscribe to
    val stream = KafkaUtils.createStream(ssc, "0.0.0.0:2181", "group", topics)
    stream.foreachRDD {r =>
      // the following happens in the workers
      r.foreach {z=>
        println(z._2+" "+x.value)
      }
      // the following happens in the driver
      if ((System.currentTimeMillis / 1000) % 10 == 0) {
        // remove broadcast variable data without blocking
        x.unpersist(false)
        // recreates broadcast variable data
        x = sc.broadcast(sc.textFile("data.txt").collect()(0))
      }
    }
    ssc.start
    ssc.awaitTermination
  }
}
