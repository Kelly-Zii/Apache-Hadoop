import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.util.Properties
import java.time.Duration
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}

object CountBeeLandings extends App {
  import Serdes._
  val T  = 20
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "velocity-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)

  val builder = new StreamsBuilder

  import org.apache.kafka.streams.scala.kstream._

  val textLines: KStream[String, String] = builder.stream[String, String]("events")

  val beeLandings: KStream[String, (Int, Int)] = textLines
    .mapValues(value => {
      val parts = value.split(",")
      (parts(2).toInt, parts(3).toInt)
    })

  val beeCounts: KTable[Windowed[String], Long] = beeLandings
    .map((_, value) => (s"${value._1},${value._2}", ""))
    .groupByKey
    .windowedBy(TimeWindows.of(Duration.ofSeconds(T)))
    .count()

  beeCounts.toStream.foreach((key, value) => {
    println(s"Bee count for square $key: $value")
  })
  beeCounts.toStream.to("bee-counts")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }

}
