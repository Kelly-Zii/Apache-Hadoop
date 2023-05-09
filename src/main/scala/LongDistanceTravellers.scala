import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import java.util.Properties
import java.time.Duration
import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.streams.kstream.{TimeWindows, Windowed}


object LongDistanceTravellers extends App {
  import Serdes._
  val T  = 20
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "long-distance-travellers-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)

  val builder = new StreamsBuilder

  val K = 2 // Number of unique squares for a bee to be considered a long-distance traveller
  val events = builder.stream[String, String]("events")

  val longDistanceTravellers = events
    .map((_, value) => {
      val Array(beeId, timestamp, x, y) = value.split(",")
      (beeId, s"$x,$y")
    })
    .groupByKey
    .windowedBy(TimeWindows.of(Duration.ofSeconds(T)))
    .reduce((v1, v2) => v1) // remove duplicates
    .filter((_, v) => v.size >= K)
    .mapValues(_.toString)


  longDistanceTravellers.toStream.foreach((key, value) => {
    println(s"$key is a long-distance traveller: $value")
  })
  longDistanceTravellers.toStream.to("long-distance-travellers")


  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }

}
