import java.util.UUID
import scala.util.Random
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

object GenerateBeeFlight extends App {

  val props: Properties = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "generate-bee-flight")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put("acks", "all")
  props.put("linger.ms", 1)
  props.put("retries", 0)

  val producer: Producer[String, String] = new KafkaProducer[String, String](props)

  val W = 10
  val H = 10

  def generateLandingEvent(): String = {
    val beeId = UUID.randomUUID().toString
    val timestamp = System.currentTimeMillis() / 1000
    val x = Random.nextInt(W)
    val y = Random.nextInt(H)
    s"$beeId,$timestamp,$x,$y"
  }

  def sendLandingEvent(producer: Producer[String, String]): Unit = {
    val event = generateLandingEvent()
    val record = new ProducerRecord[String, String]("events", null, event)
    println(s"Sent record to topic ${record.topic()}, partition ${record.partition()}, value ${record.value()}")
    producer.send(record)
  }

  while (true) {
    sendLandingEvent(producer)
    Thread.sleep(Random.nextInt(5000))
  }

  sys.ShutdownHookThread {
    producer.flush()
    producer.close()
  }
}
