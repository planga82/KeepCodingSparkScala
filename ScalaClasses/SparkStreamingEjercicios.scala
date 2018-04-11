import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object SparkStreamingKafka extends App {


  //Ejercicio 1: Definición de la configuración de Spark
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamingKafkaEjercicios")

  //Ejercicio 2: Definición del nombre del tópico de Kafka del que consumiremos
  val inputTopic = "mensajes"

  //Ejercicio 3: Definición de la configuración de Kafka, servidor, deseralizador, serializador etc
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "spark-demo",
    "kafka.consumer.id" -> "kafka-consumer-01"
  )

  //Ejercicio 4 Definición del contexto de spark streaming
  val ssc = new StreamingContext(conf, Seconds(1))


  //Ejercicio 5: Definición del DStream
  val inputStream = KafkaUtils.createDirectStream(ssc,
    PreferConsistent, Subscribe[String, String](Array(inputTopic), kafkaParams))

  //Ejercicio 6: Guardado del valor consumido de Kafka
  val processedStream = inputStream.map(record => record.value)

  //Ejercicio 7: Muestreo de los datos por pantalla
  processedStream.print()

  //Ejercicio 8: Definición de la configuración del tópico de salida de Kafka
  val broker = "localhost:9092"
  val properties = new Properties()
  properties.put("bootstrap.servers", broker)
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val outputTopic="mensajesSalida"

  //Ejercicio 9: Creación del productor de Kafka que enviará la información procesada por SparkStreaming
  val producer = new KafkaProducer[String, String](properties)
  processedStream.foreachRDD(rdd =>

    rdd.foreach {
      case data: String => {
        val message = new ProducerRecord[String, String](outputTopic, data)
        producer.send(message).get().toString
      }
    })

  //Ejercicio 10: Arranque del streaming y la espera de finalización del propio job
  ssc.start()
  ssc.awaitTermination()
}