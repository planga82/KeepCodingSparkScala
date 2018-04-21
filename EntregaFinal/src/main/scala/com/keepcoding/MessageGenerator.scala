package com.keepcoding

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source
import scala.util.Random

object MessageGenerator {



  def getKafkaConfig(): Properties = {
    val prop = new Properties()
    prop.put("bootstrap.servers","localhost:9092")
    prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    return prop
  }

  val inputCSVfile = "/home/keepcoding/Downloads/TransaccionesNew.csv"
  val numMessages: Int = 1000
  val delay: Long = 100
  val outputTopic: String = "transacciones"
  var lines: List[String] = List[String]()
  def produceMessages(): Unit = {

    val producer: KafkaProducer[String,String] = new KafkaProducer[String,String](getKafkaConfig())

    for(a <- 1 to numMessages){
      Thread.sleep(delay)
      producer.send(new ProducerRecord[String, String](outputTopic, generateRandomData()))
    }

  }

  def generateRandomData(): String = {
    if(lines.length == 0){
      lines = Source.fromFile(inputCSVfile).getLines().toList
    }
    return lines(new Random().nextInt(lines.length))


  }

  def main(args: Array[String]): Unit = {
    produceMessages()
  }
}
