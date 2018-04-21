package com.keepcoding.commonLayer

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Properties

import com.keepcoding.dominio.Cliente
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{DataFrame, Row}

object MetricasCommon {


  def cargaCategoriaCommon (descripcion:String): String = {
    val seguros = List("home insurance", "car insurace", "life insurance")
    val ocio = List("Shopping Mall","Sports","Cinema","Restaurant")
    val alquiler = List("leasing","rent")
    if(seguros.contains(descripcion)){ return "seguros"}
    else if(ocio.contains(descripcion)){ return "ocio"}
    else if(alquiler.contains(descripcion)){ return "alquiler"}
    else return "otro"
  }

  def formateaFecha(campo: String):Date = {
    val row0 = campo.trim()
    //Quitamos el corchete y nos quedamos solo con la fecha
    val fecha = row0.substring(1).split(" ")(0)
    //Lo pasamos a tipo fecha
    return new Date(new SimpleDateFormat("dd/MM/yy").parse(fecha).getTime())
  }

  def formateaFecha2(campo: String):Date = {
    val row0 = campo.trim()
    //Quitamos el corchete y nos quedamos solo con la fecha
    val fecha = row0.split(" ")(0)
    //Lo pasamos a tipo fecha
    return new Date(new SimpleDateFormat("dd/MM/yy").parse(fecha).getTime())
  }

  def getKafkaConfig(): Properties = {
    val prop = new Properties()
    prop.put("bootstrap.servers","localhost:9092")
    prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
    return prop
  }

  def writeToKafka (outputTopic1: String)(partitionOfRecords: Iterator[Row]): Unit = {
    val producer = new KafkaProducer[String, String](getKafkaConfig())
    partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic1, data.toString())))
    producer.flush()
    producer.close()
  }





}
