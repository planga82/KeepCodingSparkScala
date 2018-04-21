package com.keepcoding.speedLayer

import com.keepcoding.commonLayer.MetricasCommon
import com.keepcoding.dominio.{Cliente, Geolocalizacion, Transaccion}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}


/*
*
* */
object MetricasSparkStreamingWindow {



  def run(args: Array[String]): Unit = {

    //********************************************************************************
    // Config

    val conf = new SparkConf().setMaster("local[*]").setAppName("SpeedLayer2")

    val microBatchSize = Seconds(15)
    val windowSize = Seconds(120)

    val inputTopic = "transacciones"

    //Por simpicidad para la revision de las pruebas, todos los topicos son el mismo para revisarlo
    // tod.o desde una unica consola
    val outputTopic = "salida"


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-demo",
      "kafka.consumer.id" -> "kafka-consumer-01"
    )

    //***********************************************************************************

    //Creacion Streaming context
    val ssc = new StreamingContext(conf,microBatchSize)

    //Recepción de kafka
    val input:InputDStream[ConsumerRecord[String,String]]   = KafkaUtils.createDirectStream(ssc,PreferConsistent,Subscribe[String,String](Array(inputTopic), kafkaParams))
    //El motivo de cachearlo es porque si no se usan varios hilos que usan el kafka consumer y da un error porque no es thread safe.
    val transaccionesStream: DStream[Array[String]] = input.map(_.value().split(",")).cache()

    val transaccionesStreamWindow = transaccionesStream.window(windowSize)

    transaccionesStreamWindow.foreachRDD { rdd =>
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      //Definimos los acumuladores para simular los DNIs
      val acumulador = spark.sparkContext.longAccumulator("dniCliente")
      val acumuladorTran = spark.sparkContext.longAccumulator("dniTransaccion")

      //Creacion de un DataFrame con los objetos cliente
      val dfClientes = rdd.map(row => {
        acumulador.add(1)
        Cliente(DNI = acumulador.value, nombre = row(4), cuentaCorriente = row(6))
      }).toDF()

      //Creacion de un DataFrame con los objetos Transacción
      val dfTransacciones = rdd.map(row => {
        acumuladorTran.add(1)
        Transaccion(DNI = acumuladorTran.value, importe = row(2).toDouble, descripcion = row(10).trim, categoria = MetricasCommon.cargaCategoriaCommon(row(10).trim), tarjetaCredito = row(3),
          geolocalizacion = Geolocalizacion(latitud = row(8).toDouble, longitud = row(9).toDouble, ciudad = row(5).trim, pais = "N/A"), fecha = MetricasCommon.formateaFecha2(row(0)))
      }).toDF()

      //Registro las tablas y funciones que voy a usar en el sql
      dfClientes.createOrReplaceTempView("dfClientes")
      dfTransacciones.createOrReplaceTempView("dfTransacciones")
      spark.sqlContext.udf.register("clean", (s: String) => s.substring(0, s.length - 1))


      val tablon = spark.sql("select t.DNI,t.geolocalizacion.ciudad as ciudad from dfTransacciones t join dfClientes c on t.DNI == c.DNI")


      //SAcar el numero de transacciones de una persona(dni) que se han realizado en diferentes ciudades en la ultima hora
      // (Para detectar un posible fraude y avisar en tiempo real)
      val transaccionesSospechosas = tablon.groupBy('DNI,'Ciudad).count().filter('count > 1)

      transaccionesSospechosas.rdd.foreachPartition(MetricasCommon.writeToKafka(outputTopic))


    }
      //Arrancamos el proceso
    ssc.start()
    ssc.awaitTermination()

  }


}
