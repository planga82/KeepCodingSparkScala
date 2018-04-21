package com.keepcoding.speedLayer



import com.keepcoding.commonLayer.MetricasCommon
import com.keepcoding.dominio.{Cliente, Geolocalizacion, Transaccion}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{count, current_timestamp, datediff, desc}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object MetricasSparkStreaming {



  def run(args: Array[String]): Unit = {

    //********************************************************************************
    // Config

    val conf = new SparkConf().setMaster("local[*]").setAppName("SpeedLayer")

    val microBatchSize = Seconds(15)

    val inputTopic = "transacciones"

    //Por simpicidad para la revision de las pruebas, todos los topicos son el mismo para revisarlo
    // tod.o desde una unica consola
    val outputTopic1 = "salida"
    val outputTopic2 = "salida"
    val outputTopic3 = "salida"
    val outputTopic4 = "salida"
    val outputTopic5 = "salida"
    val outputTopic6 = "salida"

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


    transaccionesStream.foreachRDD { rdd =>
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


      val tablon = spark.sql("select t.fecha, t.DNI,t.importe,t.descripcion as descripcion,t.categoria,t.tarjetaCredito, t.geolocalizacion.ciudad as ciudad, c.nombre, c.cuentaCorriente from dfTransacciones t join dfClientes c on t.DNI == c.DNI")

      //Entiendo que todos los siguientes calculos irian en diferentes procesos streaming ya que son calculos que no tiene sentido hacer secuencialmente
      //Pero por simplicidad para la practica estan todos puestos aqui.

      //Agrupamos los clientes por ciudad y contamos es numero de lineas
      val clientesCiudad = tablon.groupBy("ciudad").agg(count("ciudad").alias("NumTransacciones")).orderBy(desc("NumTransacciones"))

      //Filtramos por pagos superiores a 500 y nos quedamos con los nombre y los DNIs de los clientes
      val clientesPagosAltos = tablon.where('importe > 500).select("nombre", "DNI")

      //Filtramos por la ciudad de Londres y segun el enunciado he entendido con agruar que se quieres que
      //salgan todos juntos, por eso ordeno
      val clientesLondres = tablon.where('ciudad === "London").orderBy("DNI")

      //Filtramos por categoria OCIO, que ya ha sido cargada anteriormente
      val categoriaOcio = tablon.where('categoria === "ocio")

      // Ultimas transacciones de cada cliente en los ultimos 30 dias
      val transaccionesUltimosDias = tablon.where(datediff(current_timestamp(), 'fecha) < 30)

      //Nuevas metricas de la tarea 4
      //Media de lo que se paga con cada tipo de tarjeta
      val mediaTarjetas = tablon.groupBy('tarjetaCredito).mean("importe").orderBy(desc("avg(importe)"))

      clientesCiudad.rdd.foreachPartition(MetricasCommon.writeToKafka(outputTopic1))
      clientesPagosAltos.rdd.foreachPartition(MetricasCommon.writeToKafka(outputTopic2))
      clientesLondres.rdd.foreachPartition(MetricasCommon.writeToKafka(outputTopic3))
      categoriaOcio.rdd.foreachPartition(MetricasCommon.writeToKafka(outputTopic4))
      transaccionesUltimosDias.rdd.foreachPartition(MetricasCommon.writeToKafka(outputTopic5))
      mediaTarjetas.rdd.foreachPartition(MetricasCommon.writeToKafka(outputTopic6))

    }
      //Arrancamos el proceso
    ssc.start()
    ssc.awaitTermination()

  }




}
