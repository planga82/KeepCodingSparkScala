package com.keepcoding.batchLayer

import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.atomic.LongAccumulator

import com.keepcoding.dominio.{Cliente, Geolocalizacion, Transaccion}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import com.keepcoding.commonLayer.MetricasCommon

object MetricasSparkSQL {



  def run(): Unit = {

    //Creacion del contexto de spark
    val sparkSession = SparkSession.builder().master(master = "local[*]").appName("Practica Final - Batch layer").getOrCreate()
    import sparkSession.implicits._

    //Definicion de rutas de entrada y salida
    val inputCSVfile = "/home/keepcoding/Downloads/TransaccionesNew.csv"
    val outputPath = "/home/keepcoding/Downloads/output"

    //Carga del fichero
    val rddTransacciones = sparkSession.read.csv(s"file:///${inputCSVfile}")

    //Eliminamos la cabecera
    val cabecera = rddTransacciones.first
    val rddSinCabecera = rddTransacciones.filter(!_.equals(cabecera)).map(_.toString().split(","))

    //Acumuladores para simular el DNI
    val acumulador = sparkSession.sparkContext.longAccumulator("dniCliente")
    val acumuladorTran = sparkSession.sparkContext.longAccumulator("dniTransaccion")

    //Creacion de un DataFrame con los objetos cliente
    val dfClientes = rddSinCabecera.map(row => {
      acumulador.add(1)
      Cliente(DNI = acumulador.value,nombre= row(4), cuentaCorriente=row(6))
    })

    //Creacion de un DataFrame con los objetos Transacción
    val dfTransacciones = rddSinCabecera.map(row => {
      acumuladorTran.add(1)
      Transaccion(DNI = acumuladorTran.value,importe = row(2).toDouble,descripcion = row(10).trim,categoria = cargaCategoria(row(10).trim),tarjetaCredito = row(3),
                  geolocalizacion = Geolocalizacion(latitud = row(8).toDouble,longitud = row(9).toDouble,ciudad = row(5).trim,pais = "N/A"), fecha=MetricasCommon.formateaFecha(row(0)))
    })


    //Registro las tablas y funciones que voy a usar en el sql
    dfClientes.createOrReplaceTempView("dfClientes")
    dfTransacciones.createOrReplaceTempView("dfTransacciones")
    //Se elimina el ] del final del registro (Lo hago aqui y no en la carga por practicar)
    sparkSession.sqlContext.udf.register("clean", (s:String) => s.substring(0,s.length-1))

    // Alterno spark SQL y dataframes por parcticar
    val tablon = sparkSession.sql("select t.fecha, t.DNI,t.importe,clean(t.descripcion) as descripcion,t.categoria,t.tarjetaCredito, t.geolocalizacion.ciudad as ciudad, c.nombre, c.cuentaCorriente  from dfTransacciones t join dfClientes c on t.DNI == c.DNI")

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
    val transaccionesUltimosDias = tablon.where(datediff(current_timestamp(),'fecha) < 30 )


    clientesCiudad.write.mode("append").csv(outputPath + "/clientesCiudad.csv")
    clientesPagosAltos.write.mode("append").json(outputPath + "/clientesPagosAltos.json")
    clientesLondres.write.mode("append").parquet(outputPath + "/clientesLondres.parquet")
    categoriaOcio.write.mode("append").format("csv").option("compression","gzip").save(outputPath + "/categoriaOcio.csv.gzip")
    transaccionesUltimosDias.write.mode("append").avro(outputPath + "/transaccionesUltimosDias.avro")


  }



  //Funcion para cargar las categorias
  def cargaCategoria (descripcion:String): String = {
    val desc = descripcion.substring(0,descripcion.length-1)
    return MetricasCommon.cargaCategoriaCommon(desc)
  }


}
