package com.fjpiqueras.keepcoding.sparksql

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLEjercicios {

  case class User (usuarioId: Int, genero: String, edad: Int, ocupacion: String, codigopostal: String)
  case class Rating (usuarioId: Int, peliculaId: Int, nota: Int, tm: Long)
  case class Pelicula (peliculaId: Int, pelicula: String, genero: String)

  def main(args: Array[String]): Unit = {

    println("Hello, SparkSQL 1.6.3!")


    val sc =  new SparkContext(
      System.getProperty("spark.sql.test.master", "local[1]"),
      "TestSQLContext",
      new SparkConf()
        .set("spark.sql.test", "")
        .set("spark.sql.hive.metastore.barrierPrefixes",
        "org.apache.spark.sql.hive.execution.PairSerDe")
        .set("spark.sql.warehouse.dir", "/home/keepcoding/KeepCoding/Workspace/SPFM/src/main/resources")
        .set("spark.ui.enabled", "false")
        .set("spark.unsafe.exceptionOnMemoryLeak", "true"))

    val sqlContext = new org.apache.spark.sql.SQLContext (sc)

    import sqlContext.implicits._

    val users = sc.textFile("file:////home/keepcoding/KeepCoding/Workspace/SPFM/dataset/users.dat")
                  .map(_.split("::")).map(row => User (row(0).toInt, row(1), row(2).toInt, row(3), row(4))).toDF

    users.show

    val ratings = sc.textFile("file:////home/keepcoding/KeepCoding/Workspace/SPFM/dataset/ratings.dat").
            map(_.split("::")).map(row => Rating (row (0).toInt, row(1).toInt, row(2).toInt, row(3).toLong)).toDF

    val ratingsCopia = sc.textFile("file:////home/keepcoding/KeepCoding/Workspace/SPFM/dataset/ratings.dat").
      map(_.split("::")).map(row => Rating (row (0).toInt, row(1).toInt, row(2).toInt, row(3).toLong)).toDF

    ratings.show

    users.registerTempTable("USUARIOS")
    ratings.registerTempTable("NOTAS")
    ratingsCopia.registerTempTable("NOTASCOPIA")


    val resultado = sqlContext.sql("SELECT u.usuarioId, AVG(nota) FROM USUARIOS u JOIN NOTAS r ON (u.usuarioId = r.usuarioId) GROUP BY u.usuarioId")

    resultado.rdd
    resultado.toJavaRDD

    resultado.persist(StorageLevel.MEMORY_AND_DISK)
    resultado.columns
    resultado.count()

    resultado.distinct()
    resultado.dropDuplicates()

    resultado.explain()

    ratings.union(ratingsCopia)

    ratings.dropDuplicates()

    resultado.show
  }
}
