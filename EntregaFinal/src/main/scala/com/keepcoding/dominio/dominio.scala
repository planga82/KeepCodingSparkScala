package com.keepcoding.dominio

import java.sql.Date

case class Geolocalizacion(latitud: Double, longitud:Double, ciudad: String, pais:String)

case class Transaccion(DNI:Long, importe: Double, descripcion:String, categoria:String, tarjetaCredito:String, geolocalizacion: Geolocalizacion, fecha:Date)

case class Cliente(DNI: Long, nombre: String, cuentaCorriente:String)