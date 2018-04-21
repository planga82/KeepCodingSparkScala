package com.keepcoding

import com.keepcoding.speedLayer.MetricasSparkStreaming

object StreamApplication {

  def main(args: Array[String]): Unit = {
    /*if(args.length==6){
      MetricasSparkStreaming.run(args)
    }else{
      println("Se esta intentando arrancar la aplicacion sin los parametros necesarios")
    }*/
    MetricasSparkStreaming.run(args)
  }
}
