package com.keepcoding

import com.keepcoding.speedLayer.{MetricasSparkStreamingWindow}

object StreamApplication2 {

  def main(args: Array[String]): Unit = {
    /*if(args.length==6){
      MetricasSparkStreaming.run(args)
    }else{
      println("Se esta intentando arrancar la aplicacion sin los parametros necesarios")
    }*/
    MetricasSparkStreamingWindow.run(args)
  }
}
