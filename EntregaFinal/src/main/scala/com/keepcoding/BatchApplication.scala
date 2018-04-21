package com.keepcoding

import com.keepcoding.batchLayer.MetricasSparkSQL

object BatchApplication {

  def main(args: Array[String]): Unit = {

    MetricasSparkSQL.run()
  }
}
