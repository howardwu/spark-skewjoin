package com.tresata.spark.skewjoin

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSuite {
  lazy val sc = {
    val conf = new SparkConf(false)
      .setMaster("local")
      .setAppName("test")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("Spark.ui.enable", "false")
    new SparkContext(conf)
  }

  lazy val jsc = new JavaSparkContext(sc)
  def javaSparkContext() = jsc
}
