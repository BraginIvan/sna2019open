package ru.sna

import org.apache.spark.{SparkConf, SparkContext}

trait SparkProvider {


  private val master = "local"
  val conf = new SparkConf()
    .setAppName(getClass.getSimpleName)
//    .setMaster("local[6]")
//    .set("spark.local.dir", s"/home/ivan/tmp2")
//    .set("spark.driver.memory", "10g")
//    .set("spark.executor.memory", "3g")
  val spark = SparkContext.getOrCreate(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(spark)

}
