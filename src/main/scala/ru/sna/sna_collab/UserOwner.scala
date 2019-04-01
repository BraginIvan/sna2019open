package ru.sna.sna_collab

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object UserOwner extends App {


  private val master = "local"
  val conf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .setMaster("local[6]")
    .set("spark.local.dir", s"/home/ivan/tmp2")
    .set("spark.driver.memory", "10g")
    .set("spark.executor.memory", "3g")
  val spark = SparkContext.getOrCreate(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(spark)

  val foldersType = FoldersType.ForSubmit

  val foldersAll = FolderProvider.getAllFoldersWithoutFeedback(foldersType)
  val foldersFB = FolderProvider.getFoldersWithFeedback(foldersType)


   sqlContext.read.parquet(foldersAll: _*).rdd.collect {
     case  x
       if x.getAs[Double]("auditweights_userOwner_CREATE_LIKE") > 0.7 =>
         x.getAs[Int]("instanceId_userId") -> List(x.getAs[Int]("metadata_ownerId"))

    }.reduceByKey(_ ::: _)

    .filter(_._2.size > 1)
      .map(x => x._1 + ";" + x._2.mkString(","))
      .saveAsTextFile("userOwner.csv")






}
