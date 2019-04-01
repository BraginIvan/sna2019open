package ru.sna.sna_images

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


import scala.collection.mutable

case class Data3(user: Int, features: List[Option[Int]])

object UserVercor3 extends App{

  private val master = "local"
  val conf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .setMaster("local[6]")
    .set("spark.local.dir", s"/home/ivan/tmp2")
    .set("spark.driver.memory", "10g")
    .set("spark.executor.memory", "3g")
  val spark = SparkContext.getOrCreate(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(spark)

  import sqlContext.implicits._


  val foldersType = FoldersType.ForTrain

  val foldersAll = FolderProvider.getAllFoldersWithoutFeedback(FoldersType.ForTest)


  val userAllData = sqlContext.read.parquet(foldersAll: _*).rdd.map {
    x =>
      val userId = x.getAs[Int]("instanceId_userId")
      val ownerId = x.getAs[Int]("metadata_ownerId")
      userId -> ownerId

  }

  val ownerId = userAllData.map(_._2 -> 1).reduceByKey(_ + _).filter(_._2 > 100).map(_._1).collect().toList.zipWithIndex.map(x => x._1 -> x._2).toMap

  Thread.sleep(1000)

  userAllData.map(x => x._1 -> List(x._2))
    .reduceByKey(_ ::: _)

    .flatMap {
      case (user, owners) if owners.size > 3 =>
        val ownersSize = owners.groupBy(identity).mapValues(_.size)


        Some(user + "," +  ownersSize.flatMap(x => ownerId.get(x._1).map(_ +"|"+ x._2)).mkString(","))

      case _ => None
  //  }.toDF().write.parquet(FolderProvider.saveUserTo(foldersType))
      }.saveAsTextFile(FolderProvider.saveUserTo(foldersType))


}
