package ru.sna.sna_collab

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


import scala.collection.mutable



object UserVercor2 extends App {


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

  case class UserData2(
                        userId: Int,
                        auditweights_svd_spark: Double,
                        auditweights_svd_prelaunch: Double,
                        auditweights_ctr_high: Double,
                        auditweights_numLikes: Double,
                        userOwnerCounters_CREATE_LIKE: Double,
                          auditweights_ctr_gender:Double

                      )

  case class Out(user:Int, features: List[Double])
  val foldersType = FoldersType.ForSubmit

  val foldersAll = FolderProvider.getAllFoldersWithoutFeedback(foldersType)
  val foldersUnknownFeedback = FolderProvider.getAllFoldersCurrentDataset(foldersType)
  val foldersWithFeedback = FolderProvider.getFoldersWithFeedback(foldersType)



    val userAllData = sqlContext.read.parquet(foldersAll: _*).rdd.map {
      x =>
        val userId = x.getAs[Int]("instanceId_userId")
        userId->List(UserData2(
          userId = userId,
          auditweights_svd_spark = x.getAs[Double]("auditweights_svd_spark"),
          auditweights_svd_prelaunch = x.getAs[Double]("auditweights_svd_prelaunch"),
          auditweights_ctr_high = x.getAs[Double]("auditweights_ctr_high"),
          auditweights_numLikes= x.getAs[Double]("auditweights_ctr_high"),
          userOwnerCounters_CREATE_LIKE = x.getAs[Double]("userOwnerCounters_CREATE_LIKE"),
          auditweights_ctr_gender = x.getAs[Double]("auditweights_ctr_gender")

        ))

    }





    .reduceByKey(_ ::: _)
    .map {
      case (user, d) =>
       val dSize = d.size
        (user,
          List(dSize,
          d.map(_.auditweights_svd_spark).max,d.map(_.auditweights_svd_spark).min, d.map(_.auditweights_svd_spark).sum / dSize,
          d.map(_.auditweights_svd_prelaunch).max,d.map(_.auditweights_svd_prelaunch).min, d.map(_.auditweights_svd_prelaunch).sum / dSize,
          d.map(_.auditweights_ctr_high).max,d.map(_.auditweights_ctr_high).min, d.map(_.auditweights_ctr_high).sum / dSize,
          d.map(_.auditweights_numLikes).max,d.map(_.auditweights_numLikes).min, d.map(_.auditweights_numLikes).sum / dSize,
          d.map(_.userOwnerCounters_CREATE_LIKE).max,d.map(_.userOwnerCounters_CREATE_LIKE).min, d.map(_.userOwnerCounters_CREATE_LIKE).sum / dSize)
        )

    }.toDF().write.parquet(FolderProvider.saveUserTo(foldersType))


}
