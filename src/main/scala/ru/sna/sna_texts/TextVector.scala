package ru.sna.sna_texts

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object TextVector extends App {


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

  case class UserData2(userId: Int,
                        preprocessed:List[String]
                      )

  case class Out(user:Int, features: List[Double])



//
//  val userAllData = sqlContext.read.parquet(List("/home/ivan/projects/sna2019/texts/textsTrain", "/home/ivan/projects/sna2019/texts/textsTest") : _*).rdd.map {
//    x =>
//      val userId = x.getAs[Int]("instanceId_userId")
//      userId-> List(UserData2(
//        userId = userId,
//        preprocessed=x.getAs[mutable.WrappedArray[String]]("preprocessed").toList
//      ))
//  }.reduceByKey(_ ::: _)
//    .map {
//      case (user, d) =>
//        val dSize = d.size
//        (user,
//          List(dSize,
//            d.flatMap(_.preprocessed).groupBy(identity).mapValues(_.size))
//        )
//
//    }.toDF().write.parquet(FolderProvider.saveUserTo(foldersType))


}


