package ru.sna.sna_images

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


import scala.collection.mutable

case class Data2(
                  user: Int,
                  f0: Double, f1: Double, f2: Double, f3: Double, f4: Double, f5: Double, f6: Double, f7: Double, f8: Double, f9: Double,
                  f10: Double, f11: Double, f12: Double, f13: Double, f14: Double, f15: Double, f16: Double, f17: Double, f18: Double, f19: Double,
                  f20: Double, f21: Double, f22: Double, f23: Double, f24: Double, f25: Double, f26: Double, f27: Double, f28: Double, f29: Double,
                  f30: Double, f31: Double, f32: Double ,f33:Double,f34:Double,f35:Double,f36:Double,f37:Double,f38:Double,f39:Double,
                                 f40:Double,f41:Double,f42:Double,f43:Double,f44:Double,f45:Double,f46:Double,f47:Double,f48:Double,f49:Double,
                                 f50:Double,f51:Double,f52:Double,f53:Double,f54:Double,f55:Double,f56:Double,f57:Double//,f58:Double,f59:Double
                )

object UserVercor2 extends App {

  val path = "/home/ivan/projects/sna2019/day7/my_classes_new2.csv"
  val path2 = "/home/ivan/projects/sna2019/day7/my_test_classes_new2.csv"

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
                        objectId: Int,
                        ownerId: Int,
                        isLiked: Int,
                        isDisLiked: Int,
                        images: List[String],
                        device: String,
                        createdAt: Long,
                        showAt: Long,
                        cat: Int = -1

                      )


  val foldersType = FoldersType.ForTest

  val foldersAll = FolderProvider.getAllFoldersWithoutFeedback(foldersType)
  val foldersUnknownFeedback = FolderProvider.getAllFoldersCurrentDataset(foldersType)
  val foldersWithFeedback = FolderProvider.getFoldersWithFeedback(foldersType)

  val imageType: RDD[(String, (Int, Double))] = spark.textFile(path)
    .union(spark.textFile(path2))
    .map {
      x =>
        val split = x.split("\t")
        val element = split.lift(1).map(_.toInt).getOrElse(-1)
        val score = split.lift(2).map(_.toDouble).getOrElse(0.0)
        val image = split.head.split("/").last.split("_").head
        image -> (element, score)
    }.distinct()

  //  val userAllData = sqlContext.read.parquet(foldersAll: _*).rdd.map {
  //    x =>
  //      val imageId = x.getAs[Int]("instanceId_userId")
  //      val obj = x.getAs[Int]("instanceId_objectId")
  //      val ownerId = x.getAs[Int]("metadata_ownerId")
  //      val feedback =x.getAs[mutable.WrappedArray[String]]("feedback").toList
  //      val device = x.getAs[String]("audit_clientType")
  //      val isLiked = if (feedback.contains("Liked")) 1 else 0
  //      val isDisLiked = if (feedback.contains("Disliked")) 1 else 0
  //      val images = x.getAs[mutable.WrappedArray[String]]("ImageId").toList
  //      UserData2(
  //        userId = imageId,
  //        objectId = obj,
  //        images = images,
  //        ownerId = ownerId,
  //        isLiked = isLiked,
  //        isDisLiked = isDisLiked,
  //        createdAt = x.getAs[Long]("metadata_createdAt"),
  //        showAt = x.getAs[Long]("audit_timestamp"),
  //        device = device)
  //  }


  val userFeedbackData = sqlContext.read.parquet(foldersWithFeedback: _*).rdd.map {
    x =>
      val userId = x.getAs[Int]("instanceId_userId")
      val obj = x.getAs[Int]("instanceId_objectId")
      val ownerId = x.getAs[Int]("metadata_ownerId")
      val feedback = x.getAs[mutable.WrappedArray[String]]("feedback").toList
      val device = x.getAs[String]("audit_clientType")
      val isLiked = if (feedback.contains("Liked")) 1 else 0
      val isDisLiked = if (feedback.contains("Disliked")) 1 else 0
      val images = x.getAs[mutable.WrappedArray[String]]("ImageId").toList
      UserData2(
        userId = userId,
        objectId = obj,
        images = images,
        ownerId = ownerId,
        isLiked = isLiked,
        isDisLiked = isDisLiked,
        createdAt = x.getAs[Long]("metadata_createdAt"),
        showAt = x.getAs[Long]("audit_timestamp"),
        device = device)
  }

    .map { x => x.images.head -> x }
    .leftOuterJoin(imageType)
    .map {
      case (_, (d, c)) =>
        d.userId -> List(d.copy(cat = c.map(_._1).getOrElse(-1)))
    }.reduceByKey(_ ::: _)

    .flatMap {
      case (user, d) if d.size > 1 =>
        val dLiked = d.filter(_.isLiked == 1)
        val dSize = d.size
        val deviceN = d.map(_.device).groupBy(identity).mapValues(_.size.toDouble)
        val deviceLikedN = dLiked.map(_.device).groupBy(identity).mapValues(_.size.toDouble)

        val meanImagesN = d.map(_.images.size).sum.toDouble / dSize
        val meanImagesNLiked = dLiked.map(_.images.size).sum.toDouble / dSize
        val dislikedCount = d.map(_.isDisLiked).sum.toDouble
        val likedCount = d.map(_.isDisLiked).sum.toDouble
        val cat = d.map(_.cat).groupBy(identity).mapValues(_.size.toDouble)
        val catLiked = dLiked.map(_.cat).groupBy(identity).mapValues(_.size.toDouble)
//        val objectDayPopulation = d.map(_.objectDayPopulation).sum.toDouble / dSize
//        val showDelay = d.map(_.showDelay).sum.toDouble / dSize
        //        val showDelayLiked = dLiked.map(_.showDelay).sum.toDouble / dSize


       Some( Data2(user, deviceN.getOrElse("MOB", 0.0), deviceN.getOrElse("API", 0.0), deviceN.getOrElse("WEB", 0.0),
          meanImagesN, dislikedCount, dislikedCount / dSize, likedCount, likedCount / dSize,
          cat.getOrElse(0, 0.0), cat.getOrElse(1, 0.0), cat.getOrElse(2, 0.0), cat.getOrElse(3, 0.0), cat.getOrElse(4, 0.0), cat.getOrElse(5, 0.0), cat.getOrElse(6, 0.0),
          cat.getOrElse(7, 0.0), cat.getOrElse(8, 0.0), cat.getOrElse(9, 0.0), cat.getOrElse(10, 0.0), cat.getOrElse(11, 0.0), cat.getOrElse(12, 0.0), cat.getOrElse(13, 0.0),
          cat.getOrElse(14, 0.0), cat.getOrElse(15, 0.0), cat.getOrElse(16, 0.0), cat.getOrElse(17, 0.0), cat.getOrElse(18, 0.0), cat.getOrElse(19, 0.0), cat.getOrElse(20, 0.0),
          cat.getOrElse(21, 0.0),
          deviceLikedN.getOrElse("MOB",0.0), deviceLikedN.getOrElse("API",0.0), deviceLikedN.getOrElse("WEB",0),
          meanImagesNLiked,
          catLiked.getOrElse(0,0.0),catLiked.getOrElse(1,0.0),catLiked.getOrElse(2,0.0),catLiked.getOrElse(3,0.0),catLiked.getOrElse(4,0.0),catLiked.getOrElse(5,0.0),catLiked.getOrElse(6,0.0),
          catLiked.getOrElse(7,0.0),catLiked.getOrElse(8,0.0),catLiked.getOrElse(9,0.0),catLiked.getOrElse(10,0.0),catLiked.getOrElse(11,0.0),catLiked.getOrElse(12,0.0),catLiked.getOrElse(13,0.0),
          catLiked.getOrElse(14,0.0),catLiked.getOrElse(15,0.0),catLiked.getOrElse(16,0.0),catLiked.getOrElse(17,0.0),catLiked.getOrElse(18,0.0),catLiked.getOrElse(19,0.0),catLiked.getOrElse(20,0.0),
          catLiked.getOrElse(21,0.0),
          d.size,dLiked.size
        ))
      case _ => None
    }.toDF().write.parquet(FolderProvider.saveUserTo(foldersType))


}
