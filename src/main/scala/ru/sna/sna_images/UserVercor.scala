package ru.sna.sna_images

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
case class Data(
               user: Int,
               f0:Double,f1:Double,f2:Double,f3:Double,f4:Double,f5:Double,f6:Double,f7:Double,f8:Double,f9:Double,
               f10:Double,f11:Double,f12:Double,f13:Double,f14:Double,f15:Double,f16:Double,f17:Double,f18:Double,f19:Double,
               f20:Double,f21:Double,f22:Double,f23:Double,f24:Double,f25:Double,f26:Double,f27:Double,f28:Double,f29:Double,
               f30:Double,f31:Double,f32:Double//,f33:Double,f34:Double,f35:Double,f36:Double,f37:Double,f38:Double,f39:Double,
//               f40:Double,f41:Double,f42:Double,f43:Double,f44:Double,f45:Double,f46:Double,f47:Double,f48:Double,f49:Double,
//               f50:Double,f51:Double,f52:Double,f53:Double,f54:Double,f55:Double,f56:Double,f57:Double,f58:Double,f59:Double
               )
object UserVercor extends App {

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

  case class UserData(
                       isLiked: Int,
                       isDisLiked: Int,
                       device: String,
                       showDelay: Long,
                       objectPopulation: Int,
                       objectDayPopulation: Int,
                       ownerPopulation: Int,
                       myCat: Int,
                       myCatScore: Double,
                       ownerCat: Int,
                       ownerCatPercent: Double,
                       imagesN: Int
                     )


  sqlContext.read.parquet(List("/home/ivan/projects/sna2019/day7/features/gt", "/home/ivan/projects/sna2019/day7/features/test", "/home/ivan/projects/sna2019/day7/features/sumb"): _*).rdd.map {
    x =>
      val userId = x.getAs[Int]("userId")
      val isLiked = x.getAs[Int]("isLiked")
      val isDisLiked = x.getAs[Int]("isDisLiked")
      val device = x.getAs[String]("device")
      val showDelay = x.getAs[Long]("showDelay")
      val objectPopulation = x.getAs[Int]("objectPopulation")
      val objectDayPopulation = x.getAs[Int]("objectDayPopulation")
      val ownerPopulation = x.getAs[Int]("ownerPopulation")
      val myCat = x.getAs[Int]("myCat")
      val myCatScore = x.getAs[Double]("myCatScore")
      val ownerCat = x.getAs[Int]("ownerCat")
      val ownerCatPercent = x.getAs[Double]("ownerCatPercent")
      val imagesN = x.getAs[Int]("imagesN")
      userId -> List(UserData(
        isLiked,
        isDisLiked,
        device,
        showDelay,
        objectPopulation,
        objectDayPopulation,
        ownerPopulation,
        myCat,
        myCatScore,
        ownerCat,
        ownerCatPercent,
        imagesN
      ))
  }.reduceByKey(_ ::: _)
    .map{
      case (user, d)=>
//        val dLiked = d.filter(_.isLiked == 1)
        val dSize = d.size
        val deviceN = d.map(_.device).groupBy(identity).mapValues(_.size.toDouble)
//        val deviceLikedN = dLiked.map(_.device).groupBy(identity).mapValues(_.size.toDouble)

        val meanImagesN = d.map(_.imagesN).sum.toDouble / dSize
//        val meanImagesNLiked = dLiked.map(_.imagesN).sum.toDouble / dSize
        val dislikedCount = d.map(_.isDisLiked).sum.toDouble
        val likedCount = d.map(_.isDisLiked).sum.toDouble
        val cat = d.map(_.myCat).groupBy(identity).mapValues(_.size.toDouble)
//        val catLiked = dLiked.map(_.myCat).groupBy(identity).mapValues(_.size.toDouble)
        val objectDayPopulation= d.map(_.objectDayPopulation).sum.toDouble / dSize
        val showDelay = d.map(_.showDelay).sum.toDouble / dSize
//        val showDelayLiked = dLiked.map(_.showDelay).sum.toDouble / dSize


        Data(user, deviceN.getOrElse("MOB",0.0), deviceN.getOrElse("API",0.0), deviceN.getOrElse("WEB",0.0),
          meanImagesN, dislikedCount, dislikedCount/dSize, likedCount, likedCount/dSize,
          cat.getOrElse(0,0.0),cat.getOrElse(1,0.0),cat.getOrElse(2,0.0),cat.getOrElse(3,0.0),cat.getOrElse(4,0.0),cat.getOrElse(5,0.0),cat.getOrElse(6,0.0),
          cat.getOrElse(7,0.0),cat.getOrElse(8,0.0),cat.getOrElse(9,0.0),cat.getOrElse(10,0.0),cat.getOrElse(11,0.0),cat.getOrElse(12,0.0),cat.getOrElse(13,0.0),
          cat.getOrElse(14,0.0),cat.getOrElse(15,0.0),cat.getOrElse(16,0.0),cat.getOrElse(17,0.0),cat.getOrElse(18,0.0),cat.getOrElse(19,0.0),cat.getOrElse(20,0.0),
          cat.getOrElse(21,0.0),objectDayPopulation,


          objectDayPopulation,
          showDelay
          //          deviceLikedN.getOrElse("MOB",0.0), deviceLikedN.getOrElse("API",0.0), deviceLikedN.getOrElse("WEB",0),

//            meanImagesNLiked
//          catLiked.getOrElse(0,0.0),catLiked.getOrElse(1,0.0),catLiked.getOrElse(2,0.0),catLiked.getOrElse(3,0.0),catLiked.getOrElse(4,0.0),catLiked.getOrElse(5,0.0),catLiked.getOrElse(6,0.0),
//          catLiked.getOrElse(7,0.0),catLiked.getOrElse(8,0.0),catLiked.getOrElse(9,0.0),catLiked.getOrElse(10,0.0),catLiked.getOrElse(11,0.0),catLiked.getOrElse(12,0.0),catLiked.getOrElse(13,0.0),
//          catLiked.getOrElse(14,0.0),catLiked.getOrElse(15,0.0),catLiked.getOrElse(16,0.0),catLiked.getOrElse(17,0.0),catLiked.getOrElse(18,0.0),catLiked.getOrElse(19,0.0),catLiked.getOrElse(20,0.0),
//          catLiked.getOrElse(21,0.0),
//          showDelayLiked



        )
    }.toDF().write.parquet("/home/ivan/projects/sna2019/day7/features/user_gt")


}
