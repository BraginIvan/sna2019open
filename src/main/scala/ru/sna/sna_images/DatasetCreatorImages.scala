package ru.sna.sna_images

import java.io.File

import org.apache.spark.rdd.RDD

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

case class Dataset(userId: Int,
                   objectId: Int,
                   ownerId: Int,
                   isLiked: Int,
                   isDisLiked: Int,
                   images: List[String],
                   device: String,
                   createdAt: Long,
                   showAt: Long,
                   showDelay: Long = 0,
                   createdAtWeekDay: Int = 0,
                   createdAtHour: Int = 0,
                   showAtWeekDay: Int = 0,
                   showAtHour: Int = 0,
                   userTrainPopulation: Int = 0,
                   imagePopulation: Int = -1,
                   imageLikedPopulation: Int = -1,
                   objectPopulation: Int = -1,
                   objectDayPopulation: Int = -1,
                   userPopulation: Int = -1,
                   userLikedPopulation: Int = -1,
                   userDisLikedPopulation: Int = -1,
                   userClickedPopulation: Int = -1,
                   userUnlikedPopulation: Int = -1,
                   userResharedPopulation: Int = -1,
                   userOwnerPopulation: Int = -1,
                   userOwnerLikedPopulation: Int = -1,
                   userDevicePopulation: Int = -1,
                   userDeviceLikedPopulation: Int = -1,
                   ownerPopulation: Int = -1,
                   ownerLikedPopulation: Int = -1,
                   myCat: Option[Int] = None,
                   myCatScore: Option[Double] = None,
                   ownerCat: Int = -1,
                   ownerCatPercent: Double = 0.0,
                   ownerCat2: Int = -1,
                   ownerCatPercent2: Double = 0.0,
                   userCat: Int = -1,
                   userCatPercent: Double = 0.0,
                   userCat2: Int = -1,
                   userCatPercent2: Double = 0.0,
                   userLikedCat: Int = -1,
                   userLikedCatPercent: Double = 0.0,
                   userLikedCat2: Int = -1,
                   userLikedCatPercent2: Double = 0.0,
                   imageNetCat: Option[Int] = None,
                   imageNetCatScore: Option[Int] = None,
                   userLikedTheCat: Option[Int] = None,
                   userDislikedTheCat: Option[Int] = None,
                   userIgnoredTheCat: Option[Int] = None,
                   userObjectFeedSize: Option[Int] = None,
                   userObjectFeedIndex: Option[Int] = None,

                   ownerCollabGender: Double = 0.5,
                   ownerCollabRegionSize: Double = 0,

                   collab_auditweights_ctr_gender: Double = 0.0,
                   collab_auditweights_svd_spark: Double = 0.0,
                   collab_auditweights_svd_sparkSTD: Double = 0.0,
                   collab_audit_experimentSize: Int = 0,
                   collab_audit_experiment: String = "",
                   collab_auditweights_userOwner_CREATE_LIKE: Double = 0.0,
                   collab_auditweights_userOwner_CREATE_LIKESTD: Double = 0.0,

                   imagesN: Int = -1,
                   imagesLookAlike: Double = -1,

                   knownLike: Int = -1,
                   userObjectTimes: Int = 1,
                   objectUsersCount: Int = -1,
                   userOwnersCount: Int = -1,
                   ownerUsersCount: Int = -1,

                   userEveningObjects: Int = -1,
                   userNightObjects: Int = -1,
                   userDeepNightObjects: Int = -1,
                   userMorningObjects: Int = -1,
                   userWorkObjects: Int = -1,
                   userLikedEveningObjects: Int = -1,
                   userLikedNightObjects: Int = -1,
                   userLikedDeepNightObjects: Int = -1,
                   userLikedMorningObjects: Int = -1,
                   userLikedWorkObjects: Int = -1,
                   userUniqueCatsCount: Int = 0,
                   ownerUniqueCatsCount: Int = 0,
                   userUniqueCatsLikedCount: Int = 0
                  )


object DatasetCreatorImages {

  //  Thread.sleep(1000*60*60*1)

  val conf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .setMaster("local[6]")
    .set("spark.local.dir", s"/home/ivan/tmp2")
    .set("spark.driver.memory", "10g")
    .set("spark.executor.memory", "3g")

  val spark = SparkContext.getOrCreate(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(spark)
  run()

  def run() {
    def getListOfFiles(dir: String): List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }


    import sqlContext.implicits._

    //    List(FoldersType.ForTrain, FoldersType.ForTest, FoldersType.ForTrainForSubmit, FoldersType.ForSubmit).foreach {
    //    List(FoldersType.ForSubmit).foreach {

    //      foldersType =>

    val foldersType = FoldersType.ForSubmit

    val foldersAll = FolderProvider.getAllFoldersWithoutFeedback(foldersType)
    val foldersUnknownFeedback = FolderProvider.getAllFoldersCurrentDataset(foldersType)
    val foldersWithFeedback = FolderProvider.getFoldersWithFeedback(foldersType)


    val initDataset = sqlContext.read.parquet(foldersUnknownFeedback: _*).rdd.map {
      x =>
        val userId = x.getAs[Int]("instanceId_userId")
        val obj = x.getAs[Int]("instanceId_objectId")
        val ownerId = x.getAs[Int]("metadata_ownerId")
        val feedback = if (foldersType == FoldersType.ForSubmit) Nil else x.getAs[mutable.WrappedArray[String]]("feedback").toList
        val device = x.getAs[String]("audit_clientType")

        val isLiked = if (feedback.contains("Liked")) 1 else 0
        val isDisLiked = if (feedback.contains("Disliked")) 1 else 0
        val images = x.getAs[mutable.WrappedArray[String]]("ImageId").toList
        (obj, ownerId, userId, device) ->
          Dataset(
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
      .reduceByKey { (a, b) =>
        if (a.isLiked == 1)
          a.copy(
            isDisLiked = if (a.isDisLiked == 1 || b.isDisLiked == 1) 1 else 0,
            userObjectTimes = a.userObjectTimes + b.userObjectTimes
          )
        else {
          a.copy(
            isLiked = if (a.isLiked == 1 || b.isLiked == 1) 1 else 0,
            isDisLiked = if (a.isDisLiked == 1 || b.isDisLiked == 1) 1 else 0,
            userObjectTimes = a.userObjectTimes + b.userObjectTimes
          )
        }
      }.map(_._2)


    //        val pornoImgs = getListOfFiles("/home/ivan/projects/disk/home/ivan/imgs/dataset/gym/")
    //          .map(_.getName.split("/").last.split("_").head).toSet
    ////
    //         initDataset
    ////          .filter(_.isLiked == 1)
    ////          .filter(x =>
    ////          x.images.exists(pornoImgs)
    ////        )
    //          .map(x => x.ownerId-> List(x.images.head)).reduceByKey(_ ::: _).filter(x => x._2.count(pornoImgs).toDouble / x._2.size > 0.3).map(_._1).collect().foreach(println)
    //val s = Set(60619, 42376 ,72675 ,30914 ,82738 ,47551 ,85017 ,45748 ,29402 ,51473)
    //    initDataset.filter(x => s(x.ownerId)).map(_.images.head).saveAsTextFile("./images.txt")
    //

    //         initDataset.filter(_.isLiked == 1).filter(x => ponoUsers(x.userId)).flatMap(_.images.map(_-> 1)).reduceByKey(_ + _).filter(_._2 > 1).map(_._1).repartition(1).distinct()
    //            .filter(x => !pornoImgs(x)).saveAsTextFile("./images.txt")


    //            val withLookAlike = LookAlike.run(initDataset, foldersWithFeedback)

    //
    //        initDataset.cache()
    //


    val withCollab = CollabData.run(initDataset)
    val init = InitPrepare(foldersAll = foldersAll, foldersWithFeedback = foldersWithFeedback, datasetUnknownFeedback = withCollab)
    val result = MyImageClasses(foldersUnknownFeedback, init)
    val result2 = MyImageClasses.ownerCat(foldersAll, result)
    val result3 = MyImageClasses.userLikeCat(foldersWithFeedback, result2)


    result3.map(_.copy(images = Nil))
      .toDF().write.parquet(FolderProvider.saveTo(foldersType))
    //    }
  }

}


