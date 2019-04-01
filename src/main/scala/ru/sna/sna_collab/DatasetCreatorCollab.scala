package ru.sna.sna_collab

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.sna.sna_images.MyImageClasses

import scala.collection.{immutable, mutable}

case class Dataset(
                    //USER OWNER membership_status EXISTS P, A, NULL
                    // audit_resourceType BASING METRICS
                    images: List[String],
                    isLiked: Int,
                    isDisLiked: Int,
                    isUnLiked: Int,
                    isReShared: Int,

                    objectId: Int,

                    userId: Int,
                    ownerId: Int,
                    device: String,
                    objectType: String,
                    metadata_authorId: Int,
                    user_region: Long,
                    membership_status: String,
                    user_ID_country: Long,

                    audit_resourceType: Long, //cat
                    metadata_totalVideoLength: Int,
                    audit_pos: Long,
                    auditweights_ctr_high: Double,
                    auditweights_ctr_gender: Double,
                    auditweights_friendLikes: Double,
                    metadata_numPhotos: Int,
                    metadata_numPolls: Int,
                    metadata_numSymbols: Int,
                    //                   auditweights_isRandom: Double,
                    auditweights_svd: Double,

                    user_create_date: Long,


                    auditweights_dailyRecency: Double,

                    auditweights_friendLikes_actors: Double,
                    auditweights_likersFeedStats_hyper: Double,
                    auditweights_likersSvd_hyper: Double,
                    metadata_platform: String,

                    userOwnerCounters_CREATE_LIKE: Double,
                    IS_EXTERNAL_SHARE: Int,
                    user_ID_Location: Int,

                    objectMean_auditweights_svd_spark: Double = 0.0,
                    objectMean_userOwnerCounters_CREATE_LIKE: Double = 0.0,
                    objectMean_auditweights_ctr_gender: Double = 0.0,
                    objectMean_auditweights_userOwner_CREATE_LIKE: Double = 0.0,
                    objectMean_auditweights_ctr_high: Double = 0.0,
                    objectMean_userOwnerCounters_USER_FEED_REMOVE: Double = 0.0,
                    //                   objectMean_audit_experiment: Double = 0.0,
                    objectMean_auditweights_numLikes: Double = 0.0,
                    userMean_auditweights_svd_spark: Double = 0.0,
                    userMean_userOwnerCounters_CREATE_LIKE: Double = 0.0,
                    userMean_auditweights_ctr_gender: Double = 0.0,
                    userMean_auditweights_userOwner_CREATE_LIKE: Double = 0.0,
                    userMean_auditweights_ctr_high: Double = 0.0,
                    userMean_userOwnerCounters_USER_FEED_REMOVE: Double = 0.0,
                    //                   userMean_audit_experiment: Double = 0.0,
                    userMean_auditweights_numLikes: Double = 0.0,
                    //                    userMean_auditweights_isRandom: Double = 0.0,
                    userCounts_membership_status: Int = 0,
                    ownerMean_auditweights_svd_spark: Double = 0.0,
                    ownerMean_userOwnerCounters_CREATE_LIKE: Double = 0.0,
                    ownerMean_auditweights_ctr_gender: Double = 0.0,
                    ownerMean_auditweights_userOwner_CREATE_LIKE: Double = 0.0,
                    ownerMean_auditweights_ctr_high: Double = 0.0,
                    ownerMean_userOwnerCounters_USER_FEED_REMOVE: Double = 0.0,
                    ownerMean_auditweights_numLikes: Double = 0.0,
                    ownerCount_authors: Int = 0,

                    authorMean_auditweights_svd_spark: Double = 0.0,
                    authorMean_userOwnerCounters_CREATE_LIKE: Double = 0.0,
                    authorMean_auditweights_ctr_gender: Double = 0.0,
                    authorMean_auditweights_userOwner_CREATE_LIKE: Double = 0.0,
                    authorMean_auditweights_ctr_high: Double = 0.0,
                    authorMean_userOwnerCounters_USER_FEED_REMOVE: Double = 0.0,
                    authorMean_auditweights_numLikes: Double = 0.0,
                    authorCount_owners: Int = 0,
                    authorPopulation: Int = 0,
                    authorLikedPopulation: Int = 0,
                    authorUsersCount: Int = 0,

                    userOwnerCounters_TEXT: Double,
                    userOwnerCounters_IMAGE: Double,

                    auditweights_numDislikes: Double,
                    auditweights_numLikes: Double,
                    auditweights_numShows: Double,

                    //                   auditweights_matrix: Double,
                    auditweights_ageMs: Double,
                    createdAt: Long,
                    showAt: Long,
                    joinAgo: Long,

                    auditweights_userOwner_CREATE_LIKE: Double,
                    auditweights_userOwner_TEXT: Double,
                    userOwnerCounters_UNKNOWN:Double,


                    dob: Int,
                    sex: Int,
                    imagesN: Int,
                    userOwnerCounters_VIDEO: Int,
                    auditweights_ctr_negative: Double,
                    textCat: Int = -1,
                    showDelay: Long = 0,
                    objectPopulation: Int = -1,
                    objectDayPopulation: Int = -1,

                    userOwnerCounters_USER_FEED_REMOVE: Double,
                    metadata_numTokens: Int,
                    metadata_numVideos: Int,
                    createdAtWeekDay: Int = 0,
                    createdAtHour: Int = 0,
                    showAtWeekDay: Int = 0,
                    showAtHour: Int = 0,
                    userTrainPopulation: Int = 0,

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
                    userObjectFeedSize: Option[Int] = None,
                    userObjectTimes: Int = 1,
                    //                   userObjectTimesAllDataset: Int = 0,
                    objectUsersCount: Int = 0,
                    userOwnersCount: Int = 0,
                    ownerUsersCount: Int = 0,


                    userOwnerCounters_CREATE_LIKENext: Double = -999,
                    userOwnerCounters_USER_FEED_REMOVENext: Double = -999,
                    auditweights_numShowsNext: Double = 0.0,
                    auditweights_numLikesNext: Double = 0.0,
                    auditweights_numDislikesNext: Double = 0.0,
                    auditweights_numShowsMax: Double = 0.0,
                    auditweights_numLikesMax: Double = 0.0,
                    auditweights_numDislikesMax: Double = 0.0,
                    auditweights_numShowsMin: Double = 0.0,
                    auditweights_numLikesMin: Double = 0.0,
                    auditweights_numDislikesMin: Double = 0.0,
                    auditweights_ctr_genderNext: Double = 0.0,
                    auditweights_ctr_highNext: Double = 0.0,
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
                    userUniqueCatsCount: Int = 0,
                    ownerUniqueCatsCount: Int = 0,
                    userUniqueCatsLikedCount: Int = 0,

                    ownerCatPopulation: Int = 0,
                    userCatPopulation: Int = 0,

                    userCatLikedPopulation: Int = -1,
                    userCatCount: Int = -1,
                    ownerCatCount: Int = -1,
                    userLikedMean_auditweights_svd_spark: Double = 0.0,
                    userLikedMean_userOwnerCounters_CREATE_LIKE: Double = 0.0,
                    userLikedMean_auditweights_ctr_gender: Double = 0.0,
                    userLikedMean_auditweights_userOwner_CREATE_LIKE: Double = 0.0,
                    userLikedMean_auditweights_ctr_high: Double = 0.0,
                    userLikedMean_metadata_numPolls: Double = 0.0,
                    userMean_metadata_numPolls: Double = 0.0

                  )


object DatasetCreatorCollab {

  //  Thread.sleep(1000*60*60*1)

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }


  private val master = "local"
  val conf = new SparkConf()
    .setAppName(getClass.getSimpleName)
  //      .setMaster("local[6]")
  //      .set("spark.local.dir", s"/home/ivan/tmp2")
  //      .set("spark.driver.memory", "10g")
  //      .set("spark.executor.memory", "3g")


  val spark = SparkContext.getOrCreate(conf)

  val sqlContext = new org.apache.spark.sql.SQLContext(spark)

  def run() {
    import sqlContext.implicits._

    val objectToTextCat: RDD[(Int, Int)] = spark.textFile("clusters70.csv").map { x =>
      val s = x.split(",")
      s.head.toInt -> s.last.toInt
    }.reduceByKey((a, b) => a)
    //    List(FoldersType.ForSubmit, FoldersType.ForTrain, FoldersType.ForTest).foreach {
    List( FoldersType.ForTrainForSubmit2).foreach {

      foldersType =>


        val foldersAll = FolderProvider.getAllFoldersWithoutFeedback(foldersType)
        val foldersUnknownFeedback = FolderProvider.getAllFoldersCurrentDataset(foldersType)
        val foldersWithFeedback = FolderProvider.getFoldersWithFeedback(foldersType)

        //        sqlContext.read.parquet(foldersWithFeedback: _*).rdd.map {
        //         x =>
        //
        //           val feedback = if (foldersType == FoldersType.ForSubmit) Nil else x.getAs[mutable.WrappedArray[String]]("feedback").toList
        //           val isLiked = if (feedback.contains("Liked")) 1 else 0
        //           val isDisLiked = if (feedback.contains("Disliked")) 1 else 0
        //
        //           val metadata_options = x.getAs[mutable.WrappedArray[String]]("metadata_options").toList
        //
        //           val objectId = x.getAs[Int]("instanceId_objectId")
        //           val ownerId = x.getAs[Int]("metadata_ownerId")
        //           val userId = x.getAs[Int]("instanceId_userId")
        //           val device = x.getAs[String]("audit_clientType")
        //           (objectId ) ->
        //             List((
        //               x.getAs[Long]("audit_timestamp"),
        ////               isLiked,
        //               feedback,
        ////               objectId,
        //               x.getAs[Int]("auditweights_ctr_gender"),
        //               x.getAs[Double]("user_gender")
        ////               x.getAs[Double]("auditweights_dailyRecency"),
        ////               x.getAs[String]("audit_resourceType"),
        ////               x.getAs[String]("audit_experiment")
        //
        ////               x.getAs[Double]("auditweights_isRandom")
        //             )
        //             )
        //        }
        //        .reduceByKey(_ ::: _)
        //         .filter(_._2.size > 1)
        ////          .filter(_._2.exists(_._2.contains("Unliked")))
        ////          .filter(_._2.exists(_._3 > 0))
        //         .mapValues{
        //           d =>
        //             val so1 = d.sortBy(_._1)
        //             println()
        //         }.count()

        val initDataset = sqlContext.read.parquet(foldersUnknownFeedback: _*).rdd.map {
          x =>

            val feedback = if (foldersType == FoldersType.ForSubmit) Nil else x.getAs[mutable.WrappedArray[String]]("feedback").toList
            val isLiked = if (feedback.contains("Liked")) 1 else 0
            val isDisLiked = if (feedback.contains("Disliked")) 1 else 0
            val isUnLiked = if (feedback.contains("Unliked")) 1 else 0
            val isReShared = if (feedback.contains("ReShared")) 1 else 0

            val metadata_options = x.getAs[mutable.WrappedArray[String]]("metadata_options").toList

            val objectId = x.getAs[Int]("instanceId_objectId")
            val ownerId = x.getAs[Int]("metadata_ownerId")
            val userId = x.getAs[Int]("instanceId_userId")
            val device = x.getAs[String]("audit_clientType")
            val md_options = if (x.isNullAt(x.fieldIndex("metadata_options"))) Nil else
              x.getAs[mutable.WrappedArray[String]]("metadata_options").toList
            val images = if (x.isNullAt(x.fieldIndex("ImageId"))) Nil else
              x.getAs[mutable.WrappedArray[String]]("ImageId").toList
            (objectId, ownerId, userId, device) -> Dataset(
              images,
              auditweights_ctr_negative= x.getAs[Double]("auditweights_ctr_negative"),
              metadata_platform= x.getAs[String]("metadata_platform"),
              user_ID_Location = x.getAs[Int]("user_ID_Location"),
              userOwnerCounters_VIDEO = x.getAs[Int]("metadata_totalVideoLength"),
              metadata_totalVideoLength  = x.getAs[Int]("metadata_totalVideoLength"),
              IS_EXTERNAL_SHARE= if (md_options.contains("IS_EXTERNAL_SHARE")) 1 else 0,
              userOwnerCounters_UNKNOWN = x.getAs[Double]("userOwnerCounters_UNKNOWN"),
              auditweights_likersSvd_hyper = x.getAs[Double]("auditweights_likersSvd_hyper"),
              user_ID_country = x.getAs[Long]("user_ID_country"),
              isLiked = isLiked,
              isDisLiked = isDisLiked,
              isUnLiked = isUnLiked,
              isReShared = isReShared,
              userId = userId,
              objectType = x.getAs[String]("instanceId_objectType"),
              objectId = objectId,
              ownerId = ownerId,
              metadata_authorId = x.getAs[Int]("metadata_authorId"),
              //              auditweights_isRandom = x.getAs[Double]("auditweights_isRandom"),
              audit_resourceType = x.getAs[Long]("audit_resourceType"),
              user_region = x.getAs[Long]("audit_resourceType"),
              metadata_numPhotos = x.getAs[Int]("metadata_numPhotos"),
              metadata_numPolls = x.getAs[Int]("metadata_numPolls"),
              metadata_numSymbols = x.getAs[Int]("metadata_numSymbols"),
              audit_pos = x.getAs[Long]("audit_pos"),
              auditweights_svd = x.getAs[Double]("auditweights_svd"),
              createdAt = x.getAs[Long]("metadata_createdAt"),
              showAt = x.getAs[Long]("audit_timestamp"),
              joinAgo = x.getAs[Long]("audit_timestamp") - x.getAs[Long]("membership_joinDate"),
              device = device,
              //              audit_experiment = x.getAs[String]("audit_experiment"),
              auditweights_numDislikes = x.getAs[Double]("auditweights_numDislikes"),
              auditweights_numLikes = x.getAs[Double]("auditweights_numLikes"),
              auditweights_numShows = x.getAs[Double]("auditweights_numShows"),
              //              auditweights_svd_prelaunch = x.getAs[Double]("auditweights_svd_prelaunch"),
              auditweights_ctr_high = x.getAs[Double]("auditweights_ctr_high"),
              auditweights_ctr_gender = x.getAs[Double]("auditweights_ctr_gender"),
              auditweights_friendLikes = x.getAs[Double]("auditweights_friendLikes"),
              membership_status = x.getAs[String]("membership_status"),
              user_create_date = x.getAs[Long]("user_create_date"),
              //              auditweights_matrix = x.getAs[Double]("auditweights_matrix"),
              auditweights_dailyRecency = x.getAs[Double]("auditweights_dailyRecency"),
              auditweights_friendLikes_actors = x.getAs[Double]("auditweights_friendLikes_actors"),
              auditweights_likersFeedStats_hyper = x.getAs[Double]("auditweights_likersFeedStats_hyper"),
              userOwnerCounters_USER_FEED_REMOVE = x.getAs[Double]("userOwnerCounters_USER_FEED_REMOVE"),
              metadata_numTokens = x.getAs[Int]("metadata_numTokens"),
              metadata_numVideos = x.getAs[Int]("metadata_numVideos"),
              auditweights_userOwner_CREATE_LIKE = x.getAs[Double]("auditweights_userOwner_CREATE_LIKE"),
              auditweights_userOwner_TEXT = x.getAs[Double]("auditweights_userOwner_TEXT"),
              userOwnerCounters_CREATE_LIKE = x.getAs[Double]("userOwnerCounters_CREATE_LIKE"),
              userOwnerCounters_TEXT = x.getAs[Double]("userOwnerCounters_TEXT"),
              userOwnerCounters_IMAGE = x.getAs[Double]("userOwnerCounters_IMAGE"),
              auditweights_ageMs = x.getAs[Double]("auditweights_ageMs"),
              sex = x.getAs[Int]("user_gender"),
              dob = x.getAs[Int]("user_birth_date"),
              imagesN = images.size

            )
        }

        initDataset.count()
        val withoutDuplicates = initDataset.reduceByKey { (a, b) =>
          if (a.isLiked == 1)
            a.copy(
              isDisLiked = if (a.isDisLiked == 1 || b.isDisLiked == 1) 1 else 0,
              isUnLiked = if (a.isUnLiked == 1 || b.isUnLiked == 1) 1 else 0,
              isReShared = if (a.isReShared == 1 || b.isReShared == 1) 1 else 0,
              userObjectTimes = a.userObjectTimes + b.userObjectTimes
            )
          else {
            val last = if (a.showAt > b.showAt) a else b
            last.copy(
              isLiked = if (a.isLiked == 1 || b.isLiked == 1) 1 else 0,
              isDisLiked = if (a.isDisLiked == 1 || b.isDisLiked == 1) 1 else 0,
              isUnLiked = if (a.isUnLiked == 1 || b.isUnLiked == 1) 1 else 0,
              isReShared = if (a.isReShared == 1 || b.isReShared == 1) 1 else 0,
              userObjectTimes = a.userObjectTimes + b.userObjectTimes
            )
          }
        }.map(_._2)


        val likesChanges: RDD[((Int, Int, Int), (Double, Double))] = withoutDuplicates.map {
          x =>

            (x.userId, x.ownerId) -> List((x.userOwnerCounters_CREATE_LIKE, x.userOwnerCounters_USER_FEED_REMOVE, x.showAt, x.objectId))
        }.reduceByKey(_ ::: _)
          .flatMap {
            case ((user, owner), data) =>
              val sortedData = data.sortBy(_._3)
              val likes = sortedData.map(_._1).distinct
              val likesWithNext = likes.zip(likes.tail).toMap
              val dislikes = sortedData.map(_._2).distinct
              val disLikesWithNext = dislikes.zip(dislikes.tail).toMap

              sortedData.map(x => (user, owner, x._4) -> (likesWithNext.getOrElse(x._1, x._1), disLikesWithNext.getOrElse(x._2, x._2)))
          }.reduceByKey((a, b) => a)

        val withNextUserOwner = withoutDuplicates.map(x => (x.userId, x.ownerId, x.objectId) -> x).leftOuterJoin(likesChanges)
          .map {
            case (_, (d, f)) =>
              d.copy(userOwnerCounters_CREATE_LIKENext = f.map(_._1).getOrElse(0.0),
                userOwnerCounters_USER_FEED_REMOVENext = f.map(_._2).getOrElse(0.0)
              )
          }


        val objectLikesChanges = withNextUserOwner.map(x => x.objectId ->
          List(
            (x.showAt, x.auditweights_numShows, x.auditweights_numLikes, x.auditweights_numDislikes, x.userId,
              x.auditweights_ctr_high
            ))
        )
          .reduceByKey(_ ::: _)
          .flatMap {
            case (objectId, data) =>
              val sortedData = data.sortBy(_._1)
              val shows = sortedData.map(_._2)
              val likes = sortedData.map(_._3)
              val dislikes = sortedData.map(_._4)
              val ctr_high = sortedData.map(_._6)

              val zipped =
                (shows.tail ::: List(shows.last))
                  .zip(likes.tail ::: List(likes.last))
                  .zip(dislikes.tail ::: List(dislikes.last))
                  .map(x => (x._1._1, x._1._2, x._2))
                  .zip(ctr_high.tail ::: List(ctr_high.last))
                  .map(x => (x._1._1, x._1._2, x._1._3, x._2))
//
//              val showsWithNext = shows.zip(shows.tail).toMap
//
//              val likesWithNext = likes.zip(likes.tail).toMap
//
//              val disLikesWithNext = dislikes.zip(dislikes.tail).toMap
              sortedData.zip(zipped).map {
                case (x, values) =>
                  (x._5, objectId) ->
                    (values._1,
                      values._2,
                      values._3,
                      shows.max,
                      likes.max,
                      dislikes.max,
                      shows.min,
                      likes.min,
                      dislikes.min,
                      values._4
                    )

              }
          }.reduceByKey((a, b) => a)

        val withNextObjectShows = withNextUserOwner.map(x => (x.userId, x.objectId) -> x).leftOuterJoin(objectLikesChanges)
          .map {
            case (_, (d, f)) =>
              d.copy(
                auditweights_numShowsNext = f.map(_._1).getOrElse(0.0),
                auditweights_numLikesNext = f.map(_._2).getOrElse(0.0),
                auditweights_numDislikesNext = f.map(_._3).getOrElse(0.0),
                auditweights_numShowsMax = f.map(_._4).getOrElse(0.0),
                auditweights_numLikesMax = f.map(_._5).getOrElse(0.0),
                auditweights_numDislikesMax = f.map(_._6).getOrElse(0.0),
                auditweights_numShowsMin = f.map(_._7).getOrElse(0.0),
                auditweights_numLikesMin = f.map(_._8).getOrElse(0.0),
                auditweights_numDislikesMin = f.map(_._9).getOrElse(0.0),
                auditweights_ctr_highNext = f.map(_._10).getOrElse(0.0)

              )

          }


        val genderCTRChanges = withNextObjectShows.map(x => (x.objectId, x.sex) ->
          List((x.showAt, x.auditweights_ctr_gender, x.userId))
        )
          .reduceByKey(_ ::: _)
          .flatMap {
            case ((objectId, sex), data) =>
              val sortedData = data.sortBy(_._1)
              val auditweights_ctr_gender = sortedData.map(_._2)
              val auditweights_ctr_genderNext = auditweights_ctr_gender.zip(auditweights_ctr_gender.tail).toMap


              sortedData.map(x => (x._3, objectId, sex) -> auditweights_ctr_genderNext.getOrElse(x._2, x._2)

              )
          }.reduceByKey((a, b) => a)

        val withNextGenderCTRShows = withNextObjectShows.map(x => (x.userId, x.objectId, x.sex) -> x).leftOuterJoin(genderCTRChanges)
          .map {
            case (_, (d, f)) => d.copy(auditweights_ctr_genderNext = f.getOrElse(0.0))
          }


        val initDatasetWithCat = withNextGenderCTRShows.map(x => x.objectId -> x).leftOuterJoin(objectToTextCat)
          .map { case (_, (d, c)) => d.copy(textCat = c.getOrElse(-1)) }


        val result = MyImageClasses2(foldersUnknownFeedback, initDatasetWithCat)
        val result2 = MyImageClasses2.ownerCat(foldersAll, result)
        val result3 = MyImageClasses2.userLikeCat(foldersWithFeedback, result2)

        val init = InitPrepare(foldersAll = foldersAll, foldersWithFeedback = foldersWithFeedback, datasetUnknownFeedback = result3, textCat = objectToTextCat)

        init.toDF().write.parquet(FolderProvider.saveTo(foldersType))
    }
  }

}

