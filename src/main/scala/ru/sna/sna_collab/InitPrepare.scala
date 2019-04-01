package ru.sna.sna_collab

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.collection.mutable

object InitPrepare {


  def apply(foldersAll: List[String], foldersWithFeedback: List[String], datasetUnknownFeedback: RDD[Dataset], textCat: RDD[(Int, Int)]): RDD[Dataset] = {

    val datasetAll = DatasetCreatorCollab.sqlContext.read.parquet(foldersAll: _*).rdd
//      .map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[Int]("instanceId_objectId"), x.getAs[String]("audit_clientType"), x.getAs[Int]("metadata_ownerId")) -> (x, 1))
//      .reduceByKey((a, b) => (b._1, b._2 + a._2)).map(_._2)

    val datasetWithFeedback = DatasetCreatorCollab.sqlContext.read.parquet(foldersWithFeedback: _*).rdd
//      .map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[Int]("instanceId_objectId"), x.getAs[String]("audit_clientType"), x.getAs[Int]("metadata_ownerId")) -> (x, 1))
//      .reduceByKey((a, b) => (b._1, b._2 + a._2)).map(_._2)

//    datasetAllWithCount.cache()
//    datasetWithFeedbackWithCount.cache()

//    val datasetAll = datasetAllWithCount.map(_._1)
//    val datasetWithFeedback = datasetWithFeedbackWithCount.map(_._1)

//    datasetAll.cache()
//    datasetWithFeedback.cache()


    val objectDayPopulation = datasetAll.map(x => (x.getAs[Int]("instanceId_objectId"), ((x.getAs[Long]("audit_timestamp") - x.getAs[Long]("metadata_createdAt")).toDouble / 1000 / 3600 / 24).toInt) -> 1).reduceByKey(_ + _)
    val userExpPopulation = datasetAll.map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[String]("audit_experiment")) -> 1).reduceByKey(_ + _)

    val objectPopulation = datasetAll
      .map(x => x.getAs[Int]("instanceId_objectId") -> List(
        (x.getAs[Int]("instanceId_userId"),
          x.getAs[Double]("auditweights_svd"),
          x.getAs[Double]("userOwnerCounters_CREATE_LIKE"),
          x.getAs[Double]("auditweights_ctr_gender"),
          x.getAs[Double]("auditweights_userOwner_CREATE_LIKE"),
          x.getAs[Double]("auditweights_ctr_high"),
          x.getAs[Double]("userOwnerCounters_USER_FEED_REMOVE"),
//          x.getAs[Double]("audit_experiment"),
          x.getAs[Double]("auditweights_numLikes")

        )

      )).reduceByKey(_ ::: _)

    val userPopulation = datasetAll.map(x => x.getAs[Int]("instanceId_userId") -> List(
      (x.getAs[Int]("metadata_ownerId"),
        x.getAs[Double]("auditweights_svd"),
        x.getAs[Double]("userOwnerCounters_CREATE_LIKE"),
        x.getAs[Double]("auditweights_ctr_gender"),
        x.getAs[Double]("auditweights_userOwner_CREATE_LIKE"),
        x.getAs[Double]("auditweights_ctr_high"),
        x.getAs[Double]("userOwnerCounters_USER_FEED_REMOVE"),

//        x.getAs[Double]("audit_experiment"),
        x.getAs[Double]("auditweights_numLikes"),
        x.getAs[String]("membership_status"),
        x.getAs[Int]("metadata_numPolls")
      )

    )).reduceByKey(_ ::: _)

    val ownerPopulation = datasetAll.map(x => x.getAs[Int]("metadata_ownerId") ->
      (1,
        Set(x.getAs[Int]("instanceId_userId")),
        x.getAs[Double]("auditweights_svd"),
        x.getAs[Double]("userOwnerCounters_CREATE_LIKE"),
        x.getAs[Double]("auditweights_ctr_gender"),
        x.getAs[Double]("auditweights_userOwner_CREATE_LIKE"),
        x.getAs[Double]("auditweights_ctr_high"),
        x.getAs[Double]("userOwnerCounters_USER_FEED_REMOVE"),
        Set(x.getAs[Int]("metadata_authorId")),
        x.getAs[Double]("auditweights_numLikes")

      )
    ).reduceByKey((a, b) => (a._1 + b._1, a._2 ++ b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 ++ b._9, a._10+b._10))

    val authorPopulation = datasetAll.map(x => x.getAs[Int]("metadata_authorId") ->
      (1,
        Set(x.getAs[Int]("instanceId_userId")),
        x.getAs[Double]("auditweights_svd"),
        x.getAs[Double]("userOwnerCounters_CREATE_LIKE"),
        x.getAs[Double]("auditweights_ctr_gender"),
        x.getAs[Double]("auditweights_userOwner_CREATE_LIKE"),
        x.getAs[Double]("auditweights_ctr_high"),
        x.getAs[Double]("userOwnerCounters_USER_FEED_REMOVE"),
        Set(x.getAs[Int]("metadata_ownerId")),
        x.getAs[Double]("auditweights_numLikes")

      )
    ).reduceByKey((a, b) => (a._1 + b._1, a._2 ++ b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 ++ b._9, a._10+b._10))

    val userOwnerPopulation = datasetAll.map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[Int]("metadata_ownerId")) -> 1).reduceByKey(_ + _)
    //    val userDevicePopulation = datasetAll.map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[String]("audit_clientType")) -> 1).reduceByKey(_ + _)





    val userCatMap = datasetAll.map(x => x.getAs[Int]("instanceId_objectId") -> x.getAs[Int]("instanceId_userId"))
      .leftOuterJoin(textCat)

    val ownerCatMap = datasetAll.map(x => x.getAs[Int]("instanceId_objectId") -> x.getAs[Int]("metadata_ownerId"))
      .leftOuterJoin(textCat)


    ownerCatMap.cache()
    userCatMap.cache()

    val userCat = userCatMap.map {
      case (_, (u, c)) => u -> Set(c.getOrElse(-1))
    }.reduceByKey(_ ++ _).mapValues {
      cats =>
        cats.size
    }
    val ownerCat = ownerCatMap.map {
      case (_, (u, c)) => u -> Set(c.getOrElse(-1))
    }.reduceByKey(_ ++ _).mapValues {
      cats =>
        cats.size
    }
    val ownerCatPopulation = ownerCatMap.map {
      case (_, (u, c)) => (u, c.getOrElse(-1)) -> 1
    }.reduceByKey(_ + _)

    val userCatPopulation = userCatMap.map {
      case (_, (u, c)) => (u, c.getOrElse(-1)) -> 1
    }.reduceByKey(_ + _)


    val ds0 = datasetUnknownFeedback.map(x => (x.userId, x.textCat) -> x).leftOuterJoin(userCatPopulation)
      .map { case (_, (d, f)) => d.copy(userCatPopulation = f.getOrElse(0)) }


    val ds0_1 = ds0.map(x => (x.ownerId, x.textCat) -> x).leftOuterJoin(ownerCatPopulation)
      .map { case (_, (d, f)) => d.copy(ownerCatPopulation = f.getOrElse(0)) }

    val ds1 = ds0_1.map(x => (x.objectId, ((x.showAt - x.createdAt).toDouble / 1000 / 3600 / 24).toInt) -> x).leftOuterJoin(objectDayPopulation)
      .map { case (_, (d, f)) => d.copy(objectDayPopulation = f.getOrElse(0)) }
    //
    val ds2 = ds1.map(x => x.objectId -> x).leftOuterJoin(objectPopulation)
      .map { case (_, (d, f)) =>
        val size = f.get.size
        d.copy(
          objectPopulation = size,
          objectUsersCount = f.get.map(_._1).distinct.size,
          objectMean_auditweights_svd_spark = f.get.map(_._2).sum / size,
          objectMean_userOwnerCounters_CREATE_LIKE = f.get.map(_._3).sum / size,
          objectMean_auditweights_ctr_gender = f.get.map(_._4).sum / size,
          objectMean_auditweights_userOwner_CREATE_LIKE = f.get.map(_._5).sum / size,
          objectMean_auditweights_ctr_high = f.get.map(_._6).sum / size,
          objectMean_userOwnerCounters_USER_FEED_REMOVE = f.get.map(_._7).sum / size,
//          objectMean_audit_experiment = f.get.map(_._8).sum / size,
          objectMean_auditweights_numLikes = f.get.map(_._8).sum / size
        )
      }


    val ds3 = ds2.map(x => x.userId -> x)
      .leftOuterJoin(userPopulation)
      .map { case (_, (d, f)) =>
        val size = f.get.size
        d.copy(
          userPopulation = size,
          userOwnersCount = f.get.map(_._1).distinct.size,
          userMean_auditweights_svd_spark = f.get.map(_._2).sum / size,
          userMean_userOwnerCounters_CREATE_LIKE = f.get.map(_._3).sum / size,
          userMean_auditweights_ctr_gender = f.get.map(_._4).sum / size,
          userMean_auditweights_userOwner_CREATE_LIKE = f.get.map(_._5).sum / size,
          userMean_auditweights_ctr_high = f.get.map(_._6).sum / size,
          userMean_userOwnerCounters_USER_FEED_REMOVE = f.get.map(_._7).sum / size,
//          userMean_audit_experiment = f.get.map(_._8).sum / size,
          userMean_auditweights_numLikes = f.get.map(_._8).sum / size,
          userCounts_membership_status = f.get.map(_._9).distinct.size,
          userMean_metadata_numPolls = f.get.count(_._6 > 0) / size

        )
      }


    val ds4 = ds3.map(x => (x.userId, x.ownerId) -> x).leftOuterJoin(userOwnerPopulation)
      .map { case (_, (d, f)) => d.copy(userOwnerPopulation = f.get) }

    val ds5 = ds4.map(x => x.ownerId -> x)
      .leftOuterJoin(ownerPopulation)
      .map { case (_, (d, f)) =>
        val size = f.get._1
        d.copy(
          ownerPopulation = size,
          ownerUsersCount = f.get._2.size,
          ownerMean_auditweights_svd_spark = f.get._3 / size,
          ownerMean_userOwnerCounters_CREATE_LIKE = f.get._4 / size,
          ownerMean_auditweights_ctr_gender = f.get._5 / size,
          ownerMean_auditweights_userOwner_CREATE_LIKE = f.get._6 / size,
          ownerMean_auditweights_ctr_high = f.get._7 / size,
          ownerMean_userOwnerCounters_USER_FEED_REMOVE = f.get._8 / size,
          ownerCount_authors = f.get._9.size,
          ownerMean_auditweights_numLikes = f.get._10 / size
          //membership_status
        )
      }

    val ds5_1 = ds5.map(x => x.metadata_authorId -> x)
      .leftOuterJoin(authorPopulation)
      .map { case (_, (d, f)) =>
        val size = f.get._1
        d.copy(
          authorPopulation = size,
          authorUsersCount = f.get._2.size,
          authorMean_auditweights_svd_spark = f.get._3 / size,
          authorMean_userOwnerCounters_CREATE_LIKE = f.get._4 / size,
          authorMean_auditweights_ctr_gender = f.get._5 / size,
          authorMean_auditweights_userOwner_CREATE_LIKE = f.get._6 / size,
          authorMean_auditweights_ctr_high = f.get._7 / size,
          authorMean_userOwnerCounters_USER_FEED_REMOVE = f.get._8 / size,
          authorCount_owners = f.get._9.size,
          authorMean_auditweights_numLikes = f.get._10 / size
        )
      }


    val ds6 = ds5_1.map(x => x.userId -> x)
      .leftOuterJoin(userCat)
      .map { case (_, (d, cats)) => d.copy(
        userCatCount = cats.getOrElse(-1)
      )
      }.map(x => x.ownerId -> x)
      .leftOuterJoin(ownerCat)
      .map { case (_, (d, cats)) => d.copy(
        ownerCatCount = cats.getOrElse(-1)
      )
      }

//      .map(x => (x.objectId, x.userId) -> x)
//      .leftOuterJoin(datasetAllUserObjectTimes)
//      .map { case (_, (d, f)) =>
//        d.copy(
//          userObjectTimesAllDataset = f.get
//        )
//      }


    val datasetLiked = datasetWithFeedback.filter(x => x.getAs[mutable.WrappedArray[String]]("feedback").toList.contains("Liked"))
    val datasetDisLiked = datasetWithFeedback.filter(x => x.getAs[mutable.WrappedArray[String]]("feedback").toList.contains("Disliked"))

    val datasetClicked = datasetWithFeedback.filter(x => x.getAs[mutable.WrappedArray[String]]("feedback").toList.contains("Clicked"))
    val datasetUnliked = datasetWithFeedback.filter(x => x.getAs[mutable.WrappedArray[String]]("feedback").toList.contains("Unliked"))
    val datasetReShared = datasetWithFeedback.filter(x => x.getAs[mutable.WrappedArray[String]]("feedback").toList.contains("ReShared"))

    val userTrainPopulation = datasetWithFeedback.map(_.getAs[Int]("instanceId_userId") -> 1).reduceByKey(_ + _)


//    val userLikedPopulation = datasetLiked.map(_.getAs[Int]("instanceId_userId") -> 1).reduceByKey(_ + _)
    val authorLikedPopulation: RDD[(Int, Int)] = datasetLiked.map(_.getAs[Int]("metadata_authorId") -> 1).reduceByKey(_ + _)

    val userDisLikedPopulation = datasetDisLiked.map(_.getAs[Int]("instanceId_userId") -> 1).reduceByKey(_ + _)
    val userClickedPopulation = datasetClicked.map(_.getAs[Int]("instanceId_userId") -> 1).reduceByKey(_ + _)
    //    val userUnlikedPopulation = datasetUnliked.map(_.getAs[Int]("instanceId_userId") -> 1).reduceByKey(_+_)
    //    val userReSharedPopulation = datasetReShared.map(_.getAs[Int]("instanceId_userId") -> 1).reduceByKey(_+_)

    //    val imageLikedPopulation = datasetLiked.flatMap(_.getAs[mutable.WrappedArray[String]]("ImageId").map(_ -> 1)).reduceByKey(_+_)
    val userDeviceLikedPopulation = datasetLiked.map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[String]("audit_clientType")) -> 1).reduceByKey(_ + _)
    val userOwnerLikedPopulation = datasetLiked.map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[Int]("metadata_ownerId")) -> 1).reduceByKey(_ + _)
    val ownerLikedPopulation = datasetLiked.map(_.getAs[Int]("metadata_ownerId") -> 1).reduceByKey(_ + _)
    val userCatLikedPopulation = datasetLiked.map(x => x.getAs[Int]("instanceId_objectId") -> x.getAs[Int]("instanceId_userId"))
      .leftOuterJoin(textCat)
      .map {
        case (_, (u, c)) => (u, c.getOrElse(-1)) -> 1
      }
      .reduceByKey(_ + _)


    val ds6_1 = ds6.map(x => (x.userId, x.textCat) -> x).leftOuterJoin(userCatLikedPopulation)
      .map { case (_, (d, f)) => d.copy(userCatLikedPopulation = f.getOrElse(0)) }

    val ds7 = ds6_1//.map(x => x.userId -> x).leftOuterJoin(userLikedPopulation)
//      .map { case (_, (d, f)) => d.copy(userLikedPopulation = f.getOrElse(0)) }

    val ds8 = ds7.map(x => x.userId -> x).leftOuterJoin(userDisLikedPopulation)
      .map { case (_, (d, f)) => d.copy(userDisLikedPopulation = f.getOrElse(0)) }

    val ds9 = ds8.map(x => (x.userId, x.ownerId) -> x).leftOuterJoin(userOwnerLikedPopulation)
      .map { case (_, (d, f)) => d.copy(userOwnerLikedPopulation = f.getOrElse(0)) }

    val ds10 = ds9.map(x => x.metadata_authorId -> x).leftOuterJoin(authorLikedPopulation)
      .map { case (_, (d, f)) => d.copy(authorLikedPopulation = f.getOrElse(0)) }

    val ds11 = ds10.map(x => (x.userId, x.device) -> x).leftOuterJoin(userDeviceLikedPopulation)
      .map { case (_, (d, f)) => d.copy(userDeviceLikedPopulation = f.getOrElse(0)) }

    val ds12 = ds11.map(x => x.ownerId -> x).leftOuterJoin(ownerLikedPopulation)
      .map { case (_, (d, f)) => d.copy(ownerLikedPopulation = f.getOrElse(0)) }

    val ds13 = ds12.map {
      d =>
        val createdAt = new DateTime(d.createdAt)
        val showAt = new DateTime(d.showAt)
        d.copy(
          showDelay = d.showAt - d.createdAt,
          createdAtHour = createdAt.hourOfDay().get(),
          showAtHour = showAt.hourOfDay().get()
        )
    }

    val ds14 = ds13.map(x => x.userId -> x).leftOuterJoin(userTrainPopulation)
      .map { case (_, (d, f)) => d.copy(userTrainPopulation = f.getOrElse(0)) }


    val userFeeds: RDD[((Int, Int), (Int, Int))] = datasetAll.map {
      x => x.getAs[Int]("instanceId_userId") -> List((x.getAs[Long]("audit_timestamp"), x.getAs[Int]("instanceId_objectId")))
    }.reduceByKey(_ ::: _)
      .map {
        case (user, objects) =>
          user -> objects.sortBy(-_._1).foldLeft(List.empty[List[(Long, Int)]]) {
            (x, y) =>
              if (x.isEmpty) {
                List(List(y))
              } else {
                val lastTime = x.last.last._1
                if ((y._1 - lastTime) > 1000 * 60 * 10) {
                  x ::: List(List(y))
                } else {
                  x.init ::: List(x.last ::: List(y))
                }
              }
          }
      }.flatMap {
      case (user, feeds) =>
        feeds.flatMap { x =>
          val theFeedSize = x.size
          x.zipWithIndex.map { x =>
            val index = x._2
            (user, x._1._2) -> (theFeedSize, index)
          }
        }
    }.reduceByKey((a, b) => a)

    val ds15 = ds14.map(x => (x.userId, x.objectId) -> x).leftOuterJoin(userFeeds)
      .map {
        case (_, (d, f)) => d.copy(userObjectFeedSize = f.map(_._1))
      }


    val ds16 = ds15

    val ds17 = ds16.map(x => x.userId -> x).leftOuterJoin(userClickedPopulation)
      .map { case (_, (d, f)) => d.copy(userClickedPopulation = f.getOrElse(0)) }


    val userLikedPopulation = datasetLiked.map(x => x.getAs[Int]("instanceId_userId") -> List(
      (
        x.getAs[Double]("auditweights_svd"),
        x.getAs[Double]("userOwnerCounters_CREATE_LIKE"),
        x.getAs[Double]("auditweights_ctr_gender"),
        x.getAs[Double]("auditweights_userOwner_CREATE_LIKE"),
        x.getAs[Double]("auditweights_ctr_high"),
        x.getAs[Int]("metadata_numPolls")
      )

    )).reduceByKey(_ ::: _)


    val ds18 = ds17.map(x => x.userId -> x)
      .leftOuterJoin(userLikedPopulation)
      .map { case (_, (d, f)) =>
        val size = f.map(_.size.toDouble).getOrElse(0.000001)
        d.copy(
          userLikedPopulation = f.map(_.size).getOrElse(0),
          userLikedMean_auditweights_svd_spark = f.getOrElse(Nil).map(_._1).sum / size,
          userLikedMean_userOwnerCounters_CREATE_LIKE = f.getOrElse(Nil).map(_._2).sum / size,
          userLikedMean_auditweights_ctr_gender = f.getOrElse(Nil).map(_._3).sum / size,
          userLikedMean_auditweights_userOwner_CREATE_LIKE = f.getOrElse(Nil).map(_._4).sum / size,
          userLikedMean_auditweights_ctr_high = f.getOrElse(Nil).map(_._5).sum / size,
          userLikedMean_metadata_numPolls = f.getOrElse(Nil).count(_._6 > 0) / size

        )
      }

    val ds19 = ds18

//    val userTime = datasetAll.map(x => x.getAs[Int]("instanceId_userId") -> List(new DateTime(x.getAs[Long]("audit_timestamp")).plusHours(3).hourOfDay().get())).reduceByKey(_ ::: _)
//    val userLikedTime = datasetLiked.map(x => x.getAs[Int]("instanceId_userId") -> List(new DateTime(x.getAs[Long]("audit_timestamp")).plusHours(3).hourOfDay().get())).reduceByKey(_ ::: _)


    val ds20 = ds19

    ds20
  }


}














































































































