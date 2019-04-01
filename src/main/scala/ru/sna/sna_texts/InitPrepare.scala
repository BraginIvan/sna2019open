package ru.sna.sna_texts

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.joda.time.DateTime

import scala.collection.mutable

object InitPrepare {


  def apply(foldersAll: List[String], foldersWithFeedback: List[String], datasetUnknownFeedback: RDD[Dataset], textCat: RDD[(Int, Int)]): RDD[Dataset] = {


    val datasetAll = TextsDatasetCreator.sqlContext.read.parquet(foldersAll: _*).rdd
      .map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[Int]("instanceId_objectId"), x.getAs[String]("audit_clientType"), x.getAs[Int]("metadata_ownerId")) -> x)
      .reduceByKey((a, b) => b).map(_._2)

    //    .map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[Int]("instanceId_objectId"), x.getAs[String]("audit_clientType"), x.getAs[Int]("metadata_ownerId")) -> (x, 1))
    //      .reduceByKey((a, b) => (b._1, b._2 + a._2)).map(_._2)


    val datasetWithFeedback: RDD[Row] = TextsDatasetCreator.sqlContext.read.parquet(foldersWithFeedback: _*).rdd
      .map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[Int]("instanceId_objectId"), x.getAs[String]("audit_clientType"), x.getAs[Int]("metadata_ownerId")) -> x)
      .reduceByKey((a, b) => b).map(_._2)

    datasetWithFeedback.cache()
    datasetAll.cache()

    val knownData = datasetWithFeedback.map { x =>
      (x.getAs[Int]("instanceId_objectId"), x.getAs[Int]("instanceId_userId")) -> {
        if (x.getAs[mutable.WrappedArray[String]]("feedback").toList.contains("Liked")) 1 else 0
      }
    }.reduceByKey((a, b) => b)

    val ds0_0 = datasetUnknownFeedback.map(x => (x.objectId, x.userId) -> x).leftOuterJoin(knownData)
      .map { case (_, (d, f)) => d.copy(knownLike = f.getOrElse(-1)) }


    val objectDayPopulation = datasetAll.map(x => (x.getAs[Int]("instanceId_objectId"), ((x.getAs[Long]("audit_timestamp") - x.getAs[Long]("metadata_createdAt")).toDouble / 1000 / 3600 / 24).toInt) -> 1)
      .reduceByKey(_ + _)

    val objectPopulation: RDD[(Int, List[Int])] = datasetAll.map(x => x.getAs[Int]("instanceId_objectId") -> List(x.getAs[Int]("instanceId_userId"))).reduceByKey(_ ::: _)
    val userPopulation = datasetAll.map(x => x.getAs[Int]("instanceId_userId") -> List(x.getAs[Int]("metadata_ownerId"))).reduceByKey(_ ::: _)
    val userOwnerPopulation = datasetAll.map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[Int]("metadata_ownerId")) -> 1).reduceByKey(_ + _)
    val ownerPopulation = datasetAll.map(x => x.getAs[Int]("metadata_ownerId") -> (1, Set(x.getAs[Int]("instanceId_userId")))).reduceByKey((a, b) => (a._1 + b._1, a._2 ++ b._2))
    val userDevicePopulation = datasetAll.map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[String]("audit_clientType")) -> 1).reduceByKey(_ + _)

    val userCatMap = datasetAll.map(x => x.getAs[Int]("instanceId_objectId") -> x.getAs[Int]("instanceId_userId"))
      .leftOuterJoin(textCat)

    val ownerCatMap = datasetAll.map(x => x.getAs[Int]("instanceId_objectId") -> x.getAs[Int]("metadata_ownerId"))
      .leftOuterJoin(textCat)

    ownerCatMap.cache()
    userCatMap.cache()


    val ownerCatPopulation = ownerCatMap.map {
      case (_, (u, c)) => (u, c.getOrElse(-1)) -> 1
    }.reduceByKey(_ + _)

    val userCatPopulation = userCatMap.map {
      case (_, (u, c)) => (u, c.getOrElse(-1)) -> 1
    }.reduceByKey(_ + _)


    val ds0 = ds0_0.map(x => (x.objectId, ((x.showAt - x.createdAt).toDouble / 1000 / 3600 / 24).toInt) -> x).leftOuterJoin(objectDayPopulation)
      .map { case (_, (d, f)) => d.copy(objectDayPopulation = f.getOrElse(0)) }
    //
    val ds1 = ds0.map(x => x.objectId -> x).leftOuterJoin(objectPopulation)
      .map { case (_, (d, f)) => d.copy(
        objectPopulation = f.get.size,
        objectUsersCount = f.get.distinct.size)
      }

    val ds2 = ds1.map(x => (x.userId, x.textCat) -> x).leftOuterJoin(userCatPopulation)
      .map { case (_, (d, f)) => d.copy(userCatPopulation = f.getOrElse(0)) }


    val ds2_1 = ds2.map(x => (x.ownerId, x.textCat) -> x).leftOuterJoin(ownerCatPopulation)
      .map { case (_, (d, f)) => d.copy(ownerCatPopulation = f.getOrElse(0)) }

    val userCat = ownerCatMap.map {
      case (_, (u, c)) => u -> Set(c.getOrElse(-1))
    }.reduceByKey(_ ++ _).mapValues {
      cats =>
        cats.size
    }


    val ds3 = ds2_1.map(x => x.userId -> x)
      .leftOuterJoin(userPopulation)
      .leftOuterJoin(userCat)


      .map { case (_, ((d, f), cats)) => d.copy(
        userPopulation = f.get.size,
        userOwnersCount = f.get.distinct.size,
        userCatCount = cats.getOrElse(-1)
      )
      }

    val ds4 = ds3.map(x => (x.userId, x.ownerId) -> x).leftOuterJoin(userOwnerPopulation)
      .map { case (_, (d, f)) => d.copy(userOwnerPopulation = f.get) }

    val ownerCat = userCatMap.map {
      case (_, (u, c)) => u -> Set(c.getOrElse(-1))
    }.reduceByKey(_ ++ _).mapValues {
      cats =>
        cats.size
    }

    val ds5 = ds4.map(x => x.ownerId -> x)
      .leftOuterJoin(ownerPopulation)
      .leftOuterJoin(ownerCat)

      .map { case (_, ((d, f), с)) => d.copy(
        ownerPopulation = f.get._1,
        ownerUsersCount = f.get._2.size,
        ownerCatCount = с.getOrElse(-1)

      )
      }

    val ds6 = ds5.map(x => (x.userId, x.device) -> x).leftOuterJoin(userDevicePopulation)
      .map { case (_, (d, f)) => d.copy(userDevicePopulation = f.get) }


    val datasetLiked = datasetWithFeedback.filter(x => x.getAs[mutable.WrappedArray[String]]("feedback").toList.contains("Liked"))
    val datasetDisLiked = datasetWithFeedback.filter(x => x.getAs[mutable.WrappedArray[String]]("feedback").toList.contains("Disliked"))
    val userCatLikedPopulation = datasetLiked.map(x => x.getAs[Int]("instanceId_objectId") -> x.getAs[Int]("instanceId_userId"))
      .leftOuterJoin(textCat)
      .map {
        case (_, (u, c)) => (u, c.getOrElse(-1)) -> 1
      }
      .reduceByKey(_ + _)


    val datasetClicked = datasetWithFeedback.filter(x => x.getAs[mutable.WrappedArray[String]]("feedback").toList.contains("Clicked"))
    val datasetUnliked = datasetWithFeedback.filter(x => x.getAs[mutable.WrappedArray[String]]("feedback").toList.contains("Unliked"))
    val datasetReShared = datasetWithFeedback.filter(x => x.getAs[mutable.WrappedArray[String]]("feedback").toList.contains("ReShared"))

    val userTrainPopulation = datasetWithFeedback.map(_.getAs[Int]("instanceId_userId") -> 1).reduceByKey(_ + _)

    val userLikedPopulation = datasetLiked.map(_.getAs[Int]("instanceId_userId") -> 1).reduceByKey(_ + _)
    val userDisLikedPopulation = datasetDisLiked.map(_.getAs[Int]("instanceId_userId") -> 1).reduceByKey(_ + _)
    val userClickedPopulation = datasetClicked.map(_.getAs[Int]("instanceId_userId") -> 1).reduceByKey(_ + _)
    val userUnlikedPopulation = datasetUnliked.map(_.getAs[Int]("instanceId_userId") -> 1).reduceByKey(_ + _)
    val userReSharedPopulation = datasetReShared.map(_.getAs[Int]("instanceId_userId") -> 1).reduceByKey(_ + _)

    val imageLikedPopulation = datasetLiked.flatMap(_.getAs[mutable.WrappedArray[String]]("ImageId").map(_ -> 1)).reduceByKey(_ + _)
    val userDeviceLikedPopulation = datasetLiked.map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[String]("audit_clientType")) -> 1).reduceByKey(_ + _)
    val userOwnerLikedPopulation = datasetLiked.map(x => (x.getAs[Int]("instanceId_userId"), x.getAs[Int]("metadata_ownerId")) -> 1).reduceByKey(_ + _)
    val ownerLikedPopulation = datasetLiked.map(_.getAs[Int]("metadata_ownerId") -> 1).reduceByKey(_ + _)

    val ds7 = ds6.map(x => x.userId -> x).leftOuterJoin(userLikedPopulation)
      .map { case (_, (d, f)) => d.copy(userLikedPopulation = f.getOrElse(0)) }

    val ds8 = ds7.map(x => x.userId -> x).leftOuterJoin(userDisLikedPopulation)
      .map { case (_, (d, f)) => d.copy(userDisLikedPopulation = f.getOrElse(0)) }

    val ds9 = ds8.map(x => (x.userId, x.ownerId) -> x).leftOuterJoin(userOwnerLikedPopulation)
      .map { case (_, (d, f)) => d.copy(userOwnerLikedPopulation = f.getOrElse(0)) }

    val ds10 = ds9.map(x => (x.userId, x.textCat) -> x).leftOuterJoin(userCatLikedPopulation)
      .map { case (_, (d, f)) => d.copy(userCatLikedPopulation = f.getOrElse(0)) }

    val ds11 = ds10.map(x => (x.userId, x.device) -> x).leftOuterJoin(userDeviceLikedPopulation)
      .map { case (_, (d, f)) => d.copy(userDeviceLikedPopulation = f.getOrElse(0)) }

    val ds12 = ds11.map(x => x.ownerId -> x).leftOuterJoin(ownerLikedPopulation)
      .map { case (_, (d, f)) => d.copy(ownerLikedPopulation = f.getOrElse(0)) }

    val ds13 = ds12.map {
      d =>
        val createdAt = new DateTime(d.createdAt).plusHours(3)
        val showAt = new DateTime(d.showAt).plusHours(3)
        d.copy(
          showDelay = d.showAt - d.createdAt,
          createdAtWeekDay = createdAt.dayOfWeek().get(),
          showAtWeekDay = showAt.dayOfWeek().get(),
          createdAtHour = createdAt.hourOfDay().get(),
          showAtHour = showAt.hourOfDay().get()
        )
    }


    val ds14 = ds13.map(x => x.userId -> x).leftOuterJoin(userTrainPopulation)
      .map { case (_, (d, f)) => d.copy(userTrainPopulation = f.getOrElse(0)) }


    val userFeeds: RDD[((Int, Int), (Int, Int))] = datasetAll.map {
      x => x.getAs[Int]("instanceId_userId") -> List((x.getAs[Long]("audit_timestamp"), x.getAs[Int]("instanceId_objectId")))
    }.reduceByKey(_ ::: _)
      .mapValues {
        objects =>
          objects.sortBy(-_._1).foldLeft(List.empty[List[(Long, Int)]]) {
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

    val ds18 = ds17.map(x => x.userId -> x).leftOuterJoin(userReSharedPopulation)
      .map { case (_, (d, f)) => d.copy(userResharedPopulation = f.getOrElse(0)) }

    val ds19 = ds18.map(x => x.userId -> x).leftOuterJoin(userUnlikedPopulation)
      .map { case (_, (d, f)) => d.copy(userUnlikedPopulation = f.getOrElse(0)) }


    val userTime = datasetAll.map(x => x.getAs[Int]("instanceId_userId") -> List(new DateTime(x.getAs[Long]("audit_timestamp")).plusHours(3).hourOfDay().get())).reduceByKey(_ ::: _)
    val userLikedTime = datasetLiked.map(x => x.getAs[Int]("instanceId_userId") -> List(new DateTime(x.getAs[Long]("audit_timestamp")).plusHours(3).hourOfDay().get())).reduceByKey(_ ::: _)


    val ds20 = ds19.map(x => x.userId -> x)
      .leftOuterJoin(userTime)
      .leftOuterJoin(userLikedTime)

      .map { case (_, ((d, f), likedF)) =>
        val f_ = f.getOrElse(Nil)
        val likedF_ = likedF.getOrElse(Nil)

        d.copy(
          userEveningObjects = f_.count(x => x > 16 && x <= 20),
          userNightObjects = f_.count(x => x > 20 && x <= 24),
          userDeepNightObjects = f_.count(x => x > 0 && x <= 5),
          userMorningObjects = f_.count(x => x > 5 && x <= 9),
          userWorkObjects = f_.count(x => x > 9 && x <= 16),
          userLikedEveningObjects = likedF_.count(x => x > 16 && x <= 20),
          userLikedNightObjects = likedF_.count(x => x > 20 && x <= 24),
          userLikedDeepNightObjects = likedF_.count(x => x > 0 && x <= 5),
          userLikedMorningObjects = likedF_.count(x => x > 5 && x <= 9),
          userLikedWorkObjects = likedF_.count(x => x > 9 && x <= 16)
        )
      }


    ds20
  }

}




