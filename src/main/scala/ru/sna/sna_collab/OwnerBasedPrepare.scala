package ru.sna.sna_collab

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.collection.mutable

object OwnerBasedPrepare {

  //  objectType = x.getAs[String]("instanceId_objectType"),
  //  objectId = x.getAs[Int]("instanceId_objectId"),
  //  ownerId = x.getAs[Int]("metadata_ownerId"),
  //  metadata_authorId = x.getAs[Int]("metadata_authorId"),
  //  audit_resourceType = x.getAs[Long]("audit_resourceType"),
  //  metadata_numPhotos = x.getAs[Int]("metadata_numPhotos"),
  //  metadata_numPolls = x.getAs[Int]("metadata_numPolls"),
  //  metadata_numSymbols = x.getAs[Int]("metadata_numSymbols"),
  //  audit_pos = x.getAs[Long]("audit_pos"),
  //  auditweights_svd_spark = x.getAs[Double]("auditweights_svd_spark"),
  //  createdAt = x.getAs[Long]("metadata_createdAt"),
  //  showAt = x.getAs[Long]("audit_timestamp"),
  //  device = x.getAs[String]("audit_clientType"),
  //  audit_experiment = x.getAs[String]("audit_experiment"),
  //  auditweights_numDislikes = x.getAs[Double]("auditweights_numDislikes"),
  //  auditweights_numLikes = x.getAs[Double]("auditweights_numLikes"),
  //  auditweights_numShows = x.getAs[Double]("auditweights_numShows"),
  //  auditweights_svd_prelaunch = x.getAs[Double]("auditweights_svd_prelaunch"),
  //  auditweights_ctr_high = x.getAs[Double]("auditweights_ctr_high"),
  //  auditweights_ctr_gender = x.getAs[Double]("auditweights_ctr_gender"),
  //  auditweights_friendLikes = x.getAs[Double]("auditweights_friendLikes"),
  //  membership_status = x.getAs[String]("membership_status"),
  //  user_create_date= x.getAs[Long]("user_create_date"),
  //  auditweights_dailyRecency = x.getAs[Double]("auditweights_dailyRecency"),
  //  auditweights_friendLikes_actors = x.getAs[Double]("auditweights_friendLikes_actors"),
  //  auditweights_likersFeedStats_hyper = x.getAs[Double]("auditweights_likersFeedStats_hyper"),
  //  userOwnerCounters_USER_FEED_REMOVE = x.getAs[Double]("userOwnerCounters_USER_FEED_REMOVE"),
  //  metadata_numTokens = x.getAs[Int]("metadata_numTokens"),
  //  metadata_numVideos = x.getAs[Int]("metadata_numVideos"),
  //  auditweights_userOwner_CREATE_LIKE = x.getAs[Double]("auditweights_userOwner_CREATE_LIKE"),
  //  auditweights_userOwner_TEXT = x.getAs[Double]("auditweights_userOwner_TEXT"),
  //  userOwnerCounters_CREATE_LIKE = x.getAs[Double]("userOwnerCounters_CREATE_LIKE"),
  //  userOwnerCounters_TEXT = x.getAs[Double]("userOwnerCounters_TEXT"),
  //  userOwnerCounters_IMAGE = x.getAs[Double]("userOwnerCounters_IMAGE"),
  //  auditweights_ageMs = x.getAs[Double]("auditweights_ageMs"),

//  case class OwnerBased(
//                         sex: Int,
//                         user_region: Long,
//                         auditweights_matrix: Double
//
//                       )
//
//  case class OwnerAnalytics(
//                             size: Int,
//                             genderPercent: Double,
//                             user_regionN:Int
//                           )
//
//  def apply(foldersAll: List[String], foldersWithFeedback: List[String], datasetUnknownFeedback: RDD[Dataset]): RDD[Dataset] = {
//
//    val datasetAll = DatasetCreator.sqlContext.read.parquet(foldersAll: _*).rdd
//    val datasetWithFeedback = DatasetCreator.sqlContext.read.parquet(foldersWithFeedback: _*).rdd
//    datasetAll.cache()
//
//    val ownerData: RDD[(Int, OwnerAnalytics)] = datasetAll.map { x =>
//
//      x.getAs[Int]("metadata_ownerId") ->
//        List(OwnerBased(
//          sex = x.getAs[Int]("user_gender"),
//          user_region = x.getAs[Long]("audit_resourceType"),
//          auditweights_matrix = x.getAs[Double]("auditweights_matrix")
//        ))
//
//    }.reduceByKey(_ ::: _)
//      .mapValues {
//        data =>
//          val size = data.size
//          OwnerAnalytics(
//            size = size,
//            genderPercent = data.count(_.sex == 1).toDouble / data.size,
//            user_regionN = data.map(_.user_region).distinct.size
//
//          )
//
//
//      }
//
//
//
//    //    val ds1 = datasetUnknownFeedback.map(x => (x.objectId, ((x.showAt - x.createdAt).toDouble / 1000 / 3600 / 24).toInt) -> x).leftOuterJoin(objectDayPopulation)
//    //      .map { case (_, (d, f)) => d.copy(objectDayPopulation = f.getOrElse(0)) }
//  }

}


































































































































































