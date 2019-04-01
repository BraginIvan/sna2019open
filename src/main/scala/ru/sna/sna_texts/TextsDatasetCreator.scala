package ru.sna.sna_texts

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

case class Dataset(userId: Int,
                   objectId: Int,
                   ownerId: Int,
                   isLiked: Int,
                   isDisLiked: Int,
                   device: String,
                   createdAt: Long,
                   showAt: Long,
                   isReShared: Int,
                   isClicked: Int,
                   textCat: Int = -1,
                   showDelay: Long = 0,
                   createdAtWeekDay: Int = 0,
                   createdAtHour: Int = 0,
                   showAtWeekDay: Int = 0,
                   showAtHour: Int = 0,
                   userTrainPopulation: Int = 0,

                   objectPopulation: Int = -1,
                   objectUsersCount: Int = -1,

                   objectDayPopulation: Int = -1,
                   userCatPopulation: Int = -1,
                   ownerCatPopulation: Int = -1,
                   userCatLikedPopulation: Int = -1,
                   userCatCount: Int = -1,

                   ownerCatCount: Int = -1,

                   userPopulation: Int = -1,

                   userOwnersCount: Int = -1,
                   userPopOwners: Int = -1,
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
                   ownerUsersCount: Int = -1,
                   //                   ownerPopUser: Int = -1,
                   ownerLikedPopulation: Int = -1,

                   userObjectFeedSize: Option[Int] = None,
                   ownerCollabGender: Double = 0.5,
                   ownerCollabRegionSize: Double = 0,
                   //                   ownerCollabAge: Int = -1,

                   collab_auditweights_ctr_gender: Double = 0.0,
                   collab_auditweights_svd_spark: Double = 0.0,
                   collab_auditweights_svd_sparkSTD: Double = 0.0,
                   collab_audit_experimentSize: Int = 0,
                   collab_audit_experiment: String = "",
                   collab_auditweights_userOwner_CREATE_LIKE: Double = 0.0,
                   collab_auditweights_userOwner_CREATE_LIKESTD: Double = 0.0,
                   tokensN: Int = 0,
                   hasUrl: Int = 0,
                   hasOk: Int = 0,
                   objectFixetTimes: Int = 0,
//                   hasC: Int = 0,
//                   hasYoutu: Int = 0,
//                   hasStar: Int = 0,
                   cluster70: Int= -1,
                   cluster120: Int= -1,
//                   emb0: Double = 0.0,emb1: Double = 0.0,emb2: Double = 0.0,emb3: Double = 0.0,emb4: Double = 0.0,
//                   emb5: Double = 0.0,emb6: Double = 0.0,emb7: Double = 0.0,emb8: Double = 0.0,emb9: Double = 0.0,
                   knownLike:Int = -1,

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
                   userObjectTimes: Int = 1
                  )


object TextsDatasetCreator {
  def getClusters120Path() = ""
  def getClusters70Path() = ""
  val conf = new SparkConf()
    .setAppName(getClass.getSimpleName)
  //    .setMaster("local[6]")
  //    .set("spark.local.dir", s"/home/ivan/tmp2")
  //    .set("spark.driver.memory", "10g")
  //    .set("spark.executor.memory", "3g")
  val spark = SparkContext.getOrCreate(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(spark)

  def run() {
    //  Thread.sleep(1000*60*60*1)

    def getListOfFiles(dir: String): List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }


    import sqlContext.implicits._

    val objectToTextCat: RDD[(Int, Int)] = spark.textFile("features/clusters120.csv").map { x =>
      val s = x.split(",")
      s.head.toInt -> s.last.toInt
    }.reduceByKey((a,b) => a)

    List(FoldersType.ForTrain, FoldersType.ForTest, FoldersType.ForTrainForSubmit, FoldersType.ForSubmit).foreach {
      //    List(FoldersType.ForTrain).foreach {

      foldersType =>


        val foldersAll = FolderProvider.getAllFoldersWithoutFeedback(foldersType)
        val foldersUnknownFeedback = FolderProvider.getAllFoldersCurrentDataset(foldersType)
        val foldersWithFeedback = FolderProvider.getFoldersWithFeedback(foldersType)


        val initDataset = sqlContext.read.parquet(foldersUnknownFeedback: _*).rdd.repartition(60).map {
          x =>
            val userId = x.getAs[Int]("instanceId_userId")
            val obj = x.getAs[Int]("instanceId_objectId")
            val ownerId = x.getAs[Int]("metadata_ownerId")
            val feedback = if (foldersType == FoldersType.ForSubmit) Nil else x.getAs[mutable.WrappedArray[String]]("feedback").toList
            val device = x.getAs[String]("audit_clientType")


            val isLiked = if (feedback.contains("Liked")) 1 else 0
            val isClicked = if (feedback.contains("Clicked")) 1 else 0
            val isReShared = if (feedback.contains("ReShared")) 1 else 0

            val isDisLiked = if (feedback.contains("Disliked")) 1 else 0
            (userId,obj) -> Dataset(
              userId = userId,
              objectId = obj,
              ownerId = ownerId,
              isLiked = isLiked,
              isDisLiked = isDisLiked,
              isClicked = isClicked,
              isReShared=isReShared,
              createdAt = x.getAs[Long]("metadata_createdAt"),
              showAt = x.getAs[Long]("audit_timestamp"),
              device = device)
        }.reduceByKey((a,b) =>
          // add timestamp to key
          a.copy(
            isLiked=if(a.isLiked == 1 || b.isLiked == 1) 1 else 0,
            userObjectTimes=a.userObjectTimes + b.userObjectTimes
          )
        )
          .map(x => x._1._2 -> x._2)





       val initDatasetWithCat =  initDataset.leftOuterJoin(objectToTextCat)
          .map {
            case (_, (d, c)) =>
              d.copy(textCat = c.getOrElse(-1))
          }





        val withCollab = CollabData.run(initDatasetWithCat)

        withCollab.cache()


        val init = InitPrepare(foldersAll = foldersAll, foldersWithFeedback = foldersWithFeedback, datasetUnknownFeedback = withCollab, textCat = objectToTextCat)
        case class TextData( tokensN: Int, hasUrl: Int, hasOk: Int, count: Int,
                            hasC: Int, hasYoutu: Int, hasStar: Int)

        val textLang: RDD[(Int, TextData)] = sqlContext.read.parquet(List("parquets/texts/textsTest", "parquets/texts/textsTrain"): _*).rdd.map {
          x =>
            val obj = x.getAs[Int]("objectId")
//            val lang = x.getAs[String]("lang")
            val tokens = x.getAs[mutable.WrappedArray[String]]("preprocessed")
            val text = x.getAs[String]("text")

            //            case class TextData(lang: String, tokensN: Int, hasUrl: Int, count: Int)

            obj -> TextData(
//              lang = lang,
              tokensN = tokens.length,
              hasUrl = if (text.contains("http")) 1 else 0,
              hasOk = if (text.contains("ok.ru")) 1 else 0,
              hasC = if (text.contains("©")) 1 else 0,
              hasYoutu = if (text.contains("youtu")) 1 else 0,
              hasStar = if (text.contains("❤")) 1 else 0,
              count = 1)
        }.reduceByKey((a, b) => a.copy(count = a.count + b.count))

        val objectToTextCat70: RDD[(Int, Int)] = spark.textFile(getClusters70Path()).map { x =>
          val s = x.split(",")
          s.head.toInt -> s.last.toInt
        }.reduceByKey((a,b) => a)
        val objectToTextCat120: RDD[(Int, Int)] = spark.textFile(getClusters120Path()).map { x =>
          val s = x.split(",")
          s.head.toInt -> s.last.toInt
        }.reduceByKey((a,b) => a)


        val res = init.map(x => x.objectId -> x)
          .leftOuterJoin(objectToTextCat70)
          .leftOuterJoin(objectToTextCat120)
          .leftOuterJoin(textLang)
          .map {
            case (_, (((ds, c70), c120),  textData)) =>
            ds.copy(
              cluster70 = c70.getOrElse(-1),
              cluster120 = c120.getOrElse(-1),
//              emb0 = v(0), emb1 = v(1), emb2 = v(2), emb3 = v(3), emb4 = v(4), emb5 = v(5), emb6 = v(6), emb7 = v(7), emb8 = v(8), emb9 = v(9),
              tokensN = textData.map(_.tokensN).getOrElse(0),
              hasUrl = textData.map(_.hasUrl).getOrElse(0),
              hasOk = textData.map(_.hasOk).getOrElse(0),
              objectFixetTimes = textData.map(_.count).getOrElse(0)
//              hasC = textData.map(_.hasC).getOrElse(0),
//              hasYoutu = textData.map(_.hasYoutu).getOrElse(0),
//              hasStar = textData.map(_.hasStar).getOrElse(0)
            )
        }

        res .toDF().write.parquet(FolderProvider.saveTo(foldersType))

    }
  }

}




