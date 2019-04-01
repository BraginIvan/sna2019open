package ru.sna.sna_texts

import org.apache.spark.rdd.RDD
import ru.sna.MathUtils
import ru.sna.sna_images.FoldersType.Value
import ru.sna.sna_images.LookAlike.getListOfFiles
object CollabData {

  //metadata_ownerType
  //user_region
  //auditweights_userAge
  def run(dataset: RDD[Dataset]): RDD[Dataset] = {

    val trainFolders = getListOfFiles("parquets/raw/collabTrain/").sorted.map(x => "parquets/raw/collabTrain/" + x.getName)
    val datasetAll = TextsDatasetCreator.sqlContext.read.parquet(trainFolders: _*).rdd
    val collabOwnerData = datasetAll.map {
      x =>
        val owner = x.getAs[Int]("metadata_ownerId")
        val sex = x.getAs[Int]("user_gender")
//        val dob = x.getAs[Int]("user_birth_date")
        val region = x.getAs[Int]("user_region")
        val auditweights_ctr_gender = x.getAs[Double]("auditweights_ctr_gender")
        val auditweights_svd_spark = x.getAs[Double]("auditweights_svd_spark")

//        val audit_experiment =   x.getAs[String]("audit_experiment")
        val auditweights_userOwner_CREATE_LIKE =   x.getAs[Double]("auditweights_userOwner_CREATE_LIKE")



        owner -> List((sex, region, auditweights_ctr_gender, auditweights_svd_spark,  auditweights_userOwner_CREATE_LIKE))
    }.reduceByKey(_ ::: _)
      .mapValues{
         data =>
          val ownerGender = data.count(_._1 == 1).toDouble / data.size
          val ownerRegion = data.map(_._2).groupBy(identity).maxBy(_._2.size)

          val auditweights_ctr_gender = data.map(_._3).sum / data.size
          val (auditweights_svd_spark, auditweights_svd_sparkSTD) = MathUtils.getMeanStd(data.map(_._4))

//          val audit_experimentSize = data.map(_._5).distinct.size
//          val  audit_experiment= data.map(_._5).groupBy(identity).maxBy(_._2.size)._1

          val (auditweights_userOwner_CREATE_LIKE, auditweights_userOwner_CREATE_LIKESTD) = MathUtils.getMeanStd(data.map(_._5))

         (ownerGender, "",  ownerRegion._2.size.toDouble / data.size, auditweights_ctr_gender, auditweights_svd_spark, auditweights_svd_sparkSTD,
            auditweights_userOwner_CREATE_LIKE, auditweights_userOwner_CREATE_LIKESTD)
      }

    dataset.map(x => x.ownerId -> x).leftOuterJoin(collabOwnerData)
      .map{case (_,(d, f)) => d.copy(
        ownerCollabRegionSize = f.map(_._3).getOrElse(0),
        ownerCollabGender = f.map(_._1).getOrElse(0.5),
        collab_auditweights_ctr_gender = f.map(_._4).getOrElse(0.0),
        collab_auditweights_svd_spark = f.map(_._5).getOrElse(0.0),
        collab_auditweights_svd_sparkSTD = f.map(_._6).getOrElse(0.0),
        collab_auditweights_userOwner_CREATE_LIKE=f.map(_._7).getOrElse(0.0),
        collab_auditweights_userOwner_CREATE_LIKESTD=f.map(_._8).getOrElse(0.0)
      )}
  }

}
