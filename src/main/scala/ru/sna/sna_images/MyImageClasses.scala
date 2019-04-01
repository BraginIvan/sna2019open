package ru.sna.sna_images
// проверить почему по user+cat так много None
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.sna.sna_images.LookAlike.{getClass, getListOfFiles}
import DatasetCreatorImages.sqlContext

import scala.collection.{immutable, mutable}
import scala.io.Source

object MyImageClasses  {


  val path = "features/my_classes_new3.csv"
  val path2 = "features/my_test_classes_new3.csv"

  val imageType: RDD[(String, (Int, Double))] = DatasetCreatorImages.spark.textFile (path)
          .union (DatasetCreatorImages.spark.textFile (path2) )
    .map {
    x =>
      val split = x.split ("\t")
      val element = split.lift(1).map(_.toInt).getOrElse(-1)
      val score = split.lift(2).map(_.toDouble).getOrElse(0.0)
      val image = split.head.split ("/").last.split ("_").head
      image -> (element, score)

  }.reduceByKey((a,b) => a)



  def apply(foldersUnknownFeedback: List[String],
            dataset: RDD[Dataset]): RDD[Dataset] = {


    lazy val df = sqlContext.read.parquet (foldersUnknownFeedback: _*).rdd




    val imgToObjects: RDD[(String, List[Int])] = df.map {
      x =>
        val object_ = x.getAs[Int] ("instanceId_objectId")
        val imageId = x.getAs[mutable.WrappedArray[String]] ("ImageId").toList.head
        imageId -> List (object_)
    }.distinct.reduceByKey (_::: _)
      .mapValues(_.distinct)

    val objectToCat: RDD[(Int, (Int, Double))] = imgToObjects.leftOuterJoin(imageType)
      .flatMap {
        case (_, (objects, ss) ) =>
          objects.map (_ -> ss.getOrElse ((-1, 0.0)) )
      }.reduceByKey((a,b) => if(a._2 > b._2) a else b)

    val objToDataset: RDD[(Int, Dataset)] = dataset.map(x => x.objectId -> x)
    objToDataset.leftOuterJoin(objectToCat)
      .map {
        case(_, (d, catId)) =>
          d.copy(myCat=catId.map(_._1),
            myCatScore=catId.map(_._2))
      }



  }

  def ownerCat(foldersAll: List[String],dataset: RDD[Dataset]): RDD[Dataset] = {


    lazy val df = sqlContext.read.parquet (foldersAll: _*).rdd

    val imgToOwner: RDD[(String, List[Int])] = df.map {
      x =>
        val owner = x.getAs[Int] ("metadata_ownerId")
        val imageId = x.getAs[mutable.WrappedArray[String]] ("ImageId").toList.head
        imageId -> List (owner)
    }.distinct.reduceByKey (_::: _)
      .mapValues(_.distinct)

    val ownerToCat: RDD[(Int, (Int, Double, Int, Double, Int))] = imgToOwner.leftOuterJoin(imageType)
      .flatMap {
        case (_, (owners, ss) ) =>
          owners.map (_ -> List(ss.getOrElse ((-1,0.0))) )
      }.reduceByKey(_:::_)
        .flatMap{
          case(owner, cats) =>
            val uniqueCatsCount = cats.map(_._1).distinct.size

            val meantCats = cats.filter(x => x._2 > 0.000001 )
            if (meantCats.isEmpty)
              None
            else {
              val (catId, size) = meantCats.map(_._1).groupBy(identity).mapValues(_.size).maxBy(_._2)
              val (catId2, importance) = meantCats.groupBy(x => x._1).mapValues(x => x.map(_._2).sum).maxBy(_._2)
              Some(owner -> (catId, size.toDouble / meantCats.size, catId2, importance / meantCats.map(_._2).sum, uniqueCatsCount))
            }
        }

    val ownerToDataset: RDD[(Int, Dataset)] = dataset.map(x => x.ownerId -> x)
    val dataset2 = ownerToDataset.leftOuterJoin(ownerToCat)
      .map {
        case(_, (d, Some((id, size, id2, size2, uniqueCatsCount)))) =>
          d.copy(ownerCatPercent=size,
            ownerCat=id,
            ownerCatPercent2=size2,
            ownerCat2=id2,
            ownerUniqueCatsCount=uniqueCatsCount
          )
        case (_, (d, _)) => d
      }


    val imgToUser: RDD[(String, List[Int])] = df.map {
      x =>
        val user = x.getAs[Int] ("instanceId_userId")
        val imageId = x.getAs[mutable.WrappedArray[String]] ("ImageId").toList.head
        imageId -> List (user)
    }.distinct.reduceByKey (_::: _)
      .mapValues(_.distinct)

    val userToCat: RDD[(Int, (Int, Double, Int, Double, Int))] = imgToUser.leftOuterJoin(imageType)
      .flatMap {
        case (_, (users, ss) ) =>
          users.map (_ -> List(ss.getOrElse ((-1,0.0))) )
      }.reduceByKey(_:::_)
      .flatMap{
        case(user, cats) =>
          val uniqueCatsCount = cats.map(_._1).distinct.size
          val meantCats = cats.filter(x => x._2 > 0.000001 )
          if (meantCats.isEmpty)
            None
          else {
            val (catId, size) = meantCats.map(_._1).groupBy(identity).mapValues(_.size).maxBy(_._2)
            val (catId2, importance) = meantCats.groupBy(_._1).mapValues(x => x.map(_._2).sum).maxBy(_._2)
            Some(user -> (catId, size.toDouble / meantCats.size, catId2, importance / meantCats.map(_._2).sum, uniqueCatsCount))
          }
      }

    val userToDataset: RDD[(Int, Dataset)] = dataset2.map(x => x.userId -> x)
    userToDataset.leftOuterJoin(userToCat)
      .map {
        case(_, (d, Some((id, size, id2, size2, uniqueCatsCount)))) =>
          d.copy(userCatPercent=size,
            userCat=id,
            userCatPercent2=size2,
            userCat2=id2,
            userUniqueCatsCount=uniqueCatsCount
          )
        case (_, (d, _)) => d
      }

  }


  def userLikeCat(foldersWithFeedback: List[String], dataset: RDD[Dataset]): RDD[Dataset] = {


    lazy val df = sqlContext.read.parquet (foldersWithFeedback: _*).rdd.filter {x => x.getAs[mutable.WrappedArray[String]]("feedback").toList.contains("Liked")}


    val imgToUser: RDD[(String, List[Int])] = df.map {
      x =>
        val user = x.getAs[Int] ("instanceId_userId")
        val imageId = x.getAs[mutable.WrappedArray[String]] ("ImageId").toList.head
        imageId -> List (user)
    }.distinct.reduceByKey (_::: _)
      .mapValues(_.distinct)

    val userToCat: RDD[(Int, (Int, Double, Int, Double, Int))] = imgToUser.leftOuterJoin(imageType)
      .flatMap {
        case (_, (users, ss) ) =>
          users.map (_ -> List(ss.getOrElse ((-1,0.0))) )
      }.reduceByKey(_:::_)
      .flatMap{
        case(user, cats) =>
          val uniqueCatsCount = cats.map(_._1).distinct.size

          val meantCats = cats.filter(x => x._2 > 0.000001 )
          if (meantCats.isEmpty)
            None
          else {
            val (catId, size) = meantCats.map(_._1).groupBy(identity).mapValues(_.size).maxBy(_._2)
            val (catId2, importance) = meantCats.groupBy(x => x._1).mapValues(x => x.map(_._2).sum).maxBy(_._2)
            Some(user -> (catId, size.toDouble / meantCats.size, catId2, importance / meantCats.map(_._2).sum, uniqueCatsCount))
          }
      }

    val userToDataset: RDD[(Int, Dataset)] = dataset.map(x => x.userId -> x)
    userToDataset.leftOuterJoin(userToCat)
      .map {
        case(_, (d, Some((id, size, id2, size2, uniqueCatsCount)))) =>
          d.copy(userLikedCatPercent=size,
            userLikedCat=id,
            userLikedCatPercent2=size,
            userLikedCat2=id,
            userUniqueCatsLikedCount=uniqueCatsCount
          )
        case (_, (d, _)) => d
      }

  }



}
