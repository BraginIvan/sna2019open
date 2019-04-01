package ru.sna.sna_texts


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object UserTextVector extends App {


  private val master = "local"
  val conf = new SparkConf()
    .setAppName(getClass.getSimpleName)
    .setMaster("local[6]")
    .set("spark.local.dir", s"/home/ivan/tmp2")
    .set("spark.driver.memory", "10g")
    .set("spark.executor.memory", "3g")
  val spark = SparkContext.getOrCreate(conf)
  val sqlContext = new org.apache.spark.sql.SQLContext(spark)

  case class ObjectData2(objectId: Int,
                         preprocessed:List[String]
                      )

  case class Out(user:Int, features: List[Double])


  val goodUsersSet = sqlContext.read.parquet(  FolderProvider.testFolders ::FolderProvider.trainFolders.take(FolderProvider.trainFolders.size - (FolderProvider.valDays+FolderProvider.testDays)) : _*).rdd
    .map(_.getAs[Int]("instanceId_userId"))
    .distinct()
    .collect()
    .toSet

  val goodUsersToObject = sqlContext.read.parquet(  FolderProvider.testFolders :: FolderProvider.trainFolders : _*).rdd
    .flatMap{x =>
      val u = x.getAs[Int]("instanceId_userId")
      if(goodUsersSet(u))
        Some(x.getAs[Int]("instanceId_objectId") -> List(u))
      else
        None
    }
    .reduceByKey(_ ::: _)




  private val trainFolders = "/home/ivan/projects/sna2019/texts/textsTrain/"
  private val testFolders = "/home/ivan/projects/sna2019/texts/textsTest/"

  val userToTokens: RDD[(Int, List[String])] = sqlContext.read.parquet(List(testFolders,trainFolders) : _*).rdd
    .map{ x =>
      x.getAs[Int]("objectId") -> x
    }.rightOuterJoin(goodUsersToObject)
    .flatMap{
      case (o, (Some(oData), users))=>
        users.map(_ -> oData.getAs[mutable.WrappedArray[String]]("preprocessed").toList)
      case _ => Nil
    }

  userToTokens.cache()


  val tokenId = userToTokens.flatMap{_._2.distinct.map(_ -> 1)
  }.reduceByKey(_ + _)
    .filter(_._2 > 15)
    .map(_._1)
    .collect()
    .zipWithIndex.map(x => x._1 -> x._2).toMap

  val docsSize = userToTokens.count()


  val tokenIdf = userToTokens.flatMap{
    _._2.distinct.map(x => tokenId.getOrElse(x,-1) -> 1).distinct
  }.reduceByKey(_ + _)
    .mapValues{x => Math.log(docsSize.toDouble/x)}
    .collect().toMap


  userToTokens
    .reduceByKey(_ ::: _)
    .flatMap {
      case (u, t) =>
        val docTokensSize=t.size
        val tokens = t.groupBy(identity).map{
          x =>
            val id = tokenId.getOrElse(x._1, -1)
            val tf = x._2.size.toDouble/docTokensSize
            id +"|"+ (tf*tokenIdf(id))

        }.toList
        if (tokens.size > 1)
          Some(u + "," + tokens.mkString(","))
        else
          None

    }.saveAsTextFile(FolderProvider.userTokensTo(FoldersType.ForTrain))


}


