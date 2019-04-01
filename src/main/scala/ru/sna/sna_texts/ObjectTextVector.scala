package ru.sna.sna_texts

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

object ObjectTextVector extends App {


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
  private val trainFolders = "/home/ivan/projects/sna2019/texts/textsTrain/"
  private val testFolders = "/home/ivan/projects/sna2019/texts/textsTest/"

  val xxx = sqlContext.read.parquet(List(testFolders,trainFolders) : _*).rdd.flatMap{_.getAs[mutable.WrappedArray[String]]("preprocessed").toList.distinct.map(_ -> 1)
  }.reduceByKey(_ + _)
    .map(x => x._2 -> List(x._1))
    .reduceByKey(_ ::: _)
    .collect()
    .sortBy(_._1)

  case class ObjectData2(objectId: Int,
                         preprocessed:List[String]
                      )

  case class Out(user:Int, features: List[Double])

  val goodObjects = sqlContext.read.parquet(  FolderProvider.testFolders ::FolderProvider.trainFolders.take(FolderProvider.trainFolders.size - (FolderProvider.valDays+FolderProvider.testDays)) : _*).rdd
    .map(_.getAs[Int]("instanceId_objectId")).distinct().collect().toSet




  val df = sqlContext.read.parquet(List(testFolders,trainFolders) : _*).rdd
    .filter{    x =>
      goodObjects(x.getAs[Int]("objectId"))
    }
  df.cache()
  val docsSize = df.count()
  val tokenId = df.flatMap{_.getAs[mutable.WrappedArray[String]]("preprocessed").toList.distinct.map(_ -> 1)
  }.reduceByKey(_ + _)
    .filter(_._2 > 15)
    .map(_._1)
    .collect()
    .zipWithIndex.map(x => x._1 -> x._2).toMap


  val tokenIDF: Map[Int, Double] = df.flatMap{
    _.getAs[mutable.WrappedArray[String]]("preprocessed").toList.distinct.map(x => tokenId.getOrElse(x,-1) -> 1).distinct
  }.reduceByKey(_ + _)
    .mapValues{x => Math.log(docsSize.toDouble/x)}
      .collect().toMap


  df.map {
    x =>
      val objectId = x.getAs[Int]("objectId")
      objectId-> List(ObjectData2(
        objectId = objectId,
        preprocessed=x.getAs[mutable.WrappedArray[String]]("preprocessed").toList
      ))
  }.reduceByKey((a, b) => a)
    .flatMap {
      case (ob, d) =>
        val docTokens = d.flatMap(_.preprocessed)
        val docTokensSize=docTokens.size
        val tokens = d.flatMap(_.preprocessed).groupBy(identity).map{
          x =>
            val id = tokenId.getOrElse(x._1, -1)
            val tf = x._2.size.toDouble/docTokensSize
            id +"|"+ (tf*tokenIDF(id))
        }.toList
        if (tokens.size > 1)
          Some(ob + "," + tokens.mkString(","))
        else
          None


    }.saveAsTextFile(FolderProvider.objectTokensTo(FoldersType.ForTrain))


}


