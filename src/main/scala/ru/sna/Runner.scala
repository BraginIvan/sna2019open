package ru.sna


//import ru.sna.sna.DatasetCreatorImages
import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import ru.sna.sna_collab.DatasetCreatorCollab
import ru.sna.sna_texts.TextsDatasetCreator.getClass

import scala.io.Source
//import ru.sna.sna_texts.TextsDatasetCreator

object Runner {
  def main(args: Array[String]): Unit = {
//    TextsDatasetCreator.run()
//    DatasetCreatorImages.run()
    DatasetCreatorCollab.run()

//    val conf = new SparkConf()
//      .setAppName(getClass.getSimpleName)
//
//    val spark = SparkContext.getOrCreate(conf)
//
//    val sqlContext = new org.apache.spark.sql.SQLContext(spark)
//    sqlContext.setConf("spark.sql.parquet.compression.codec", "gzip")
//
//    sqlContext.read.parquet("parquets/collab_day/features/gt4")
//      .write.parquet("parquets/collab_day/features/gt5")

//    import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsScalaMapConverter}
//
//    import org.apache.parquet.hadoop.ParquetFileReader
//    import org.apache.hadoop.fs.{FileSystem, Path}
//    import org.apache.hadoop.conf.Configuration
//    val conf1 = new SparkConf()
//      .setAppName(getClass.getSimpleName)
//    //    .setMaster("local[6]")
//    //    .set("spark.local.dir", s"/home/ivan/tmp2")
//    //    .set("spark.driver.memory", "10g")
//    //    .set("spark.executor.memory", "3g")
//    val spark = SparkContext.getOrCreate(conf1)
//    val conf = spark.hadoopConfiguration
//
//    def getFooters(conf: Configuration, path: String) = {
//      val fs = FileSystem.get(conf)
//      val footers = ParquetFileReader.readAllFootersInParallel(conf, fs.getFileStatus(new Path(path)))
//      footers
//    }
//    def getFileMetadata(conf: Configuration, path: String) = {
//      getFooters(conf, path)
//        .asScala.map(_.getParquetMetadata.getFileMetaData.getKeyValueMetaData.asScala)
//    }
//
//
//
//    import org.apache.parquet.hadoop.ParquetFileWriter
//
//    def createMetadata(conf: Configuration, path: String) = {
//      val footers = getFooters(conf, path)
//      ParquetFileWriter.writeMetadataFile(conf, new Path(path), footers)
//    }
//
//    createMetadata(conf, "parquets/collab_day/features/gt5")




//    val p = new PrintWriter("./image_classes.csv")
//    p.write("hash,clazz,score\n")
//    (Source.fromFile("./my_classes_new3.csv").getLines().toList ::: Source.fromFile("./my_test_classes_new3.csv").getLines().toList)
//      .map{
//        x =>
//          val split = x.split("\t")
//          val id = split.head.split("_").head
//          val clazz = split.apply(1)
//          val score = split.apply(2)
//          id -> (clazz, score)
//      }.groupBy(_._1)
//      .foreach{
//        x => p.write(x._1 + "," + x._2.head._2._1 + "," + x._2.head._2._2 + "\n")
//      }
//    p.close()
    //Reader.run()

//    ConvertToCommon.convert(CompetitionType.Images)
//    ConvertToCommon.convert(CompetitionType.Texts) //20108184
//    ConvertToCommon.convert(CompetitionType.Collab)
//
//    ConvertToCommon.addExternalData(CompetitionType.Texts) //155830418 20108184
//    ConvertToCommon.addExternalData(CompetitionType.Images)
//    ConvertToCommon.addExternalData(CompetitionType.Collab)
//
//    OwnerCommonAnalyzer.apply(CompetitionType.Texts)
//    OwnerCommonAnalyzer.apply(CompetitionType.Collab)
//    OwnerCommonAnalyzer.apply(CompetitionType.Images)
//
//    CreateFeatures.apply(CompetitionType.Texts, true, true)
//    CreateFeatures.apply(CompetitionType.Texts, true, false)
//    MergeFeatures.applyTexts(true, true)
//    MergeFeatures.applyTexts(true, false)
//
//
//    CreateFeatures.apply(CompetitionType.Texts, false, true)
//    CreateFeatures.apply(CompetitionType.Texts, false, false)
//    MergeFeatures.applyTexts(false, true)
//    MergeFeatures.applyTexts(false, true)


    //    CreateFeatures.apply(CompetitionType.Texts, false)
//    MergeFeatures.applyTexts(false)
//
//    CreateFeatures.apply(CompetitionType.Images, true)
//    MergeFeatures.applyImages(true)
//
//    CreateFeatures.apply(CompetitionType.Images, false)
//    MergeFeatures.applyImages(false)
//
//    CreateFeatures.apply(CompetitionType.Collab, true)
//    MergeFeatures.applyImages(true)
//
//    CreateFeatures.apply(CompetitionType.Collab, false)
//    MergeFeatures.applyImages(false)
  }
}

