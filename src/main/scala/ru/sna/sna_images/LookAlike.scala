package ru.sna.sna_images

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SQLContext

import scala.collection.{immutable, mutable}


object LookAlike {

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isDirectory).toList
    } else {
      Nil
    }
  }

}