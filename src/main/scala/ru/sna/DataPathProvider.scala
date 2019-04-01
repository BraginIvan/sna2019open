package ru.sna

import java.io.File

import org.apache.commons.io.FileUtils

trait DataPathProvider {

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isDirectory).toList
    } else {
      Nil
    }
  }

  def getRawTrainFolders(compType: CompetitionType.Value): List[String] = {
    val name = compType match {
      case CompetitionType.Collab => "collab"
      case CompetitionType.Texts => "texts"
      case CompetitionType.Images => "images"
    }
    val trainPath = s"parquets/raw/${name}Train/"
    val trainFolders = getListOfFiles(trainPath).sorted.map(x => trainPath + x.getName)
    trainFolders
  }

  def getRawTestFolder(compType: CompetitionType.Value): List[String] = {
    val name = compType match {
      case CompetitionType.Collab => "collab"
      case CompetitionType.Texts => "texts"
      case CompetitionType.Images => "images"
    }
    List(s"parquets/raw/${name}Test")
  }

  def getCommonFormatFolder(compType: CompetitionType.Value): String = {
   compType match {
      case CompetitionType.Collab => "parquets/commonFormat/collab"
      case CompetitionType.Texts => "parquets/commonFormat/texts"
      case CompetitionType.Images => "parquets/commonFormat/images"
    }
  }

  def getWithExternalDataFolder(compType: CompetitionType.Value): String = {
    compType match {
      case CompetitionType.Collab => "parquets/withExternalData/collab"
      case CompetitionType.Texts => "parquets/withExternalData/texts"
      case CompetitionType.Images => "parquets/withExternalData/images"
    }
  }


  def getCommonOwnerFolder(compType: CompetitionType.Value): String = {
    compType match {
      case CompetitionType.Collab => "parquets/OWNER/collab"
      case CompetitionType.Texts => "parquets/OWNER/texts"
      case CompetitionType.Images => "parquets/OWNER/images"
    }
  }


  def getFeaturesFolder(compType: CompetitionType.Value, isInternal: Boolean, isTest: Boolean): String = {
    val path = compType match {
      case CompetitionType.Collab => "parquets/features/collab"
      case CompetitionType.Texts => "parquets/features/texts"
      case CompetitionType.Images => "parquets/features/images"
    }
    val path2 = if (isInternal)
      path + "/intenal"
    else
      path + "/sumbit"
    if (isTest)
      path2 + "/test"
    else
      path2 + "/train"
  }

  def getFeaturesFinalFolder(compType: CompetitionType.Value, isInternal: Boolean, isTest: Boolean): String = {
    val path = compType match {
      case CompetitionType.Collab => "parquets/features_final/collab"
      case CompetitionType.Texts => "parquets/features_final/texts"
      case CompetitionType.Images => "parquets/features_final/images"
    }
    val path2 = if (isInternal)
      path + "/intenal"
    else
      path + "/sumbit"
    if (isTest)
      path2 + "/test"
    else
      path2 + "/train"
  }


  def ownerBasedFolder(compType: CompetitionType.Value, isInternal: Boolean) ={
    val path = compType match {
      case CompetitionType.Collab => "parquets/features_ownerBased/collab"
      case CompetitionType.Texts => "parquets/features_ownerBased/texts"
      case CompetitionType.Images => "parquets/features_ownerBased/images"
    }
    if (isInternal)
      path + "/intenal"
    else
      path + "/sumbit"
  }

  def userBasedFolder(compType: CompetitionType.Value, isInternal: Boolean) ={
    val path = compType match {
      case CompetitionType.Collab => "parquets/features_userBased/collab"
      case CompetitionType.Texts => "parquets/features_userBased/texts"
      case CompetitionType.Images => "parquets/features_userBased/images"
    }
    if (isInternal)
      path + "/intenal"
    else
      path + "/sumbit"
  }

  def userOwnerFolder(compType: CompetitionType.Value, isInternal: Boolean) ={
    val path = compType match {
      case CompetitionType.Collab => "parquets/features_userownerBased/collab"
      case CompetitionType.Texts => "parquets/features_userownerBased/texts"
      case CompetitionType.Images => "parquets/features_userownerBased/images"
    }
    if (isInternal)
      path + "/intenal"
    else
      path + "/sumbit"
  }

  def objectBasedFolder(compType: CompetitionType.Value, isInternal: Boolean) ={
    val path = compType match {
      case CompetitionType.Collab => "parquets/features_objectBased/collab"
      case CompetitionType.Texts => "parquets/features_objectBased/texts"
      case CompetitionType.Images => "parquets/features_objectBased/images"
    }
    if (isInternal)
      path + "/intenal"
    else
      path + "/sumbit"
  }


  def getClusters70Path(): String = "features/clusters70.csv"
  def getClusters120Path(): String = "features/clusters120.csv"
  def getDoc2Vec10Path(): String = "features/doc2vec10.csv"
  def getParquetTexts(): List[String] = List("parquets/texts/textsTest","parquets/texts/textsTrain")


  def deleteFolder(folder: String) = {
    FileUtils.deleteDirectory(new File( folder))

  }



}
