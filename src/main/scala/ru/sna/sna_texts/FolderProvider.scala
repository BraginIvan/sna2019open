package ru.sna.sna_texts

import ru.sna.sna_images.LookAlike.getListOfFiles
// сделать количество дней одинаковыми
object FoldersType extends Enumeration {
  val ForSubmit, ForTrain, ForTest, ForTrainForSubmit = Value
}

object FolderProvider {
  import FoldersType._

  val trainFolders = getListOfFiles("parquets/raw/textsTrain/").sorted.map(x => "parquets/raw/textsTrain/" + x.getName)
  val testFolders = "parquets/raw/textsTest/"
  val testDays = 7
  val valDays = 7
  val trainSubmitDays = 14
  def getFoldersWithFeedback(foldersType:FoldersType.Value): List[String] ={
    foldersType match {
      case ForSubmit => trainFolders
      case ForTest => trainFolders.take(trainFolders.size - testDays)
      case ForTrain => trainFolders.take(trainFolders.size - (valDays+testDays))
      case ForTrainForSubmit => trainFolders.take(trainFolders.size - trainSubmitDays)

    }
  }

  def getAllFoldersWithoutFeedback(foldersType:FoldersType.Value): List[String] ={
    foldersType match {
      case ForSubmit => testFolders::trainFolders
      case ForTest => trainFolders
      case ForTrain => trainFolders.take(trainFolders.size - testDays)
      case ForTrainForSubmit => trainFolders

    }
  }

  def getAllFoldersCurrentDataset(foldersType:FoldersType.Value): List[String] ={
    foldersType match {
      case ForSubmit => List(testFolders)
      case ForTest => trainFolders.drop(trainFolders.size - testDays)
      case ForTrain => trainFolders.slice(trainFolders.size - (valDays+testDays), trainFolders.size - testDays)
      case ForTrainForSubmit => trainFolders.drop(trainFolders.size - trainSubmitDays)
    }
  }

  def saveTo(foldersType:FoldersType.Value): String ={
    foldersType match {
      case ForSubmit => "parquets/features/sumb"
      case ForTest => "parquets/features/test"
      case ForTrain => "parquets/features/gt"
      case ForTrainForSubmit => "parquets/features/gt2"
    }

  }

  def userTokensTo(foldersType:FoldersType.Value): String = {
    foldersType match {
      case ForSubmit => "parquets/features/user_sumb"
      case ForTest => "parquets/features/user_test"
      case ForTrain => "parquets/features/user_gt"
    }
  }

  def objectTokensTo(foldersType:FoldersType.Value): String = {
    foldersType match {
      case ForSubmit => "parquets/features/ob_sumb"
      case ForTest => "parquets/features/ob_test"
      case ForTrain => "parquets/features/ob_gt"
    }
  }

}

