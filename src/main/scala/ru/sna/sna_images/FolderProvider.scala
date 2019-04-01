package ru.sna.sna_images

import ru.sna.sna_images.LookAlike._
// сделать количество дней одинаковыми
object FoldersType extends Enumeration {
  val ForSubmit, ForTrain, ForTest, ForTrainForSubmit = Value
}

object FolderProvider {
  import FoldersType._

  private val trainFolders = getListOfFiles("parquets/raw/imagesTrain/").sorted.map(x => "parquets/raw/imagesTrain/" + x.getName)
  private val testFolders = "parquets/raw/imagesTest/"
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
      case ForSubmit => "parquets/images/features/sumb"
      case ForTest => "parquets/images/features/test"
      case ForTrain => "parquets/images/features/gt"
      case ForTrainForSubmit => "parquets/images/features/gt2"
    }

  }

  def saveUserTo(foldersType:FoldersType.Value): String = {
    foldersType match {
      case ForSubmit => "parquets/images/features/user_sumb"
      case ForTest => "parquets/images/features/user_test"
      case ForTrain => "parquets/images/features/user_gt"
    }
  }

}

