package ru.sna.sna_collab

import ru.sna.sna_images.LookAlike.getListOfFiles
// сделать количество дней одинаковыми
object FoldersType extends Enumeration {
  val ForSubmit, ForTrain, ForTrainForSubmit,ForTrainForSubmit2, ForTest = Value
}

object FolderProvider {
  import FoldersType._

  private val trainFolders = getListOfFiles("dataset/train/").sorted.map(x => "dataset/train/" + x.getName)
  private val testFolders = "dataset/test/"
  val testDays = 7
  val valDays = 7
  val trainForSubmitDays = 21
  val trainForSubmitDays2 = 27

  def getFoldersWithFeedback(foldersType:FoldersType.Value): List[String] ={
    foldersType match {
      case ForSubmit => trainFolders
      case ForTest => trainFolders.take(trainFolders.size - testDays)
      case ForTrain => trainFolders.take(trainFolders.size - (valDays+testDays))
      case ForTrainForSubmit => trainFolders.take(trainFolders.size - trainForSubmitDays)
      case ForTrainForSubmit2 => trainFolders.take(trainFolders.size - trainForSubmitDays2)
    }
  }

  def getAllFoldersWithoutFeedback(foldersType:FoldersType.Value): List[String] ={
    foldersType match {
      case ForSubmit => testFolders::trainFolders
      case ForTest => trainFolders
      case ForTrain => trainFolders.take(trainFolders.size - testDays)
      case ForTrainForSubmit => testFolders::trainFolders
      case ForTrainForSubmit2 => testFolders::trainFolders
    }
  }

  def getAllFoldersCurrentDataset(foldersType:FoldersType.Value): List[String] ={
    foldersType match {
      case ForSubmit => List(testFolders)
      case ForTest => trainFolders.drop(trainFolders.size - testDays)
      case ForTrain => trainFolders.slice(trainFolders.size - (valDays+testDays), trainFolders.size - testDays)
      case ForTrainForSubmit => trainFolders.drop(trainFolders.size - trainForSubmitDays)
      case ForTrainForSubmit2 => trainFolders.drop(trainFolders.size - trainForSubmitDays2)
    }
  }

  def saveTo(foldersType:FoldersType.Value): String ={
    foldersType match {
      case ForSubmit => "parquets/collab_day/features/sumb"
      case ForTest => "parquets/collab_day/features/test"
      case ForTrain => "parquets/collab_day/features/gt"
      case ForTrainForSubmit =>  "parquets/collab_day/features/gt3"
      case ForTrainForSubmit2 =>  "parquets/collab_day/features/gt4"
    }

  }

  def saveUserTo(foldersType:FoldersType.Value): String = {
    foldersType match {
      case ForSubmit => "parquets/collab_day/features/user_sumb"
      case ForTest => "parquets/collab_day/features/user_test"
      case ForTrain => "parquets/collab_day/features/user_gt"
    }
  }

}

