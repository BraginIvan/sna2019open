package ru.sna

object MathUtils {

  def getMeanStd(data: List[Double]): (Double, Double) ={
    val mean= data.sum/data.size
    val variance = data.map(x => (x-mean)*(x-mean)).sum/data.size
    (mean,Math.sqrt(variance))
  }


}
