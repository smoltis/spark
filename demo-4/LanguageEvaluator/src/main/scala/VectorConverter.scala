package main

import breeze.linalg.{Vector => BreezeVector}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.{Vector =>SparkVector}

object VectorConverter{
  def getBreezeFor(sparkVector: SparkVector) = BreezeVector(sparkVector.toArray)
  def getSparkFor(breezeVector: BreezeVector[Double]) = Vectors.dense(breezeVector.toArray)
}