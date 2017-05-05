package com.pgs.spark.crimes

import com.pgs.spark.crimes.model.CrimeModel
import org.apache.spark.sql._

object SparkCrimes extends App {

  val crimesFile = "crimes.csv"
  val sparkSession = SparkSession.builder()
    .appName("spark-crimes")
    .master("local[4]")
    .getOrCreate()

  try {
    val dataFrame = sparkSession.read.option("header", "true").option("inferSchema", "true").csv(crimesFile)
    dataFrame.printSchema()
    val arrests = getArrests(dataFrame)
    val arrestCount = arrests.count()
    println(s"Arrests count: $arrestCount")
  }
  finally {
    sparkSession.stop()
  }

  def getArrests(sqlContext: SQLContext) : Dataset[Row] = getArrests(sqlContext.table("crimes"))

  def getArrests(dataFrame: DataFrame) : Dataset[Row] = {
    val arrests = dataFrame.where("Arrest = true")
    arrests
  }

  def getArrestsFromDS(dataSet: Dataset[CrimeModel]) : Dataset[CrimeModel] = {
    val arrests = dataSet.filter(crime => crime.arrested == true)
    arrests
  }
}