package com.pgs.spark.crimes

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

object CrimeCount {

  def main(args: Array[String]) {
    val crimesFilePath = "crimes.csv"
    val sparkSession = SparkSession.builder().appName("spark-crimes-crime-count").master("local[4]").getOrCreate()

    val dataFrame = renameColumns(loadDataFrame(sparkSession, crimesFilePath))

    val columns = if (args.length > 0) args(0).split(",") else Array[String]()
    val filtersSplit = if (args.length > 1) args(1).split(",") else Array[String]()
    val filters = buildFiltersArray(filtersSplit)

    val result = doCount(columns, filters, dataFrame)
    result.foreach(println)

    sparkSession.stop()
  }

  def loadDataFrame(sparkSession: SparkSession, filePath: String): DataFrame = {
    sparkSession.read.option("header", "true").option("inferSchema", "true").csv(filePath)
  }

  def buildFiltersArray(filtersSplit: Array[String]): Array[(String, String)] = {
    var filters = Array[(String, String)]()
    for (i <- 0 until filtersSplit.length / 2) {
      filters :+=(filtersSplit(i * 2), filtersSplit(i * 2 + 1))
    }
    filters
  }

  def doCount(cols: Array[String], filters: Array[(String, String)], dataFrame: DataFrame): Array[Row] = {
    val filterString = buildFilterString(filters)
    val columns = cols.map(col => new Column(col))

    var filtered = dataFrame
    if (filterString.nonEmpty) {
      filtered = dataFrame.where(filterString)
    }
    filtered.groupBy(columns: _*).count().orderBy(columns: _*).collect()
  }

  def buildFilterString(filters: Array[(String, String)]): String = {
    val builder = new StringBuilder
    filters.foreach(filter => builder.append(filter._1).append("=").append(filter._2).append(" and "))
    if (builder.nonEmpty) {
      return builder.substring(0, builder.length - 5)
    }
    builder.toString()
  }

  def renameColumns(dataFrame: DataFrame): DataFrame = {
    val columns = List(("ID", "id"), ("Case Number", "caseNumber"), ("Date", "date"), ("Block", "block"), ("IUCR", "iucr"), ("Primary Type", "primaryType"), ("Description", "description"),
      ("Location Description", "locationDescription"), ("Arrest", "arrest"), ("Domestic", "domestic"), ("Beat", "beat"), ("District", "district"), ("Ward", "ward"),
      ("Community Area", "communityArea"), ("FBI Code", "fbiCode"), ("X Coordinate", "xCoordinate"), ("Y Coordinate", "yCoordinate"), ("Year", "year"), ("Updated On", "updatedOn"),
      ("Latitude", "latitude"), ("Longitude", "longitude"), ("Location", "location"))

    var df = dataFrame
    columns.foreach(column => df = df.withColumnRenamed(column._1, column._2))
    df
  }
}