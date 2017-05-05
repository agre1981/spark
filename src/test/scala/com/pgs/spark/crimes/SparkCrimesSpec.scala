package com.pgs.spark.crimes

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.pgs.spark.crimes.model.CrimeModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

/**
  * Created by ogrechanov on 3/21/2017.
  */
class SparkCrimesSpec extends FunSuite with DataFrameSuiteBase  {

  test("test arrest count - dataFrame") {

    val schema = createSchema
    val rdd = getCrimesRDD

    val crimesDataFrame = sqlContext.createDataFrame(rdd, schema)

    val arrests = SparkCrimes.getArrests(crimesDataFrame)
    val arrestsCount = arrests.count()

    assert(arrestsCount === 2)
  }

  test("test arrest count - dataView") {

    val schema = createSchema
    val rdd = getCrimesRDD

    val crimesDataFrame = sqlContext.createDataFrame(rdd, schema)
    crimesDataFrame.createOrReplaceTempView("crimes")

    val arrests = SparkCrimes.getArrests(sqlContext)
    val arrestsCount = arrests.count()

    assert(arrestsCount === 2)
  }

  test("test arrest count - dataSet") {

    val dataSet = getCrimesDataSet

    val arrests = SparkCrimes.getArrestsFromDS(dataSet)
    val arrestsCount = arrests.count()

    assert(arrestsCount === 2)
  }

  private def createSchema = {
    val idField = new StructField("Id", IntegerType, nullable = false)
    val descField = new StructField("Description", StringType, nullable = true)
    val arrestField = new StructField("Arrest", BooleanType, nullable = false)
    val schema = StructType(Seq(idField, descField, arrestField))
    schema
  }

  private def getCrimesRDD = {
    val rdd = sqlContext.sparkContext.parallelize( Seq(
      Row(1, "desc 1", true),
      Row(2, "desc 2", false),
      Row(3, "desc 3", true)) )
    rdd
  }

  private def getCrimesDataSet = {
    import spark.implicits._

    val dataSet = sqlContext.sparkSession.createDataset( Seq(
      CrimeModel(1, "desc 1", true),
      CrimeModel(2, "desc 2", false),
      CrimeModel(3, "desc 3", true)) )
    dataSet
  }
}
