package com.pgs.spark.crimes

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.collection.mutable

/**
  * Created by ogrechanov on 5/5/2017.
  */
class SparkDatasetSpec extends FunSuite with DataFrameSuiteBase {
  import spark.implicits._

  test("dataSet - convert from dataFrame") {
    val dataFrame = Seq(("name1", 100),("name2", 200),("name3", 300)).toDF("name", "salary")

    val dataSet = dataFrame.as[Employee]

    assert(dataSet.collect() === Array(Employee("name1", 100), Employee("name2", 200), Employee("name3", 300)))
  }

  test("dataSet - filter") {
    val dataSet = Seq(Employee("name1", 100), Employee("name2", 200), Employee("name3", 300)).toDS()

    val filteredDataSet = dataSet.filter(emp => emp.salary>200)

    assert(filteredDataSet.collect() === Array(Employee("name3", 300)))
  }

  test("dataSet - groupBy - collect to Array") {
    val dataSet = Seq(Employee("name1", 200), Employee("name2", 200), Employee("name3", 300)).toDS()

    val groupedDataSet = dataSet.groupBy($"salary").agg(collect_list($"name").alias("names"))

    groupedDataSet.collect() should contain theSameElementsAs Array(Row(200, Seq("name1", "name2")), Row(300, Seq("name3")))
  }

  test("dataSet - groupBy - collect to String") {
    val dataSet = Seq(Employee("name1", 200), Employee("name2", 200), Employee("name3", 300)).toDS()

    val groupedDataSet = dataSet.groupBy($"salary").agg(concat_ws(", ", collect_list($"name")).alias("names"))

    groupedDataSet.collect() should contain theSameElementsAs Array(Row(200, "name1, name2"), Row(300, "name3"))
  }
}
