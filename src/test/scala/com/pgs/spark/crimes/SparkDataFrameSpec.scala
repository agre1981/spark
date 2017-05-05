package com.pgs.spark.crimes

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Row}
import org.scalatest.FunSuite

case class Employee(name: String, salary: Int)

/**
  * Created by ogrechanov on 5/5/2017.
  */
class SparkDataFrameSpec extends FunSuite with DataFrameSuiteBase {
  import spark.implicits._

  test("dataFrame - filter") {
    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3",1000)).toDF()
    val employees = dataFrame.filter($"salary" === 500).collect()

    assert(employees === Array(Row("User2", 500)))
  }

  test("dataFrame - sort") {
    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3", 1000)).toDF()
    val employees = dataFrame.sort($"salary".desc).collect()
    val naFunctions = dataFrame.na

    assert(employees === Array(Row("User3",1000), Row("User2", 500), Row("User1", 100)))
  }

  test("dataFrame - null replacing") {
    val dataFrame = Seq("aaa", "bbb", null, "ccc").toDF()
    val naFunctions = dataFrame.na
    val notNullDataFrame = naFunctions.fill("unknown").collect()

    assert(notNullDataFrame === Array(Row("aaa"), Row("bbb"), Row("unknown"), Row("ccc")))
  }

  test("dataFrame - null removing") {
    val dataFrame = Seq("aaa", "bbb", null, "ccc").toDF()
    val naFunctions = dataFrame.na
    val notNullDataFrame = naFunctions.drop().collect()

    assert(notNullDataFrame === Array(Row("aaa"), Row("bbb"), Row("ccc")))
  }

  test("dataFrame - select") {
    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3",1000)).toDF()
    val employees = dataFrame.select($"name").collect()

    assert(employees === Array(Row("User1"), Row("User2"), Row("User3")))
  }

  test("dataFrame - agg count") {
    val dataFrame = Seq(Employee("User1", 500), Employee("User2", 500), Employee("User3",1000)).toDF()
    val employees = dataFrame.groupBy($"salary").agg(count($"name") as "cnt").collect()

    assert(employees === Array(Row(500, 2), Row(1000, 1)))
  }

  test("dataFrame - agg sum1") {
    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3",1000)).toDF()
    val summ = dataFrame.agg("salary" -> "sum").collect()

    assert(summ === Array(Row(1600)))
  }

  test("dataFrame - agg sum2") {
    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3",1000)).toDF()
    val summ = dataFrame.agg(sum($"salary")).collect()

    assert(summ === Array(Row(1600)))
  }

  test("dataFrame - withColumn") {
    import org.scalatest.Matchers._

    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3",1000)).toDF()
    val doubleFunction: (Column) => Column = (x) => { x*2 }
    val dataFrameWithColumn = dataFrame.withColumn("doubled", doubleFunction($"salary")).collect()

    dataFrameWithColumn should contain theSameElementsAs Array(Row("User3", 1000, 2000), Row("User2", 500, 1000), Row("User1", 100, 200))
  }

  test("dataFrame - toJSON") {
    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3",1000)).toDF()
    val jsonDataset = dataFrame.toJSON.collect()

    assert(jsonDataset === Array(
      """{"name":"User1","salary":100}""",
      """{"name":"User2","salary":500}""",
      """{"name":"User3","salary":1000}"""))

  }
}
