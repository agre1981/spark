package com.pgs.spark.crimes

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Row, SQLContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

case class Employee(name: String, salary: Int)

/**
  * Created by ogrechanov on 5/5/2017.
  */
class SparkDataFrameSpec extends FunSuite with BeforeAndAfterAll {
  var sc : SparkContext =null
  var sqlContext: SQLContext = null

  override def beforeAll() {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("Spark app")
    sc = new SparkContext(sparkConf)
    sqlContext = new SQLContext(sc)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }

  test("dataFrame - sql") {
    val employees = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3",1000))
    val employeesDF = sqlContext.createDataFrame(employees)
    employeesDF.registerTempTable("employees")

    val filteredDF = sqlContext.sql("SELECT name FROM employees WHERE salary<1000")

    assert(filteredDF.collect() === Array(Row("User1"), Row("User2")))
  }

  test("dataFrame - filter") {
    val sql = sqlContext
    import sql.implicits._

    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3",1000)).toDF()
    val employees = dataFrame.filter($"salary" === 500).collect()

    assert(employees === Array(Row("User2", 500)))
  }

  test("dataFrame - sort") {
    val sql = sqlContext
    import sql.implicits._

    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3", 1000)).toDF()
    val employees = dataFrame.sort($"salary".desc).collect()
    val naFunctions = dataFrame.na

    assert(employees === Array(Row("User3",1000), Row("User2", 500), Row("User1", 100)))
  }

  test("dataFrame - select") {
    val sql = sqlContext
    import sql.implicits._

    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3",1000)).toDF()
    val employees = dataFrame.select($"name").collect()

    assert(employees === Array(Row("User1"), Row("User2"), Row("User3")))
  }

  test("dataFrame - agg count") {
    val sql = sqlContext
    import sql.implicits._

    val dataFrame = Seq(Employee("User1", 500), Employee("User2", 500), Employee("User3",1000)).toDF()
    val employees = dataFrame.groupBy($"salary").agg(count("name") as "cnt").collect()

    assert(employees === Array(Row(2), Row(1)))
  }

  test("dataFrame - agg sum1") {
    val sql = sqlContext
    import sql.implicits._

    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3",1000)).toDF()
    val summ = dataFrame.agg("salary" -> "sum").collect()

    assert(summ === Array(Row(1600)))
  }

  test("dataFrame - agg sum2") {
    val sql = sqlContext
    import sql.implicits._

    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3",1000)).toDF()
    val summ = dataFrame.agg(sum($"salary")).collect()

    assert(summ === Array(Row(1600)))
  }

  test("dataFrame - withColumn") {
    val sql = sqlContext
    import sql.implicits._
    import org.scalatest.Matchers._

    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3",1000)).toDF()
    val doubleFunction: (Column) => Column = (x) => { x*2 }
    val dataFrameWithColumn = dataFrame.withColumn("doubled", doubleFunction($"salary")).collect()

    dataFrameWithColumn should contain theSameElementsAs Array(Row("User3", 1000, 2000), Row("User2", 500, 1000), Row("User1", 100, 200))
  }

  test("dataFrame - toJSON") {
    val sql = sqlContext
    import sql.implicits._

    val dataFrame = Seq(Employee("User1", 100), Employee("User2", 500), Employee("User3",1000)).toDF()
    val jsonDataset = dataFrame.toJSON.collect()

    assert(jsonDataset === Array(
      """{"name":"User1","salary":100}""",
      """{"name":"User2","salary":500}""",
      """{"name":"User3","salary":1000}"""))
  }
}
