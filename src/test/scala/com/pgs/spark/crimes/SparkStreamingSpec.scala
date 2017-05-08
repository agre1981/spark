package com.pgs.spark.crimes

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FunSuite

/**
  * Created by ogrechanov on 4/25/2017.
  */
class SparkStreamingSpec extends FunSuite with StreamingSuiteBase {
  test("word count - flatMap") {

    val input = Seq(
      Seq("aaa aaa", "aaa"),
      Seq("bbb", "ccc"),
      Seq("aaa")
    )

    val output = Seq(
      Seq("aaa", "aaa", "aaa"),
      Seq("bbb", "ccc"),
      Seq("aaa")
    )

    val operation = (lines: DStream[String]) => {
      val words = lines.flatMap(_.split(" "))
      words
    }

    testOperation(input, operation , output)
  }

  test("word count - map") {

    val input = Seq(
      Seq("aaa aaa", "aaa"),
      Seq("bbb", "ccc"),
      Seq("aaa")
    )

    val output = Seq(
      Seq(("aaa", 1), ("aaa", 1), ("aaa", 1)),
      Seq(("bbb", 1), ("ccc", 1)),
      Seq(("aaa", 1))
    )

    val operation = (lines: DStream[String]) => {
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      pairs
    }

    testOperation(input, operation , output)
  }

  test("word count - reduceByKey") {

    val input = Seq(
      Seq("aaa aaa", "aaa"),
      Seq("bbb", "ccc"),
      Seq("aaa")
    )

    val output = Seq(
      Seq(("aaa", 3)),
      Seq(("bbb", 1), ("ccc", 1)),
      Seq(("aaa", 1))
    )

    val operation = (lines: DStream[String]) => {
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)
      wordCounts
    }

    testOperation(input, operation , output)
  }

  test("word count - stream join") {

    val input1 = Seq(
      Seq("aaa aaa", "aaa"),
      Seq("bbb", "ccc"),
      Seq("aaa")
    )
    val input2 = Seq(
      Seq("aaa"),
      Seq("bbb bbb", "ccc ccc ccc"),
      Seq("ddd")
    )

    val output = Seq(
      Seq(("aaa", (3, 1))),
      Seq(("bbb", (1, 2)), ("ccc", (1, 3))),
      Seq()
    )

    val operation = (lines: DStream[String], lines2: DStream[String]) => {
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)
      val wordCounts2 = lines2.flatMap(_.split(" ")).map(w=>(w, 1)).reduceByKey(_ + _)
      val joinedCounts = wordCounts.join(wordCounts2)

      //joinedCounts.print()
      joinedCounts
    }
    testOperation(input1, input2, operation , output)
  }

  test("word count - transformation join") {

    val input = Seq(
      Seq("aaa aaa", "aaa"),
      Seq("bbb", "ccc"),
      Seq("aaa")
    )

    val output = Seq(
      Seq(("aaa", (3,8))),
      Seq(("bbb", (1,9))),
      Seq(("aaa", (1,8)))
    )

    val operation = (lines: DStream[String]) => {
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)

      val dataset = sc.parallelize(Seq(("aaa",8), ("bbb",9)))
      val joinedDataset = wordCounts.transform(rdd => rdd.join(dataset))
      //joinedDataset.print()
      joinedDataset
    }

    testOperation(input, operation , output)
  }

  test("word count - transformation fullOuterJoin") {

    val input = Seq(
      Seq("aaa aaa", "aaa"),
      Seq("bbb", "ccc"),
      Seq("aaa")
    )

    val output = Seq(
      Seq(("aaa", (Some(3),Some(8))), ("bbb", (None,Some(9)))),
      Seq(("aaa",(None,Some(8))), ("bbb", (Some(1),Some(9))), ("ccc", (Some(1),None))),
      Seq(("aaa", (Some(1),Some(8))), ("bbb", (None,Some(9))))
    )

    val operation = (lines: DStream[String]) => {
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)

      val dataset = sc.parallelize(Seq(("aaa",8), ("bbb",9)))
      val joinedDataset = wordCounts.transform(rdd => rdd.fullOuterJoin(dataset))
      //joinedDataset.print()
      joinedDataset
    }

    testOperation(input, operation , output)
  }

  test("word count - transformation rightOuterJoin") {

    val input = Seq(
      Seq("aaa aaa", "aaa"),
      Seq("bbb", "ccc"),
      Seq("aaa")
    )

    val output = Seq(
      Seq(("aaa", (Some(3),8)), ("bbb", (None,9))),
      Seq(("aaa", (None,8)), ("bbb", (Some(1),9))),
      Seq(("aaa", (Some(1),8)), ("bbb", (None,9)))
    )

    val operation = (lines: DStream[String]) => {
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)

      val dataset = sc.parallelize(Seq(("aaa",8), ("bbb",9)))
      val joinedDataset = wordCounts.transform(rdd => rdd.rightOuterJoin(dataset))
      //joinedDataset.print()
      joinedDataset
    }

    testOperation(input, operation , output)
  }

  test("word count - transformation leftOuterJoin") {

    val dataset = sc.parallelize(Seq(("bbb",9)))
    val input = Seq(
      Seq("aaa aaa", "aaa"),
      Seq("bbb", "ccc"),
      Seq("aaa")
    )

    val output = Seq(
      Seq(("aaa", (3,None))),
      Seq(("bbb", (1,Some(9))), ("ccc", (1,None))),
      Seq(("aaa", (1,None)))
    )

    val operation = (lines: DStream[String]) => {
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)
      val joinedDataset = wordCounts.transform(rdd => rdd.leftOuterJoin(dataset))
      //joinedDataset.print()
      joinedDataset
    }

    testOperation(input, operation , output)
  }

  test("word count - transformation double join") {

    val input = Seq(
      Seq("aaa aaa", "aaa"),
      Seq("bbb", "ccc"),
      Seq("aaa")
    )

    val output = Seq(
      Seq(("aaa", ((3,8),11))),
      Seq(("bbb", ((1,9),12))),
      Seq(("aaa", ((1,8),11)))
    )

    val operation = (lines: DStream[String]) => {
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)

      val dataset = sc.parallelize(Seq(("aaa",8), ("bbb",9)))
      val dataset2 = sc.parallelize(Seq(("aaa",11), ("bbb",12)))
      val joinedDataset = wordCounts.transform(rdd => rdd.join(dataset).join(dataset2))
      //joinedDataset.print()
      joinedDataset
    }

    testOperation(input, operation , output)
  }

  test("word count - updateStateByKey") {

    val input = Seq(
      Seq("aaa aaa", "aaa"),
      Seq("bbb", "ccc"),
      Seq("aaa")
    )

    val output = Seq(
      Seq(("aaa", 3)),
      Seq(("aaa", 3), ("bbb", 1), ("ccc", 1)),
      Seq(("aaa", 4), ("bbb", 1), ("ccc", 1))
    )

    def updateFunction(newValues: Seq[(Int)], runningCount: Option[(Int)]): Option[(Int)] = {
      Some(runningCount.getOrElse(0) + newValues.sum)
    }

    val operation = (lines: DStream[String]) => {
      val words = lines.flatMap(_.split(" "))
      val pairs = words.map(word => (word, 1))
      val wordCounts = pairs.reduceByKey(_ + _)
      val updatedCounts = wordCounts.updateStateByKey( updateFunction )
      //updatedCounts.print()
      updatedCounts
    }

    testOperation(input, operation , output)
  }
}
