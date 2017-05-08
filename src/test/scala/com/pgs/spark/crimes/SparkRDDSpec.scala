package com.pgs.spark.crimes

import com.holdenkarau.spark.testing.SharedSparkContext
import com.pgs.spark.crimes.model.CrimeModel
import org.scalatest.FunSuite

import scala.collection.immutable.HashSet

/**
  * Created by ogrechanov on 4/26/2017.
  */
class SparkRDDSpec extends FunSuite with SharedSparkContext {

  test("rdd transformation - filter") {
    val rdd = sc.parallelize(Seq(1,3,7,9))
    val aboveFiveRDD = rdd.filter( _ > 5)

    assert(aboveFiveRDD.collect() === Seq(7,9))
  }

  test("rdd transformation - filter complex") {
    val rdd = sc.parallelize(Seq((1,9),(3,8),(6,7)))
    val aboveFiveRDD = rdd.filter{case (id, _) => id > 5}

    assert(aboveFiveRDD.collect() === Seq((6,7)))
  }

  test("rdd transformation - filter complex object") {
    val rdd = sc.parallelize(Seq(CrimeModel(1,"crime1",false), CrimeModel(2,"crime2",false), CrimeModel(3,"crime3",false)))
    val aboveFiveRDD = rdd.filter{case CrimeModel(id, desc, arrested) => id > 2}

    assert(aboveFiveRDD.collect() === Seq(CrimeModel(3,"crime3",false)))
  }

  test("rdd transformation - reduceByKey") {
    val rdd = sc.parallelize(Seq(("aaa",1), ("aaa",2),("bbb",2),("ccc",3)))
    val wordCountRDD = rdd.reduceByKey(_+_)

    assert(wordCountRDD.collect() === Seq(("aaa",3), ("bbb",2),("ccc",3)))
  }

  test("rdd transformation - sortByKey desc") {
    val rdd = sc.parallelize(Seq(("ccc",3), ("aaa",1),("bbb",2), ("aaa",2)))
    val sortedRDD = rdd.sortByKey(false)

    assert(sortedRDD.collect() === Seq(("ccc",3), ("bbb",2),("aaa",1),("aaa",2)))
  }

  test("rdd transformation - sortBy desc") {
    val rdd = sc.parallelize(Seq(("ccc",3), ("aaa",1),("bbb",2), ("aaa",2)))
    val sortedRDD = rdd.sortBy(_._1, ascending=false)

    assert(sortedRDD.collect() === Seq(("ccc",3), ("bbb",2),("aaa",1),("aaa",2)))
  }

  test("rdd transformation - aggregateByKey - int") {
    val rdd = sc.parallelize(Seq(("aaa",1), ("aaa",2),("bbb",2),("ccc",3)))
    val aggregateRDD = rdd.aggregateByKey(0)(_+_, _+_)

    assert(aggregateRDD.collect() === Seq(("aaa",3), ("bbb",2),("ccc",3)))
  }

  test("rdd transformation - aggregateByKey - Set") {
    val rdd = sc.parallelize(Seq(("aaa",1), ("aaa",2),("bbb",2),("ccc",3)))
    val aggregateRDD = rdd.aggregateByKey(new HashSet[Int])(_ + _, _ ++ _)

    assert(aggregateRDD.collect() === Array(("aaa",Set(1,2)), ("bbb",Set(2)),("ccc",Set(3))))
  }

  test("rdd transformation - cartesian") {
    val rdd1 = sc.parallelize(Seq("A","B"))
    val rdd2 = sc.parallelize(Seq(1,2))
    val cartesianRDD = rdd1.cartesian(rdd2)

    assert(cartesianRDD.collect() === Seq(("A", 1),("A", 2),("B", 1),("B", 2)))
  }

  test("rdd action - collect") {
    val rdd = sc.parallelize(Seq(1,3,7,9))
    val aboveFiveRDD = rdd.filter( _ > 5)

    assert(aboveFiveRDD.collect() === Seq(7,9))
  }

  test("rdd action - first") {
    val rdd = sc.parallelize(Seq(1,3,7,9))
    val first = rdd.first()

    assert(first === 1)
  }

  test("rdd action - take") {
    val rdd = sc.parallelize(Seq(1,3,7,9))
    val firstThree = rdd.take(3)

    assert(firstThree === Seq(1,3,7))
  }

  test("rdd action - takeSample") {
    val vals = Array(1, 3, 7, 9)
    val rdd = sc.parallelize(vals)
    val randomThree = rdd.takeSample(false, 3)

    assert(randomThree.length === 3)
    assert(vals.intersect(randomThree).length === 3 )
  }

  test("rdd action - takeOrdered") {
    val vals = Array(9, 3, 7, 1)
    val rdd = sc.parallelize(vals)
    val sortedArray = rdd.takeOrdered(3)( Ordering[Int].on(x=>x) )

    assert(sortedArray === Array(1,3,7))
  }

  test("rdd action - count") {
    val rdd = sc.parallelize(Seq(1,3,7,9))
    val count = rdd.count()

    assert(count === 4)
  }

  test("rdd action - reduce") {
    val rdd = sc.parallelize(Seq(1,3,7,9))
    val sum = rdd.reduce(_ + _)

    assert(sum === 20)
  }

  test("rdd - broadcast variables") {
    val broadcastVar = sc.broadcast(Array(1, 2, 3))
    val rdd = sc.parallelize(Seq(1,3,5))
    val filteredRDD = rdd.filter(broadcastVar.value.contains(_))

    assert(filteredRDD.collect() === Seq(1,3))
  }

  test("rdd - longAccumulator variables - sum") {
    val accum = sc.longAccumulator("My Accumulator")
    val rdd = sc.parallelize(Seq(1,3,5))
    rdd.foreach(accum.add(_))

    assert(accum.value === 9)
  }

  test("rdd - longAccumulator variables - avg") {
    val accum = sc.longAccumulator("My Accumulator")
    val rdd = sc.parallelize(Seq(1,3,5))
    rdd.foreach(accum.add(_))

    assert(accum.avg === 3)
  }

  test("rdd - longAccumulator variables - merge") {
    val accum1 = sc.longAccumulator("My Accumulator1")
    val accum2 = sc.longAccumulator("My Accumulator2")
    val rdd1 = sc.parallelize(Seq(1,3,5))
    val rdd2 = sc.parallelize(Seq(1,10))
    rdd1.foreach(accum1.add(_))
    rdd2.foreach(accum2.add(_))

    accum1.merge(accum2)

    assert(accum1.value === 20)
  }
}
