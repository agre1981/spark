package com.pgs.spark.crimes

import com.pgs.spark.crimes.model.CrimeModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.Map
import scala.collection.immutable.HashSet

/**
  * Created by ogrechanov on 4/26/2017.
  */
class SparkRDDSpec extends FunSuite with BeforeAndAfterAll {

  var sparkSession: SparkSession = null
  var sc : SparkContext =null

  override def beforeAll() {
    sparkSession = SparkSession.builder.appName("Spark app").master("local[4]").getOrCreate()
    sc = sparkSession.sparkContext
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    super.afterAll()
  }

  test("rdd creation - from array") {
    val rdd = sc.parallelize(Seq(1,3,7,9))

    assert(rdd.collect() === Seq(1,3,7,9))
  }

  test("rdd creation - from file") {
    val rdd = sc.textFile("src/main/resources/myfile.txt")

    assert(rdd.collect() === Seq("aaa bbb ccc", "ddd"))
  }

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

  test("rdd transformation - map int") {
    val rdd = sc.parallelize(Seq(1,3,7,9))
    val wordsRDD = rdd.map(i => i+1)

    assert(wordsRDD.collect() === Seq(2,4,8,10))
  }

  test("rdd transformation - map to string") {
    val rdd = sc.parallelize(Seq(1,3,7,9))
    val wordsRDD = rdd.map(i => "name_" + i.toString)

    assert(wordsRDD.collect() === Seq("name_1","name_3","name_7","name_9"))
  }

  test("rdd transformation - flatMap") {
    val rdd = sc.parallelize(Seq("aaa bbb", "ccc"))
    val wordsRDD = rdd.flatMap(str => str.split(" "))

    assert(wordsRDD.collect() === Seq("aaa", "bbb", "ccc"))
  }

  test("rdd transformation - distinct") {
    val rdd1 = sc.parallelize(Seq(1,1,3,7))
    val distinctRDD = rdd1.distinct()

    assert(distinctRDD.collect() === Seq(1,3,7))
  }

  test("rdd transformation - distinct pair") {
    import org.scalatest.Matchers._

    val rdd1 = sc.parallelize(Seq((1,1),(1,2),(3,7),(1,1)))
    val distinctRDD = rdd1.distinct()

    distinctRDD.collect() should contain theSameElementsAs  Seq((1,1),(1,2),(3,7))
  }

  test("rdd transformation - union") {
    val rdd1 = sc.parallelize(Seq(1,1,3,7))
    val rdd2 = sc.parallelize(Seq(1,5))
    val unionRDD = rdd1.union(rdd2)

    assert(unionRDD.collect() === Seq(1,1,3,7,1,5))
  }

  test("rdd transformation - union pair") {
    val rdd1 = sc.parallelize(Seq((1,1),(2,2)))
    val rdd2 = sc.parallelize(Seq((1,1)))
    val unionRDD = rdd1.union(rdd2)

    assert(unionRDD.collect() === Seq((1,1),(2,2),(1,1)))
  }

  test("rdd transformation - intersection") {
    val rdd1 = sc.parallelize(Seq(1,1,3,7))
    val rdd2 = sc.parallelize(Seq(1,1,5))
    val unionRDD = rdd1.intersection(rdd2)

    assert(unionRDD.collect() === Seq(1))
  }

  test("rdd transformation - subtract") {
    val rdd1 = sc.parallelize(Seq(1,1,3,7))
    val rdd2 = sc.parallelize(Seq(1,3))
    val unionRDD = rdd1.subtract(rdd2)

    assert(unionRDD.collect() === Seq(7))
  }

  test("rdd transformation - cartesian") {
    val rdd1 = sc.parallelize(Seq("A","B"))
    val rdd2 = sc.parallelize(Seq(1,2))

    val cartesianRDD = rdd1.cartesian(rdd2)

    assert(cartesianRDD.collect() === Seq(("A", 1),("A", 2),("B", 1),("B", 2)))
  }

  test("rdd transformation - subtractByKey") {
    val rdd1 = sc.parallelize(Seq(("aaa",1), ("aaa",2),("bbb",2),("ccc",3)))
    val rdd2 = sc.parallelize(Seq(("aaa",111), ("bbb",222), ("ddd", 444)))
    val wordCountRDD = rdd1.subtractByKey( rdd2 )

    assert(wordCountRDD.collect() === Seq(("ccc",3)))
  }

  test("rdd transformation - join") {
    val rdd1 = sc.parallelize(Seq(("aaa",1), ("aaa",2),("bbb",2),("ccc",3)))
    val rdd2 = sc.parallelize(Seq(("aaa",111), ("bbb",222)))
    val wordCountRDD = rdd1.join( rdd2 )

    assert(wordCountRDD.collect() === Seq(("aaa",(1,111)), ("aaa",(2,111)), ("bbb",(2,222))))
  }

  test("rdd transformation - leftOuterJoin") {
    val rdd1 = sc.parallelize(Seq(("aaa",1), ("aaa",2),("bbb",2),("ccc",3)))
    val rdd2 = sc.parallelize(Seq(("aaa",111), ("bbb",222)))
    val wordCountRDD = rdd1.leftOuterJoin( rdd2 )

    assert(wordCountRDD.collect() === Seq(("aaa",(1,Some(111))), ("aaa",(2,Some(111))), ("bbb",(2,Some(222))), ("ccc",(3,None))))
  }

  test("rdd transformation - rightOuterJoin") {
    val rdd1 = sc.parallelize(Seq(("aaa",1), ("aaa",2),("ccc",3)))
    val rdd2 = sc.parallelize(Seq(("aaa",111), ("bbb",222)))
    val wordCountRDD = rdd1.rightOuterJoin( rdd2 )

    assert(wordCountRDD.collect() === Seq(("aaa",(Some(1),111)), ("aaa",(Some(2),111)), ("bbb",(None,222))))
  }

  test("rdd transformation - fullOuterJoin") {
    val rdd1 = sc.parallelize(Seq(("aaa",1), ("aaa",2),("ccc",3)))
    val rdd2 = sc.parallelize(Seq(("aaa",111), ("bbb",222)))
    val wordCountRDD = rdd1.fullOuterJoin( rdd2 )

    assert(wordCountRDD.collect() === Seq(("aaa",(Some(1),Some(111))), ("aaa",(Some(2),Some(111))),
      ("bbb",(None,Some(222))), ("ccc",(Some(3),None))))
  }

  test("rdd transformation - cogroup") {
    val rdd1 = sc.parallelize(Seq(("aaa",1), ("aaa",2),("ccc",3)))
    val rdd2 = sc.parallelize(Seq(("aaa",111), ("bbb",222)))
    val wordCountRDD = rdd1.cogroup( rdd2 )

    assert(wordCountRDD.collect() === Seq(("aaa",(Seq(1,2),Seq(111))), ("bbb",(Seq(),Seq(222))), ("ccc",(Seq(3),Seq()))))
  }

  test("rdd pair transformation - aggregateByKey - int") {
    val rdd = sc.parallelize(Seq(("aaa",1), ("aaa",2),("bbb",2),("ccc",3)))
    val aggregateRDD = rdd.aggregateByKey(0)(_+_, _+_)

    assert(aggregateRDD.collect() === Seq(("aaa",3), ("bbb",2),("ccc",3)))
  }

  test("rdd pair transformation - aggregateByKey - Set") {
    val rdd = sc.parallelize(Seq(("aaa",1), ("aaa",2),("bbb",2),("ccc",3)))
    val aggregateRDD = rdd.aggregateByKey(HashSet.empty[Int])(_ + _, _ ++ _)

    assert(aggregateRDD.collect() === Array(("aaa",Set(1,2)), ("bbb",Set(2)),("ccc",Set(3))))
  }

  test("rdd pair transformation - reduceByKey") {
    val rdd = sc.parallelize(Seq(("aaa",1), ("aaa",2),("bbb",2),("ccc",3)))
    val wordCountRDD = rdd.reduceByKey( _ + _ )

    assert(wordCountRDD.collect() === Seq(("aaa",3), ("bbb",2),("ccc",3)))
  }

  test("rdd pair transformation - foldByKey") {
    val rdd = sc.parallelize(Seq(("aaa",1), ("aaa",2),("bbb",2),("ccc",3)))
    val wordCountRDD = rdd.foldByKey(0)( _ + _ )

    assert(wordCountRDD.collect() === Seq(("aaa",3), ("bbb",2),("ccc",3)))
  }

  test("rdd pair transformation - groupByKey") {
    val rdd = sc.parallelize(Seq(("aaa",1), ("aaa",2),("bbb",2),("ccc",3)))
    val groupedRDD = rdd.groupByKey()

    assert(groupedRDD.collect() === Seq(("aaa",Seq(1,2)), ("bbb",Seq(2)),("ccc",Seq(3))))
  }

  test("rdd pair transformation - combineByKey - avg") {
    val rdd = sc.parallelize(Seq(("aaa",1),("bbb",2),("bbb",2),("aaa",3)))

    val sumAndCountRDD = rdd.combineByKey( (x: Int) => (x, 1),
      (pair: (Int, Int), x: Int) => (pair._1+x, pair._2+1),
      (pair1: (Int, Int), pair2: (Int, Int)) => (pair1._1 + pair2._1, pair1._2 + pair2._2) )

    val avgRDD = sumAndCountRDD.map{case (key, (sum, count)) => (key, sum.toDouble/count)}

    assert(avgRDD.collect() === Seq(("aaa",2), ("bbb",2)))
  }

  test("rdd pair transformation - mapValues") {
    val rdd = sc.parallelize(Seq(("aaa",1), ("aaa",2),("bbb",2),("ccc",3)))
    val mapedValuesRDD =  rdd.mapValues(x => x+1)

    assert(mapedValuesRDD.collect() === Seq(("aaa",2), ("aaa",3),("bbb",3),("ccc",4)))
  }

  test("rdd pair transformation - flatMapValues") {
    val rdd = sc.parallelize(Seq(("aaa",1), ("bbb",5)))
    val mapedValuesRDD =  rdd.flatMapValues(x => Seq(x,x+1))

    assert(mapedValuesRDD.collect() === Seq(("aaa",1), ("aaa",2),("bbb",5), ("bbb",6)))
  }

  test("rdd pair transformation - keys") {
    val rdd = sc.parallelize(Seq(("aaa",1), ("bbb",5), ("bbb",4)))
    val keysRDD =  rdd.keys

    assert(keysRDD.collect() === Seq("aaa", "bbb", "bbb"))
  }

  test("rdd pair transformation - values") {
    val rdd = sc.parallelize(Seq(("aaa",1), ("bbb",5), ("bbb",4)))
    val valuesRDD =  rdd.values

    assert(valuesRDD.collect() === Seq(1, 5, 4))
  }

  test("rdd pair transformation - sortByKey desc") {
    val rdd = sc.parallelize(Seq(("ccc",3), ("aaa",1),("bbb",2), ("aaa",2)))
    val sortedRDD = rdd.sortByKey(false)

    assert(sortedRDD.collect() === Seq(("ccc",3), ("bbb",2),("aaa",1),("aaa",2)))
  }

  test("rdd transformation - sortByKey customSort desc int.toString") {
    val rdd = sc.parallelize(Seq((2,3), (5,3),(4,2), (12,2)))

    implicit val sortIntegersByString = new Ordering[Int] {
      override def compare(a: Int, b: Int) = a.toString.compare(b.toString)
    }

    val sortedRDD = rdd.sortByKey(ascending = false)

    assert(sortedRDD.collect() === Seq((5,3),(4,2),(2,3),(12,2)))
  }

  test("rdd transformation - sortBy desc pair") {
    val rdd = sc.parallelize(Seq(("ccc",3), ("aaa",1),("bbb",2), ("aaa",2)))
    val sortedRDD = rdd.sortBy(_._1, ascending=false)

    assert(sortedRDD.collect() === Seq(("ccc",3), ("bbb",2),("aaa",1),("aaa",2)))
  }

  test("rdd transformation - sortBy desc int.toString") {
    val rdd = sc.parallelize(Seq(3,2,16))
    val sortedRDD = rdd.sortBy(_.toString, ascending=false)

    assert(sortedRDD.collect() === Seq(3,2,16))
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

  test("rdd action - top") {
    val vals = Array(9, 3, 7, 1)
    val rdd = sc.parallelize(vals)
    val topElements = rdd.top(2)

    assert(topElements === Array(9,7) )
  }

  test("rdd action - top custom ordering") {
    val vals = Array(9, 3, 7, 1)
    val rdd = sc.parallelize(vals)
    val topElements = rdd.top(2)(Ordering[Int].on(x => -x))

    assert(topElements === Array(1,3) )
  }

  test("rdd action - top pair") {
    val vals = Array((1,1), (2,1), (3,3), (2,2))
    val rdd = sc.parallelize(vals)
    val topElements = rdd.top(3)

    assert(topElements === Array((3,3),(2,2),(2,1)) )
  }

  test("rdd action - takeOrdered") {
    val rdd = sc.parallelize(Array(9, 3, 7, 1))

    val sortedArray = rdd.takeOrdered(3)( Ordering[Int].on( x => x) )

    assert(sortedArray === Array(1,3,7))
  }

  test("rdd action - count") {
    val rdd = sc.parallelize(Seq(1,3,7,9))

    val count = rdd.count()

    assert(count === 4)
  }

  test("rdd action - sum") {
    val rdd = sc.parallelize(Seq(1,3,7,9))

    val count = rdd.sum()

    assert(count === 20)
  }

  test("rdd action - mean") {
    val rdd = sc.parallelize(Seq(1,3,7,9))

    val count = rdd.mean() // avg

    assert(count === 5)
  }

  test("rdd action - countByValue") {
    val rdd = sc.parallelize(Seq(1,1,3,7,9))

    val countRDD: Map[Int, Long] = rdd.countByValue()

    assert(countRDD === Map(1->2, 3->1, 7->1, 9->1))
  }

  test("rdd action - countByKey") {
    val rdd = sc.parallelize(Seq((1, 2), (3, 4), (3, 6)))

    val countRDD = rdd.countByKey()

    assert(countRDD === Map(1->1, 3->2))
  }

  test("rdd action - lookup") {
    val rdd = sc.parallelize(Seq((1, 2), (3, 4), (3, 6)))

    val countRDD = rdd.lookup(3)

    assert(countRDD === Seq(4,6))
  }

  test("rdd action - collectAsMap") {
    val rdd = sc.parallelize(Seq((1, 2), (2, 4), (3, 6)))

    val countRDD = rdd.collectAsMap()

    assert(countRDD === Map(1->2, 2->4, 3->6))
  }

  test("rdd action - reduce") {
    val rdd = sc.parallelize( Seq(1,3,7,9) )

    val sum = rdd.reduce((x,y) => x + y)

    assert(sum === 20)
  }

  test("rdd action - fold") {
    val rdd = sc.parallelize( Seq(1,3,7,9) )

    val sum = rdd.fold(0)((x,y) => x + y)

    assert(sum === 20)
  }

  test("rdd action - aggregate sum") {
    val rdd = sc.parallelize( Seq(1,3,7,9) )

    val sum = rdd.aggregate(0)((x,y) => x + y, (x,y) => x + y)

    assert(sum === 20)
  }

  test("rdd action - aggregate avg") {
    val rdd = sc.parallelize( Seq(1,3,7,9) )

    val sumAndCountPair = rdd.aggregate((0, 0))(
      (pair,y) => (pair._1 + y, pair._2 + 1),
      (pair1,pair2) => (pair1._1 + pair2._1, pair1._2 + pair2._2))

    val avg = sumAndCountPair._1.toDouble / sumAndCountPair._2

    assert(avg === 5)
  }

  test("rdd action - foreach") {
    val accum = sc.accumulator(0)
    val rdd = sc.parallelize( Seq(1,3,7,9) )

    rdd.foreach(i => accum.add(i))

    assert(accum.value === 20)
  }

  test("rdd - broadcast variables") {
    val broadcastVar = sc.broadcast(Array(1, 2, 3))
    val rdd = sc.parallelize(Seq(1,3,5))
    val filteredRDD = rdd.filter(broadcastVar.value.contains(_))

    assert(filteredRDD.collect() === Seq(1,3))
  }

  test("rdd - longAccumulator variables - sum") {
    val accum = sc.accumulator(0)
    val rdd = sc.parallelize(Seq(1,3,5))
    rdd.foreach(accum.add(_))

    assert(accum.value === 9)
  }

  test("rdd transformation - mapPartitions") {
    val rdd = sc.parallelize(Seq(1,3,5))
    val addedRDD = rdd.mapPartitions( list => list.map(_+1) )

    assert(addedRDD.collect() === (Array(2,4,6)))
  }

  test("rdd transformation - mapPartitions - fastAvg") {
    val rdd = sc.parallelize(Seq(1,3,5))
    val sumAndCountRDD = rdd.mapPartitions[(Int,Int)]( list => {
      val arr = Array(0, 0)
      list.foreach(i => {arr(0) += i; arr(1) += 1;})
      Iterator((arr(0), arr(1)))
    })
    val sumAndCount = sumAndCountRDD.reduce( (x,y) => (x._1+y._1, x._2+y._2 ) )

    val avg = sumAndCount._1 / sumAndCount._2

    assert(avg === 3)
  }

  test("rdd transformation - mapPartitionsWithIndex") {
    val rdd = sc.parallelize(Seq(1,3,5))
    val addedRDD = rdd.mapPartitionsWithIndex( (index, list)=> list.map(_+1))

    assert(addedRDD.collect() === (Array(2,4,6)))
  }

  test("rdd action - foreachPartition") {
    val accum = sc.accumulator(0)
    val rdd = sc.parallelize(Seq(1,3,5))

    rdd.foreachPartition( list => list.foreach( i => accum += i ))

    assert(accum.value === 9)
  }

  test("rdd action - foreach - data modification in partition") {
    val rdd = sc.parallelize(Seq(CrimeModel(1,"desc1",false), CrimeModel(2,"desc2",false), CrimeModel(3,"desc3",false)), 2)

    rdd.foreach(m => {
      m.description = "updated"
      //println("in RDD: " + m.description)
    })

    //rdd.foreach(println)
    // modified data does not affect other partitions and driver !!!
    assert(rdd.collect() === Seq(CrimeModel(1,"desc1",false), CrimeModel(2,"desc2",false), CrimeModel(3,"desc3",false)))
  }
}
