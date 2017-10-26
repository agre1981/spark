package com.pgs.spark.crimes

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.Matchers._

/**
  * Created by ogrechanov on 4/27/2017.
  */
class SparkGraphXSpec extends FunSuite with BeforeAndAfterAll {

  var sparkSession: SparkSession = null
  var sc : SparkContext=null

  override def beforeAll() {
    sparkSession = SparkSession.builder.appName("Spark app").master("local[4]").getOrCreate()
    sc = sparkSession.sparkContext
    super.beforeAll()

    users = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // Create an RDD for edges
    relationships = sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // Build the initial Graph
    graph = Graph(users, relationships, defaultUser)
  }

  override def afterAll(): Unit = {
    sparkSession.stop()
    users = null
    relationships = null
    graph = null
    super.afterAll()
  }

  val defaultUser = ("John Doe", "Missing")
  var users: RDD[(VertexId, (String, String))] = null
  var relationships: RDD[Edge[String]] = null
  var graph: Graph[(String, String), String] = null

  test("graphX - inDegrees") {
    val inDegrees = graph.inDegrees
    assert(inDegrees.collect() === Array((5,1), (3,1), (7,2)))
  }

  test("graphX - inDegrees max") {
    val inDegreesMax = graph.inDegrees.reduce((a, b) => if (a._2 > b._2) a else b)
    assert(inDegreesMax._1 === 7)
    assert(inDegreesMax._2 === 2)
  }

  test("graphX - outDegrees") {
    val outDegrees = graph.outDegrees
    assert(outDegrees.collect() === Array((5,2), (2,1), (3,1)))
  }

  test("graphX - vertices find ") {
    val rxins = graph.vertices.filter(_._1 == 3)
    val rxin = rxins.first()
    assert(rxin._1 === 3)
  }

  test("graphX - vertices find complex") {
    val postdocs = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }
    assert(postdocs.count() === 1)
  }

  test("graphX - vertices top order by id ") {
    val rxins = graph.vertices.top(2) { Ordering.by((entry: (VertexId, (String, String))) => entry._1).reverse }
    assert(rxins === Array((2L, ("istoica", "prof")), (3,("rxin","student"))))
  }

  test("graphX - edges find ") {
    val aboveEdges = graph.edges.filter(e => e.srcId > e.dstId)
    assert(aboveEdges.count() === 1)
  }

  test("graphX - edges find complex") {
    val aboveEdges = graph.edges.filter { case Edge(src, dst, prop) => src > dst }
    assert(aboveEdges.count() === 1)
  }

  test("graphX - triplets filter") {
    val facts: RDD[EdgeTriplet[(String, String), String]] = graph.triplets.filter(triplet => triplet.attr == "advisor")
    assert(facts.count() === 1)
    assert(facts.first().srcAttr === ("franklin", "prof"))
    assert(facts.first().dstAttr === ("rxin", "student"))
  }

  test("graphX - triplets map") {
    val facts: RDD[String] = graph.triplets.map(triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    assert(facts.first() === "rxin is the collab of jgonzal")
  }

  test("graphX - mapVertices") {
    val newgraph: Graph[(String, String), String] = graph.mapVertices{case (id, attr) => (attr._1, attr._2 + "_new")}

    newgraph.vertices.collect() should contain theSameElementsAs Array(
      (3L, ("rxin", "student_new")), (7L, ("jgonzal", "postdoc_new")),
      (5L, ("franklin", "prof_new")), (2L, ("istoica", "prof_new")))
  }

  test("graphX - subgraph vpred") {
    val subgraph: Graph[(String, String), String] = graph.subgraph(vpred = (id, attr) => attr._2 == "prof")

    subgraph.vertices.collect() should contain theSameElementsAs Array(
      (2,("istoica", "prof")), (5L, ("franklin", "prof")))
  }

  test("graphX - subgraph epred") {
    val subgraph: Graph[(String, String), String] = graph.subgraph(epred = (triple) => triple.attr == "advisor")

    subgraph.edges.collect() should contain theSameElementsAs Array(
      Edge(5L, 3L, "advisor"))
  }

  test ("graphX - aggregateMessages") {
    val users = sc.parallelize(Array((1L, 17), (2L, 19), (3L, 27), (4L, 13), (5L, 25),(6L, 32),(7L, 35),(8L, 29),(9L,13)))
    val follows = sc.parallelize(Array(Edge(1L, 3L,"follow"), Edge(2L, 3L,"follow"), Edge(2L, 4L,"follow"), Edge(4L, 5L,"follow"), Edge(4L, 7L,"follow"),
      Edge(5L, 2L,"follow"), Edge(6L, 7L,"follow"), Edge(6L, 4L,"follow"), Edge(9L, 8L,"follow"), Edge(9L, 1L,"follow"), Edge(9L, 3L,"follow")))
    val graph = Graph(users, follows)

    val followers: VertexRDD[(Int, Int)] = graph.aggregateMessages[(Int, Int)](
      triplet => { // Map Function
        if (triplet.srcAttr < 20) {
          // Send message to destination vertex containing counter and age
          triplet.sendToDst(1, triplet.srcAttr)
        }
      },
      // Add counter and age
      (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
    )
    // Divide total age by number of older followers to get average age of older followers
    val avgAgeOfFollowers = followers.mapValues( (id, value) =>
        value match { case (count, totalAge) => totalAge/count } )
    // Display the results
    //avgAgeOfFollowers.collect.foreach(println(_))
    avgAgeOfFollowers.collect should contain theSameElementsAs Array((4,19),(8,13),(1,13),(5,13),(3,16),(7,13))
  }

  test("graphX - pregel") {
    // A graph with edge attributes containing distances
    val users = sc.parallelize(Seq((1L, "aaa"), (2L, "bbb"), (3L, "ccc"), (4L, "ddd"), (5L, "eee"), (6L, "fff"), (7L, "ggg")))

    val followers = Array( (2,1),(4,1),(1,2),(6,3),(7,3),(7,6),(6,7),(3,7) )
    val edges = sc.parallelize(followers.map(f => Edge(f._1, f._2, Double.PositiveInfinity)))

    val graph = Graph(users, edges)

    val sourceId: VertexId = 3 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)

    //println(initialGraph.vertices.collect.mkString("\n"))
    //println("---------\n")
    //println(initialGraph.edges.collect.mkString("\n"))

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => {
        math.min(dist, newDist) // Vertex Program
      },
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => {
        math.min(a, b) // Merge Message
      }
    )
    //println(sssp.vertices.collect.mkString("\n"))
  }

  test("graphX - pageRank") {
    val users = sc.parallelize(Seq((1L, "aaa"), (2L, "bbb"), (3L, "ccc"), (4L, "ddd"), (5L, "eee"), (6L, "fff"), (7L, "ggg")))

    val followers = Array( (2,1),(4,1),(1,2),(6,3),(7,3),(7,6),(6,7),(3,7) )
    val edges = sc.parallelize(followers.map(f => Edge(f._1, f._2, "follower")))

    val graph = Graph(users, edges)

    val ranks = graph.pageRank(0.0001).vertices

    val ranksByUsername = users.join(ranks)
      .map {case (id, (username, rank)) => (username, rank)}
        .sortBy(_._2, false).collect()

    //println(ranksByUsername.mkString("\n"))
    ranksByUsername(0)._1 shouldEqual "aaa"
    ranksByUsername(0)._2 should be (1.66 +- 0.01)

    ranksByUsername(1)._1 shouldEqual "bbb"
    ranksByUsername(1)._2 should be (1.58 +- 0.01)

    ranksByUsername(2)._1 shouldEqual "ggg"
    ranksByUsername(2)._2 should be (1.47 +- 0.01)

    ranksByUsername(3)._1 shouldEqual "ccc"
    ranksByUsername(3)._2 should be (1.13 +- 0.01)

    ranksByUsername(4)._1 shouldEqual "fff"
    ranksByUsername(4)._2 should be (0.79 +- 0.01)

    ranksByUsername(5)._1 shouldEqual "ddd"
    ranksByUsername(5)._2 should be (0.17 +- 0.01)

    ranksByUsername(6)._1 shouldEqual "eee"
    ranksByUsername(6)._2 should be (0.17 +- 0.01)
  }

  test("graphX - connectedComponents") {
    val users = sc.parallelize(Seq((1L, "aaa"), (2L, "bbb"), (3L, "ccc"), (4L, "ddd"), (5L, "eee"), (6L, "fff"), (7L, "ggg")))

    val followers = Array((2, 1), (4, 1), (1, 2), (6, 3), (7, 3), (7, 6), (6, 7), (3, 7))
    val edges = sc.parallelize(followers.map(f => Edge(f._1, f._2, "follower")))

    val graph = Graph(users, edges)

    val connections = graph.connectedComponents().vertices

    val connectionsByUsername = users.join(connections)
      .map { case (id, (username, connections)) => (username, connections) }
      .sortBy(_._2, false).collect()

    //println(connectionsByUsername.mkString("\n"))
    connectionsByUsername should contain theSameElementsAs Array(("eee",5),("fff",3),("ccc",3),("ggg",3),("ddd",1),("bbb",1),("aaa",1))
  }
}
