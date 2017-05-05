package com.pgs.spark.crimes

import com.pgs.spark.crimes.model.CrimeModel
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
/**
  * Created by ogrechanov on 4/19/2017.
  */
object SparkCrimesStreaming extends App {

  def findArrestsStringStream(crimes: DStream[String]) : DStream[CrimeModel] = {
    val crimesModelStream = crimes.map(l=> {
      val arr=l.split(",")
      CrimeModel(arr(0).toLong, arr(7), arr(8).toBoolean)
    })
    findArrestsModelStream(crimesModelStream)
  }

  def findArrestsModelStream(crimes: DStream[CrimeModel]) : DStream[CrimeModel] = {
    val filteredStream = crimes.filter(crime => crime.arrested == true)
    filteredStream
  }

  val conf = new SparkConf().setMaster("local[4]").setAppName("spark-crimes-streaming")
  val ssc = new StreamingContext(conf, Seconds(1))
  val crimesStream: DStream[String] = ssc.socketTextStream("localhost", 9999)

  findArrestsStringStream(crimesStream)

  ssc.start()
  ssc.awaitTermination()
}
