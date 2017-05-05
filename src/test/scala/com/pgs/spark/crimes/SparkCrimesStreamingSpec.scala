package com.pgs.spark.crimes

import com.holdenkarau.spark.testing.StreamingSuiteBase
import com.pgs.spark.crimes.model.CrimeModel
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FunSuite

/**
  * Created by ogrechanov on 4/19/2017.
  */
class SparkCrimesStreamingSpec extends FunSuite with StreamingSuiteBase {

  test("test arrest count - model streaming") {

    val input = Seq(
      Seq(CrimeModel(1, "Desc 1", true), CrimeModel(2, "Desc 2", false), CrimeModel(3, "Desc 3", true)),
      Seq(CrimeModel(4, "Desc 4", false), CrimeModel(5, "Desc 5", false), CrimeModel(6, "Desc 6", true))
    )

    val output = Seq(
      Seq(CrimeModel(1, "Desc 1", true), CrimeModel(3, "Desc 3", true)),
      Seq(CrimeModel(6, "Desc 6", true))
    )

    val operation = (r: DStream[CrimeModel]) => SparkCrimesStreaming.findArrestsModelStream(r)
    testOperation(input, operation , output)
  }

  test("test arrest count - string streaming") {
    val input = Seq(
      Seq("1,HS314836,05/18/2010 11:10:00 PM,0000X W TERMINAL ST,0560,ASSAULT,SIMPLE,Desc 1,true",
        "2,HS314836,05/18/2010 11:10:00 PM,0000X W TERMINAL ST,0560,NARCOTICS,SIMPLE,Desc 2,false",
        "3,HS314836,05/18/2010 11:10:00 PM,0000X W TERMINAL ST,0560,THEFT,SIMPLE,Desc 3,true"),
      Seq("4,HS314836,05/18/2010 11:10:00 PM,0000X W TERMINAL ST,0560,THEFT,SIMPLE,Desc 4,false",
        "5,HS314836,05/18/2010 11:10:00 PM,0000X W TERMINAL ST,0560,ASSAULT,SIMPLE,Desc 5,false",
        "6,HS314836,05/18/2010 11:10:00 PM,0000X W TERMINAL ST,0560,ASSAULT,SIMPLE,Desc 6,true")
    )

    val output = Seq(
      Seq(CrimeModel(1, "Desc 1", true), CrimeModel(3, "Desc 3", true)),
      Seq(CrimeModel(6, "Desc 6", true))
    )

    val operation = (r: DStream[String]) => SparkCrimesStreaming.findArrestsStringStream(r)
    testOperation(input, operation , output)
  }
}
