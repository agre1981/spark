package com.pgs.spark.crimes

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.regression.LassoWithSGD
import org.scalatest.FunSuite

/**
  * Created by ogrechanov on 4/27/2017.
  */
class SparkMLSpec extends FunSuite with DataFrameSuiteBase {

  test("ML - regression") {

    //sqlContext.createDataFrame()
  }
}
