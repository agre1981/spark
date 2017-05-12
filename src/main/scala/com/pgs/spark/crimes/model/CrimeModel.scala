package com.pgs.spark.crimes.model

/**
  * Created by ogrechanov on 4/19/2017.
  */
case class CrimeModel(id: Long, var description: String, arrested: Boolean) {

  override def toString: String = {
    id.toString + "|" + description + "|" + arrested.toString
  }
}
