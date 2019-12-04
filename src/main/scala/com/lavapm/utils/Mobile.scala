package com.lavapm.utils

import org.apache.spark.sql._
import org.apache.spark._

/**
  * Mobile
  * Created by dalei on 6/26/2018.
  */




object Mobile {

  /**
    * check the validity of a mobile
    */
  def validMobile(mobile: String): Boolean = {

    val pattern =
      """^((130|131|132|133|134|135|136|137|138|139|145|147|149|150|151|152|153|155|156|157|158|159|170|171|173|175|176|177|178|180|181|182|183|184|185|186|187|188|189)\d{8})$""".r

    mobile match {
      case pattern(_*) => true
      case _ => false
    }
  }


  /**
    * get the mobile belonging place (province, city, operator)
    */
  def getMobilePlace(mobile: String): (String, String, String) = {

    val conf = new SparkConf().setAppName("Get the Mobile Location")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val rowArr = sqlContext.sql("select province, city, operator from raw.mobile_affiliation where substring("
                                  + mobile + ", 1, 7) = prephonenumber").rdd.map {
      case Row(province: String, city: String, operator: String) => (province, city, operator)
      case _ => ("", "", "")
    }.collect()

    rowArr(0)
  }



  def main(args: Array[String]): Unit = {
    getMobilePlace("13916915940")
  }
}
