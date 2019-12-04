package com.lavapm.utils

import scala.util._

/**
  * BasicInfo
  * Created by dalei on 4/18/19.
  */
object BasicInfo {

  /**
    * check if a string content is a int.
    */
  def isInt(str: String): Boolean = {

    val r1 = scala.util.Try(str.toInt)

    r1 match {
      case Success(_) => true
      case _ => false
    }
  }



  /**
    * get the age range by age.
    */
  def getAgeRange(ageStr: String): String = {

    val age = ageStr.toInt

    if(age <= 18)
      "18-"
    else if(age >= 19 && age <= 24)
      "19~24"
    else if(age >= 25 && age <= 29)
      "25~29"
    else if(age >= 30 && age <= 34)
      "30~34"
    else if(age >= 35 && age <= 39)
      "35~39"
    else if(age >= 40 && age <= 49)
      "40~49"
    else if(age >= 50)
      "50+"
    else
      ""
  }


  def main(args: Array[String]): Unit = {
    println(getAgeRange("123"))
  }

}
