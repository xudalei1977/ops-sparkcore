package com.lavapm.utils

import java.util.Calendar


/**
  * IdNo
  * Created by dalei on 6/26/2018.
  */
object IdNo {

  /**
    * check the validity of a id_no.
    */
  def validIdNo(idNo: String): Boolean = {

    var part = ""

    if(idNo.last.toUpper.equals("X"))
      part = idNo.substring(0, idNo.length-1)
    else
      part = idNo

    val pattern = """^(\d+)$""".r
    part match {
      case pattern(_*) => true
      case _ => false
    }
  }


  /**
    * get the age, age range, birthday, birth place from a id_no.
    */
  def getInfoFromIdNo(idNo: String): (String, String, String, String, String, String) = {

    var birthday = ""
    var age = 0
    var genderCode = 0


    val year = Calendar.getInstance().get(Calendar.YEAR)

    // get the birthday, gender code, age
    if (idNo.length() == 15) {
      birthday = "19" + idNo.substring(6, 12)
      genderCode = idNo.substring(idNo.length - 1).toInt
      age = year - ("19" + idNo.substring(6, 8)).toInt
    } else if (idNo.length() == 18) {
      birthday = idNo.substring(6, 14)
      genderCode = (idNo.substring(idNo.length - 2, idNo.length - 1)).toInt
      age = year - (idNo.substring(6, 10)).toInt
    }

    // get the gender by gender code
    val gender = if(genderCode % 2 == 0) "F" else "M"

    // get the birth season by birthday
    val month = birthday.substring(4, 6).toInt
    val birthSeason = {
      month match {
        case 1 => "冬"
        case 2 => "冬"
        case 3 => "春"
        case 4 => "春"
        case 5 => "春"
        case 6 => "夏"
        case 7 => "夏"
        case 8 => "夏"
        case 9 => "秋"
        case 10 => "秋"
        case 11 => "秋"
        case 12 => "冬"
        case _ => ""
      }
    }

    // get the age range by age
    val ageRange = {
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


    val mmdd = birthday.substring(4)

    // get the zodiac by birthday
    val zodiac = {
      if(mmdd >= "0321" && mmdd <= "0419")
        "白羊座"
      else if(mmdd >= "0420" && mmdd <= "0520")
        "金牛座"
      else if(mmdd >= "0521" && mmdd <= "0621")
        "双子座"
      else if(mmdd >= "0622" && mmdd <= "0722")
        "巨蟹座"
      else if(mmdd >= "0723" && mmdd <= "0822")
        "狮子座"
      else if(mmdd >= "0823" && mmdd <= "0922")
        "处女座"
      else if(mmdd >= "0923" && mmdd <= "1023")
        "天秤座"
      else if(mmdd >= "1024" && mmdd <= "1122")
        "天蝎座"
      else if(mmdd >= "1123" && mmdd <= "1221")
        "射手座"
      else if(mmdd >= "1222" && mmdd <= "1231")
        "摩羯座"
      else if(mmdd >= "0101" && mmdd <= "0119")
        "摩羯座"
      else if(mmdd >= "0120" && mmdd <= "0218")
        "水瓶座"
      else if(mmdd >= "0219" && mmdd <= "0320")
        "双鱼座"
      else
        ""
    }

    (gender, age.toString, ageRange, birthday, birthSeason, zodiac)
  }



  /**
    * get the mobile belonging place (province, city, address)
    */
  def getMobilePlace(mobile: String): (String, String, String) = {


    ("", "", "")
  }

  def main(args: Array[String]): Unit = {
    val (p1, p2, p3, p4, p5, p6) = IdNo.getInfoFromIdNo("372330197706020110")
    println(p6)
  }

}
