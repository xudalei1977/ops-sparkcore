package com.lavapm.utils

/**
  * Place
  * Created by dalei on 7/23/2018.
  */
object Place {

  /**
    * get the province and city from an address
    */
  def getProvinceAndCityFromAddress(address: String): (String, String) = {

    //val province = "(.*?)(?=省)|(.*?)(?=自治区)".r.findFirstIn(address)
    //val city = "(?<=省)(.*?)(?=市)|(?<=自治区)(.*?)(?=市)".r.findFirstIn(address)
    val province = "(.*省)|(.*自治区)".r.findFirstIn(address)
    val city = "(?<=省)(.*市)|(?<=自治区)(.*市)".r.findFirstIn(address)

    (province.getOrElse(""), city.getOrElse(""))
  }

  /**
    * get the province from an address
    */
  def getProvinceFromAddress(address: String): String = {

    //val province = "(.*?)(?=省)|(.*?)(?=自治区)".r.findFirstIn(address)
    val province = "(.*省)|(.*自治区)".r.findFirstIn(address)

    province.getOrElse("")
  }


  /**
    * get the province from an address
    */
  def getCityFromAddress(address: String): String = {

    //val city = "(?<=省)(.*?)(?=市)|(?<=自治区)(.*?)(?=市)".r.findFirstIn(address)
    val city = "(?<=省)(.*市)|(?<=自治区)(.*市)".r.findFirstIn(address)
    city.getOrElse("")
  }


  def main(args: Array[String]): Unit = {
    println(getCityFromAddress("黔江"))
    println((getProvinceAndCityFromAddress("山东省")_2).getClass.getName)
  }
}
