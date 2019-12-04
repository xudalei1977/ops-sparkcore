package com.lavapm.utils

import java.security.MessageDigest
import java.text.SimpleDateFormat


/**
  * Md5 operation
  * Created by ryan on 24/12/2018
  */
object Md5 {

  def MD5(input: String): String = {
    var md5: MessageDigest = null
    try {
      md5 = MessageDigest.getInstance("MD5")
    }
    catch {
      case e: Exception => {
        e.printStackTrace
        println(e.getMessage)
      }
    }
    val byteArray: Array[Byte] = input.getBytes
    val md5Bytes: Array[Byte] = md5.digest(byteArray)
    var hexValue: String = ""
    //var i: Integer = 0
    for ( i <- 0 to md5Bytes.length-1) {
      val str: Int = (md5Bytes(i).toInt) & 0xff
      if (str < 16) {
        hexValue=hexValue+"0"
      }
      hexValue=hexValue+(Integer.toHexString(str))
    }
    return hexValue.toString
  }



  def main(args: Array[String]): Unit = {
    println(MD5("864691027769646"))
    println(MD5("864691027769646").length)
    println(new SimpleDateFormat("yyyy-MM-dd").format("1542849637000".toLong))
  }
}
