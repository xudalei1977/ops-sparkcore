package com.lavapm.utils

/**
  * UUID
  * Created by dalei on 6/26/18.
  */

import java.util.UUID.randomUUID


object UUID {

  /**
    * generate a new UUID
    */
  def generateUUID(): String = {
    val uuid: String = randomUUID.toString.replace("-", "").toLowerCase
    uuid
  }

  def main(args: Array[String]): Unit = {
    println(UUID.generateUUID())
  }

}
