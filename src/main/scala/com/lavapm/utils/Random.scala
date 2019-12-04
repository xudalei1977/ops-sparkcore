package com.lavapm.utils

/**
  * Random
  * Created by dalei on 7/22/18.
  */
object Random {

  /**
    * return a random item from the 4 choices.
    */
  def randomItem(item1: String, item2: String, item3: String, item4: String): String = {

    val i = (1 + Math.random * 4).toInt

    i match {
      case 1 => item1
      case 2 => item2
      case 3 => item3
      case 4 => item4
      case _ => ""
    }
  }

  def main(args: Array[String]): Unit = {

    val ret = randomItem("aaa", "bbb", "ccc", "ddd")
    println("*********************** " + ret)

  }

}
