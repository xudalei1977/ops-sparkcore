package com.lavapm.utils

import org.apache.spark.{SparkConf, SparkContext}
import com.huaban.analysis.jieba.JiebaSegmenter
import scala.collection.JavaConversions._


/**
  * Interest
  * Created by dalei on 11/1/18.
  */
object Interest {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Map the app to interest")
    val sc = new SparkContext(conf)

    val hdfsPathSrc = "/user/hive/warehouse/dim_dsp.db/dim_app_package"
    val hdfsPathTag = "/user/dmp_app_user/output/dim_app"

    val map = Map( "世界" -> "游戏|游戏-手机游戏",
                  "游戏" -> "游戏|游戏-手机游戏",
                  "消灭" -> "游戏|游戏-手机游戏",
                  "斗" -> "游戏|游戏-手机游戏",
                  "杀" -> "游戏|游戏-手机游戏",
                  "乐" -> "游戏|游戏-手机游戏",
                  "玩" -> "游戏|游戏-手机游戏",
                  "儿童" -> "母婴育儿|母婴育儿-早教",
                  "宝宝" -> "母婴育儿|母婴育儿-早教",
                  "读" -> "文化娱乐|文化娱乐-书籍",
                  "书" -> "文化娱乐|文化娱乐-书籍")

    println("**************** before mapApp2Interest ")
    mapApp2Interest(sc, hdfsPathSrc, map, hdfsPathTag)

    sc.stop
  }

  /**
    * map the app name to interest
    */
  def mapApp2Interest(sc: SparkContext,
                      hdfsPathSrc: String,
                      map: Map[String,String],
                      hdfsPathTag: String): Unit = {
    try{

      // read from the hdfs and process each column
//      val rdd = sc.textFile(hdfsPathSrc).map{  line =>
//        val parts = line.split("\001")
//        val packageName = parts(0)
//        val appName = parts(1)
//
//        var interestLevel1 = ""
//        var interestLevel2 = ""
//        var notFound = ""
//
//        if (parts(1).length > 0 && parts(1) != "null"){
//          val words = new JiebaSegmenter().sentenceProcess(parts(1))
//          words.toArray[String]
//          for(word <- words){
//            if(map.contains(word)){
//              val interest = map.get(word).toString.split("|")
//              interestLevel1 = interest(0)
//              interestLevel2 = interest(1)
//            }
//          }
//
//          if(interestLevel1.length == 0)
//            parts(1)
//        }
//
////        if(interestLevel1.length == 0)
////          packageName + "\t" + appName + "\t" + interestLevel1 + "\t" + interestLevel2
//      }
//
//      rdd.saveAsTextFile(hdfsPathTag)

    } catch {
      case ex: Exception => {println("*************** error in mapApp2Interest ex := " + ex.toString)}
    }
  }
}
