package com.lavapm.utils

import org.apache.spark.SparkContext

/**
  * Rdd operation.
  * Created by dalei on 11/26/18.
  */
object Rdd {

  /**
    * view the partition data
    */
  def partitionData(sc: SparkContext): Array[Any] = {
      val rdd = sc.parallelize(1 to 8,3)
      rdd.mapPartitionsWithIndex{
        (partid,iter)=>{
          var part_map = scala.collection.mutable.Map[String,List[Int]]()
          var part_name = "part_" + partid
          part_map(part_name) = List[Int]()
          while(iter.hasNext){
            part_map(part_name) :+= iter.next()
          }
          part_map.iterator
        }
      }.collect
    null
  }
}
