package com.lavapm.streaming

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


object JedisConnectionPools {
  val redisHost = "r-uf64396eb85ada04.redis.rds.aliyuncs.com"
  val redisPort = 6379
  val redisTimeout = 30000

  val conf = new JedisPoolConfig()
  //最大连接数
  conf.setMaxTotal(20)
  //最大空闲连接数
  conf.setMaxIdle(10)
  //调用borrow Object方法时，是否进行有效检查
  //conf.setTestOnBorrow(true)

  //ip地址， redis的端口号，连接超时时间
  val pool = new JedisPool(conf,"r-uf64396eb85ada04.redis.rds.aliyuncs.com",6379,10000,"LavaHeat2017")

  def getConnection():Jedis={
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val  conn = JedisConnectionPools.getConnection()
    //val r1: String = conn.get("oid::mid::*")
    val r1 = conn.keys("oid::mid::*")
    //  conn.hset("test:mm","aa","1")
    println(r1)
    //  conn.incrBy("bb",50)
    //  val r2 = conn.get("bb")
    //  println(r2)
    conn.close()

    /*val cli =  new Jedis("r-uf64396eb85ada04.redis.rds.aliyuncs.com",6379)
    cli.auth("LavaHeat2017")
    println("connect succeed!")
    println(cli.hgetAll("bes_setting_207752").toString())
    cli.close();*/

  }

}
