package com.lavapm.utils

/**
  * HBase operation
  * Created by dalei on 6/28/18.
  * Modified by ryan on 12/27/2018.
  */


import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.spark.{HBaseContext, KeyFamilyQualifier}
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


object HBase {

  val conf = HBaseConfiguration.create()


  /**
    * check whether a namespace exist.
    */
  def isNamespaceExist(namespace: String): Boolean = {

    // open the connection
    val connection = ConnectionFactory.createConnection(conf)

    val found = isNamespaceExist(connection, namespace)

    // close the connection
    connection.close

    return found
  }

  /**
    * check whether a namespace exist.
    */
  def isNamespaceExist(connection: Connection, namespace: String): Boolean = {

    // open the connection
    val admin = connection.getAdmin

    var found = false

    val list = admin.listNamespaceDescriptors()
    for (nd <- list){
      if (nd.getName.equals(namespace)) {
        found = true
      }
    }

    // close the connection
    admin.close

    return found
  }


  /**
    * delete a namespace.
    */
  def deleteNamespace(namespace: String): Unit = {
    // open the connection
    val connection = ConnectionFactory.createConnection(conf)

    deleteNamespace(connection, namespace)

    // close the connection
    connection.close
  }


  /**
    * delete a namespace.
    */
  def deleteNamespace(connection: Connection, namespace: String): Unit = {
    try {
      // open the connection
      val admin = connection.getAdmin

      val tableNames = admin.listTableNamesByNamespace(namespace)
      for (tablename <- tableNames) {
        admin.disableTable(tablename)
        admin.deleteTable(tablename)
      }

      admin.deleteNamespace(namespace)

      // close the connection
      admin.close

    } catch {
      case e : Exception =>
        print("error in deleting namespace : " + e.toString)
    }
  }


  /**
    * create a namespace
    */
  def createNamespace(namespace: String): Unit = {
    // open the connection
    val connection = ConnectionFactory.createConnection(conf)

    createNamespace(connection, namespace)

    // close the connection
    connection.close
  }


  /**
    * create a namespace
    */
  def createNamespace(connection: Connection, namespace: String): Unit = {

    // open the connection
    val admin = connection.getAdmin

    val namespaceDescriptor = NamespaceDescriptor.create(namespace).build()
    admin.createNamespace(namespaceDescriptor)

    // close the connection
    admin.close
  }


  /**
    * check whether a table exist.
    */
  def isTableExist(namespace: String, table: String): Boolean = {
    // open the connection
    val connection = ConnectionFactory.createConnection(conf)

    val result = isTableExist(connection, namespace, table)

    // close the connection
    connection.close

    return result
  }


  /**
    * check whether a table exist.
    */
  def isTableExist(connection: Connection, namespace: String, table: String): Boolean = {
    // open the connection
    val admin = connection.getAdmin

    val tableName = TableName.valueOf(namespace + ":" + table)
    val result = admin.tableExists(tableName)

    // close the connection
    admin.close

    return result
  }


  /**
    * delete a table if exists.
    */
  def deleteTable(namespace: String, table: String): Unit = {
    // open the connection
    val connection = ConnectionFactory.createConnection(conf)

    deleteTable(connection, namespace, table)

    // close the connection
    connection.close
  }


  /**
    * delete a table if exists.
    */
  def deleteTable(connection: Connection, namespace: String, table: String): Unit = {
    // open the connection
    val admin = connection.getAdmin

    val tableName = TableName.valueOf(namespace + ":" + table)

    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }

    // close the connection
    admin.close
  }

  /**
    * create a table, delete the old one with same name if exists.
    */
  def createTable(namespace:String, table: String, familiyArray: Array[String]): Unit = {
    // open the connection
    val connection = ConnectionFactory.createConnection(conf)

    createTable(connection, namespace, table, familiyArray)

    // close the connection
    connection.close
  }


  /**
    * create a table, delete the old one with same name if exists.
    */
  def createTable(connection: Connection, namespace:String, table: String, familiyArray: Array[String]): Unit = {
    // open the connection
    val admin = connection.getAdmin

    val tableName = TableName.valueOf(namespace + ":" + table)
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(namespace + ":" + table))

    for(family <- familiyArray)
      tableDescriptor.addFamily(new HColumnDescriptor(family.getBytes))


    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }

    admin.createTable(tableDescriptor)

    // close the connection
    admin.close
  }


  /**
    * select a table by the key.
    */
  def selectTableByKey(connection: Connection, namespace:String, table: String, keyValue: String, family: String, column: String): String = {
    if (keyValue.length != 0){
      val tableName = TableName.valueOf(namespace + ":" + table)
      val hbaseTable = connection.getTable(tableName)

      val get = new Get(keyValue.getBytes)
      val result = hbaseTable.get(get)

      hbaseTable.close()
      Bytes.toString(result.getValue(family.getBytes, column.getBytes))
    }else{
      ""
    }
  }


  /**
    * select a table by the filter.
    */
  def selectTableByFilter(namespace:String, table: String, keyValue: String): Unit = {

//    val scan = new Scan()
//    val filterArr = new java.util.ArrayList[Filter](1)
//    val filter1 = new SingleColumnValueFilter(Bytes.toBytes("basic"),
//      Bytes.toBytes("age_range"),
//      CompareFilter.CompareOp.EQUAL,
//      new BinaryComparator(Bytes.toBytes("55-64")))
//
//    filter1.setFilterIfMissing(true)
//    filterArr.add(filter1)
//
//    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filterArr)
//    scan.setFilter(filterList)
//
//    hbaseConf.set(TableInputFormat.SCAN, convertScanToString(scan))
//    result
  }


  /**
    * Prepare the Put object for bulkload function.
    * @param put The put object.
    * @throws java.io.IOException
    * @throws java.lang.InterruptedException
    * @return Tuple of (KeyFamilyQualifier, bytes of cell value)*/
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def putForLoad(put: Put): Iterator[(KeyFamilyQualifier, Array[Byte])] = {
    val ret: mutable.MutableList[(KeyFamilyQualifier, Array[Byte])] = mutable.MutableList()
    import scala.collection.JavaConversions._
    for (cells <- put.getFamilyCellMap.entrySet().iterator()) {
      val family = cells.getKey
      for (value <- cells.getValue) {
        val kfq = new KeyFamilyQualifier(CellUtil.cloneRow(value), family, CellUtil.cloneQualifier(value))
        ret.+=((kfq, CellUtil.cloneValue(value)))
      }
    }
    ret.iterator
  }


  /**
    * bulk load data to HBase
    * @param spark    spark session
    * @param rdd      rdd of HBase put
    */
  def bulkLoad(spark: SparkSession,rdd:RDD[Put])={

    val hbaseConf = HBaseConfiguration.create()
    val hBaseContext = new HBaseContext(spark.sparkContext, hbaseConf)

    Hdfs.deleteFile(spark, "/tmp/bulkLoad")
    println("*********************before hfile load**************************")
    hBaseContext.bulkLoad[Put](rdd, TableName.valueOf("persona:user_info"), (put: Put) => putForLoad(put), "/tmp/bulkLoad")
    println("*********************hfile loaded**************************")

    val conn = ConnectionFactory.createConnection(hbaseConf)
    val tableName = TableName.valueOf("persona:user_info".getBytes)
    val regionLocator = new HRegionLocator(tableName, classOf[ClusterConnection].cast(conn))
    val realTable = conn.getTable(tableName)
    HFileOutputFormat2.configureIncrementalLoad(Job.getInstance(), realTable, regionLocator)

    println("*********************bulk load start**************************")
    val loader = new LoadIncrementalHFiles(hbaseConf)
    val admin = conn.getAdmin()
    loader.doBulkLoad(new Path("/tmp/bulkLoad"), admin, realTable, regionLocator)
    conn.close()

    println("*********** hbase table saved")
  }

  def getConnection() = {
    ConnectionFactory.createConnection(conf)
  }


  def main(args: Array[String]): Unit = {
    createTable("persona", "user_basic_info", Array("info"))
  }

}
