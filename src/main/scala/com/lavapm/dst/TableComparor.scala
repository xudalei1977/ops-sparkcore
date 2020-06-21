package com.lavapm.dst


import org.apache.spark.sql.{SaveMode,SparkSession}
import org.apache.spark.sql.catalog.Column


/**
 * Compare two table column values, and output the difference.
 *
 * Input parameter:
 * 1) left table to compare, e.g. "dalei_xu.test1"
 * 2) left table primary key, e.g. "id1"
 * 3) left table columns, departed by ',', e.g. "age1,name1"
 * 4) right table to compare, e.g. "dalei_xu.test2"
 * 5) right table primary key, e.g. "id2"
 * 6) right table columns, departed by ',', e.g. "age2,name2"
 *
 * Output files:
 * 1) record only exist in left table, content is the primary key of left table.
 * 2) record only exist in right table, content is the primary key of right table.
 * 3) different column value, content like "left table"."left column" "value" -- "right table"."right column" "value"
 *
 * @author dalei.xu
 * @date 2020-06-21
 */
object TableComparor {

  def main(args: Array[String]): Unit = {

//    val leftTable = args(0)
//    val leftPrimaryKey = args(1)
//    val leftColumn = args(2)
//    val rightTable = args(3)
//    val rightPrimaryKey = args(4)
//    val rightColumn = args(5)

    val leftTable = "dalei_xu.test1"
    val leftPrimaryKey = "id1"
    val leftColumn = "age1,name1"
    val rightTable = "dalei_xu.test2"
    val rightPrimaryKey = "id21"
    val rightColumn = "age2,name2"

    val resPath4LeftRecord = "/user/dalei.xu/" + leftTable
    val resPath4RightRecord = "/user/dalei.xu/" + rightTable
    val resPath4DifferentRecord = "/user/dalei.xu/" + leftTable + " - " + rightTable


    val spark = SparkSession
      .builder()
      .appName("Table Comparor")
      .getOrCreate()

    println(" **************** TableComparor:: confirmTableAndColumnExist(): started.")
    confirmTableAndColumnExist(spark, leftTable, leftPrimaryKey, leftColumn, rightTable, rightPrimaryKey, rightColumn)

    println(" **************** TableComparor:: compareTableRecordCount(): started.")
    compareTableRecordCount(spark, leftTable, leftPrimaryKey, rightTable, rightPrimaryKey, resPath4LeftRecord, resPath4RightRecord)

    println(" **************** TableComparor:: compareTableRecordCount(): started.")
    compareTableRecordCount(spark, leftTable, leftPrimaryKey, rightTable, rightPrimaryKey, resPath4LeftRecord, resPath4RightRecord)

    spark.stop()
  }


  /**
   * Confirm the table and columns exist.
   *
   */
  def confirmTableAndColumnExist(spark: SparkSession,
                                 leftTable: String,
                                 leftPrimaryKey: String,
                                 leftColumn: String,
                                 rightTable: String,
                                 rightPrimaryKey: String,
                                 rightColumn: String) : Unit = {

    import spark.implicits._

    // Confirm the table exist.
    if (!spark.catalog.tableExists(leftTable))
      throw new Exception(s" Table ${leftTable} does NOT exist!")

    if (!spark.catalog.tableExists(rightTable))
      throw new Exception(s" Table ${rightTable} does NOT exist!")

    // Confirm the column exist.
    val leftColumnInDB = spark.catalog.listColumns(leftTable).map(_.name).collect

    if (!leftColumnInDB.contains(leftPrimaryKey))
      throw new Exception(s" Primary Key ${leftPrimaryKey} does NOT exist!")

    leftColumn.split(",").foreach { column => checkExist(column, leftColumnInDB) }

    val rightColumnInDB = spark.catalog.listColumns(rightTable).map(_.name).collect

    if (!rightColumnInDB.contains(rightPrimaryKey))
      throw new Exception(s" Primary Key ${rightPrimaryKey} does NOT exist!")

    rightColumn.split(",").foreach { column => checkExist(column, rightColumnInDB) }

  }


  def checkExist(str : String, strArr : Array[String]) : Unit = {
    if(!strArr.contains(str))
      throw new Exception(s" Column ${str} does NOT exist!");
  }


  /**
   * Compare the count.
   *
   */
  def compareTableRecordCount(spark: SparkSession,
                              leftTable: String,
                              leftPrimaryKey: String,
                              rightTable: String,
                              rightPrimaryKey: String,
                              resPath4LeftRecord: String,
                              resPath4RightRecord: String) : Unit = {

    import spark.implicits._

    // Read the primary key into dataframe.
    val leftDf = spark.sql(s"select ${leftPrimaryKey} from ${leftTable}")
    val rightDf = spark.sql(s"select ${leftPrimaryKey} from ${leftTable}")

    leftDf.except(rightDf).write.mode(SaveMode.Overwrite).text(resPath4LeftRecord)
    rightDf.except(leftDf).write.mode(SaveMode.Overwrite).text(resPath4RightRecord)

  }
}
