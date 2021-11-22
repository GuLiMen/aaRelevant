package com.qtech.bigdata.util

import org.apache.spark.sql.{SQLContext, SparkSession}
import com.qtech.bigdata.common.sourceScheam.kuduMaster

trait readOracleView {

  def erpdsaws(spark: SparkSession, areaTable: String, areaView: String): Unit = {

//    new KuduClient()

    spark.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:@10.170.1.196:1521/topprd")
      .option("dbtable", areaTable)
      .option("user", "dsaws")
      .option("password", "dsaws")
      //          .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("driver", "oracle.jdbc.OracleDriver")
      .load.createOrReplaceTempView(areaView)
  }


  def mes_db(spark: SparkSession, areaTable: String, areaView: String): Unit = {

    spark.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:@10.170.1.36:1521/qtreport")
      .option("dbtable", areaTable)
      .option("user", "qtmes")
      .option("password", "qtmes#2020")
      //          .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("driver", "oracle.jdbc.OracleDriver")
      .load.createOrReplaceTempView(areaView)
  }

  def erpdsdata(spark: SparkSession, areaTable: String, areaView: String): Unit = {

    spark.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:@10.170.1.196:1521/topprd")
      .option("dbtable", areaTable)
      .option("user", "dsdata")
      .option("password", "dsdata")
      //          .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("driver", "oracle.jdbc.OracleDriver")
      .load.createOrReplaceTempView(areaView)
  }

  def kuduTable(spark: SparkSession, areaTable: String, areaView: String): Unit = {
    spark.read.format("kudu")
      .options(Map("kudu.master" -> kuduMaster,
        "kudu.table" -> areaTable))
      .load
      .createOrReplaceTempView(areaView)
  }

//  def kuduView(spark: SparkSession, areaView: String, areaVieweResult: String): Unit = {
//    spark.read.format("jdbc")
//      .options(Map(
//        "url" -> IMPALA_CONNECTION_URL,
//        "driver" -> IMPALA_JDBC_DRIVER,
//        "dbtable" -> areaView
//      ))
//      .load().createOrReplaceTempView(areaVieweResult)
//  }

  def ems(spark: SparkSession, areaTable: String, areaView: String): Unit = {

    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://10.170.6.160:3306/ziyun-iot")
      .option("dbtable", areaTable)
      .option("user", "ziyunIot")
      .option("password", "Pass1234")
      //          .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("driver", "com.mysql.jdbc.Driver")
      .load.createOrReplaceTempView(areaView)

  }

  def iqc_table(spark: SparkSession, areaTable: String, areaView: String): Unit = {

    spark.read.format("jdbc")
      .option("url", "jdbc:sqlserver://10.170.1.246:1433;DatabaseName=IQC")
      .option("dbtable", areaTable)
      .option("user", "bigdata")
      .option("password", "bigdata2021")
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .load.createOrReplaceTempView(areaView)
  }

  def oa_table(spark: SparkSession, areaTable: String, areaView: String): Unit = {

    spark.read.format("jdbc")
      .option("url", "jdbc:oracle:thin:@10.170.3.156:1521/oadb")
      .option("dbtable", areaTable)
      .option("user", "oadb")
      .option("password", "a1SDhGsv9")
                .option("driver", "oracle.jdbc.driver.OracleDriver")
//      .option("driver", "oracle.jdbc.OracleDriver")
      .load.createOrReplaceTempView(areaView)
  }




  def ems_idm(spark: SparkSession, areaTable: String, areaView: String): Unit = {

    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://10.170.6.160:3306/ziyun-idm")
      .option("dbtable", areaTable)
      .option("user", "ziyunIot")
      .option("password", "Pass1234")
      //          .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("driver", "com.mysql.jdbc.Driver")
      .load.createOrReplaceTempView(areaView)

  }

//  def deleteMysqlTableData(sqlContext: SQLContext, mysqlTableName: String, condition: String): Boolean = {
//    val conn = MySQLPoolManager.getMysqlManager.getConnection //从连接池中获取一个连接
//    val preparedStatement = conn.createStatement()
//    try {
//      preparedStatement.execute(s"delete from $mysqlTableName where $condition")
//    } catch {
//      case e: Exception =>
//        println(s"mysql deleteMysqlTable error:${e.getMessage}")
//        false
//    } finally {
//      preparedStatement.close()
//      conn.close()
//    }
//  }
}
