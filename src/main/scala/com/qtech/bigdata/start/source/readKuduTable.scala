package com.qtech.bigdata.start.source

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 一些读取表的方法
  */
object readKuduTable {

  def readKuduTableDF(spark: SparkSession, areaTable: String): DataFrame = {
    spark.read.format("kudu")
      .options(Map("kudu.master" -> "bigdata01,bigdata02,bigdata03",
        "kudu.table" -> areaTable))
      .load
  }

  def readKuduTableDF(spark: SparkSession, areaTable: String,areaView:String): Unit = {
    spark.read.format("kudu")
      .options(Map("kudu.master" -> "bigdata01,bigdata02,bigdata03",
        "kudu.table" -> areaTable))
      .load
      .createOrReplaceTempView(areaView)
  }

  def dftTransformationView(spark: SparkSession,df:DataFrame,areaView:String): Unit ={
    df.createOrReplaceTempView(areaView)
  }

}
