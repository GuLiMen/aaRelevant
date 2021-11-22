package com.qtech.bigdata.common

import org.apache.spark.sql.types._


//定义结果表结构
object sourceScheam {

  val schema = new StructType
  schema.add("area",StringType)
  schema.add("cob",StringType)
  schema.add("eqid",StringType)
  schema.add("device_num",StringType)
  schema.add("uploadtime",StringType)
  schema.add("timeslot",StringType)
  schema.add("is_network",StringType)
  schema.add("is_production",StringType)
  schema.add("num",LongType)


  final val resultTableSchema = StructType(
    StructField("AREA", StringType, false) ::
      StructField("COB", StringType, false) ::
      StructField("EQID", StringType, false) ::
      StructField("DEVICE_NUMBER", StringType, false) ::
      StructField("TIMESLOT", StringType, false) ::
      StructField("UPLOADTIME", StringType, true) ::
      StructField("IS_NETWORK", StringType, true) ::
      StructField("IS_PRODUCTION", StringType, true) ::
      StructField("LOT", StringType, true) ::
      StructField("REMARKS", StringType, true) ::Nil)

  final lazy  val kuduMaster = "10.170.3.11:7051,10.170.3.12:7051,10.170.3.13:7051"
}
