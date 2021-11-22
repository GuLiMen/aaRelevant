package com.qtech.bigdata.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * 一些时间工具方法
  */
object date {

  /**
    * 返回时间段，以便于BI端展示
    *
    * @param date
    */
  def dateslot(date: String): String = {

    lazy val newtime: Date = new SimpleDateFormat("yyyy-MM-dd HH").parse(date)
    var cal = Calendar.getInstance()
    cal.setTime(newtime)
    cal.add(Calendar.HOUR, 1)
    val sourceTime: String = new SimpleDateFormat("yyyy-MM-dd HH").format(newtime)
    val targetTime: String = new SimpleDateFormat("HH").format(cal.getTime)
    val timeslot = sourceTime.toString + ":00:00" + "--" + targetTime + ":00:00"
    timeslot

  }

  /**
    * 返回时间段，以便于BI端展示
    *
    * @param date
    */
  def dateslot_test(date: String): String = {

    lazy val newtime: Date = new SimpleDateFormat("yyyy-MM-dd HH").parse(date)
    var cal1 = Calendar.getInstance()
    cal1.setTime(newtime)
    var cal = Calendar.getInstance()
    cal.setTime(newtime)

    //切割时间段，判断时间
    val timesolt: Array[String] = date.split(":")
    var newtime_test = timesolt {
      0
    } + ":30:00"
    if (date.compareTo(newtime_test) < 0) {
      cal1.add(Calendar.HOUR, -1)
      cal.add(Calendar.HOUR, 0)
      val sourceTime: String = new SimpleDateFormat("yyyy-MM-dd HH").format(cal1.getTime)
      val targetTime: String = new SimpleDateFormat("HH").format(cal.getTime)
      newtime_test = sourceTime.toString + ":00:00" + "--" + targetTime + ":00:00"
    } else {
      cal1.add(Calendar.HOUR, 0)
      cal.add(Calendar.HOUR, 1)
      val sourceTime: String = new SimpleDateFormat("yyyy-MM-dd HH").format(cal1.getTime)
      val targetTime: String = new SimpleDateFormat("HH").format(cal.getTime)
      newtime_test = sourceTime.toString + ":00:00" + "--" + targetTime + ":00:00"
    }
    newtime_test
  }

  //获取当前系统时间
  def systemTime(): String = {
    lazy val cal = Calendar.getInstance
    cal.add(Calendar.MINUTE, -60)
    val date = cal.getTime
    val endTime = new SimpleDateFormat("yyyy-MM-dd HH").format(date)

    val time = endTime + ":00:00"
    time
  }

  //获取当前系统时间
  def dataPutTime(): String = {
    lazy val cal = Calendar.getInstance
    cal.add(Calendar.MINUTE, 0)
    val date = cal.getTime
    val endTime = new SimpleDateFormat("yyyy-MM-dd").format(date)

    val time = endTime + " 10:00:00"
    time
  }

  //返回当前时间
  def now(): String = {
    lazy val cal = Calendar.getInstance
    cal.add(Calendar.MINUTE, 0)
    val date = cal.getTime
    val endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)

    endTime
  }

  //返回昨天的时间
  def yesterday(): String = {
    lazy val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val date = cal.getTime
    val endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date)

    endTime
  }


  //返回HDFS上时间，示例：7-9-D 或7-9-N
  def getHdfsDataDate(): String = {

    var time: String = null
    val cal: Calendar = Calendar.getInstance
    val month: Int = cal.get(Calendar.MONTH) + 1
    var day: Int = cal.get(Calendar.DATE) - 0

    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //设置日期格式
    val date: String = df.format(new Date)

    val s: Array[String] = date.split(" ")

    //判断当前时间属于白班还是夜班，若属于白班，，则返回白班的字符串
    if (s(1).compareTo("08:00:00") > 0 && s(1).compareTo("20:00:00") < 0) time = month + "-" + day + "-D"
    else if (s(1).compareTo("20:00:00") >= 0 && s(1).compareTo("24:00:00") < 0) time = month + "-" + day + "-N"
    else {
      day = cal.get(Calendar.DATE) - 1
      time = month + "-" + day + "-N"
    }
    return time
  }

}
