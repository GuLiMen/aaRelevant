package com.qtech.bigdata.util

import java.util.regex.Pattern

object eqError {

  def main(args: Array[String]): Unit = {
    val res = """^(\d+)$"""
    val pattern = Pattern.compile(res)
    val s = "6"

    if(pattern.matcher(s"$s").matches()){
      print("0")
    }else{
      print("1")
    }
  }

}
