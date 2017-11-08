package com.hongya.bigdata.conf

/**
  * Created by dengziming on 07/11/2017.
  * ${Main}
  */
object StringTest {


  def main(args: Array[String]): Unit = {

    val s1 =
      s"""
        |aaa\\ aa \aaa$$"
      """.stripMargin

    println(s1)
  }
}
