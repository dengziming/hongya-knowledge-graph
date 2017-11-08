package com.hongya.bigdata

import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}

import scala.collection.mutable.ArrayBuffer


/**
 * Hello world!
 *
 */
object App {

  val file = "/Users/dengziming/ideaspace/hongya/hongya-knowledge-graph/src/main/resources/input/"
  val header = Array(
    "id,user_id,mobile,city,cre_dt,updated_at",
    "id,user_id,amount,create_at,update_at",
    "userid,phones,contact_name,contact_phone,createtime,lastmodifytime",
    "user_id,people_id,other_mobile,other_address,call_start,call_seconds,call_type,call_address,address,reciprocal_in"
  )

  // 修改这里的数据，结果数据量会变化
  val user_count = 100
  val contact_count = 100
  val order_count = 1000
  val call_count = 1000
  /**
    * 随机生成数据
    * 用户id 1-1000
    *
    */
  def main(args: Array[String]): Unit = {

    println("开始registerinfo")
    val registerinfo_writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file + "registerInfo.csv", true), "utf-8"))
    registerinfo_writer.write(header(0))
    registerinfo_writer.newLine()
    for (line <- registerInfo()){

      registerinfo_writer.write(line)
      registerinfo_writer.newLine()
    }
    registerinfo_writer.flush()
    registerinfo_writer.close()

    println("结束registerinfo")


    println("开始order")
    val order_writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file + "loanOrder.csv", true), "utf-8"))
    order_writer.write(header(1))
    order_writer.newLine()
    for (line <- loanOrder()){

      order_writer.write(line)
      order_writer.newLine()
    }
    order_writer.flush()
    order_writer.close()
    println("结束order")

    println("开始contact")
    val contact_writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file + "contactInfo.csv", true), "utf-8"))
    contact_writer.write(header(2))
    contact_writer.newLine()
    for (line <- contact()){

      contact_writer.write(line)
      contact_writer.newLine()
    }
    contact_writer.flush()
    contact_writer.close()
    println("结束contact")


    println("开始call")
    val call_writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file + "callDetail.csv", true), "utf-8"))
    call_writer.write(header(3))
    call_writer.newLine()
    for (line <- calls()){

      call_writer.write(line)
      call_writer.newLine()
    }
    call_writer.flush()
    call_writer.close()
    println("结束call")


  }

  /**
    * 用户注册信息
    * 这部分数据量大， 我们只取几个字段用于分析：
    * `id,user_id,mobile,city,province,cre_dt,updated_at`，分别是客户注册信息的id，系统分配的用户id，电话，城市，省份，创建时间，修改时间，以下是测试数据：
    * ```
    * 66,2600,NULL,NULL,NULL,1490703433,1421763128000
    * 69,5165698,13578787778,嘉兴,330000,1497443679,1481080002000
    * ```
    */
  def registerInfo(): Array[String] ={

    val buffer = new ArrayBuffer[String]()

    for (i <- Range(0,user_count)){

      val id = i + 1
      val user_id = i + 1
      val mobile:Long = 18511110000L + user_id
      val city = if (mobile % 2 == 0) "北京" else "上海"
      val cre_dt = System.currentTimeMillis() / 1000
      val updated_at = System.currentTimeMillis() / 1000

      val builder = new java.lang.StringBuilder

      val value = builder.append(id).append(",")
        .append(user_id).append(",")
        .append(mobile).append(",")
        .append(city).append(",")
        .append(cre_dt).append(",")
        .append(updated_at).toString

      buffer += value
    }

    buffer.toArray
  }

  /**
    *
    * id,user_id,amount,create_at,update_at，分别是订单的id，用户id，数额，时间。下面是测试数据
    */
  def loanOrder(): Array[String] ={

    val buffer = new ArrayBuffer[String]()

    for (i <- Range(0,order_count)){

      val id = i + 1
      val user_id = id / 10 + 1
      val amount = 10000 * Math.random()
      val create_at = System.currentTimeMillis() / 1000
      val updated_at = System.currentTimeMillis() / 1000

      val builder = new StringBuilder

      val value = builder.append(id).append(",")
        .append(user_id).append(",")
        .append(amount).append(",")
        .append(create_at).append(",")
        .append(updated_at).toString

      buffer += value
    }

    buffer.toArray
  }

  /**
    * `userid,	phones,	contact_name,contact_phone,	createtime,lastmodifytime`，分别是：用户id，联系人所有的电话，联系人姓名，创建时间，修改时间
    */
  def contact(): Array[String] ={

    val buffer = new ArrayBuffer[String]()

    for (i <- Range(0,user_count)){

      val user_id = i + 1
      val mobile:Long = 18511110000L + user_id


      // 为每个人生成一百个联系人
      for (j <- Range(0,contact_count)){

        val contact_phone = 18511111000L - user_id + j
        val contact_name = contact_phone+"同志"

        val create_at = System.currentTimeMillis() / 1000
        val updated_at = System.currentTimeMillis() / 1000

        val builder = new StringBuilder

        val value = builder.append(user_id).append(",")
          .append(mobile).append(",")
          .append(contact_name).append(",")
          .append(contact_phone).append(",")
          .append(create_at).append(",")
          .append(updated_at).toString

        buffer += value
      }

    }

    buffer.toArray
  }


  /**
    * 通话记录数据
    * `user_id,	people_id,other_mobile,other_address,call_start,call_seconds,call_type,call_address,address,reciprocal_in`
    * @return
    */
  def calls(): Array[String] ={


    val buffer = new ArrayBuffer[String]()

    for (i <- Range(0,call_count)){

      val user_id = i + 1
      val people_id = 18511110000L + user_id


      // 为每个人生成一千个通话记录
      for (j <- Range(0,1000)){

        val other_mobile = 18511111000L - user_id + j
        val other_address = if (other_mobile % 2 == 0) "北京" else "上海"

        val call_start = "2018-01-01 00:00:00"
        val call_seconds = (1000 * Math.random()).toInt

        val call_type = if(Math.random() > 0.5) 1 else 0

        val call_address = if (people_id % 2 == 0) "北京" else "上海"

        val address = call_address

        val reciprocal_in = address

        val builder = new StringBuilder

        val value = builder.append(user_id).append(",")
          .append(people_id).append(",")
          .append(other_mobile).append(",")
          .append(other_address).append(",")
          .append(call_start).append(",")
          .append(call_seconds).append(",")
          .append(call_type).append(",")
          .append(call_address).append(",")
          .append(address).append(",")
          .append(reciprocal_in)
          .toString

        buffer += value
      }

    }

    buffer.toArray
  }
}
