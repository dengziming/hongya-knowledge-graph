package com.hongya.bigdata.neo4j

import java.io.File

import org.neo4j.driver.v1.{AuthTokens, GraphDatabase, Session}
import org.neo4j.driver.v1.Values.parameters

/**
  * Created by dengziming on 07/11/2017.
  * ${Main}
  */
object InsertWork {

  val neo4j_name = "neo4j"
  val neo4j_password = "123456"
  val input_path = "/Users/dengziming/ideaspace/hongya/hongya-knowledge-graph/src/main/resources/output/"

  def main(args: Array[String]): Unit = {


    val driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( neo4j_name, neo4j_password ) )
    val session = driver.session()


//    insertVertex(session)
//    insertEdge(session)

    /**
    MATCH (a:PHONE) WHERE a.number = {a_number}
    MATCH (b:PHONE) WHERE b.number = {b_number}
    CREATE (a)-[r:PHONE_PHONE_CALL { total_cnt:{total_cnt},month_cnt:{month_cnt},month_call_cnt:{month_call_cnt},month_call_hours:{month_call_hours},create_time: {create_time} }]->(b)
      */
    println("开始插入phone_phone_call边表")
    val phone_phone_call_path = s"$input_path/edge/edge_phone_phone_call"
    val phone_phone_call_files = new File(phone_phone_call_path).listFiles()
    for (phone_phone_call_file <- phone_phone_call_files){

      for( line <- scala.io.Source.fromFile(phone_phone_call_file).getLines()){

        val split = line.split(",")
        session.run("""
                      |MATCH (a:PHONE) WHERE a.number = {a_number}
                      |MATCH (b:PHONE) WHERE b.number = {b_number}
                      |CREATE (a)-[r:PHONE_PHONE_CALL { total_cnt:{total_cnt},month_cnt:{month_cnt},month_call_cnt:{month_call_cnt},month_call_hours:{month_call_hours},create_time: {create_time} }]->(b)
                      |""".stripMargin,
          parameters("a_number",split(1),"b_number", split(2),
            "total_cnt",split(3),"month_cnt",split(4),"month_call_cnt",split(5),"month_call_hours",split(6),
            "create_time",(System.currentTimeMillis()/1000).asInstanceOf[AnyRef]))

      }
    }

    session.close()
    driver.close()
  }

  def insertVertex(session:Session): Unit ={

    /**
        CREATE (a:PHONE {number: {number}})
        CREATE (a:USERID {number: {number}})
        CREATE (a:ORDERID {number: {number}})
      */

    val phone_path = s"$input_path/vertex/phone"
    val phone_files = new File(phone_path).listFiles()

    println("开始插入phone顶点表")
    for (phone_file <- phone_files){

      for( line <- scala.io.Source.fromFile(phone_file).getLines()){

        session.run("CREATE (a:PHONE {number: {number}})",parameters("number",line))

      }
    }

    println("开始插入orderid顶点表")
    val orderid_path = s"$input_path/vertex/orderid"
    val orderid_files = new File(orderid_path).listFiles()

    for (orderid_file <- orderid_files){

      for( line <- scala.io.Source.fromFile(orderid_file).getLines()){

        session.run("CREATE (a:ORDERID {number: {number}})",parameters("number",line))

      }
    }

    println("开始插入userid顶点表")
    val userid_path = s"$input_path/vertex/userid"
    val userid_files = new File(userid_path).listFiles()

    for (userid_file <- userid_files){

      for( line <- scala.io.Source.fromFile(userid_file).getLines()){

        session.run("CREATE (a:USERID {number: {number}})",parameters("number",line))

      }
    }

  }


  def insertEdge(session:Session): Unit ={

    // 插入userid_phone_custinfo
    /**
    MATCH (a:PHONE) WHERE a.number = {a_number}
    MATCH (b:USERID) WHERE b.number = {b_number}
    CREATE (a)-[r:USERID_PHONE_CUSTINFO { create_time: {create_time} }]->(b)
    */

    println("开始插入userid_phone_custinfo边表")
    val userid_phone_custinfo_path = s"$input_path/edge/edge_userid_phone_custinfo"
    val userid_phone_custinfo_files = new File(userid_phone_custinfo_path).listFiles()
    for (userid_phone_custinfo_file <- userid_phone_custinfo_files){

      for( line <- scala.io.Source.fromFile(userid_phone_custinfo_file).getLines()){

        val split = line.split(",")
        session.run("""
                      |    MATCH (a:USERID) WHERE a.number = {a_number}
                      |    MATCH (b:PHONE) WHERE b.number = {b_number}
                      |    CREATE (a)-[r:USERID_PHONE_CUSTINFO { create_time: {create_time} }]->(b)""".stripMargin,
          parameters("a_number",split(0),"b_number",split(1),"create_time",(System.currentTimeMillis()/1000).asInstanceOf[AnyRef]))

      }
    }

    /**
    MATCH (a:USERID) WHERE a.number = {a_number}
    MATCH (b:ORDERID) WHERE b.number = {b_number}
    CREATE (a)-[r:USERID_ORDERID { create_time: {create_time} }]->(b)
      */
    println("开始插入userid_orderid_loan边表")
    val userid_orderid_loan_path = s"$input_path/edge/edge_userid_orderid_loan"
    val userid_orderid_loan_files = new File(userid_orderid_loan_path).listFiles()
    for (userid_orderid_loan_file <- userid_orderid_loan_files){

      for( line <- scala.io.Source.fromFile(userid_orderid_loan_file).getLines()){

        val split = line.split(",")
        session.run("""
                      |    MATCH (a:USERID) WHERE a.number = {a_number}
                      |    MATCH (b:ORDERID) WHERE b.number = {b_number}
                      |    CREATE (a)-[r:USERID_ORDERID { create_time: {create_time} }]->(b)
                      |""".stripMargin,
          parameters("a_number",split(0),"b_number",split(1),"create_time",(System.currentTimeMillis()/1000).asInstanceOf[AnyRef]))

      }
    }

    /**
    MATCH (a:PHONE) WHERE a.number = {a_number}
    MATCH (b:PHONE) WHERE b.number = {b_number}
    CREATE (a)-[r:PHONE_PHONE_CONTACT { contact_name:{contact_name},relation:{relation},create_time: {create_time} }]->(b)
      */
    println("开始插入phone_phone_contact边表")
    val phone_phone_contact_path = s"$input_path/edge/edge_phone_phone_contact"
    val phone_phone_contact_files = new File(phone_phone_contact_path).listFiles()
    for (phone_phone_contact_file <- phone_phone_contact_files){

      for( line <- scala.io.Source.fromFile(phone_phone_contact_file).getLines()){

        val split = line.split(",")
        session.run("""
                      |    MATCH (a:PHONE) WHERE a.number = {a_number}
                      |    MATCH (b:PHONE) WHERE b.number = {b_number}
                      |    CREATE (a)-[r:PHONE_PHONE_CONTACT { contact_name:{contact_name},relation:{relation},create_time: {create_time} }]->(b)
                      |""".stripMargin,
          parameters("a_number",split(1),"b_number", split(2),
            "contact_name",split(5),"relation",split(6),
            "create_time",split(3)))

      }
    }


    /**
    MATCH (a:PHONE) WHERE a.number = {a_number}
    MATCH (b:PHONE) WHERE b.number = {b_number}
    CREATE (a)-[r:PHONE_PHONE_CALL { total_cnt:{total_cnt},month_cnt:{month_cnt},month_call_cnt:{month_call_cnt},month_call_hours:{month_call_hours},create_time: {create_time} }]->(b)
    */
    println("开始插入phone_phone_call边表")
    val phone_phone_call_path = s"$input_path/edge/edge_phone_phone_call"
    val phone_phone_call_files = new File(phone_phone_call_path).listFiles()
    for (phone_phone_call_file <- phone_phone_call_files){

      for( line <- scala.io.Source.fromFile(phone_phone_call_file).getLines()){

        val split = line.split(",")
        session.run("""
                      |MATCH (a:PHONE) WHERE a.number = {a_number}
                      |MATCH (b:PHONE) WHERE b.number = {b_number}
                      |CREATE (a)-[r:PHONE_PHONE_CALL { total_cnt:{total_cnt},month_cnt:{month_cnt},month_call_cnt:{month_call_cnt},month_call_hours:{month_call_hours},create_time: {create_time} }]->(b)
                      |""".stripMargin,
          parameters("a_number",split(1),"b_number", split(2),
            "total_cnt",split(3),"month_cnt",split(4),"month_call_cnt",split(5),"month_call_hours",split(6),
            "create_time",(System.currentTimeMillis()/1000).asInstanceOf[AnyRef]))

      }
    }

  }
}
