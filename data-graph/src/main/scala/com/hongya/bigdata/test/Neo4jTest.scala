package com.hongya.bigdata.test

import org.neo4j.driver.v1._

import org.neo4j.driver.v1.Values.parameters
/**
  * Created by dengziming on 07/11/2017.
  * ${Main}
  */
object Neo4jTest {

  def main(args: Array[String]): Unit = {


    val driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "123456" ) )
    val session = driver.session()

    session.run( "CREATE (a:Person {name: {name}, title: {title}})",
      parameters( "name", "Hongya", "title", "success company" ) )

    val result = session.run( "MATCH (a:Person) WHERE a.name = {name} " +
      "RETURN a.name AS name, a.title AS title",
      parameters( "name", "Hongya" ) )
    while ( result.hasNext )
    {
      val record = result.next()
      println( record.get( "title" ).asString() + " " + record.get( "name" ).asString() )
    }

    session.close()
    driver.close()
  }
}
