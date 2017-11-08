package com.hongya.bigdata.conf

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory


/**
  * 项目配置信息
  */
object Settings {


  val logger = LoggerFactory.getLogger(Settings.getClass)
  val config = ConfigFactory.load("config")

  val env = get("env")

  logger.info("###############################################")
  logger.info("############# 当前使用 " + env + " 配置 ################")
  logger.info("###############################################")

  logger.info(config.toString)


  def getConfig(path: String): Config = {
    config.getConfig(path)
  }

  def get(key: String): String = {
    config.getString(key)
  }

  def get(key: String,default:String): String = {
    try{
      val v = config.getString(key)
      if (v.isEmpty){
        default
      }else{
        v
      }
    }catch{
      case e: Exception => {
        logger.warn("get config err,usage default value : " + default)
        default
      }
    }
  }

  def getInt(key: String): Int = {
    config.getInt(key)
  }

  def getInt(key: String,default:Int): Int = {
    get(key,default + "").toInt
  }

  def getLong(key: String): Long = {
    config.getLong(key)
  }

  def getLong(key: String,default:Long): Long = {
    get(key,default + "").toLong
  }

  def main(args: Array[String]): Unit = {
    //    println(get("mongodb.address"))
        println(get("kafka.servers"))
//    getModuleVariableMap(variableConfig)
//    println(ConfigFactory.load("aa").getString("test.name"))

    println("dev" == Settings.get("env"))
  }

}
