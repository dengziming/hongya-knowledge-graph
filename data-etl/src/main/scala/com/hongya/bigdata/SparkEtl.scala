package com.hongya.bigdata

import com.hongya.bigdata.conf.Settings
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Hello world!
 *
 */
object SparkEtl {

  val input_path = "/Users/dengziming/ideaspace/hongya/hongya-knowledge-graph/src/main/resources/input"
  val output_path = "/Users/dengziming/ideaspace/hongya/hongya-knowledge-graph/src/main/resources/output"

  def main(args: Array[String]): Unit = {

    val builder = SparkSession
      .builder
      .appName("SparkEtl")
      .enableHiveSupport()

    if (Settings.get("env") == "dev") {
      builder.master("local[2]")
      builder.config("spark.testing.memory", "2048000000")
      builder.config("spark.sql.shuffle.partitions",10)
    }

    val spark = builder.getOrCreate()

    // test3(spark)

    save(spark)

  }

  def save(spark: SparkSession): Unit ={

    // 读取数据
    var registerInfo = spark.read.option("header","true").csv(s"$input_path/registerInfo.csv")
    registerInfo.createOrReplaceTempView("registerInfo")


    var loanOrder = spark.read.option("header","true").csv(s"$input_path/loanOrder.csv")
    loanOrder.createOrReplaceTempView("loanOrder")


    var contactInfo = spark.read.option("header","true").csv(s"$input_path/contactInfo.csv")
    contactInfo.createOrReplaceTempView("contactInfo")


    var callDetail = spark.read.option("header","true").csv(s"$input_path/callDetail.csv")
    callDetail.createOrReplaceTempView("callDetail")


    // 处理边表
    registerInfo = edge_custinfo(spark,"registerInfo")
    registerInfo.write.csv(s"$output_path/edge/edge_userid_phone_custinfo")
    registerInfo.createOrReplaceTempView("registerInfo")

    loanOrder = edge_order(spark,"loanOrder")
    loanOrder.write.csv(s"$output_path/edge/edge_userid_orderid_loan")
    loanOrder.createOrReplaceTempView("loanOrder")

    contactInfo = edge_contact(spark,"contactInfo")
    contactInfo.write.csv(s"$output_path/edge/edge_phone_phone_contact")
    contactInfo.createOrReplaceTempView("contactInfo")

    callDetail = edge_call(spark,"callDetail")
    callDetail.write.csv(s"$output_path/edge/edge_phone_phone_call")
    callDetail.createOrReplaceTempView("callDetail")

    // 处理顶点表
    vertex_userid(spark).write.csv(s"$output_path/vertex/userid")
    vertex_orderid(spark).write.csv(s"$output_path/vertex/orderid")
    vertex_phone(spark).write.csv(s"$output_path/vertex/phone")

  }



  def edge_custinfo(spark:SparkSession,table:String): DataFrame ={

    // id,user_id,mobile,city,province,cre_dt,updated_at

    val sql =
      s"""
        |select
        |    user_id AS userid,
        |    mobile AS phone
        |from
        |    $table
        |where
        |    mobile is not null
        |    and
        |    mobile regexp '^1\\\\d{10}$$$$'
      """.stripMargin

    spark.sql(sql)
  }


  def edge_order(spark:SparkSession,table:String): DataFrame ={

    // id,user_id,amount,create_at,update_at

    val sql =
      s"""
        |select
        |    user_id as userid,
        |    id AS orderid
        |from
        |    $table
      """.stripMargin

    spark.sql(sql)
  }

  def edge_contact(spark:SparkSession,table:String): DataFrame ={

    // userid,	phones,	contact_name,contact_phone,	createtime,lastmodifytime
    val sql1 =
      s"""
        |select
        |  regexp_replace(phones,'\\\\s+|^,|,$$$$|\\\\+86|\\\\-','') as phone,
        |  regexp_replace(contact_phone,'\\\\s+|\\\\+86|\\\\-','') as contact_phone,
        |  cast(userid as bigint) as user_id,
        |  regexp_replace(contact_name,'\\\\(.*?\\\\)|[^(a-zA-Z0-9\\\\u2E80-\\\\u9FFF)]|[\\\\( \\\\)《》。，〈〉一、⼂\\\\⼁]|\\\\s+|,|\\'|"','') as contact_name,
        |  createtime  as createtime,
        |  lastmodifytime as lastmodifytime
        |from $table
        |where phones is not null
        |and contact_phone is not null
        |and regexp_replace(phones,'\\\\s+|^,|,$$$$|\\\\+86','') regexp '^1\\\\d{10}(,1\\\\d{10})*$$$$'
        |and regexp_replace(contact_phone,'\\\\s+|\\\\+86','') regexp '^1\\\\d{10}$$$$'
        |and not phones regexp '^10'
        |and not contact_phone regexp '^10'
      """.stripMargin



    val df = spark.sql(sql1)

    val view = "tmp"
    df.createOrReplaceTempView(view)

    val sql2 =
      """
        |select
        |    phone,
        |    contact_phone,
        |    user_id,
        |    createtime,
        |    lastmodifytime,
        |    case when contact_name is null then '' else contact_name end as contact_name,
        |    case when contact_name regexp '宝贝|亲爱' or
        |    contact_name regexp '^[老]?[爸|妈|爹|娘]' or
        |    (contact_name regexp '^(老婆|妻子|媳妇|老公|丈夫|儿子|女儿|老[头爷]子|丈母娘|丈人|婆婆|内人|内子|太太|夫人|外子|爱人)'
        |    and not contact_name   regexp '的') then 1
        |    when contact_name regexp '^[大小二三四五六七八九十]?[哥兄弟姐妹伯叔姑婶舅姨]'
        |    or contact_name regexp '^[爷奶]'
        |    or contact_name regexp '^老[大二三四五六七八九十]'
        |    then 2
        |    when  contact_name regexp '先生|老板|老板娘|小姐|女士|总$$|经理$$' then 3
        |    when  contact_name regexp '老师' then 4
        |    when  contact_name regexp '同学' then 5
        |    when  contact_name regexp '^老' then 6
        |    when  contact_name regexp '提额|大额|高额|白户|黑户|额度|面签|信用|消费|金融|融资|款|借|POS|套现'
        |            OR (contact_name regexp '贷|中介' and contact_name NOT regexp '房')
        |            OR (contact_name regexp '办|刷|养' and contact_name regexp '卡')
        |            OR (contact_name regexp '代' and contact_name regexp '办')
        |    then 7
        |    else 0
        |    end as relation
        |from
        |    (
        |    select
        |      phone,
        |      contact_phone,
        |      user_id,
        |      contact_name,
        |      createtime,
        |      lastmodifytime
        |    from
        |      tmp
        |    where phone regexp '^1\\d{10}$$'
        |    UNION ALL
        |    select
        |      phone,
        |      contact_phone,
        |      user_id,
        |      contact_name,
        |      createtime,
        |      lastmodifytime
        |    from
        |      (
        |      select
        |         b.myphone phone,
        |         a.contact_phone,
        |         a.user_id,
        |         a.contact_name,
        |         a.createtime ,
        |         a.lastmodifytime
        |      from
        |        (select * from
        |          tmp
        |         where  phone not regexp '^1\\d{10}$$'
        |        ) a
        |        lateral view
        |        explode(split(phone,',')) b as myphone
        |      ) c
        |    ) d
        |    where phone is not null
        |and contact_phone is not null
        |and user_id is not null
        |and phone regexp '^1\\d{10}$$'
        |and contact_phone regexp '^1\\d{10}$$'
        |and phone!=contact_phone
      """.stripMargin


    spark.sql(sql2)

  }

  def edge_call(spark:SparkSession,table:String): DataFrame ={

    // user_id,	people_id,other_mobile,other_address,call_start,call_seconds,call_type,call_address,address,reciprocal_in
    val sql =
      s"""
        |select
        |    user_id,
        |    people_id as phone,
        |    other_mobile,
        |    sum(cnt) total_cnt,
        |    count(*) month_cnt,
        |    sum(cnt)*1.0/count(1) month_call_cnt,
        |    sum(call_time)*1.0/3600 month_call_hours
        |from
        |    ( -- c 按照用户和月份进行聚合，得到每月通话总时间，总次数
        |    select
        |        user_id,people_id,other_mobile,yearmonth,count(*) cnt,sum(call_seconds) call_time
        |    from
        |        (-- b 比a多一个月份时间字段
        |        select
        |            user_id,people_id,other_mobile,other_address,call_start,
        |            call_seconds,call_type,call_address,address,reciprocal_in,
        |            cast( concat(substring(call_start,1,4),substring(call_start,6,2)) as int) AS yearmonth
        |        from
        |            (-- a 表示去重后的结果
        |            select *,
        |                row_number() over (partition by user_id,people_id,other_mobile,call_start,call_seconds order by call_type) AS num
        |            from
        |                $table where other_mobile is not null
        |                and other_mobile regexp '^1\\\\d{10}$$$$'
        |                and not other_mobile regexp '^10'
        |            ) a
        |            where a.num=1
        |        ) b
        |    group by user_id,people_id,other_mobile,yearmonth
        |    ) c group by user_id,people_id,other_mobile
      """.stripMargin

    spark.sql(sql)
  }


  def vertex_userid(spark:SparkSession): DataFrame ={

    val sql =
      """
        |select distinct userid from
        |(
        |select userid from registerInfo
        |union all
        |select userid from loanOrder
        |)
      """.stripMargin
    spark.sql(sql)
  }

  def vertex_orderid(spark:SparkSession): DataFrame ={

    val sql =
      """
        |select distinct orderid from loanOrder
      """.stripMargin
    spark.sql(sql)
  }

  def vertex_phone(spark:SparkSession): DataFrame ={

    val sql =
      """
        |select distinct phone from
        |(
        |select phone from registerInfo
        |union all
        |select phone from contactInfo
        |union all
        |select contact_phone as phone from contactInfo
        |union all
        |select phone from callDetail
        |union all
        |select other_mobile as phone from callDetail
        |)
      """.stripMargin
    spark.sql(sql)
  }




  def test1(spark: SparkSession): Unit ={


    val registerInfo = spark.read.option("header","true").csv(s"$input_path/callDetail.csv")

    registerInfo.show()
    registerInfo.createOrReplaceTempView("callDetail")

    val df = edge_call(spark,"callDetail")

    df.show()

    df.printSchema()
  }

  def test2(spark: SparkSession): Unit ={


    val registerInfo = spark.read.option("header","true").csv(s"$input_path/loanOrder.csv")

    registerInfo.show()
    registerInfo.createOrReplaceTempView("loanOrder")

    val df = edge_order(spark,"loanOrder")

    df.show()

    df.printSchema()
  }

  def test3(spark: SparkSession): Unit ={


    val registerInfo = spark.read.option("header","true").csv(s"$input_path/contactInfo.csv")

    registerInfo.show()
    registerInfo.createOrReplaceTempView("contactInfo")

    val df = edge_contact(spark,"contactInfo")

    df.show()

    df.printSchema()
  }

}
