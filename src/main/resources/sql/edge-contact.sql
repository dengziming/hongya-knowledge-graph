

select
  regexp_replace(phones,'\\s+|^,|,$$|\\+86|\\-','') as phone,
  regexp_replace(contact_phone,'\\s+|\\+86|\\-','') as contact_phone,
  cast(userid as bigint) as user_id,
  regexp_replace(contact_name,'\\(.*?\\)|[^(a-zA-Z0-9\\u2E80-\\u9FFF)]|[\\( \\)《》。，〈〉一、⼂\\⼁]|\\s+|,|\'|"','') as contact_name , --- IDEA sql不识别"
  createtime  as createtime,
  lastmodifytime as lastmodifytime
from $table
where phones is not null
and contact_phone is not null
and regexp_replace(phones,'\\s+|^,|,$$|\\+86','') regexp '^1\\d{10}(,1\\d{10})*$$'
and regexp_replace(contact_phone,'\\s+|\\+86','') regexp '^1\\d{10}$$'
and not phones regexp '^10'
and not contact_phone regexp '^10';




select
    phone,
    contact_phone,
    user_id,
    createtime,
    lastmodifytime,
    case when contact_name is null then '' else contact_name end as contact_name,
    case when contact_name regexp '宝贝|亲爱' or
    contact_name regexp '^[老]?[爸|妈|爹|娘]' or
    (contact_name regexp '^(老婆|妻子|媳妇|老公|丈夫|儿子|女儿|老[头爷]子|丈母娘|丈人|婆婆|内人|内子|太太|夫人|外子|爱人)'
    and not contact_name   regexp '的') then 1
    when contact_name regexp '^[大小二三四五六七八九十]?[哥兄弟姐妹伯叔姑婶舅姨]'
    or contact_name regexp '^[爷奶]'
    or contact_name regexp '^老[大二三四五六七八九十]'
    then 2
    when  contact_name regexp '先生|老板|老板娘|小姐|女士|总$$|经理$$' then 3
    when  contact_name regexp '老师' then 4
    when  contact_name regexp '同学' then 5
    when  contact_name regexp '^老' then 6
    when  contact_name regexp '提额|大额|高额|白户|黑户|额度|面签|信用|消费|金融|融资|款|借|POS|套现'
            OR (contact_name regexp '贷|中介' and contact_name NOT regexp '房')
            OR (contact_name regexp '办|刷|养' and contact_name regexp '卡')
            OR (contact_name regexp '代' and contact_name regexp '办')
    then 7
    else 0
    end as relation
from
    (
    select
      phone,
      contact_phone,
      user_id,
      contact_name,
      createtime,
      lastmodifytime
    from
      $view
    where phone regexp '^1\\d{10}$$'
    UNION ALL
    select
      phone,
      contact_phone,
      user_id,
      contact_name,
      createtime,
      lastmodifytime
    from
      (
      select
         b.myphone phone,
         a.contact_phone,
         a.user_id,
         a.contact_name,
         a.createtime ,
         a.lastmodifytime
      from
        (select * from
          $view
         where  phone not regexp '^1\\d{10}$$'
        ) a
        lateral view
        explode(split(phone,',')) b as myphone
      ) c
    ) d
    where phone is not null
and contact_phone is not null
and user_id is not null
and phone regexp '^1\\d{10}$$'
and contact_phone regexp '^1\\d{10}$$'
and phone!=contact_phone;