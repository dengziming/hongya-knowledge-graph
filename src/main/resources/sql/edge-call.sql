select
    user_id,
    people_id as phone,
    other_mobile,
    sum(cnt) total_cnt,
    count(*) month_cnt,
    sum(cnt)*1.0/count(1) month_call_cnt,
    sum(call_time)*1.0/3600 month_call_hours
from
    ( -- c 按照用户和月份进行聚合，得到每月通话总时间，总次数
    select
        user_id,people_id,other_mobile,yearmonth,count(*) cnt,sum(call_seconds) call_time
    from
        (-- b 比a多一个月份时间字段
        select
            user_id,people_id,other_mobile,other_address,call_start,
            call_seconds,call_type,call_address,address,reciprocal_in,
            cast( concat(substring(call_start,1,4),substring(call_start,6,2)) as int) AS yearmonth
        from
            (-- a 表示去重后的结果
            select *,
                row_number() over (partition by user_id,people_id,other_mobile,call_start,call_seconds order by call_type) AS num
            from
                $table where other_mobile is not null
                and other_mobile regexp '^1\\d{10}$$'
                and not other_mobile regexp '^10'
            ) a
            where a.num=1
        ) b
    group by user_id,people_id,other_mobile,yearmonth
    ) c group by user_id,people_id,other_mobile