-- 指标统计使用的sql

select
       user_id,
       max(in_time) over (partition by user_id) last_visit_time,
       last_value(session_id) over (partition by user_id order by in_time) last_session,
       last_value(cookie_id) over (partition by user_id order by in_time) last_cookie,
       last_value(pv) over (partition by user_id order by in_time) last_pv,
       last_value(browser_name) over (partition by user_id order by in_time) last_browser,
       last_value(visit_os) over (partition by user_id order by in_time) last_visit_os,
       min(in_time) over (partition by user_id) first_visit_time,
       first_value(session_id) over (partition by user_id order by in_time) first_session,
       first_value(cookie_id) over (partition by user_id order by in_time) first_cookie,
       first_value(pv) over (partition by user_id order by in_time) first_pv,
       first_value(browser_name) over (partition by user_id order by in_time) first_browser,
       first_value(visit_os) over (partition by user_id order by in_time) first_visit_os,
       case when in_time >= date_add(current_date, -6) then 1 end last_7_day,
       case when in_time >= date_add(current_date, -15) then 1 end last_15_day,
       case when in_time >= date_add(current_date, -30) then 1 end last_30_day,
       case when in_time >= date_add(current_date, -60) then 1 end last_60_day,
       case when in_time >= date_add(current_date, -90) then 1 end last_90_day,
       case when in_time >= date_add(current_date, -30) then 1 end last_30_day_,
       case when in_time >= date_add(current_date, -29) then pv end last_30_pv,
       case when in_time >= date_add(current_date, -29) and hour(in_time) between 0 and 5 then pv end 30_025,
       case when in_time >= date_add(current_date, -29) and hour(in_time) between 6 and 7 then pv end 30_627,
       case when in_time >= date_add(current_date, -29) and hour(in_time) between 8 and 9 then pv end 30_829,
       case when in_time >= date_add(current_date, -29) and hour(in_time) between 10 and 11 then pv end 30_10211,
       case when in_time >= date_add(current_date, -29) and hour(in_time) between 12 and 13 then pv end 30_12213,
       case when in_time >= date_add(current_date, -29) and hour(in_time) between 14 and 16 then pv end 30_14216,
       case when in_time >= date_add(current_date, -29) and hour(in_time) between 17 and 19 then pv end 30_17219,
       case when in_time >= date_add(current_date, -29) and hour(in_time) between 18 and 19 then pv end 30_18219,
       case when in_time >= date_add(current_date, -29) and hour(in_time) between 20 and 21 then pv end 30_20221,
       case when in_time >= date_add(current_date, -29) and hour(in_time) between 22 and 23 then pv end 30_22223
from
     qfbap_dwd.dwd_user_pc_pv;


select temp.user_id,
    temp.last_session,
    count(temp.l7)
from (
         select user_id,
                case when in_time >= date_add("2019-07-01", -6) then 1 end l7,
                last_value(session_id) over (partition by user_id order by in_time) last_session
         from qfbap_dwd.dwd_user_pc_pv
     ) temp
group by temp.user_id,temp.last_session
order by temp.user_id
