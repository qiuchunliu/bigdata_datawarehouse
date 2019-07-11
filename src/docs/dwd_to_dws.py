# 从dwd层查询插入到dws层
import os
import datetime


def get_yesterday():  # 获取前一天日期，并按格式输出
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today - oneday
    return yesterday.strftime('%Y%m%d')


sql_dwd_to_dws = "insert into table " \
                 "qfbap_dws.dws_user_visit_month1 " \
                 "partition(dt={0}) " \
                 "select " \
                 "temp.user_id," \
                 "temp.type," \
                 "temp.content," \
                 "temp.cnt," \
                 "row_number() over(partition by temp.user_id, temp.type order by temp.cnt) rn," \
                 "{0} dw_date " \
                 "from " \
                 "(" \
                 "select " \
                 "user_id," \
                 "'visit_ip' type," \
                 "visit_ip content," \
                 "sum(pv) cnt " \
                 "from " \
                 "qfbap_dwd.dwd_user_pc_pv " \
                 "where unix_timestamp(in_time) > (unix_timestamp() - 29 * 24 * 3600) " \
                 "group by user_id,visit_ip " \
                 "union " \
                 "select " \
                 "user_id," \
                 "'cookie_id' type," \
                 "cookie_id content," \
                 "sum(pv) cnt " \
                 "from " \
                 "qfbap_dwd.dwd_user_pc_pv " \
                 "where unix_timestamp(in_time) > (unix_timestamp() - 29 * 24 * 3600) " \
                 "group by " \
                 "user_id," \
                 "cookie_id " \
                 "union " \
                 "select " \
                 "user_id," \
                 "'visit_os' type," \
                 "visit_os content," \
                 "sum(pv) cnt " \
                 "from " \
                 "qfbap_dwd.dwd_user_pc_pv " \
                 "where unix_timestamp(in_time) > (unix_timestamp() - 29 * 24 * 3600) " \
                 "group by " \
                 "user_id," \
                 "visit_os " \
                 "union " \
                 "select " \
                 "user_id," \
                 "'browser_name' type," \
                 "browser_name content," \
                 "sum(pv) cnt " \
                 "from " \
                 "qfbap_dwd.dwd_user_pc_pv " \
                 "where unix_timestamp(in_time) > (unix_timestamp() - 29 * 24 * 3600) " \
                 "group by " \
                 "user_id," \
                 "browser_name" \
                 ") temp".format(get_yesterday())

if __name__ == '__main__':
    os.system("hive -e '" + sql_dwd_to_dws + "'")
