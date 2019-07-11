# 需要统计的表
# qfbap_ods.ods_user_pc_click_log
# qfbap_ods.ods_user_app_click_log

import datetime
import os


def get_yesterday():
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today-oneday
    return yesterday.strftime('%Y%m%d')


sql_dwd_user_pc_pv = "insert into table qfbap_dwd.dwd_user_pc_pv partition(dt={}) " \
                     "select " \
                     "max(log_id) `log_id`," \
                     "user_id," \
                     "session_id," \
                     "cookie_id," \
                     "min(visit_time) in_time," \
                     "max(visit_time) out_time," \
                     "case when max(visit_time) = min(visit_time) then 3 else " \
                        "max(visit_time) - min(visit_time) end stay_time," \
                     "count(1) pv," \
                     "visit_os," \
                     "browser_name," \
                     "visit_ip," \
                     "province," \
                     "city," \
                     "{} " \
                     "from " \
                     "qfbap_ods.ods_user_pc_click_log " \
                     "group by " \
                     "user_id," \
                     "session_id," \
                     "cookie_id," \
                     "visit_os," \
                     "browser_name," \
                     "visit_ip," \
                     "province," \
                     "city;".format(get_yesterday(), get_yesterday())


sql_dwd_user_app_pv = "insert into table qfbap_dwd.dwd_user_app_pv partition(dt={}) " \
                      "select " \
                      "log_id," \
                      "user_id," \
                      "imei," \
                      "log_time," \
                      "hour(log_time) log_hour," \
                      "visit_os," \
                      "os_version," \
                      "app_name," \
                      "app_version," \
                      "device_token," \
                      "visit_ip," \
                      "province," \
                      "city," \
                      "{} " \
                      "from " \
                      "qfbap_ods.ods_user_app_click_log;".format(get_yesterday(), get_yesterday())


def execut():
    os.system("hive -e '" + sql_dwd_user_pc_pv + "'")
    os.system("hive -e '" + sql_dwd_user_app_pv + "'")


if __name__ == '__main__':
    execut()
