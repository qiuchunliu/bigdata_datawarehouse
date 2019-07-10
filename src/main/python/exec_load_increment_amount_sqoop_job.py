# 执行增量导入的job任务，并将hdfs中的数据load到hive中
import os
import datetime

table_list = ['user_pc_click_log',
              'user_app_click_log',
              'user_order',
              'order_item',
              'order_delivery',
              'cart',
              'biz_trade'
              ]
# t_today = datetime.datetime.now().strftime('%Y%m%d')


def getYesterday():
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today-oneday
    return yesterday.strftime('%Y%m%d')


def executing():
    # 执行增量导入job任务
    for i in table_list:
        exec_str = "sqoop job -exec bap_" + i  # sqoop 要配全局变量
        os.system(exec_str)


def load_increment_data():
    # load数据到hive
    for i in table_list:
        load_str = "load data inpath '/qfbap/ods/tmp/ods_{0}' into table qfbap_ods.ods_{0} partition(dt={1});".\
            format(i, getYesterday())
        f = open("/projectfile/load_temp_file.hql", "w")
        f.write(load_str)
        f.close()
        os.system("hive -f /projectfile/load_temp_file.hql")  # hive 要全局变量


if __name__ == '__main__':
    executing()
    load_increment_data()
