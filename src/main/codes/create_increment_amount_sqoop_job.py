# 创建从mysql增量拉取数据到hdfs的job脚本
# 执行一次，将job创建出来即可

import os

table_list = [('user_pc_click_log', 'log_id'),
              ('user_app_click_log', 'log_id'),
              ('user_order', 'order_id'),
              ('order_item', 'user_id'),
              ('order_delivery', 'order_id'),
              ('cart', 'cart_id'),
              ('biz_trade', 'trade_id')
              ]


def create_increment_job():
    for i, j in table_list:
        os.system("sqoop job --create bap_" + i +
                  " -- import "
                  "--connect jdbc:mysql://192.168.163.21:3306/qfbap_ods "
                  "--driver com.mysql.jdbc.Driver "
                  "--username root "
                  "--password 123456 "
                  "--table " + i +
                  " --target-dir /qfbap/ods/tmp/ods_" + i +
                  " --fields-terminated-by '\001' "
                  "--check-column " + j +
                  " --incremental append "
                  "--last-value 0"
                  )


if __name__ == '__main__':
    create_increment_job()
