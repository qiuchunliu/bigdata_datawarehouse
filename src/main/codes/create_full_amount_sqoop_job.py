# 创建从mysql全量拉取数据到hdfs的job任务
# 执行一次，将job任务创建出来即可

import os

table_list = ['user',
              'user_extend',
              'user_addr',
              'code_category'
              ]


def cre():
    for i in table_list:
        os.system("sqoop job --create bap_" +
                  i +
                  " -- import --connect jdbc:mysql://192.168.163.21:3"
                  "306/qfbap_ods --driver com.mysql.jdbc.Driver "
                  "--username root "
                  "--password 123456 "
                  "--table " + i +
                  " --delete-target-dir "
                  "--target-dir /qfbap/ods/tmp/ods_" + i +
                  "--fields-terminated-by '\001'"
                  )


if __name__ == '__main__':
    cre()
