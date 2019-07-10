# 执行创建的全量导入的job任务，并将hdfs中的数据load到hive中

import os

table_list = ['user',
              'user_extend',
              'user_addr',
              'code_category'
              ]


def executing():
    # 执行全量导入job任务
    for i in table_list:
        exec_str = "sqoop job -exec bap_" + i  # sqoop 要配全局变量
        os.system(exec_str)


def load_data_to_hive():
    # load数据到hive
    for i in table_list:
        load_str = "load data inpath '/qfbap/ods/tmp/ods_{0}' into table qfbap_ods.ods_{0};".format(i)
        f = open("/projectfile/load_temp_file.hql", "w")
        f.write(load_str)
        f.close()
        os.system("hive -f /projectfile/load_temp_file.hql")  # hive 要全局变量


if __name__ == '__main__':
    executing()
    load_data_to_hive()
