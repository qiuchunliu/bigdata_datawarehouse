# 将ods的数据导入到dwd层
# 要注意查询语句的字段名
# 要使用源表的字段名，否则有可能报错查不到

import datetime
import os


def get_yesterday():
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    yesterday = today-oneday
    return yesterday.strftime('%Y%m%d')


def get_today():
    return datetime.datetime.today().strftime('%Y%m%d')


sql_dwd_user = "insert into table qfbap_dwd.dwd_user " \
               "select " \
               "user_id," \
               "user_name," \
               "user_gender," \
               "user_birthday," \
               "user_age," \
               "constellation," \
               "province," \
               "city," \
               "city_level," \
               "e_mail," \
               "op_mail," \
               "mobile," \
               "num_seg_mobile," \
               "op_mobile," \
               "register_time," \
               "login_ip," \
               "login_source," \
               "request_user," \
               "total_score," \
               "used_score," \
               "is_blacklist," \
               "is_married," \
               "education," \
               "monthly_income," \
               "profession," \
               "create_date " \
               "from " \
               "qfbap_ods.ods_user;"


sql_dwd_user_extend = "insert into table qfbap_dwd.dwd_user_extend " \
                      "select " \
                      "user_id," \
                      "user_gender," \
                      "is_pregnant_woman," \
                      "is_have_children," \
                      "is_have_car," \
                      "phone_brand," \
                      "phone_brand_level," \
                      "phone_cnt," \
                      "change_phone_cnt," \
                      "is_maja," \
                      "majia_account_cnt," \
                      "loyal_model," \
                      "shopping_type_model," \
                      "weight," \
                      "height," \
                      "{} " \
                      "from " \
                      "qfbap_ods.ods_user_extend".format(get_today())


sql_dwd_user_addr = "insert into table qfbap_dwd.dwd_user_addr " \
                    "select " \
                    "user_id," \
                    "order_addr," \
                    "user_order_flag," \
                    "addr_id," \
                    "arear_id," \
                    "{} " \
                    "from " \
                    "qfbap_ods.ods_user_addr".format(get_today())


sql_dwd_user_order = "insert into table qfbap_dwd.dwd_us_order partition(dt={0}) " \
                     "select " \
                     "order_id," \
                     "order_no," \
                     "order_date," \
                     "user_id," \
                     "user_name," \
                     "order_money," \
                     "order_type," \
                     "order_status," \
                     "pay_status," \
                     "pay_type," \
                     "order_source," \
                     "update_time," \
                     "{1} " \
                     "from " \
                     "qfbap_ods.ods_user_order".format(get_yesterday(), get_today())


sql_dwd_order_item = "insert into table qfbap_dwd.dwd_order_item partition(dt={0}) " \
                     "select " \
                     "user_id," \
                     "order_id," \
                     "order_no," \
                     "goods_id," \
                     "goods_no," \
                     "goods_name," \
                     "goods_amount," \
                     "shop_id," \
                     "shop_name," \
                     "curr_price," \
                     "market_price," \
                     "discount," \
                     "cost_price," \
                     "first_cart," \
                     "first_cart_name," \
                     "second_cart," \
                     "second_cart_name," \
                     "third_cart," \
                     "third_cart_name," \
                     "goods_desc," \
                     "{1} " \
                     "from " \
                     "qfbap_ods.ods_order_item".format(get_yesterday(), get_today())


sql_dwd_order_delivery = "insert into table qfbap_dwd.dwd_order_delivery partition(dt={0}) " \
                         "select " \
                         "order_id," \
                         "order_no," \
                         "consignee," \
                         "area_id," \
                         "area_name," \
                         "address," \
                         "mobile," \
                         "phone," \
                         "coupon_id," \
                         "coupon_money," \
                         "carriage_money," \
                         "create_time," \
                         "update_time," \
                         "addr_id," \
                         "{1} " \
                         "from qfbap_ods.ods_order_delivery".format(get_yesterday(), get_today())


sql_dwd_cart = "insert into table qfbap_dwd.dwd_cart partition(dt={0}) " \
                     "select " \
                     "cart_id," \
                     "session_id," \
                     "user_id," \
                     "goods_id," \
                     "goods_num," \
                     "add_time," \
                     "cancle_time," \
                     "sumbit_time," \
                     "create_date," \
                     "{1} " \
                     "from qfbap_ods.ods_cart".format(get_yesterday(), get_today())


sql_dwd_biz_trade = "insert into table qfbap_dwd.dwd_biz_trade " \
                    "partition(dt={0}) " \
                    "select " \
                    "trade_id," \
                    "order_id," \
                    "user_id," \
                    "amount," \
                    "trade_type," \
                    "trade_time," \
                    "{1} " \
                    "from " \
                    "qfbap_ods.ods_biz_trade".format(get_yesterday(), get_today())


sql_dwd_code_category = "insert into table qfbap_dwd.dwd_code_category " \
                        "select " \
                        "first_category_id," \
                        "first_category_name," \
                        "second_category_id," \
                        "second_catery_name," \
                        "third_category_id," \
                        "third_category_name," \
                        "category_id," \
                        "{} " \
                        "from qfbap_ods.ods_code_category".format(get_today())


sql_list = [sql_dwd_user,
            sql_dwd_user_extend,
            sql_dwd_user_addr,
            sql_dwd_user_order,
            sql_dwd_order_item,
            sql_dwd_order_delivery,
            sql_dwd_cart,
            sql_dwd_biz_trade,
            sql_dwd_code_category
            ]


def load_data():
    for i in sql_list:
        os.system("hive -e '" + i + "'")


if __name__ == '__main__':
    load_data()
