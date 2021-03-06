create table dm_user_basic(
user_id bigint,
user_name string,
user_gender tinyint,
user_birthday string,
user_age int,
constellation string,
province string,
city string,
city_level tinyint,
e_mail string,
op_mail string,
mobile bigint,
num_seg_mobile int,
op_mobile string,
register_time string,
login_ip string,
login_source string,
request_user string,
total_score decimal(18,2),
used_score decimal(18,2),
user_level string,
is_blacklist int,
is_married int,
education string,
monthly_income decimal(18,2),
profession string,
is_pregnant_woman int,
is_have_children int,
is_have_car int,
phone_brand string,
phone_brand_level string,
phone_cnt int,
change_phone_cnt int,
is_maja int,
majia_account_cnt int,
loyal_model string,
shopping_type_model string,
weight int,
height int
)
location '/qfbap/dm/dm_user_basic';
