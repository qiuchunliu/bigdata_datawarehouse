-- 从 qfbap_dwd.dwd_user / qfbap_dwd.dwd_user_extend 查询数据 导入到MySQL的 dm_user_basic
-- 和 dm_user_basic(hive表)
-- 用户id,登录名	,用户性别,出生日期,年龄,星座,省份,城市	,
-- 城市等级,邮箱	,邮箱运营商	,手机号	,手机号段（前三位）,
-- 手机运营商,注册时间,登录ip,登录来源,邀请人,会员积分,已使用积分,
-- 会员等级名称,是否黑名单,是否结婚,学历,月收入	,职业,是否孕妇,
-- 是否有小孩,是否有车,使用手机品牌,使用手机等级,使用手机种类数量,
-- 更换手机数量	,是否马甲用户,马甲账户数量,用户忠诚度,用户购物类型,
-- 体重,身高

select
       us.user_id,
       us.user_name,
       us.user_gender,
       us.user_birthday,
       us.user_age,
       us.constellation,
       us.province,
       us.city,
       us.city_level,
       us.e_mail,
       us.op_mail,
       us.mobile,
       us.num_seg_mobile,
       us.op_mobile,
       us.register_time,
       us.login_ip,
       us.login_source,
       us.request_user,
       us.total_score,
       us.used_score,
       us.会员等级名称,
       us.is_blacklist,
       us.is_married,
       us.education,
       us.monthly_income,
       us.profession,
       ue.is_pregnant_woman,
       ue.is_have_children,
       ue.is_have_car,
       ue.phone_brand,
       ue.phone_brand_level,
       ue.phone_cnt,
       ue.change_phone_cnt,
       ue.is_maja,
       ue.majia_account_cnt,
       ue.loyal_mobile,
       ue.shopping_type_model,
       ue.weight,
       ue.height
from
     qfbap_dwd.dwd_user us
join
     qfbap_dwd.dwd_user_extend ue
on
    us.user_id = ue.user_id;

