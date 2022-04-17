import pandas
import pymysql
import sys
# 1.配置数据库连接
conn = pymysql.connect(host='localhost', port=3306, user='root', password='root', database='ne_resource', charset='utf8', autocommit=True)
# conn = pymysql.connect(host='10.30.107.206', port=3810, user='rc_llsf_province_user', password='u_M40mDB6y', database='rc_llsf_province', charset='utf8', autocommit=True)

# 2.调用方法返回uuid
def uuid():
    # 获取游标
    cursor = conn.cursor()
    # 执行sql
    cursor.execute('select uuid()')
    # 获取查询uuid数据
    all = cursor.fetchall()
    for i in all:
        for y in i:
            return y

pd = pandas.read_excel('./告警基础数据.xlsx', names=None)
pd['局向'].replace('A端', '1', inplace=True)
pd['局向'].replace('Z端', '2', inplace=True)

# open为内置函数，w参数是为了打开文件的写权限
a = open("./insert.sql", "w")
sys.stdout = a

for row in pd.itertuples():
    str1 = getattr(row, '告警名称')
    str2 = getattr(row, '设备类型')
    str3 = getattr(row, '网元名称')
    str4 = getattr(row, '告警发生时间')
    str5 = getattr(row, '告警清除时间')
    str6 = getattr(row, '局向')
    str7 = getattr(row, '工单流水号')
    # print(str1, str2, str3, str4, str5)
    strx = uuid()
    strx2 = uuid()
    sql_1 = '''INSERT INTO ru_alarm_verify (UUID, alarm_data_source, create_by, update_by, alarm_level , create_time, update_time, class_id,alarm_remark,alarm_title,alarm_object_type,alarm_object_name,alarm_create_time,alarm_recovery_time) VALUE('%s','后台跳过','admin','admin', '3',SYSDATE(),SYSDATE(),'','用户邮件申请后台跳过告警','%s','%s','%s','%s','%s');''' % (strx, str1, str2, str3, str4, str5)
    sql_2 = '''INSERT INTO ext_ru_alarm_verify (uuid, create_time, update_time) VALUE('%s',SYSDATE(),SYSDATE());''' % strx
    sql_3 = '''INSERT INTO ru_connectivity(UUID, real_connectivity_uuid, service_uuid, service_type, create_by, update_by, create_time, update_time, class_id) VALUES('%s','%s',(SELECT p.uuid FROM ru_sf_order rso,ru_product p WHERE rso.uuid = p.order_uuid AND order_no = '%s' AND p.direction = %s), '9', 'admin', 'admin', SYSDATE(),SYSDATE(), '');''' % (strx2, strx, str7, str6)
    # 输入print结果到文件
    print(sql_1, sql_2, sql_3, file=a)
