"""
调度库:circuit_to_ip_address同步资源库circuit_to_ip_address
"""
import pymysql
import time
# 配置数据库连接信息
# conn1 = pymysql.connect(host='localhost', port=3306, database='nc_resource', user='root', password='root', charset='utf8',autocommit=True)
# conn2 = pymysql.connect(host='localhost', port=3306, database='rc_tncm', user='root', password='root', charset='utf8',autocommit=True)
conn1 = pymysql.connect(host='10.30.107.152', port=3810, database='nc_resource', user='nc_resource_user', password='Li_9n8xTa6', charset='utf8', autocommit=True)
conn2 = pymysql.connect(host='10.30.107.156', port=3810, user='rc_tncm_user', password='W+R56Lr6Ky', database='rc_tncm', charset='utf8', autocommit=True)

sql_1 = '''select uuid from nc_resource.circuit_to_ip_address'''
sql_2 = '''select uuid from rc_tncm.circuit_to_ip_address'''
sql_3 = '''SELECT (SELECT NAME FROM rc_tncm.circuit WHERE UUID = cp.circuit_uuid) AS `circuit_name`,`remark`,`ip_adress_uuid`,`circuit_uuid`,`ip_address_type`,`ip_address_name`,`uuid`,`create_by`,`update_by`,`create_time`,`update_time`,`class_id`,`ipv6_addr_name`,`ipv6_addr_uuid` FROM `circuit_to_ip_address` cp where cp.uuid in %s'''
sql_4 = '''insert into nc_resource.circuit_to_ip_address values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) '''

# 获取游标信息,并执行sql,返回结果集,并存入列表,nc库circuit_to_ip_address查询结果
cursor1 = conn1.cursor()
cursor1.execute(sql_1)
nc_circuit_to_ip = cursor1.fetchall()
nc_ip = []
for i in nc_circuit_to_ip:
    for j in i:
        nc_ip.append(j)
# rc-tncm库查询结果
cursor2 = conn2.cursor()
cursor2.execute(sql_2)
tncm_circuit_to_ip = cursor2.fetchall()
tncm_ip = []
for i in tncm_circuit_to_ip:
    for j in i:
        tncm_ip.append(j)

# 求两个列表的差集,列表tncm_ip与列表nc_ip差集,使用集合高效
res = list(set(tncm_ip).difference(set(nc_ip)))
print('资源库数据差量为:%s' % len(res))
# 输出更新的uuid
st = time.strftime("%Y%m%d%H%M%S", time.localtime())
doc = open('./' + st + 'circuit_to_ip_address.log', 'w')
for i in res:
    print(i, file=doc)
# res = []
# for i in tncm_ip:
#     if i not in nc_ip:
#         res.append(i)
# 执行更新资源库不存在调度库的数据
if len(res) != 0:
    # 根据差集,资源中不存在调度库的数据
    cursor2 = conn2.cursor()
    cursor2.execute(sql_3, [res])
    rc_cha = cursor2.fetchall()
    # 将调度库数据插入资源库
    cursor1 = conn1.cursor()
    cursor1.executemany(sql_4, rc_cha)
print('数量差异执行完毕!!!')
# ------------------------------执行uuid相等,电路名称不等的数据更新--------------------------------------
sql_11 = '''select uuid,circuit_name from nc_resource.circuit_to_ip_address'''
sql_22 = '''SELECT UUID,(SELECT NAME FROM circuit WHERE UUID = cp.circuit_uuid) AS `circuit_name` FROM rc_tncm.circuit_to_ip_address cp'''
# 资源库
cursor1 = conn1.cursor()
cursor1.execute(sql_11)
nc_uuid_name = cursor1.fetchall()
list_nc_uuid_name = set()
for i in nc_uuid_name:
    list_nc_uuid_name.add(i)
# 调度库
cursor2 = conn2.cursor()
cursor2.execute(sql_22)
rc_uuid_name = cursor2.fetchall()
list_rc_uuid_name = set()
for i in rc_uuid_name:
    list_rc_uuid_name.add(i)
# uuid相等circuit_name不相等数据,存入新列表update_ip
# update_ip = {}
# for i in list_rc_uuid_name:
#     for j in list_nc_uuid_name:
#         if i[0] == j[0] and i[1] != j[1]:
#             update_ip.append(j)
# 存入集合
# 差集,前者独有的,调度库独有的需要更新资源库
res1 = list(list_rc_uuid_name.difference(list_nc_uuid_name))
# 查询出与资源库不一致的uuid
ex_tncm = []
for i in res1:
    ex_tncm.append(i[0])
print('调度库与资源库电路名称不一致数量为%s' %len(ex_tncm))
print('-------------------------------------------------------', file=doc)
sql_33 = '''SELECT UUID,(SELECT NAME FROM rc_tncm.circuit WHERE UUID = cp.circuit_uuid) AS `circuit_name` FROM rc_tncm.circuit_to_ip_address cp where uuid in %s'''
if len(ex_tncm) != 0:
    cursor2 = conn2.cursor()
    cursor2.execute(sql_33, [ex_tncm])
    findAll = cursor2.fetchall()
# 执行更新sql
    for i in findAll:
        print(i, file=doc)
        sql_44 = '''update circuit_to_ip_address set circuit_name = %s where uuid = %s'''
        cursor1 = conn1.cursor()
        cursor1.execute(sql_44, (i[1], i[0]))
print('数据更新完毕!!!')
