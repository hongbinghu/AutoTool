"""
传输设备调整流程归档报错:
    电路进行搬迁,站点在资源库不存在,导致无法搬迁
"""
import pymysql
import time

st = time.strftime("%Y%m%d%H%M%S", time.localtime())
doc = open('./' + st + 'siteBlank.log', 'w')

# 1.配置资源库与调度库连接信息
conn1 = pymysql.connect(host='10.30.107.152', port=3810, database='nc_resource', user='nc_resource_user', password='Li_9n8xTa6', charset='utf8', autocommit=True)
conn2 = pymysql.connect(host='10.30.107.156', port=3810, user='rc_tncm_user', password='W+R56Lr6Ky', database='rc_tncm', charset='utf8', autocommit=True)

# 1.1-配置sql资源库根据工单号查询网元uuid
ne_uuid = '''SELECT rp.trans_ne_uuid FROM ru_nccm_order ro,rc_business rc,t_nccm_trans_plan rp WHERE ro.uuid = rc.order_uuid AND rc.uuid = rp.rc_business_uuid AND ro.sheet_num = %s'''
# 2.从控制台输入工单号,查询网元uuid信息
sheet_no = input("请输入设备入网工单号:")
# 2.1开始查询数据
cursor1 = conn1.cursor()
cursor1.execute(ne_uuid, sheet_no)
ne_uuid_tuple = cursor1.fetchall()
# 2.1将查询出的数据存入列表
ne_uuid_list = []
for i in ne_uuid_tuple:
        for j in i:
            ne_uuid_list.append(j)
print('-------------------------工单中传输网元信息----------------------------', file=doc)
print(ne_uuid_list, file=doc)
# 3.查询电路调度库网元关联电路信息
tncm_site = '''
SELECT c.name,c.`a_site_uuid`,c.`z_site_uuid` FROM circuit c,circuit_route cr,route_to_service rts,otn_service os
WHERE c.uuid = cr.circuit_uuid
AND cr.uuid = rts.circuit_route_uuid
AND rts.service_type = 3
AND rts.service_uuid = os.uuid
AND (os.orig1_ne_uuid in %s OR os.orig2_trans_ne_uuid in %s OR os.dest1_ne_uuid in %s OR os.dest2_trans_ne_uuid in %s)
UNION
SELECT c.name,c.`a_site_uuid`,c.`z_site_uuid` FROM circuit c,circuit_route cr,route_to_service rts,l2vpn_service os
WHERE c.uuid = cr.circuit_uuid
AND cr.uuid = rts.circuit_route_uuid
AND rts.service_type = 1
AND rts.service_uuid = os.uuid
AND (os.orig1_ne_uuid in %s OR os.orig2_ne_uuid in %s OR os.dest1_ne_uuid in %s OR os.dest2_ne_uuid in %s)
UNION
SELECT c.name,c.`a_site_uuid`,c.`z_site_uuid` FROM circuit c,circuit_route cr,route_to_service rts,l3vpn_ac_service os
WHERE c.uuid = cr.circuit_uuid
AND cr.uuid = rts.circuit_route_uuid
AND rts.service_type = 2
AND rts.service_uuid = os.uuid
AND (os.orig1_ne_uuid in %s OR os.orig2_ne_uuid in %s OR os.dest_ne_uuid in %s)
UNION
SELECT c.name,c.`a_site_uuid`,c.`z_site_uuid` FROM circuit c,circuit_route cr,route_to_service rts,route_to_service mstp,otn_service os
WHERE c.uuid = cr.circuit_uuid
AND cr.uuid = rts.circuit_route_uuid
AND rts.uuid = mstp.circuit_route_uuid
AND mstp.service_type = 3
AND mstp.service_uuid = os.uuid
AND (os.orig1_ne_uuid in %s OR os.orig2_trans_ne_uuid in %s OR os.dest1_ne_uuid in %s OR os.dest2_trans_ne_uuid in %s)
UNION
SELECT c.name,c.`a_site_uuid`,c.`z_site_uuid` FROM circuit c,circuit_route cr,route_to_service rts,route_to_service mstp,l2vpn_service os
WHERE c.uuid = cr.circuit_uuid
AND cr.uuid = rts.circuit_route_uuid
AND rts.uuid = mstp.circuit_route_uuid
AND mstp.service_type = 1
AND mstp.service_uuid = os.uuid
AND (os.orig1_ne_uuid in %s OR os.orig2_ne_uuid in %s OR os.dest1_ne_uuid in %s OR os.dest2_ne_uuid in %s)
UNION
SELECT c.name,c.`a_site_uuid`,c.`z_site_uuid` FROM circuit c,circuit_route cr,route_to_service rts,route_to_service mstp,l3vpn_ac_service os
WHERE c.uuid = cr.circuit_uuid
AND cr.uuid = rts.circuit_route_uuid
AND rts.uuid = mstp.circuit_route_uuid
AND mstp.service_type = 2
AND mstp.service_uuid = os.uuid
AND (os.orig1_ne_uuid in %s OR os.orig2_ne_uuid in %s OR os.dest_ne_uuid in %s)
'''
# 3.1开始查询数据
cursor2 = conn2.cursor()
cursor2.execute(tncm_site, [ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list, ne_uuid_list])
tncm_site_tuple = cursor2.fetchall()
# 3.2遍历查询结果数据,输入AZ站点资源库不存在的电路信息
result = []
for i in tncm_site_tuple:
    sql = """select * from site where uuid = %s"""
    conn = conn1.cursor()
    row1 = conn.execute(sql, i[1])
    row2 = conn.execute(sql, i[2])
    if row1 == 0 or row2 == 0:
        result.append(list(i))
print("-------------------------传输电路所属站点信息在资源库不存在数据----------------------------", file=doc)
for i in result:
    print(i, file=doc)
