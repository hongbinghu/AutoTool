"""
清理trans_port重复端口数据:
    1.查询某个EMS下重复端口(分两步一步重复,一步rmuid为空)
    2.判断端口是否有关联电路
    3.判断端口是否有关联光路
    4.如果未关联光电路信息执行删除
"""
import pymysql
import time
st = time.strftime("%Y%m%d%H%M%S", time.localtime())
doc = open('./' + st + 'delete_Port.log', 'w')
# 1.配置资源库与调度库连接信息
conn1 = pymysql.connect(host='10.30.107.152', port=3810, database='nc_resource', user='nc_resource_user', password='Li_9n8xTa6', charset='utf8', autocommit=True)
conn2 = pymysql.connect(host='10.30.107.156', port=3810, user='rc_tncm_user', password='W+R56Lr6Ky', database='rc_tncm', charset='utf8', autocommit=True)

# 2.配置rmuid为空的sql
blank = '''SELECT tt.uuid FROM (
SELECT tp.uuid ,tp.name,tp.port_state ,tp.create_time ,tp.update_time ,tp.rmuid 
FROM trans_port tp WHERE TP.fdn LIKE 'EMS=ENS-T31-1-P%' AND NAME LIKE '恩施%' AND tp.rmuid = ''
) tt limit 10'''
# 2.1配置rmuid重复sql
repeat = '''SELECT tt.uuid FROM (
SELECT tp.uuid ,tp.name,tp.port_state ,tp.create_time ,tp.update_time ,tp.rmuid 
,row_number()over(PARTITION BY tp.rmuid  ORDER BY tp.create_time DESC) rn
FROM trans_port tp WHERE TP.fdn LIKE 'EMS=ENS-T31-1-P%' AND NAME LIKE '恩施%'
) tt WHERE tt.rn>1 AND tt.port_state=1
LIMIT 0, 20000'''
# 2.2查询端口是否有电路
circuit = '''SELECT c.* FROM circuit c,circuit_route cr,route_to_service rts,otn_service os
WHERE c.uuid = cr.circuit_uuid
AND cr.uuid = rts.circuit_route_uuid
AND rts.service_type = 3
AND rts.service_uuid = os.uuid
AND (os.orig1_port_uuid = %s OR os.orig2_trans_port_uuid = %s OR os.dest1_port_uuid = %s OR os.dest2_trans_port_uuid = %s)
UNION
SELECT c.* FROM circuit c,circuit_route cr,route_to_service rts,l2vpn_service os
WHERE c.uuid = cr.circuit_uuid
AND cr.uuid = rts.circuit_route_uuid
AND rts.service_type = 1
AND rts.service_uuid = os.uuid
AND (os.orig1_port_uuid = %s OR os.orig2_port_uuid = %s OR os.dest1_port_uuid = %s OR os.dest2_port_uuid = %s)
UNION
SELECT c.* FROM circuit c,circuit_route cr,route_to_service rts,l3vpn_ac_service os
WHERE c.uuid = cr.circuit_uuid
AND cr.uuid = rts.circuit_route_uuid
AND rts.service_type = 2
AND rts.service_uuid = os.uuid
AND (os.orig1_port_uuid = %s OR os.orig2_port_uuid = %s OR os.dest_port_uuid = %s)
UNION
SELECT c.* FROM circuit c,circuit_route cr,route_to_service rts,route_to_service mstp,otn_service os
WHERE c.uuid = cr.circuit_uuid
AND cr.uuid = rts.circuit_route_uuid
AND rts.uuid = mstp.circuit_route_uuid
AND mstp.service_type = 3
AND mstp.service_uuid = os.uuid
AND (os.orig1_port_uuid = %s OR os.orig2_trans_port_uuid = %s OR os.dest1_port_uuid = %s OR os.dest2_trans_port_uuid = %s)
UNION
SELECT c.* FROM circuit c,circuit_route cr,route_to_service rts,route_to_service mstp,l2vpn_service os
WHERE c.uuid = cr.circuit_uuid
AND cr.uuid = rts.circuit_route_uuid
AND rts.uuid = mstp.circuit_route_uuid
AND mstp.service_type = 1
AND mstp.service_uuid = os.uuid
AND (os.orig1_port_uuid = %s OR os.orig2_port_uuid = %s OR os.dest1_port_uuid = %s OR os.dest2_port_uuid = %s)
UNION
SELECT c.* FROM circuit c,circuit_route cr,route_to_service rts,route_to_service mstp,l3vpn_ac_service os
WHERE c.uuid = cr.circuit_uuid
AND cr.uuid = rts.circuit_route_uuid
AND rts.uuid = mstp.circuit_route_uuid
AND mstp.service_type = 2
AND mstp.service_uuid = os.uuid
AND (os.orig1_port_uuid = %s OR os.orig2_port_uuid = %s OR os.dest_port_uuid = %s)'''
# 2.3查询端口是否有光路
optical_way = '''SELECT * FROM optical_link ol WHERE (start_port_uuid = %s OR end_port_uuid = %s)'''
# 2.4执行删除
delete = '''delete from trans_port where uuid = %s'''
# 3.开始执行sql,查询rmuid为空是端口数量,存入列表
cursor1 = conn1.cursor()
total = cursor1.execute(blank)
print('rmuid为空的数据量为:' + str(total))
result = cursor1.fetchall()
blank_list = []
for i in result:
    for j in i:
        blank_list.append(j)
# 4.遍历列表,循环判断电路光路
print("-----------------------------------正在查询端口是否有业务,请稍后!!!--------------------------------------")
cursor2 = conn2.cursor()
for i in blank_list:
    row1 = cursor2.execute(circuit, [i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i])
    row2 = cursor1.execute(optical_way, [i, i])
    if row1 == 0 and row2 == 0:
        print('端口<'+i+'>无业务!!!')
        print(i, file=doc)
        print('端口<'+i+'>端口已删除!!!')
        cursor1.execute(delete, i)
        print(i + ' --端口删除成功!', file=doc)
    else:
        print('端口<'+i+'>有业务!!!')