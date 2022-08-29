"""
    更新电路关联的ip地址:
        1.修改调度库存量库circuit_to_ip_address
        2.修改ip地址状态
"""
import pandas
import pymysql
import sys
import time
# open为内置函数，w参数是为了打开文件的写权限
a = open("./update.sql", "w")
sys.stdout = a

# 配置调度库连接信息
conn = pymysql.connect(host='10.30.107.156', port=3810, user='rc_tncm_user', password='W+R56Lr6Ky', database='rc_tncm', charset='utf8', autocommit=True)
# conn = pymysql.connect(host='localhost', port=3306, user='root', password='root', database='rc_tncm', charset='utf8', autocommit=True)

# 1.读取Excel数据到集合中
df = pandas.read_excel('C:\\Users\\HongBingHu\\Desktop\\IP地址替换8-29.xlsx', names=None)
# 2.读取数据存入二维列表中
A_list = df.values.tolist()

cursor = conn.cursor()
sql_1 = '''SELECT cp.uuid FROM circuit_to_ip_address cp,circuit ct 
	WHERE cp.circuit_uuid = ct.uuid AND ct.name = %s AND ip_address_name = %s limit 1
'''
# 定义一个列表
new_cir_ip_exists = []
new_cir_ip_no = []
c = 0
# 3.遍历列表查询uuid信息
for i in A_list:
    total = cursor.execute(sql_1,[i[0],i[1]])
    uuid = cursor.fetchall()
    #print(uuid[0][0])
    if total != 0:
        new_cir_ip_exists.append(i)
        new_cir_ip_exists[c].append(uuid[0][0])
    else:
        # 存入为查询到uuid的信息
        new_cir_ip_no.append(i)
    c = c + 1

# 遍历uuid存在的集合
print("###################################调度库与资源库更新电路ip地址信息############################################")
for i in new_cir_ip_exists:
    sql_2 = "UPDATE circuit_to_ip_address SET ip_address_name = '%s' WHERE UUID = '%s';" % (i[2], i[3])
    print(sql_2, file=a)
print("###################################调度库与资源库更新电路ip地址信息############################################")
print("###################################资源库更新新ip为占用地址信息############################################")
for i in new_cir_ip_exists:
    # 修改新ip为占用
    sql_3 = "UPDATE ipv4_addr set use_state = '1' WHERE NAME = '%s';" % (i[2])
    print(sql_3, file=a)
print("###################################资源库更新新ip为占用地址信息############################################")
print("###################################资源库更新老ip为空闲地址信息############################################")
for i in new_cir_ip_exists:
    # 修改老ip为空闲
    sql_4 = "UPDATE ipv4_addr set use_state = '2' WHERE NAME = '%s';" % (i[1])
    print(sql_4, file=a)
print("###################################资源库更新老ip为空闲地址信息############################################")

# 输出结果到excel中
st = time.strftime("%Y%m%d%H%M%S", time.localtime())
lujin = pandas.ExcelWriter('./电路ip地址更新信息导出' + st + '.xlsx')
pandas.DataFrame(new_cir_ip_exists).to_excel(lujin, sheet_name='已更新电路IP信息', index=None)
pandas.DataFrame(new_cir_ip_no).to_excel(lujin, sheet_name='ip+电路未查询数据信息', index=None)

lujin.save()
