import pymysql
import pandas
import time
import sys

# open为内置函数，w参数是为了打开文件的写的权限
a = open("./insert_address.sql", "w")
sys.stdout = a

# 配置数据库连接信息
# conn3 = pymysql.connect(host='localhost', port=3306, database='nc_resource', user='root', password='root', charset='utf8', autocommit=True)
conn3 = pymysql.connect(host='10.30.107.152', port=3810, database='nc_resource', user='nc_resource_user', password='Li_9n8xTa6', charset='utf8', autocommit=True)
# pandas读取Excel中A端标准地址信息
A = pandas.read_excel('./标准地址导入模板.xlsx', usecols=[1], names=None)
# 读取到的控制替换成None
A.fillna('None', inplace=True)
# 读取结果存入list
A_list = A.values.tolist()
# 将列表中的数据遍历出存入新的列表
A_new_list = []
for i in A_list:
    A_new_list.append(i[0])
# 查询标准地址在数据库中是否存在
sql_1 = '''SELECT tt.name,tt.uuid FROM (SELECT NAME,UUID,row_number() OVER (PARTITION BY NAME) AS bm FROM address WHERE NAME IN %s)tt WHERE tt.bm = 1'''
cursor = conn3.cursor()
cursor.execute(sql_1, [A_new_list])
a_res = cursor.fetchall()
# 定义一个列表,存入A端标准地址在数据库中存在的信息
A_chaxun = []
for i in a_res:
    A_chaxun.append(list(i))
# --------------------------------以上以列表的形式返回A端标准地址在数据中存在的数据---------------------------------------------
# 读取AB列数据存入列表AB[]
AB = pandas.read_excel('./标准地址导入模板.xlsx', usecols=[0, 1], names=None)
# 替换Excel中的空值
AB.fillna('', inplace=True)
# 返回list结构值,将AB列数据存入列表中
AB_list = AB.values.tolist()
# ---------------------------------以下开始判断Excel中A端标准地址存在的数据---------------------------------------------
# 定义一个新列表将存在的标准地址信息存入新列表
A_exists = []
for i in A_chaxun:
    for j in AB_list:
        if i[0] == j[1]:
            A_exists.append(j)
# 将标准地址存在的数据,列表中加一列uuid数据
count = 0
for i in A_exists:
    for j in A_chaxun:
        if i[1] == j[0]:
            A_exists[count].append(j[1])
    count = count + 1
# A端标准地址存在信息输出sql
for i in A_exists:
    sql_X = "UPDATE design d SET d.standard_address='%s',d.address_uuid='%s' WHERE d.uuid = (SELECT real_connectivity_uuid FROM product p,connectivity c WHERE p.uuid = c.service_uuid AND c.service_type=1 AND p.product_code='%s' AND p.direction=1);" % (i[1], i[2], i[0])
    print(sql_X, file=a)
# ---------------------------------以下开始判断Excel中A端标准地址不存在存在的数据---------------------------------------------
# 将ExcelAB列数据复制一份,A_chaxun为标准地址在存量中存在
# 复制一份AB列数据
A_cp_list = AB_list
def sc():
    for i in A_chaxun:
        for j in A_cp_list:
            if i[0] == j[1]:
                A_cp_list.remove(j)
for i in A_chaxun:
    for j in A_cp_list:
        if i[0] == j[1]:
            A_cp_list.remove(j)
    sc()
# --------------------------------------------------------------------------------------------------------------
# -------------------------------------------------计算Z端信息-------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------
# pandas读取Excel中Z端标准地址信息
Z = pandas.read_excel('./标准地址导入模板.xlsx', usecols=[2], names=None)
# 读取到的空数据替换成None
Z.fillna('None', inplace=True)
# 读取结果存入list
Z_list = Z.values.tolist()
# 将列表中的数据遍历出存入新的列表
Z_new_list = []
for i in Z_list:
    Z_new_list.append(i[0])
# 查询标准地址在数据库中是否存在
sql_2 = '''SELECT tt.name,tt.uuid FROM (SELECT NAME,UUID,row_number() OVER (PARTITION BY NAME) AS bm FROM address WHERE NAME IN %s)tt WHERE tt.bm = 1'''
cursor = conn3.cursor()
cursor.execute(sql_2, [Z_new_list])
Z_res = cursor.fetchall()
# 定义一个列表,存入Z端标准地址在数据库中存在的信息
Z_chaxun = []
for i in Z_res:
    Z_chaxun.append(list(i))
# --------------------------------以上以列表的形式返回z端标准地址在数据中存在的数据---------------------------------------------
# 读取AB列数据存入列表AC[]
AC = pandas.read_excel('./标准地址导入模板.xlsx', usecols=[0, 2], names=None)
# 替换Excel中的空值
AC.fillna('', inplace=True)
# 返回list结构值,将AC列数据存入列表中
AC_list = AC.values.tolist()
# --------------------------------------------------------------------------------------------------------------
# ---------------------------------以下开始判断Excel中Z端标准地址存在的数据---------------------------------------------
# 定义一个新列表将存在的标准地址信息存入新列表
Z_exists = []
for i in Z_chaxun:
    for j in AC_list:
        if i[0] == j[1]:
            Z_exists.append(j)
# 将标准地址存在的数据,列表中加一列uuid数据
count = 0
for i in Z_exists:
    for j in Z_chaxun:
        if i[1] == j[0]:
            Z_exists[count].append(j[1])
    count = count + 1
for i in Z_exists:
    sql_Z = "UPDATE design d SET d.standard_address='%s',d.address_uuid='%s' WHERE d.uuid = (SELECT real_connectivity_uuid FROM product p,connectivity c WHERE p.uuid = c.service_uuid AND c.service_type=1 AND p.product_code='%s' AND p.direction=2);" % (i[1], i[2], i[0])
    print(sql_Z, file=a)
# ------------------------
# ---------以下开始判断Excel中Z端标准地址不存在存在的数据---------------------------------------------
# 将ExcelAC列数据复制一份,Z_chaxun为标准地址在存量中存在
# 复制一份AB列数据
Z_cp_list = AC_list
def zsc():
    for i in Z_chaxun:
        for j in Z_cp_list:
            if i[0] == j[1]:
                Z_cp_list.remove(j)
for i in Z_chaxun:
    for j in Z_cp_list:
        if i[0] == j[1]:
            Z_cp_list.remove(j)
    zsc()
# --------------------------------------------------------------------------------------------------------------
pd = pandas.read_excel('./标准地址导入模板.xlsx', usecols='A:C', names=None)
pd.fillna('', inplace=True)
pdlist = pd.values.tolist()
# --------------------------------------------------------------------------------------------------------------
# 获取系统当前时间,按指定格式输出
st = time.strftime("%Y%m%d%H%M%S", time.localtime())
lujin = pandas.ExcelWriter('./标准地址更新数据导出' + st + '.xlsx')

form_header = ['产品实例标识', 'A端标准地址', 'Z端标准地址']
form_header1 = ['产品实例标识', 'A端标准地址', 'A端标准地址uuid']
form_header2 = ['产品实例标识', 'A端标准地址']
form_header3 = ['产品实例标识', 'Z端标准地址', 'Z端标准地址uuid']
form_header4 = ['产品实例标识', 'Z端标准地址']
pandas.DataFrame(pdlist, columns=form_header).to_excel(lujin, sheet_name='基础数据', index=None)
pandas.DataFrame(A_exists, columns=form_header1).to_excel(lujin, sheet_name='A端标准地址已更新数据', index=None)
pandas.DataFrame(A_cp_list, columns=form_header2).to_excel(lujin, sheet_name='A端标准地址不存在数据', index=None)
pandas.DataFrame(Z_exists, columns=form_header3).to_excel(lujin, sheet_name='Z端标准地址已更新数据', index=None)
pandas.DataFrame(Z_cp_list, columns=form_header4).to_excel(lujin, sheet_name='Z端标准地址不存在数据', index=None)

lujin.save()
