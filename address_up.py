"""
根据集客标准地址导入模板更新集客标准地址
"""
import pandas
import pymysql
import sys

# open为内置函数，w参数是为了打开文件的写权限
a = open("./insert_address.sql", "w")
sys.stdout = a

conn3 = pymysql.connect(
    host='10.30.107.152',
    port=3810,
    database='nc_resource',
    user='nc_resource_user',
    password='Li_9n8xTa6',
    charset='utf8',
    autocommit=True
)

# conn3 = pymysql.connect(
#     host='localhost',
#     port=3306,
#     database='nc_resource',
#     user='root',
#     password='root',
#     charset='utf8',
#     autocommit=True
# )

# 读取Excel中B列A端标准地址信息
df = pandas.read_excel('./标准地址导入模板.        xlsx', usecols=[1], names=None)
# 读取数据中为空的nan替换成None
# df[pandas.isna(df)] = None
df.fillna('None', inplace=True)
# 返回list结构值
A_list = df.values.tolist()
# 将数据追加到列表中
A_result = []
for i in A_list:
    A_result.append(i[0])

# 查询标准地址是否存在
sql_1 = '''SELECT NAME,UUID FROM `nc_resource`.`address` WHERE NAME in %s'''
cursor = conn3.cursor()
cursor.execute(sql_1, [A_result])
# 返回查询结果
a_res = cursor.fetchall()

# 定义一个列表
chaxun = []
for i in a_res:
    chaxun.append(list(i))

# 读取A列B列数据存入列表AB = []
df1 = pandas.read_excel('./标准地址导入模板.xlsx', usecols=[0, 1], names=None)
# 替换Excel中的空值
df1.fillna('None', inplace=True)
# 返回list结构值,将AB列数据存入列表中
AB_list = df1.values.tolist()

# A端标准地址在存量中能查询到的数据 19条
new_AB = []
for i in chaxun:
    for k in AB_list:
        if i[0] == k[1]:
            new_AB.append(k)

c = 0
for i in new_AB:
    for j in chaxun:
        if i[1] == j[0]:
            new_AB[c].append(j[1])
            c = c + 1
for i in new_AB:
    sql_2 = "UPDATE design d SET d.standard_address='%s',d.address_uuid='%s' WHERE d.uuid = (SELECT real_connectivity_uuid FROM product p,connectivity c WHERE p.uuid = c.service_uuid AND c.service_type=1 AND p.product_code='%s' AND p.direction=1);" % (i[1], i[2], i[0])
    print(sql_2)

print('-- --------------------------------------------------------------------------------------')
# 读取Excel中C列Z端标准地址信息
df2 = pandas.read_excel('./标准地址导入模板.xlsx', usecols=[2], names=None)
# 读取数据中为空的nan替换成None
# df[pandas.isna(df)] = None
df2.fillna('None', inplace=True)
# 返回list结构值
Z_list = df2.values.tolist()
# 将数据追加到列表中
Z_result = []
for i in Z_list:
    Z_result.append(i[0])
# 查询标准地址是否存在
sql_3 = '''SELECT NAME,UUID FROM `nc_resource`.`address` WHERE NAME in %s'''
cursor = conn3.cursor()
cursor.execute(sql_3, [Z_result])
# 返回查询结果
Z_res = cursor.fetchall()

# 定义一个列表
Z_chaxun = []
for i in Z_res:
    Z_chaxun.append(list(i))

# 读取A列C列数据存入列表AB = []
df3 = pandas.read_excel('./标准地址导入模板.xlsx', usecols=[0, 2], names=None)
# 替换Excel中的空值
df3.fillna('None', inplace=True)
# 返回list结构值,将AB列数据存入列表中
AB_list2 = df3.values.tolist()

# Z端标准地址在存量中能查询到的数据 56条
new_AC = []
for i in Z_chaxun:
    for k in AB_list2:
        if i[0] == k[1]:
            new_AC.append(k)

cc = 0
for i in new_AC:
    for j in Z_chaxun:
        if i[1] == j[0]:
            new_AC[cc].append(j[1])
            cc = cc + 1
for i in new_AC:
    sql_4 = "UPDATE design d SET d.standard_address='%s',d.address_uuid='%s' WHERE d.uuid = (SELECT real_connectivity_uuid FROM product p,connectivity c WHERE p.uuid = c.service_uuid AND c.service_type=1 AND p.product_code='%s' AND p.direction=2);" % (i[1], i[2], i[0])
    print(sql_4)


# A端标准地址在存量中查不到的数据 AB_list:excel中AB列数据  new_AB:AB列标准地址存在数据  chaxun:查询返回结果
cp_AB = AB_list

def sc():
    for i in chaxun:
        for j in cp_AB:
            if i[0] == j[1]:
                cp_AB.remove(j)

for i in chaxun:
    for j in AB_list:
        if i[0] == j[1]:
            cp_AB.remove(j)
            sc()

# Z端标准地址在存量中查询不到的数据
cp_AC = AB_list2

def sc1():
    for i in Z_chaxun:
        for j in cp_AC:
            if i[0] == j[1]:
                cp_AC.remove(j)
for i in Z_chaxun:
    for j in cp_AC:
        if i[0] == j[1]:
            cp_AC.remove(j)
            sc1()


# 开始生成excel,基础数据,A端标准地址已更新数据,Z端标准地址已更新数据,A端标准地址未更新数据Z端标准地址未更新数据
ps1 = pandas.read_excel('./标准地址导入模板.xlsx', usecols='A:C', names=None)
# 替换Excel中的空值
ps1.fillna('', inplace=True)
# 返回list结构值,将AB列数据存入列表中
ps_list = ps1.values.tolist()


lujin = pandas.ExcelWriter('./标准地址更新数据导出.xlsx')

form_header = ['产品实例标识', 'A端标准地址', 'Z端标准地址']
form_header1 = ['产品实例标识', 'A端标准地址', 'A端标准地址uuid']
form_header2 = ['产品实例标识', 'Z端标准地址', 'Z端标准地址uuid']
form_header3 = ['产品实例标识', 'A端标准地址存量不存在']
form_header4 = ['产品实例标识', 'Z端标准地址存量不存在']
pandas.DataFrame(ps_list, columns=form_header).to_excel(lujin, sheet_name='基础数据', index=None)
pandas.DataFrame(new_AB, columns=form_header1).to_excel(lujin, sheet_name='A端标准地址已更新数据', index=None)
pandas.DataFrame(new_AC, columns=form_header2).to_excel(lujin, sheet_name='Z端标准地址已更新数据', index=None)
pandas.DataFrame(cp_AB, columns=form_header3).to_excel(lujin, sheet_name='A端标准地址不存数据', index=None)
pandas.DataFrame(cp_AC, columns=form_header4).to_excel(lujin, sheet_name='Z端标准地址不存在数据', index=None)

lujin.save()
