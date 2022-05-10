"""
跨数据库更新表数据:
    1.将查询出来的数据存入元组中
    2.执行insert 语句插入(使用此方法插入:executemany)
"""
import pymysql

# 1.配置数据库连接信息
conn1 = pymysql.connect(host='localhost', port=3306, database='test', user='root', password='root', charset='utf8', autocommit=True)
conn2 = pymysql.connect(host='localhost', port=3306, database='test1', user='root', password='root', charset='utf8', autocommit=True)

# 2.定义查询语句
sql_1 = "select * from address"

# 获取两个连接游标
cursor1 = conn1.cursor()
cursor2 = conn2.cursor()

cursor1.execute(sql_1)
result1 = cursor1.fetchall()

# 插入多条数据使用 executemany效率更高result1:为元组嵌套元组
sql_2 = "insert into address values (%s, %s)"
cursor2.executemany(sql_2, result1)

# 插入单条数据使用execute,需先遍历数据,一条一条执行sql脚本
values = ()
for i in range(len(result1)):
    values = (result1[i])
    sql_2 = "insert into address values (%s, %s)"
    cursor2.execute(sql_2, values)