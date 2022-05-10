"""
时间类详解
"""
import datetime
import time
# 1.记录程序运行开始时间
starttime = datetime.datetime.now()

# 休眠10秒
time.sleep(10)

# 2.记录程序运行结束时间
endtime = datetime.datetime.now()
# 3.计算程序运行时间
print("执行成功......共运行：", (endtime - starttime).seconds, 's')