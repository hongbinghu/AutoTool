"""
集客:楼宇数据提取
读取两个excel表格数据导入数据库
"""
import pandas
import sqlalchemy
import time
# 配置连接数据库信息
engine = sqlalchemy.create_engine("mysql+pymysql://root:root@localhost:3306/ne_resource", encoding='utf-8')
# 读取数据库excel信息
pd1 = pandas.read_excel('C:\\Users\\HongBingHu\\Desktop\\楼宇基础数据.xlsx', names=None, sheet_name='Sheet1')
pd2 = pandas.read_excel('C:\\Users\\HongBingHu\\Desktop\\楼宇基础数据.xlsx', names=None, sheet_name='Sheet2')
pd1.columns = ['sheng', 'city', 'quxian', 'jiedao', 'NAME', 'TYPE', 'weidu', 'jingdu', 'laiyuan', 'bianma', 'biaoshi', 'dizhiduan', 'dizhiwanz', 'jibie', 'dizhishiput', 'yipipei', 'weipipei', 'beizhu']
pd2.columns = ['xuhao', 'sheng', 'dishi', 'quxian', 'jiedao', 'name', 'type', 'louyuid', 'yn', 'x', 'y', 'loucount', 'beizhu1', 'beizhu2']
pd1.to_sql('zg_louyu', con=engine, if_exists='replace', index=False)
pd2.to_sql('wg_louyu', con=engine, if_exists='replace', index=False)

# 1.按地市汇总
sql_1 = '''
SELECT kk.dishi '地市',kk.集团楼宇数,tt.已录入楼宇数,CONCAT(tt.已录入楼宇数/kk.集团楼宇数 * 100,'%')'录入合规率' FROM (
SELECT IFNULL(dishi,'全省') dishi,COUNT(*) '集团楼宇数' FROM wg_louyu wl GROUP BY dishi WITH ROLLUP) kk
LEFT JOIN 
(SELECT IFNULL(w.dishi,'全省') dishi,COUNT(*) '已录入楼宇数' FROM wg_louyu w WHERE EXISTS(SELECT 1 FROM zg_louyu z WHERE z.city = w.dishi AND z.name = w.name) GROUP BY w.dishi WITH ROLLUP)tt
ON kk.dishi = tt.dishi
'''
# 2.按区县汇总
sql_2 = '''
SELECT kk.quxian '地市',kk.集团楼宇数,IFNULL(tt.已录入楼宇数,0)'已录入楼宇数',CONCAT(IFNULL(tt.已录入楼宇数,0)/kk.集团楼宇数 * 100,'%')'录入合规率' FROM (
SELECT IFNULL(quxian,'全省')'quxian',COUNT(*)'集团楼宇数' FROM wg_louyu GROUP BY quxian WITH ROLLUP)kk
LEFT JOIN
(SELECT IFNULL(w.quxian,'全省')'quxian',COUNT(*)'已录入楼宇数' FROM wg_louyu w WHERE EXISTS(SELECT 1 FROM zg_louyu z WHERE z.city = w.dishi AND z.name = w.name) GROUP BY quxian WITH ROLLUP)tt
ON kk.quxian = tt.quxian
'''
# 3.综资录入楼宇明细
sql_3 = '''
SELECT sheng '省',city '地市',quxian '区县',jiedao '街道',NAME '楼宇名称',TYPE '场景类型',weidu '纬度',jingdu '纬度',laiyuan '来源',bianma '楼宇编码',biaoshi '地址标识',dizhiduan '地址分段名称',dizhiwanz '地址完整名称',jibie '级别',dizhishiput '地址是否上传总部',yipipei '已覆盖地址是否有覆盖',weipipei '未匹配原因',beizhu '备注' FROM zg_louyu
'''
# 4.集团下发楼宇明细
sql_4 = '''
SELECT xuhao '序号',sheng '省',dishi '地市',quxian '区县',jiedao '街道门牌号',NAME '楼宇名称',TYPE '场景类型',louyuid '楼宇编码',yn '是否为金牌楼宇',X '经纬度X',Y '经纬度Y',loucount '楼内企业数量',beizhu1 '备注1-省内楼宇编码',beizhu2 '备注2-省内填报场景类型' FROM wg_louyu
'''
# 5.未录入楼宇明细
sql_5 = '''
SELECT xuhao '序号',sheng '省',dishi '地市',quxian '区县',jiedao '街道门牌号',NAME '楼宇名称',TYPE '场景类型',louyuid '楼宇编码',yn '是否为金牌楼宇',X '经纬度X',Y '经纬度Y',loucount '楼内企业数量',beizhu1 '备注1-省内楼宇编码',beizhu2 '备注2-省内填报场景类型' FROM wg_louyu w WHERE NOT EXISTS(SELECT 1 FROM zg_louyu z WHERE z.city = w.dishi AND z.name = w.name)
'''

# 获取系统当前时间,按指定格式输出
st = time.strftime("%Y%m%d%H%M%S", time.localtime())
# 设置excel文件生成路径
lujin = pandas.ExcelWriter('C:\\Users\\HongBingHu\\Desktop\\楼宇录入基本信息' + st + '.xlsx')

pandas.read_sql_query(sqlalchemy.text(sql_1), engine).to_excel(lujin, '地市汇总', index=False)
pandas.read_sql_query(sqlalchemy.text(sql_2), engine).to_excel(lujin, '区县汇总', index=False)
pandas.read_sql_query(sqlalchemy.text(sql_3), engine).to_excel(lujin, '综资录入楼宇明细', index=False)
pandas.read_sql_query(sqlalchemy.text(sql_4), engine).to_excel(lujin, '集团下发楼宇明细', index=False)
pandas.read_sql_query(sqlalchemy.text(sql_5), engine).to_excel(lujin, '未录入楼宇明细', index=False)
# 保存excel
lujin.save()

