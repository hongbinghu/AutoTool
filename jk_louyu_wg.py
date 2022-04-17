"""
导入集团楼宇基础数据:cmc_hb
"""
import pandas
import sqlalchemy
# 配置数据库连接信息
engine = sqlalchemy.create_engine("mysql+pymysql://cmc_hb_user:Ic_423GGIZ@10.30.106.68:3505/cmc_hb", encoding='utf-8')

pd1 = pandas.read_excel('./楼宇基础集团数据.xlsx', names=None)
pd1.columns = ['xuhao', 'sheng', 'dishi', 'quxian', 'jiedao', 'name', 'type', 'louyuid', 'yn', 'x', 'y', 'loucount', 'beizhu1', 'beizhu2']

pd1.to_sql('wg_louyu', con=engine, if_exists='replace', index=False)
