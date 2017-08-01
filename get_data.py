import tushare as ts
import os
import numpy as np
from sqlalchemy import create_engine, inspect, Table, Column, Integer, String, MetaData
from sqlalchemy.sql.expression import Cast
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects.mysql import \
        BIGINT, BINARY, BIT, BLOB, BOOLEAN, CHAR, DATE, \
        DATETIME, DECIMAL, DECIMAL, DOUBLE, ENUM, FLOAT, INTEGER, \
        LONGBLOB, LONGTEXT, MEDIUMBLOB, MEDIUMINT, MEDIUMTEXT, NCHAR, \
        NUMERIC, NVARCHAR, REAL, SET, SMALLINT, TEXT, TIME, TIMESTAMP, \
        TINYBLOB, TINYINT, TINYTEXT, VARBINARY, VARCHAR, YEAR

engine = create_engine('mysql+pymysql://root:@127.0.0.1/bonds?charset=utf8')
insp = inspect(engine)
def typeMap(type):
    if type == np.int64:
        return BIGINT()
    elif type == np.float64:
        return DOUBLE()
    elif type == np.object_:
        return VARCHAR(length=255)

def convertType(type_series, map_func):
    sql_type = {}
    for i in type_series.index:
        sql_type[i] = map_func(type_series[i].type)
    return sql_type


basics = ts.get_stock_basics()
basics['code'] = basics.index
basics_table_name = "stock_basics"
basics.to_sql(name=basics_table_name, con=engine, if_exists="append", index=False, dtype=convertType(basics.dtypes, typeMap))
if insp.get_primary_keys(basics_table_name) == []:#判断是否存在主键
    with engine.connect() as con:
        con.execute('ALTER TABLE `stock_basics` ADD PRIMARY KEY (`code`);')

years = range(2007, 2017) #2007-2016
quarter = range(1,5)
#
# profit_table_name = "profit_data"
# for y in years:
#     for q in quarter:
#         profit = ts.get_profit_data(y, q)
#         profit["year"] = y
#         profit["quarter"] = q
#         basics.to_sql(name=profit_table_name, con=engine, if_exists="append", index=False, dtype=convertType(profit.dtypes, typeMap))
#         if insp.get_primary_keys(profit_table_name) == []:#判断是否存在主键
#             with engine.connect() as con:
#                 con.execute('ALTER TABLE `stock_basics` ADD PRIMARY KEY (`code`, `year`, `quarter`);')

