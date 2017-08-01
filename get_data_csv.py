import tushare as ts
import pandas as pd
import os
import time

cur_time = time.localtime(time.time())
cur_year = cur_time.tm_year
cur_quarter = int(cur_time.tm_mon / 4) + 1

def save2Csv(data_frame, file_name, index_col=['code'], dtype_ = {'code': 'str'}):
    time1 = time.time()
    if os.path.exists(file_name):
        old_df = pd.read_csv(file_name, encoding='gbk', dtype=dtype_)
        old_df = old_df.set_index(index_col, drop=False)
        diff_data = data_frame[~data_frame.index.isin(old_df.index)]
        print("diff_data shape" + str(diff_data.shape))
        diff_data.to_csv(file_name, mode='a', header=None, index=False)
    else:
        data_frame.to_csv(file_name, index=False)
    print("save time" + str(time.time() - time1))




def getStockBasics():
    time1 = time.time()
    basics = ts.get_stock_basics()
    basics["code"] = basics.index
    print("request basics time" + str(time.time() - time1))
    file_name = './data/stock_basics.csv'
    # index 已经设置为 code, 不必再去重
    save2Csv(basics, file_name)


def getProfitData():
    year = range(2000, cur_year + 1)
    quarter = range(1, 5)
    file_name = './data/profit_data.csv'
    for y in year:
        for q in quarter:
            if y < cur_year or quarter < cur_quarter:
                profit_data = ts.get_profit_data(y, q)
                profit_data['year'] = y
                profit_data['quarter'] = q
                index = ['code', 'year', 'quarter']
                profit_data = profit_data.drop_duplicates(index)
                profit_data = profit_data.set_index(index, drop=False)
                save2Csv(profit_data, file_name, index_col=index)


# getStockBasics()
getProfitData()