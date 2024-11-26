# -*- coding: utf-8 -*-
# @Time : 2022/12/29 16:29
# @File : A_stock_daily_info.py
# @Author : Rocky C@www.30daydo.com

# A 股日线行情导入数据库

import sys
sys.path.append('..')
from configure.settings import get_tushare_pro,DBSelector
import tushare as ts
from configure.util import calendar1
import time

class AStockBaseInfo():

    def __init__(self):
        self.pro = get_tushare_pro()
        self.conn = DBSelector().get_engine('stock','qq')

    # 获取所有股票列表，保存数据库
    def save_stock_basic(self):
        data = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        res = data.to_sql(name='stock_basic', con=self.conn,schema='stock', index=False, if_exists='replace', chunksize=5000)
        print(res)

    def run(self):
        data = self.pro.daily(ts_code='000002.SZ', start_date='20241120', end_date='20241125')
        # data = self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        print(data)
        print("")

        # date = calendar1('2022-01-01','2022-12-28')
        #
        # for d in date:
        #     print(d)
        #     df = self.pro.daily(trade_date=d)
        #     df.to_sql('tb_{}'.format(d),con=self.conn)
        #     time.sleep(1)

def main():
    app=AStockBaseInfo()
    #app.save_stock_basic()
    app.run()


if __name__ == '__main__':
    main()

    # import tushare as ts
    # # data= ts.get_hist_data('600848')
    # ts.set_token('99ac2ab94cad8c4898ef6797007b382585aa068f5ba7e411867d0ffa')
    # pro = ts.pro_api() # 初始化接口

    # data = pro.daily(ts_code='000001.SZ', start_date='20241120', end_date='20241125')
    # data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    # print(data)