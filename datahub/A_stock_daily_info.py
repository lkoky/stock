# -*- coding: utf-8 -*-
# @Time : 2022/12/29 16:29
# @File : A_stock_daily_info.py
# @Author : Rocky C@www.30daydo.com

# A 股日线行情导入数据库

import sys
sys.path.append('..')
from configure.settings import get_tushare_pro_xc,DBSelector
from configure.util import calendar1
import time

class AStockDailyInfo():

    def __init__(self):
        self.pro = get_tushare_pro_xc()
        self.conn = DBSelector().get_engine('stock','qq')


    def run(self):
        date = calendar1('2022-01-01','2022-12-28')

        for d in date:
            print(d)
            df = self.pro.daily(trade_date=d)
            df.to_sql('tb_{}'.format(d),con=self.conn)
            time.sleep(1)

def main():
    app=AStockDailyInfo()
    app.run()


if __name__ == '__main__':
    main()