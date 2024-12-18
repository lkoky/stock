# -*- coding: utf-8 -*-
# website: http://30daydo.com
# @Time : 2020/2/20 17:06
# @File : etf_info.py
# 获取etf的成分股数据

# 重构 2021-01-21

#import datetime
import time
from datetime import date, datetime
import pymongo
import re
import requests
import sys
from parsel.selector import Selector
from sqlalchemy.orm import sessionmaker
from loguru import logger
import random
import demjson3
import json
sys.path.append('..')

from common.BaseService import BaseService
from configure.settings import DBSelector
from fund.ttjj_etf_models import IndexObject, Base

TIMEOUT = 30 # 超时


class Fund(BaseService):

    def __init__(self, first_use=False):
        super(Fund, self).__init__(f'../log/{self.__class__.__name__}.log')
        self.first_use = first_use
        self.engine = self.get_engine()

    def get_engine(self):
        return DBSelector().get_engine('db_fund')

    def create_table(self):
        # 初始化数据库连接:
        Base.metadata.create_all(self.engine)  # 创建表结构

    def get_session(self):
        return sessionmaker(bind=self.engine)

    def get_conn(self):
        try:
            DB = DBSelector()
            conn = DB.get_mysql_conn('db_fund', 'qq')
            #cursor = conn.cursor()
            return conn
        except Exception as e:
            print(e)



    def get(self, url, retry=5, js=True):
        start = 0
        while start < retry:
            try:
                response = self.session.get(url, headers=self.headers,
                                            verify=False)
            except Exception as e:
                self.logger.error(e)
                start += 1

            else:
                if js:
                    content = response.json()
                else:
                    content = response.text

                return content

        if start == retry:
            self.logger.error('重试太多')
            return None


class IndexSpider(Fund):

    def __init__(self, first_use=False):
        super(IndexSpider, self).__init__(first_use)

        if first_use:
            self.create_table()

        self.sess = self.get_session()()

    def basic_info(self,fund_code):
        global endDate
        print(" 获取基金持仓股票信息：%s" %(fund_code))
        # http://www.csindex.com.cn/zh-CN/search/indices?about=1
        # https://www.csindex.com.cn/zh-CN/search/indices?about=1#/indices/family/list

        # 获取当前日期
        today = date.today()
        # 获取当前月份
        current_year = today.year
        current_month = today.month

        # print(f"当前月份是：{current_month}月")
        month=current_month
        if current_month>=12 or current_month<=2  :
            month=12
        elif current_month>=9 or current_month<=11 :
            month=9
        elif current_month>=6  or current_month<=8 :
            month=6
        elif current_month>=3 or current_month<=5 :
            month=3

        if self.getEndMonth(fund_code,current_year,month) > 5:
            print("当前月份跳过")
            return
        self.delete_By_fundCode(fund_code,current_year,month)
        # 生成一个0到1之间的随机浮点数
        random_number = random.random()
        # 格式化随机数，保留16位小数
        formatted_random_number = f"{random_number:.16f}"
        _url = 'http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={0}&topline=10&year=&month={1}&rt={2}'.format(
            fund_code, month, formatted_random_number)
        print(f"请求路径：{_url}")
        r = requests.get(url=_url,headers = {'User-Agent': 'Molliza Firefox Chrome'})
        endIdx=r.text.find("arryear")
        html=r.text[23:endIdx-2]
        # print(html)
        #rank_rawdata = json.loads(_json)
        if html=='' :
            self.logger.info('获取内容为空')
            return

        response = Selector(text=html)

        # print(response)
        label=response.xpath('//label[@class="right lab2 xq505"]')
        endDateStr=label[0].xpath(".//font/text()").extract_first()
        if endDateStr != None:
            endDate = datetime.strptime(endDateStr,"%Y-%m-%d")
            month=endDate.month
            # print(f"月份是: {month}")

        table = response.xpath('//table')
        index_list = table[0].xpath('.//tbody/tr')

        print(f"基金：{fund_code}，持仓top股票数量：{len(index_list)}，截止日期：{endDateStr}")

        seen = set()
        for idx in index_list:
            td_list = idx.xpath('.//td')

            seq=idx.xpath('.//td[1]/text()').extract_first()
            seq=seq.replace("*","")
            code = idx.xpath('.//td[2]/a/text()').extract_first()
            if code in seen:
                continue
            seen.add(code)
            # detail_url = idx.xpath('.//td[2]/a/@href').extract_first()
            name = idx.xpath('.//td[3]/a/text()').extract_first()

            if len(td_list) > 7:
                # 净值比例
                _ratio=idx.xpath('.//td[7]/text()').extract_first()[:-1]
                # 持股数量
                _count= idx.xpath('.//td[8]/text()').extract_first()
                # 持仓价值
                price = idx.xpath('.//td[9]/text()').extract_first()
            else:
                # 净值比例
                _ratio = idx.xpath('.//td[5]/text()').extract_first()[:-1]
                # 持股数量
                _count = idx.xpath('.//td[6]/text()').extract_first()
                # 持仓价值
                price = idx.xpath('.//td[7]/text()').extract_first()

            _count = _count.replace(',', '') if _count != None else "0"
            price = price.replace(',', '') if price != None else "0"

            obj = IndexObject(
                seq=int(seq),
                fund_code=fund_code,
                stock_code=code,
                # stock_url=detail_url,
                stock_name=name,
                nv_radio=float(_ratio),
                stock_num=float(_count),
                market_value=float(price),
                end_month=month,
                end_date = endDate,
                end_year=endDate.year
            )
            # print(obj.stock_code)

            try:
                self.sess.add(obj)
            except Exception as e:
                logger.error(e)
                self.sess.rollback()
            else:
                self.sess.commit()
        time.sleep(0.25)

    def check_content(self, content):
        if content is None:
            self.logger.error('获取内容为空')
            return False
        else:
            return True
    def full_etf_fund(self,type="etf"):
        # 清理数据
        select_sql = "select fund_code from db_ttjj_fund_ranking where fund_type = %s "

        result=self.execute(select_sql, (type), self.get_conn(), self.logger)
        # self.sess.execute(select_sql,("etf"))
        if self.check_content(result) :
            for code in result :
                # self.deleteByfundCode(code[0])
                # self.basic_info(code[0])
                # 511180 511380
                try:
                    # self.current_year("159718")
                    # self.basic_info("159934")
                    self.basic_info(code[0])

                except Exception as e:
                    logger.error('解析错误 {},基金：{}'.format(e, code[0]))
                #time.sleep(0.25)

    def delete_By_fundCode(self, fund_code,year,month):
        # 清理数据
        delete_sql = "delete from tb_etf_stock where fund_code =%s and end_year=%s and end_month>=%s"
        self.execute(delete_sql, (fund_code,year,month), self.get_conn(), self.logger)

    def getEndMonth(self, fund_code,year,month):
        # 清理数据
        # select_sql = "select count(1) from tb_etf_stock where fund_code ='%s' and end_year=%d and end_month>=%d" %(fund_code,year,month)
        select_sql = "select count(1) from tb_etf_stock where fund_code =%s and end_year=%s and end_month>=%s"
        rt=self.execute(select_sql, (fund_code,year,month), self.get_conn(), self.logger)
        #if len(rt) <=0 : return 0
        return int(rt[0][0]) if rt[0][0] !=None else 0

if __name__ == '__main__':
    app = IndexSpider(first_use=True)
    # app.basic_info()

    # 股票 混合 债券 qdii fof etf
    app.full_etf_fund("股票")
    # app.full_etf_fund("混合")
    app.full_etf_fund("etf")

    # app.etf_detail_with_product_inuse()
