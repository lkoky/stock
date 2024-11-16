# -*- coding: utf-8 -*-
# website: http://30daydo.com
# @Time : 2020/2/20 17:06
# @File : etf_info.py
# 获取etf的成分股数据

# 重构 2021-01-21

import datetime
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

        '''
        基本数据，没有仓位的
        拿到的只是上证的数据, ??? 中证吧
        :return:
        '''
        # http://www.csindex.com.cn/zh-CN/search/indices?about=1
        # https://www.csindex.com.cn/zh-CN/search/indices?about=1#/indices/family/list
        # 生成一个0到1之间的随机浮点数
        random_number = random.random()
        # 格式化随机数，保留16位小数
        formatted_random_number = f"{random_number:.16f}"
        r = requests.get(url='http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={0}&topline=30&year=&month=&rt={1}'.format(fund_code,formatted_random_number),
                         headers={'User-Agent': 'Molliza Firefox Chrome'})
        endIdx=r.text.find("arryear")
        html=r.text[23:endIdx-2]
        # print(html)
        #rank_rawdata = json.loads(_json)

        response = Selector(text=html)
        print(response)
        table = response.xpath('//table')
        index_list = table[0].xpath('.//tbody/tr')

        for idx in index_list:
            seq=idx.xpath('.//td[1]/text()').extract_first()
            code = idx.xpath('.//td[2]/a/text()').extract_first()
            # detail_url = idx.xpath('.//td[2]/a/@href').extract_first()
            name = idx.xpath('.//td[3]/a/text()').extract_first()
            # 净值比例
            _ratio=idx.xpath('.//td[7]/text()').extract_first()[:-1]
            # 持股数量
            _count= idx.xpath('.//td[8]/text()').extract_first()
            _count = _count.replace(',', '')
            # 持仓价值
            price = idx.xpath('.//td[9]/text()').extract_first()
            price=price.replace(',', '')


            obj = IndexObject(
                seq=int(seq),
                fund_code=fund_code,
                stock_code=code,
                # fund_url=detail_url,
                fund_name=name,
                nv_radio=float(_ratio),
                stock_num=float(_count),
                market_value=float(price)
            )

            try:
                self.sess.add(obj)
            except Exception as e:
                logger.error(e)
                self.sess.rollback()
            else:
                self.sess.commit()

    def check_content(self, content):
        if content is None:
            self.logger.error('获取内容为空')
            return False
        else:
            return True
    def full_etf_fund(self):
        # 清理数据
        select_sql = "select  fund_code  from db_ttjj_fund_ranking where fund_type =%s "
        result=self.execute(select_sql, ("etf"), self.get_conn(), self.logger)
        # self.sess.execute(select_sql,("etf"))
        if self.check_content(result) :
            for code in result :
                self.deleteByfundCode(code[0])
                try:
                    self.basic_info(code[0])
                except Exception as e:
                    logger.error('解析错误 {},基金：{}'.format(e, code[0]))
    def deleteByfundCode(self, fund_code):
        # 清理数据
        delete_sql = "delete from tb_etf_stock where fund_code =%s "
        self.execute(delete_sql, (fund_code), self.get_conn(), self.logger)

if __name__ == '__main__':
    app = IndexSpider(first_use=True)
    # app.basic_info()

    app.full_etf_fund()

    # app.etf_detail_with_product_inuse()
