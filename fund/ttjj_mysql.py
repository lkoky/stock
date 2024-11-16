# -*- coding: utf-8 -*-
# website: http://30daydo.com
# @Time : 2020/8/26 19:58
# @File : ttjj.py

import sys
# import execjs
# import fire
from parsel import Selector
import demjson3
sys.path.append('..')
import requests
import datetime
import time
import json
from configure.settings import DBSelector
from common.BaseService import BaseService
import loguru
import re
import random


LOG = loguru.logger
now = datetime.datetime.now()
TODAY = now.strftime('%Y-%m-%d')
_time = now.strftime('%H:%M:%S')

try:
    DB = DBSelector()
    conn = DB.get_mysql_conn('db_fund', 'qq')
    cursor = conn.cursor()
except Exception as e:
    print(e)


class TTFund(BaseService):
    ''''
    爬取天天基金网的排名数据
    '''

    def __init__(self, key='股票'):
        super(TTFund, self).__init__()

        self.ft_dict = {'混合': 'hh',  # 类型 gp： 股票 hh： 混合
                        '股票': 'gp',
                        'qdii': 'qdii',
                        'lof': 'lof', # not working now
                        'fof': 'fof',
                        '指数': 'zs',
                        '债券': 'zq',
                        'etf': 'etf'
                        }
        self.key = key
        self.date_format = datetime.datetime.now().strftime('%Y_%m_%d')
        # self.date_format = '2021_12_15'

    @property
    def headers(self):
        return {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh,en;q=0.9,en-US;q=0.8,zh-CN;q=0.7",
            "Cache-Control": "no-cache",
            "Cookie": "AUTH_FUND.EASTMONEY.COM_GSJZ=AUTH*TTJJ*TOKEN; em_hq_fls=js; HAList=a-sh-603707-%u5065%u53CB%u80A1%u4EFD%2Ca-sz-300999-%u91D1%u9F99%u9C7C%2Ca-sh-605338-%u5DF4%u6BD4%u98DF%u54C1%2Ca-sh-600837-%u6D77%u901A%u8BC1%u5238%2Ca-sh-600030-%u4E2D%u4FE1%u8BC1%u5238%2Ca-sz-300059-%u4E1C%u65B9%u8D22%u5BCC%2Cd-hk-06185; EMFUND1=null; EMFUND2=null; EMFUND3=null; EMFUND4=null; qgqp_b_id=956b72f8de13e912a4fc731a7845a6f8; searchbar_code=163407_588080_501077_163406_001665_001664_007049_004433_005827_110011; EMFUND0=null; EMFUND5=02-24%2019%3A30%3A19@%23%24%u5357%u65B9%u6709%u8272%u91D1%u5C5EETF%u8054%u63A5C@%23%24004433; EMFUND6=02-24%2021%3A46%3A42@%23%24%u5357%u65B9%u4E2D%u8BC1%u7533%u4E07%u6709%u8272%u91D1%u5C5EETF@%23%24512400; EMFUND7=02-24%2021%3A58%3A27@%23%24%u6613%u65B9%u8FBE%u84DD%u7B79%u7CBE%u9009%u6DF7%u5408@%23%24005827; EMFUND8=03-05%2015%3A33%3A29@%23%24%u6613%u65B9%u8FBE%u4E2D%u5C0F%u76D8%u6DF7%u5408@%23%24110011; EMFUND9=03-05 23:47:41@#$%u5929%u5F18%u4F59%u989D%u5B9D%u8D27%u5E01@%23%24000198; ASP.NET_SessionId=ntwtbzdkb0vpkzvil2a3h1ip; st_si=44251094035925; st_asi=delete; st_pvi=77351447730109; st_sp=2020-08-16%2015%3A54%3A02; st_inirUrl=https%3A%2F%2Fwww.baidu.com%2Flink; st_sn=3; st_psi=20210309200219784-0-8081344721",
            "Host": "fund.eastmoney.com",
            "Pragma": "no-cache",
            "Proxy-Connection": "keep-alive",
            "Referer": "http://fund.eastmoney.com/data/fundranking.html",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36",
        }

    def rank(self):
        time_interval = 'jnzf'  # jnzf:今年以来 3n: 3年

        # key='混合'
        # key='股票'
        self.category_rank(self.key, time_interval)

    def category_rank(self, key, time_interval):
        ft = self.ft_dict[key]
        td_str = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
        td_dt = datetime.datetime.strptime(td_str, '%Y-%m-%d')
        # 去年今日
        last_dt = td_dt - datetime.timedelta(days=365)
        last_str = datetime.datetime.strftime(last_dt, '%Y-%m-%d')
        # 生成一个0到1之间的随机浮点数
        random_number = random.random()

        # 格式化随机数，保留16位小数
        formatted_random_number = f"{random_number:.16f}"

        # rank_url = 'http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft={0}&rs=&gs=0&sc={1}zf&st=desc&sd={2}&ed={3}&qdii=&tabSubtype=,,,,,&pi=1&pn=10000&dx=1'.format(
        #     ft, time_interval, last_str, td_str)
        # 自定义，查询etf
        # http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=fb&ft=ct&rs=&gs=0&sc=1yzf&st=desc&pi=1&pn=50&v=0.5488320266221767
        if ft=="etf":
            rank_url=("http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=fb&ft=ct&rs=&gs=0&sc=1yzf&st=desc&pi=1&pn=2000&dx=0&v={0}"
                      .format(formatted_random_number))
        else:
            # http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=etf&rs=&gs=0&sc=jnzf&st=desc&sd=2023-11-08&ed=2024-11-07&qdii=&tabSubtype=,,,,,&pi=1&pn=10000&dx=1
            rank_url = 'http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft={0}&rs=&gs=0&sc={1}&st=desc&sd={2}&ed={3}&qdii=&tabSubtype=,,,,,&pi=1&pn=10000&dx=1&v={4}'.format(
                ft, time_interval, last_str, td_str,formatted_random_number)
        print("rank_url:",rank_url)
        content = self.get(url=rank_url)

        rank_data = self.parse(content)
        rank_list = self.key_remap(rank_data, key)
        self.save_data(rank_list,key)

    def save_data(self, rank_list,type_):
        # 清理数据
        delete_sql="delete from db_ttjj_fund_ranking where fund_type=%s"
        self.execute(delete_sql, (type_), conn, self.logger)

        for rank in rank_list:
            fund_type = rank.get("type")
            update_date=rank.get("crawl_date")
            fund_code = rank.get('基金代码')
            fund_name = rank.get('基金简称')
            fund_name_sx = rank.get('缩写')
            fund_stat_date = rank.get('日期')
            ass_net = self.convert(rank.get('单位净值'))
            acc_net = self.convert(rank.get('累计净值'))
            d_rate = self.convert(rank.get('日增长率(%)'))
            w = self.convert(rank.get('近1周增幅'))
            m = self.convert(rank.get('近1月增幅'))
            m3 = self.convert(rank.get('近3月增幅'))
            m6 = self.convert(rank.get('近6月增幅'))
            y = self.convert(rank.get('近1年增幅'))
            y2 = self.convert(rank.get('近2年增幅'))
            y3 = self.convert(rank.get('近3年增幅'))
            in_y = self.convert(rank.get('今年来'))
            all_y = self.convert(rank.get('成立来'))
            fund_create_date = rank.get('成立日期')

            # if fund_stat_date=='' : continue
            if fund_stat_date == '': fund_stat_date = '1900-01-01'

            insert_data= 'INSERT INTO db_ttjj_fund_ranking (fund_type, update_date, fund_code, fund_name, fund_name_sx, fund_stat_date, ass_net, acc_net, d_rate, w, m, m3, m6, y, y2, y3, in_y, all_y, fund_create_date) \
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        # insert_data = 'insert into `{}` VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(TODAY)

            self.execute(insert_data, (fund_type, update_date, fund_code, fund_name, fund_name_sx, fund_stat_date, ass_net, acc_net, d_rate, w, m, m3, m6, y, y2, y3, in_y, all_y, fund_create_date), conn, self.logger)

    def execute(self, cmd, data, conn, logger=None):

        cursor = conn.cursor()

        if not isinstance(data, tuple):
            data = (data,)
        try:
            cursor.execute(cmd, data)
        except Exception as e:
            conn.rollback()
            logger.error('执行数据库错误 {},{}'.format(e, cmd))
            ret = None
        else:
            ret = cursor.fetchall()
            conn.commit()

        return ret

    def convert(self, float_str):
        try:
            return_float = float(float_str)
        except:
            return_float = None
        return return_float

    def parse(self, rp):
        # print(content)
        # js_content = execjs.compile(content)
        # rank = js_content.eval("rankData")
        rank_txt = rp[rp.find('=') + 2:rp.rfind(';')]
        rank_rawdata = demjson3.decode(rank_txt)
        return rank_rawdata['datas']

    def key_remap(self, rank_data, type_):
        '''
        映射key value
        '''
        # print(rank_data)
        colums = ['基金代码', '基金简称', '缩写', '日期', '单位净值', '累计净值',
                  '日增长率(%)', '近1周增幅', '近1月增幅', '近3月增幅', '近6月增幅', '近1年增幅', '近2年增幅', '近3年增幅',
                  '今年来', '成立来', '成立日期', '购买手续费折扣', '自定义', '手续费原价？', '手续费折后？',
                  '布吉岛1', '布吉岛2', '布吉岛3', '布吉岛4']
        if self.key=='etf' :
            colums = ['基金代码', '基金简称', '缩写', '日期', '单位净值', '累计净值', '近1周增幅', '近1月增幅', '近3月增幅', '近6月增幅', '近1年增幅', '近2年增幅',
                      '近3年增幅',
                      '今年来', '成立来', '成立日期', '购买手续费折扣', '自定义', '手续费原价？', '手续费折后？','布吉岛1'
                      '类型', '布吉岛2']
        return_rank_data = []
        for rank in rank_data:
            rand_dict = {}
            rand_dict['type'] = type_
            rand_dict['crawl_date'] = self.today
            rank_ = rank.split(',')
            for index, colum in enumerate(colums):
                rand_dict[colum] = rank_[index]
            return_rank_data.append(rand_dict)

        return return_rank_data

    def __turnover_rate(self, code):
        url = 'http://api.fund.eastmoney.com/f10/JJHSL/?callback=jQuery18301549281364854147_1639139836416&fundcode={}&pageindex=1&pagesize=100&_=1639139836475'.format(
            code)
        ret_txt = self.get(url, _json=False)
        self.__parse_turnover_data(ret_txt, code)

    def __parse_turnover_data(self, jquery_data, code):
        js_format = jquery_data[jquery_data.find('{'):jquery_data.rfind('}') + 1]
        js_data = json.loads(js_format)
        turnover_rate_dict = {}
        turnover_rate_dict['code'] = code
        turnover_rate_dict['kind'] = self.key
        turnover_rate_dict['turnover_rate'] = js_data['Data']
        turnover_rate_dict['update'] = datetime.datetime.now()
        self.DB.insert(turnover_rate_dict)

    def fund_detail(self, code):
        url = 'http://fundf10.eastmoney.com/jbgk_{}.html'.format(code)

        def __get(url, headers, retry=5):
            start = 0
            while start < retry:

                try:
                    r = requests.get(
                        url=url,
                        headers=headers,
                    )

                except Exception as e:
                    print('base class error', e)
                    time.sleep(1)
                    start += 1
                    continue

                else:
                    return r.text
            return None

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh,en;q=0.9,en-US;q=0.8,zh-CN;q=0.7',
            'Host': 'fundf10.eastmoney.com',
            'Cookie': 'em_hq_fls=js; searchbar_code=005827; qgqp_b_id=98846d680cc781b1e4a70c935431c5c1; intellpositionL=1170.55px; intellpositionT=555px; HAList=a-sz-123030-%u4E5D%u6D32%u8F6C%u503A%2Ca-sz-300776-%u5E1D%u5C14%u6FC0%u5149%2Ca-sz-300130-%u65B0%u56FD%u90FD%2Ca-sz-300473-%u5FB7%u5C14%u80A1%u4EFD%2Ca-sz-300059-%u4E1C%u65B9%u8D22%u5BCC%2Ca-sz-000411-%u82F1%u7279%u96C6%u56E2%2Ca-sz-300587-%u5929%u94C1%u80A1%u4EFD%2Ca-sz-000060-%u4E2D%u91D1%u5CAD%u5357%2Ca-sz-002707-%u4F17%u4FE1%u65C5%u6E38%2Ca-sh-605080-%u6D59%u5927%u81EA%u7136%2Ca-sz-001201-%u4E1C%u745E%u80A1%u4EFD%2Ca-sz-300981-%u4E2D%u7EA2%u533B%u7597; em-quote-version=topspeed; st_si=90568564737268; st_asi=delete; ASP.NET_SessionId=otnhaxvqrwnmj4nuorygjua4; EMFUND0=11-29%2015%3A40%3A32@%23%24%u5DE5%u94F6%u4E0A%u8BC1%u592E%u4F01ETF@%23%24510060; EMFUND1=12-11%2000%3A51%3A58@%23%24%u524D%u6D77%u5F00%u6E90%u65B0%u7ECF%u6D4E%u6DF7%u5408A@%23%24000689; EMFUND2=12-11%2000%3A57%3A17@%23%24%u4E2D%u4FE1%u5EFA%u6295%u667A%u4FE1%u7269%u8054%u7F51A@%23%24001809; EMFUND3=12-11%2000%3A56%3A12@%23%24%u9E4F%u534E%u4E2D%u8BC1A%u80A1%u8D44%u6E90%u4EA7%u4E1A%u6307%u6570%28LOF%29A@%23%24160620; EMFUND4=12-11%2000%3A47%3A36@%23%24%u4E2D%u4FE1%u4FDD%u8BDA%u7A33%u9E3FA@%23%24006011; EMFUND5=12-11%2000%3A54%3A13@%23%24%u878D%u901A%u6DF1%u8BC1100%u6307%u6570A@%23%24161604; EMFUND6=12-11%2000%3A55%3A27@%23%24%u56FD%u6CF0%u7EB3%u65AF%u8FBE%u514B100%u6307%u6570@%23%24160213; EMFUND7=12-15%2023%3A05%3A04@%23%24%u534E%u5546%u65B0%u5174%u6D3B%u529B%u6DF7%u5408@%23%24001933; EMFUND8=12-15%2023%3A14%3A53@%23%24%u91D1%u4FE1%u6C11%u5174%u503A%u5238A@%23%24004400; EMFUND9=12-15 23:15:15@#$%u5929%u5F18%u4E2D%u8BC1%u5149%u4F0F%u4EA7%u4E1A%u6307%u6570A@%23%24011102; st_pvi=77351447730109; st_sp=2020-08-16%2015%3A54%3A02; st_inirUrl=https%3A%2F%2Fwww.baidu.com%2Flink; st_sn=10; st_psi=20211215231519394-112200305283-4710014236',
            'Referer': 'http://fund.eastmoney.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36'}

        content = __get(url, headers)
        built_date, scale = self.parse_detail_info(content)
        # db.insert_one(
        #     {'成立日期': built_date, '规模': scale, '基金代码': code, 'type': self.key, 'update': datetime.datetime.now()})
        insert_sql="INSERT INTO db_ttjj_basic\
                ( built_date, `scale`, fund_code, fund_type,scale_num,scale_stat_date)\
                VALUES( %s, %s, %s, %s,%s,%s)"
        if built_date=='' : built_date='1900-01-01'
        # print(scale.find("亿"))
        index=scale.find("亿")
        try:
            scale_num=scale[0:index]
            scale_num=float(scale_num.replace(',', ''))
        except  Exception as e:
            scale_num=0
            print(e)

        match = re.search(r'.*?截止至：(\d{4}-\d{2}-\d{2})' ,scale)

        if match:
            # 提取匹配到的数字和日期
            scale_stat_date = match.group(1)  # 截止日期
        else:
            scale_stat_date='1900-01-01'

        self.execute(insert_sql,(built_date,scale,code,self.key,scale_num,scale_stat_date),conn,self.logger)

    def parse_detail_info(self, content):

        resp = Selector(text=content)
        labels = resp.xpath('//div[@class="bs_gl"]/p/label')
        if len(labels) < 5:
            print("解析报错")
            return '', ''

        built_date = labels[0].xpath('./span/text()').extract_first()
        scale = labels[4].xpath('./span/text()').extract_first()
        scale = scale.strip()
        return built_date, scale

    def update_basic_info(self):
        pass

    def check_content(self, content):
        if content is None:
            self.logger.error('获取内容为空')
            return False
        else:
            return True

    def basic_info(self):
        '''
        基本数据
        '''
        # self.basic_DB = self.get_basic_db()
        query_sql='select fund_code from db_ttjj_fund_ranking where fund_type=%s'
        result=self.execute(query_sql,(self.key),conn,self.logger)

        # td_str = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
        # td_dt = datetime.datetime.strptime(datetime.datetime.now(), '%Y-%m-%d')
        # 90日之前
        last_dt = datetime.datetime.now() - datetime.timedelta(days=91)
        last_str = datetime.datetime.strftime(last_dt, '%Y-%m-%d')

        if self.check_content(result) :
            for code in result :
                q_sql="select count(1) from db_ttjj_basic where fund_code = %s and scale_stat_date > %s"
                rt = self.execute(q_sql, (code[0],last_str), conn, self.logger)
                if rt[0][0]>0 :
                    continue
                else:
                    d_sql = "delete from db_ttjj_basic where fund_code=%s"
                    self.execute(d_sql, (code[0]), conn, self.logger)

                LOG.info("爬取{}".format(code[0]))
                self.fund_detail(code[0])

    def convert_data_type(self):
        '''
        转换mongodb的字段
        '''
        for item in self.doc.find({},{'成立来':1}):
            try:
                p1=float(item['成立来'])
            except:
                p1=None
            self.doc.update_one({'_id':item['_id']},{'$set':{'成立来':p1}})

def main(kind, option):
    #  不支撑
    _dict = {1: '指数', 2: '股票', 3: '混合',4: '债券', 5: 'qdii',  6: 'fof',7: 'lof', 8:'etf'}

    app = TTFund(key=_dict.get(kind))  # key 基金类型，股票，混合，

    if option == 'basic':
        LOG.info('获取“{}”排名'.format(_dict.get(kind)))
        app.rank()

    # elif option == 'turnover':
    #     LOG.info('获取换手率')
    #     app.turnover_rate()

    elif option == 'info':
        LOG.info('获取<{}>基本信息'.format(_dict.get(kind)))
        app.basic_info()

    else:
        LOG.error("请输入正确参数")


if __name__ == '__main__':
    # fire.Fire(main)
    # app=TTFund()
    # app.convert_data_type()
    # '指数'
    main(kind=1, option='basic')
    main(kind=1, option='info')
    #'股票'
    main(kind=2,option='basic')
    main(kind=2, option='info')
    #'混合'
    main(kind=3, option='basic')
    main(kind=3, option='info')
    #'债券'
    main(kind=4, option='basic')
    main(kind=4, option='info')
    #'qdii'
    main(kind=5, option='basic')
    main(kind=5, option='info')
    # fof
    main(kind=6, option='basic')
    main(kind=6, option='info')
    #'etf'
    # main(kind=8, option='basic')
    # main(kind=8, option='info')
