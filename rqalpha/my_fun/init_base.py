from rqalpha.apis import *

import sys
sys.path.append('../../')
from configure.settings import DBSelector

try:
    DB = DBSelector()
    conn = DB.get_mysql_conn('stock', 'qq')
    cursor = conn.cursor()
except Exception as e:
    print(e)

# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
def init(context):
    # 'order_book_id', 'industry_code', 'market_tplus', 'symbol',
    #        'special_type', 'exchange', 'status', 'type', 'de_listed_date',
    #        'listed_date', 'sector_code_name', 'abbrev_symbol', 'sector_code',
    #        'round_lot', 'trading_hours', 'board_type', 'industry_name',
    #        'issue_price', 'trading_code', 'office_address', 'province'



    # df=all_instruments(type="CS")
    # print("column:\n")
    # print(df.columns)
    # for index, row in df.iterrows():
    #     res=""
    #     for i,var in row.items():
    #         res=res+str(var)+" , "
    #     print(f"Index: {index}, Row: {res}")
    #     res2 = str(row["order_book_id"])+" , "+str(row["symbol"])+" , "+str(row["exchange"])+" , "+str(row["type"])  +" , "+str(row["listed_date"])+" , "+str(row["sector_code"])+" , "+str(row["sector_code_name"])+" , "+str(row["industry_code"])+" , "+str(row["industry_name"])+" , "+str(row["trading_code"])+" , "+str(row["office_address"])+" , "+str(row["province"])
    #     print(f"Index: {index}, Row: {res2}")

    # CS	Common Stock, 即股票
    # ETF	Exchange Traded Fund, 即交易所交易基金
    # LOF	Listed Open-Ended Fund，即上市型开放式基金 （以下分级基金已并入）
    # INDX	Index, 即指数
    # Future	Futures，即期货，包含股指、国债和商品期货
    # Spot	Spot，即现货，目前包括上海黄金交易所现货合约
    # Option	期权，包括目前国内已上市的全部期权合约
    # Convertible	沪深两市场内有交易的可转债合约
    # Repo	沪深两市交易所交易的回购合约

    save_stock()
    save_etf()
    save_indx()

# 初始化股票信息
def save_stock():
        # 清理数据
        delete_sql="delete from rqa_instruments_stock"
        execute(delete_sql, (), conn, logger)

        df = all_instruments(type="CS")
        print("column:")
        print(df.columns)
        for index, row in df.iterrows():
            # res = ""
            # for i, var in row.items():
            #     res = res + str(var) + " , "
            # print(f"Index: {index}, Row: {res}")
            res2 = str(row["order_book_id"]) + " , " + str(row["symbol"]) + " , " + str(row["exchange"]) + " , " + str(
                row["type"]) + " , " + str(row["listed_date"]) + " , " + str(row["sector_code"]) + " , " + str(
                row["sector_code_name"]) + " , " + str(row["industry_code"]) + " , " + str(
                row["industry_name"]) + " , " + str(row["trading_code"]) + " , " + str(
                row["office_address"]) + " , " + str(row["province"])
            print(f"Index: {index}, Row: {res2}")

            insert_data= 'INSERT INTO rqa_instruments_stock (order_book_id, symbol, exchange, type, listed_date, sector_code, sector_code_name, industry_code, industry_name, trading_code, office_address, province) \
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

            execute(insert_data, (str(row["order_book_id"]),  str(row["symbol"]), str(row["exchange"]), str(row["type"]), str(row["listed_date"]), str(row["sector_code"]), str(
                row["sector_code_name"]),  str(row["industry_code"]), str(
                row["industry_name"]), str(row["trading_code"]), str(
                row["office_address"]), str(row["province"])), conn, logger)

# 初始化etf信息
def save_etf():
    # 清理数据
    delete_sql = "delete from rqa_instruments_etf"
    execute(delete_sql, (), conn, logger)

    # 'order_book_id', 'market_tplus', 'symbol', 'exchange', 'status',
    #        'establishment_date', 'de_listed_date', 'type', 'trading_hours',
    #        'abbrev_symbol', 'round_lot', 'listed_date', 'underlying_order_book_id',
    #        'underlying_name', 'least_redeem', 'trading_code', 'board_type'

    df = all_instruments(type="ETF")
    print("column:")
    print(df.columns)
    for index, row in df.iterrows():
        # res = ""
        # for i, var in row.items():
        #     res = res + str(var) + " , "
        # print(f"Index: {index}, Row: {res}")
        # res2 = str(row["order_book_id"]) + " , " + str(row["symbol"]) + " , " + str(row["exchange"]) + " , " + str(
        #     row["type"]) + " , " + str(row["listed_date"]) + " , " + str(row["underlying_name"]) + " , " + str(row["trading_code"])
        # print(f"Index: {index}, Row: {res2}")

        insert_data = 'INSERT INTO rqa_instruments_etf (order_book_id, symbol, exchange, type, listed_date, underlying_name,  trading_code) \
VALUES(%s,%s,%s,%s,%s,%s,%s)'

        execute(insert_data, (
        str(row["order_book_id"]), str(row["symbol"]), str(row["exchange"]), str(row["type"]), str(row["listed_date"]), str(
            row["underlying_name"]), str(row["trading_code"])), conn, logger)
# 初始化指数信息
def save_indx():
    # 清理数据
    delete_sql = "delete from rqa_instruments_indx"
    execute(delete_sql, (), conn, logger)

    # 'order_book_id', 'trading_hours', 'market_tplus', 'symbol', 'exchange',
    #        'abbrev_symbol', 'round_lot', 'type', 'de_listed_date', 'listed_date',
    #        'status'

    df = all_instruments(type="INDX")
    print("column:")
    print(df.columns)
    for index, row in df.iterrows():
        res = ""
        for i, var in row.items():
            res = res + str(var) + " , "
        print(f"Index: {index}, Row: {res}")
        res2 = str(row["order_book_id"]) + " , " + str(row["symbol"]) + " , " + str(row["exchange"]) + " , " + str(
            row["type"]) + " , " + str(row["listed_date"]) + " , "
        print(f"Index: {index}, Row: {res2}")

        insert_data = 'INSERT INTO rqa_instruments_indx (order_book_id, symbol, exchange, type, listed_date) \
VALUES(%s,%s,%s,%s,%s)'

        execute(insert_data, (
        str(row["order_book_id"]), str(row["symbol"]), str(row["exchange"]), str(row["type"]), str(row["listed_date"])), conn, logger)

def execute(cmd, data, conn, logger=None):

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

def convert( float_str):
        try:
            return_float = float(float_str)
        except:
            return_float = None
        return return_float


config = {
    "base": {
        "accounts": {
            "STOCK": 100000,
        },
        "start_date": "20190101",
        "end_date": "20191231",
    },
    "mod": {
        "sys_analyser": {
            #"plot": True,
            "benchmark": "000300.XSHG"
        }
    }
}

if __name__ == "__main__":
    from rqalpha import run_func
    run_func(config=config, init=init)