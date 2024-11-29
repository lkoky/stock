from rqalpha.apis import *
from rqalpha import run_func
import sys
sys.path.append('../../')
from configure.settings import DBSelector
import talib

try:
    DB = DBSelector()
    conn = DB.get_mysql_conn('stock', 'qq')
    # cursor = conn.cursor()
except Exception as e:
    print(e)

# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
def init(context):

    # 选择我们感兴趣的股票
    # context.s1 = "000001.XSHE"
    # context.s1 = "600660.XSHG"
    # context.s1 = "159901.XSHE"
    # context.s2 = "601988.XSHG"
    # context.s3 = "000068.XSHE"
    # context.stocks = [context.s1]

    context.stocks = []
    result=getStockByMysql(context.limit_start,context.limit_end)
    for stock in result :
        context.stocks.append(stock[0])

    context.multiple=[1,2,3,5,8,13,21]
    # context.endBarDate="2024-01-04"
    context.lastBarDate=None
    context.amount=100
    context.TIME_PERIOD = 20

    context.ma0 = 5
    context.ma1 = 10
    context.ma2 = 20

    context.params={}

    context.orders= {}
    context.stats= {}

def getStockByMysql(limit_start,limit_end) :
    return execute("select order_book_id from rqa_instruments_etf limit %s,%s", (limit_start,limit_end), conn)

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

# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_bar(context, bar_dict):

    # 开始编写你的主要的算法逻辑

    # bar_dict[order_book_id] 可以拿到某个证券的bar信息
    # context.portfolio 可以拿到现在的投资组合状态信息

    # 使用order_shares(id_or_ins, amount)方法进行落单
    for stock in context.stocks:
        curr_bar = bar_dict[stock]

        print(curr_bar)
        context.lastBarDate = str(curr_bar["datetime"])[:10]


        if stock not  in context.orders :
            context.orders[stock] = []
        if stock not in context.params:
            context.params[stock] = {"pre_res5":None,"pre_res10":None,"flag":True}
        if stock not in context.stats:
            context.stats[stock]={"sell_count":0,"buy_count":0,"profit":0.0,"max_order_count":0,"last_profit":0.0,"max_cost":0,"max_orders":0}

        ## 计算成本
        cost=0.0
        for item in context.orders[stock]:
            print(item)
            cost+=float(item["market_value"])

        print(f"总成本：{cost}")
        if context.stats[stock]["max_cost"] <cost :
            context.stats[stock]["max_cost"]=cost

        count = len(context.orders[stock])
        if context.stats[stock]["max_orders"] <count:
            context.stats[stock]["max_orders"]=count

        position = get_position(stock, POSITION_DIRECTION.LONG)
        # print(f"position:{position}")
        # print(context.portfolio)
        # 盈利，计算退出机制
        if  position.market_value>0:
            price_rate = (float(curr_bar["close"]) - float(context.orders[stock][-1]["price"])) / float(context.orders[stock][-1]["price"])

            profit=(position.market_value-cost)
            rate = profit / position.market_value
            # print(rate)
            if rate > 0.1:
                amount=position.quantity
                order_shares(stock, 0 - amount)
                print(f"sell-cycle:{stock} , date:{curr_bar["datetime"]} , price2:{curr_bar["close"]} , rate:{rate}, amount:{amount} ,val:{profit}")
                # print(context.portfolio)
                context.stats[stock]["profit"] += round(profit,2)
                context.stats[stock]["sell_count"] += 1
                context.orders[stock] = []

            if price_rate > -0.1:
                context.params[stock]["flag"] = False
            else:
                print(f"---------------rate:{price_rate}")
                # context.params[stock]["flag"] = True
                # count = len(context.orders[stock])
                factor=1
                if count > len(context.multiple):
                    factor=context.multiple[-1]
                else:
                    factor = context.multiple[count]
                amount = context.amount * factor
                order_shares(stock, amount)
                context.orders[stock].append({"stock": stock, "price": curr_bar["close"], "amount": amount,
                                              "market_value": amount * curr_bar["close"],
                                              "date": str(curr_bar["datetime"])[:10]})
                print(f"buy-cycle:{stock},date:{curr_bar["datetime"]} , price:{curr_bar["close"]} ")
                context.stats[stock]["buy_count"] += 1
                if (count + 1) > context.stats[stock]["max_order_count"]:
                    context.stats[stock]["max_order_count"] = count + 1
        else:
            context.params[stock]["flag"] = True

        ## 策略结束全部结清
        if context.lastBarDate == context.endBarDate:
            if  position.market_value>0 :
                profit = (position.market_value - cost)
                rate = profit / position.market_value
                amount = position.quantity
                order_shares(stock, 0 - amount)
                context.stats[stock]["sell_count"] += 1
                context.stats[stock]["last_profit"] += round(profit, 2)
                print(f"sell-end:{stock} , date:{curr_bar["datetime"]} , price2:{curr_bar["close"]} , rate:{rate}, amount:{amount} ,val:{profit}")
            # context.orders = []
            del context.orders[stock]
            # context.orders.clear()
            continue

        # 读取历史数据 df['Close'].values
        prices = history_bars(stock, context.TIME_PERIOD+1, '1d', 'close' ,adjust_type = "pre",skip_suspended=True)
        logger.info(f"prices1:{prices}")
        if prices is None or len(prices) < context.TIME_PERIOD+1:
            continue
        # 用Talib计算RSI值
        sma5 = talib.EMA(prices, timeperiod=context.ma0)[-1]
        sma10 = talib.EMA(prices, timeperiod=context.ma1)[-1]
        sma20 = talib.EMA(prices, timeperiod=context.ma2)[-1]
        res10 =sma10- sma20
        res5 = sma5 - sma10

        pre_res5=context.params[stock]["pre_res5"]
        pre_res10=context.params[stock]["pre_res10"]
        # print(f"rsi_data1:{sma1} , rsi_data2:{sma2} , res：{res}")
        if pre_res10 is not None:
            # and context.flag
            # count = len(context.orders[stock])
            if count == 0  and res10>0 and pre_res10<=0:
                amount=context.amount * context.multiple[count]
                order_shares(stock, amount)
                context.orders[stock].append({"stock":stock,"price":curr_bar["close"],"amount":amount,"market_value":amount*curr_bar["close"],"date":str(curr_bar["datetime"])[:10]})
                print(f"buy-init:{stock},date:{curr_bar["datetime"]} , price:{curr_bar["close"]} ")
                context.stats[stock]["buy_count"] += 1

            # if count>0 and res5>0 and pre_res5<=0 and context.params[stock]["flag"]==True:
            #     amount = context.amount * context.multiple[count]
            #     order_shares(stock, amount)
            #     context.orders[stock].append({"stock": stock, "price": curr_bar["close"], "amount": amount,
            #                            "market_value": amount * curr_bar["close"],
            #                            "date": str(curr_bar["datetime"])[:10]})
            #     print(f"buy-cycle:{stock},date:{curr_bar["datetime"]} , price:{curr_bar["close"]} ")
            #     context.stats[stock]["buy_count"] += 1
            #     if (count+1)>context.stats[stock]["max_order_count"]:
            #         context.stats[stock]["max_order_count"]=count+1

        context.params[stock]["pre_res10"]=res10
        context.params[stock]["pre_res5"] = res5


def after_trading (context):
    print(f"context.lastBarDate：{context.lastBarDate},after_trading:{context}")
    if context.lastBarDate==context.endBarDate :
        print(f"------ context.lastBarDate==context.endBarDate---------")
        for stock in context.stocks:
            print(f"stock:{stock} , buy_count:{context.stats[stock]['buy_count']} ， sell_count:{context.stats[stock]['sell_count']} "
                  f"， profit:{context.stats[stock]['profit']} , max_order_count:{context.stats[stock]['max_order_count']} "
                  f"， last_profit:{context.stats[stock]['last_profit']} , max_cost:{context.stats[stock]['max_cost']} , max_orders:{context.stats[stock]['max_orders']}")
            save_data(stock,context.stats[stock],context.startBarDate,context.endBarDate)

def save_data(stock,stats,start_date,end_date):
        # 清理数据
        delete_sql="delete from rqa_strategy_stats where stock=%s and start_date=%s and end_date=%s"
        execute(delete_sql, (stock,start_date,end_date), conn)

        insert_data= 'INSERT INTO rqa_strategy_stats \
(start_date, end_date, stock, buy_count, sell_count, profit, max_order_count, last_profit, max_cost, max_orders) \
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
        # insert_data = 'insert into `{}` VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(TODAY)

        execute(insert_data, (start_date, end_date, stock, stats["buy_count"], stats["sell_count"], stats["profit"], stats["max_order_count"], stats["last_profit"], stats["max_cost"], stats["max_orders"]), conn, logger)

# config = {
#   "base": {
#     "start_date": "2020-01-01",
#     "end_date": "2024-01-04",
#     "accounts": {
#       "stock": 1000000
#     }
#   },
#   "extra": {
#     "log_level": "verbose",
#   },
#   "mod": {
#     "sys_analyser": {
#       "benchmark":"000300.XSHG",#基准参照
#       # "output_file":"result1.pkl",
#       "enabled": True,
#       # "plot": True
#     }
#   }
# }
#
# # 您可以指定您要传递的参数
# run_func(init=init,  handle_bar=handle_bar,after_trading=after_trading, config=config)

# 如果你的函数命名是按照 API 规范来，则可以直接按照以下方式来运行
# run_func(**globals())