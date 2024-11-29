# -*- coding: utf-8 -*-
import concurrent.futures
import multiprocessing
from rqalpha import run_file
import sys
sys.path.append('../../')
from configure.settings import DBSelector

start_date="2017-01-01"
end_date="2017-12-29"
# end_date="2018-12-28"
# end_date="2019-12-31"
# end_date="2020-12-31"
# end_date="2021-12-31"
# end_date="2022-12-30"
# end_date="2023-12-29"
def getTotalCountByMysql():
  try:
    DB = DBSelector()
    conn = DB.get_mysql_conn('stock', 'qq')
    # cursor = conn.cursor()
  except Exception as e:
    print(e)

  return execute("select count(1) from rqa_instruments_etf", (), conn)


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

result=getTotalCountByMysql()
total=result[0][0]
# total=20
pageSize=20
pages=total/pageSize

tasks = []
for i in range(0,int(pages)):
  limit_start=i*pageSize
  limit_end=(i+1)*pageSize-1
  print(f"limit_start:{limit_start},limit_end:{limit_end}")
  config = {
    "base": {
      "start_date": start_date,
      "end_date": end_date,
      "accounts": {
        "stock": 1000000
      }
    },
    "extra": {
      "log_level": "verbose",
      "context_vars": {
                      "startBarDate": start_date,
                      "endBarDate": end_date,
                      "ma0": 5,
                      "ma1": 10,
                      "ma2": 20,
                      "limit_start": limit_start,
                      "limit_end": limit_end,
                  }
    },
    "mod": {
      "sys_analyser": {
        "benchmark":"000300.XSHG",#基准参照
        # "output_file":"result1.pkl",
        "enabled": True,
        # "plot": True
      }
    }
  }
  tasks.append(config)


def run_bt(config):
  strategy_file_path = "./ma.py"
  run_file(strategy_file_path, config)


if __name__ == '__main__':
  with concurrent.futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
    for task in tasks:
      executor.submit(run_bt, task)