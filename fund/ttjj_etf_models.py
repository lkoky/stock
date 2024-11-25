# -*- coding: UTF-8 -*-
"""
@author:xda
@file:etf_models.py
@time:2021/01/23
"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, FLOAT, TEXT,Date

Base = declarative_base()


class IndexObject(Base):
    __tablename__ = 'tb_etf_stock'

    id = Column(Integer, primary_key=True, autoincrement=True)
    seq = Column(Integer, unique=False)
    fund_code = Column(String(32), unique=False)
    stock_code = Column(String(32), unique=False)
    stock_url = Column(String(100), unique=False)
    stock_name = Column(String(100), unique=False)
    nv_radio = Column(FLOAT, unique=False)
    stock_num = Column(FLOAT, unique=False)
    market_value = Column(FLOAT, unique=False)
    end_year = Column(Integer, unique=False)
    end_month=Column(Integer, unique=False)
    end_date=Column(Date, unique=False)

