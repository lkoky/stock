# -*- coding: UTF-8 -*-
"""
@author:xda
@file:etf_models.py
@time:2021/01/23
"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, FLOAT,TEXT
Base = declarative_base()


class IndexObject(Base):
    __tablename__ = 'tb_etf_stock'

    id = Column(Integer, primary_key=True, autoincrement=True)
    seq = Column(Integer, unique=False)
    fund_code = Column(String(10), unique=False)
    stock_code = Column(String(10), unique=False)
    fund_url = Column(String(100), unique=False)
    fund_name = Column(String(20), unique=False)
    nv_radio = Column(FLOAT, unique=False)
    stock_num = Column(FLOAT, unique=False)
    market_value = Column(String(20), unique=False)

