import backtrader as bt
import datetime
import os
import sys


class ETFData(bt.feeds.GenericCSVData):
    """
    自定义ETF数据源，适配A股ETF数据格式
    """

    # 添加字段 成交额     振幅        涨跌幅        涨跌额        换手率
    lines = ("amount", "amplitude", "changePer", "changeAm", "turnover")
    params = (
        ("dtformat", "%Y-%m-%d"),
        ("datetime", 0),
        ("open", 1),
        ("high", 2),
        ("low", 3),
        ("close", 4),
        ("volume", 5),
        ("openinterest", -1),  # A股ETF通常没有持仓量数据
        ("amount", 6),
        ("amplitude", 7),
        ("changePer", 8),
        ("changeAm", 9),
        ("turnover", 10),
    )
