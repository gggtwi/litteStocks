import os
import sys
import matplotlib.pyplot as plt

import datetime
import logging
import backtrader as bt
from data_analyzer.etf_data import ETFData
from strategies.simpleStrategy import SimpleStrategy

plt.rcParams["font.sans-serif"] = ["SimHei"]  # Windows
plt.rcParams["axes.unicode_minus"] = False  # 解决负号显示问题

logging.basicConfig(level=logging.INFO)
cerebro = bt.Cerebro()

# 初始参数
initial_cash = 10_0000  # 初始资金10万元
initial_commission = 0.005  # 交易佣金0.5%
aimEtf = "159934"  # 已155934黄金ETF作为例子


cerebro.broker.setcash(initial_cash)
cerebro.broker.setcommission(commission=initial_commission)

# 添加数据
modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
datapath = os.path.join(modpath, "download").join("etfs")
data_ls = os.listdir(datapath)
data_ls.pop(0)

for f_path in data_ls:
    dataname = os.path.join(datapath, f_path)
    data = ETFData(
        dataname=dataname,
        timeframe=bt.TimeFrame.Days,
        fromdate=datetime.datetime(2014, 9, 1),
        todate=datetime.datetime(2024, 6, 1),
        encoding="utf-8-sig",
    )
    cerebro.adddata(data)

logging.info(f"初始资金: {cerebro.broker.getvalue():.2f} 元")
logging.info(f"使用策略: SimpleStrategy")
logging.info(f"交易标的: {dataname}")

cerebro.addstrategy(SimpleStrategy)
cerebro.run()

final_cash = cerebro.broker.getvalue()
logging.info(f"最终资金: {final_cash:.2f} 元")
logging.info(f"总收益率: {(final_cash - initial_cash) / initial_cash * 100:.2f} %")
cerebro.plot(style="candlestick")
