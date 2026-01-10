import os
import sys
import matplotlib.pyplot as plt

import datetime
import logging
import backtrader as bt
from data_analyzer.etf_data import ETFData
from data_analyzer.etf_data_loader import EefDataLoader
from strategies.simpleStrategy import SimpleStrategy
from strategies.unpopularStrategy import UnpopularStrategy

plt.rcParams["font.sans-serif"] = ["SimHei"]  # Windows
plt.rcParams["axes.unicode_minus"] = False  # 解决负号显示问题

logging.basicConfig(level=logging.INFO)
cerebro = bt.Cerebro()

# 初始参数
initial_cash = 10_0000  # 初始资金10万元
initial_commission = 0.005  # 交易佣金0.5%


cerebro.broker.setcash(initial_cash)
cerebro.broker.setcommission(commission=initial_commission)

# 添加数据
logging.info(f"正在加载数据...")
dataloader = EefDataLoader(data_source="D:/stocks/download/etfs/daily")
train_data_list = dataloader.data_load()
logging.info(f"{len(train_data_list)}条数据加载完成")
for data in train_data_list:
    cerebro.adddata(data)

logging.info(f"初始资金: {cerebro.broker.getvalue():.2f} 元")
logging.info(f"使用策略: UnpopularStrategy")
logging.info(f"交易标的: multi")

cerebro.addstrategy(UnpopularStrategy)

logging.info(f"正在启动回测...")
cerebro.run()

final_cash = cerebro.broker.getvalue()
logging.info(f"最终资金: {final_cash:.2f} 元")
logging.info(f"总收益率: {(final_cash - initial_cash) / initial_cash * 100:.2f} %")
# cerebro.plot(style="candlestick")
