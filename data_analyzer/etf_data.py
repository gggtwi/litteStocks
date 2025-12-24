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
        ("close", 2),
        ("high", 3),
        ("low", 4),
        ("volume", 5),
        ("openinterest", -1),  # A股ETF通常没有持仓量数据
        ("amount", 6),
        ("amplitude", 7),
        ("changePer", 8),
        ("changeAm", 9),
        ("turnover", 10),
        ("etf_code", None),  # ETF代码
        ("etf_name", None),  # ETF名称
    )

    def __init__(self):
        super().__init__()
        # 创建元数据属性，避免使用lines（节省内存）
        self.etf_code = self.p.etf_code
        self.etf_name = self.p.etf_name
        self.data_name = self.p.dataname

    def start(self):
        """在数据开始前解析元数据"""
        super().start()

        # 从文件名自动解析元数据
        if not self.etf_code:
            filename = self.p.dataname
            if isinstance(filename, str):
                # 提取纯文件名（不含路径和扩展名）
                base_name = filename.split("/")[-1].split("\\")[-1]
                base_name = base_name.replace(".csv", "")

                # 假设格式: 代码_名称.csv
                if "_" in base_name:
                    code_part, name_part = base_name.split("_", 1)
                    self.etf_code = code_part
                    self.etf_name = name_part

        # 确保有默认值
        if not self.etf_code:
            self.etf_code = "UNKNOWN"
        if not self.etf_name:
            self.etf_name = "Unknown ETF"


if __name__ == "__main__":
    cerebro = bt.Cerebro()
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.005)
    dataname = os.path.join(
        "D:/",
        "stocks",
        "download",
        "etfs",
        "daily",
        "510330_沪深300ETF华夏.csv",
    )
    data = ETFData(
        dataname=dataname,
        timeframe=bt.TimeFrame.Days,
        fromdate=datetime.datetime(2014, 9, 1),
        todate=datetime.datetime(2025, 6, 1),
        # encoding="utf-8-sig",
    )
    cerebro.addstrategy(bt.strategies.MA_CrossOver)
    cerebro.adddata(data, name="510330")
    cerebro.run()
    final_value = cerebro.broker.getvalue()
    final_cash = cerebro.broker.getvalue()
    print(f"最终资金: {final_cash:.2f} 元")
    print(f"最终价值: {final_value:.2f} 元")
    # cerebro.plot(style="candlestick")
    print("end")
