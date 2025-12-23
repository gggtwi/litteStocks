import backtrader as bt
import logging
import math


def buy_size_func(**kwargs):
    return ((kwargs["cash"] // kwargs["price"]) // 100) * 20


class UnpopularStrategy(bt.Strategy):
    def __init__(
        self,
        etf_target_numbers: int = 5,
        min_trans_day: int = 180,
        pos_days=30,
        buy_size: callable = buy_size_func,
    ):
        super().__init__()
        self.order = None
        self.volumes_ems = {}
        self.lookback_period = 999999
        self.min_trans_day = min_trans_day
        self.etf_target_numbers = etf_target_numbers
        self.pos_days = pos_days  # 持仓时间
        self.num_days = 0  # 已持仓天数
        self.bought_etfs = None  # 持仓记录

        self.buy_size = buy_size
        self.logger = logging.getLogger("UnpopularStrategy")
        self.logger_init()
        self.logger.info("策略初始化完成")

    def logger_init(self):
        formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")

        # 创建文件处理器
        file_handler = logging.FileHandler("backtest_results.log")
        file_handler.setLevel(logging.INFO)

        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                self.logger.info(
                    f"买入 {self.data0} 100 股, 价格: {order.executed.price}"
                )
            elif order.issell():
                self.logger.info(
                    f"卖出 {self.data0} 100 股, 价格: {order.executed.price}"
                )

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.logger.info("订单 Canceled/Margin/Rejected")

    def buy_etfs(self, datas):
        if type(datas) is not list:
            datas = [datas]
        for data in datas:
            self.order = self.buy(
                data=data,
                size=self.buy_size(
                    cash=self.broker.getcash(),
                    price=data.close[0],
                    strategy=self,
                    # 在此处传递尽可能多的信息给回调函数
                ),
            )

    def sell_etfs(self, datas):
        for data in datas:
            position = self.getposition(data)
            size = position.size

            if size > 0:
                self.order = self.close(data)
                self.logger.info(
                    f"清仓: 已卖出{data.etf_code}_{data.etf_name} {size}股"
                )

    def next(self):
        for i, data in enumerate(self.datas):
            d_volumes = data.volume.get(size=self.lookback_period)
            if self.datas[i].etf_code in self.volumes_ems.keys():
                etf_code = self.datas[i].etf_code
                self.volumes_ems[etf_code] += data.volume[-1] / (len(d_volumes) - 1)
            else:
                if len(d_volumes) == 1:  # ETF历史中第一个成交量特殊处理
                    volumes_avg = 0
                else:
                    volumes_avg = sum(d_volumes[:-1]) / (len(d_volumes) - 1)
                self.volumes_ems[self.datas[i].etf_code] = volumes_avg

        rates = list(self.volumes_ems)
        rates.sort(key=lambda x: x[1])
        min_volume_datas = []
        for i, data in enumerate(self.datas):
            d_volumes = data.volume.get(size=self.lookback_period)
            if len(d_volumes) < self.min_trans_day:
                self.logger.info(
                    f"标的:{data.etf_code}_{self.datas[i].etf_name}, 历史交易日未达到{self.min_trans_day}, 现在是第{len(d_volumes)}天"
                )
            else:
                # 若数据源中etf数量大于要买入的etf数量
                if len(rates) > self.etf_target_numbers:
                    # 若该etf为前n个交易量最小的
                    if data.etf_code in rates[-1 * self.etf_target_numbers :]:
                        # 将etf放入列表中
                        min_volume_datas.append(data)
                else:  # 若数据源中etf数量小于等于要买入的etf数量
                    if data.etf_code in rates:  # 全部填入待购买列表
                        min_volume_datas.append(data)

                    self.logger.warning(f"填入的标的数不足{self.etf_target_numbers}个")

        # TODO:此处存在bug
        if len(min_volume_datas) != 0:
            if self.bought_etfs == None:  # 若未持仓
                self.buy_etfs(data=min_volume_datas)  # 购买所有目标etf
                self.bought_etfs = min_volume_datas  # 记录持仓的etf表
            else:  # 若已持仓
                if self.num_days != self.pos_days:  # 在持仓时间未达标时
                    self.num_days += self.pos_days
                else:  # 持仓时间达标时
                    self.sell_etfs(self.bought_etfs)
                    self.bought_etfs = None


if __name__ == "__main__":
    import os
    import sys
    import datetime

    sys.path.insert(0, "D:/stocks/")
    from data_analyzer import ETFData

    dataname = "D:/stocks/download/etfs/daily"
    # dataloader = EefDataLoader(data_source=dataname)
    # cerebro = dataloader.data_load()
    cerebro = bt.Cerebro()
    data_path = os.path.join("D:/", "stocks", "download", "etfs", "daily")
    dataname1 = os.path.join(data_path, "510330_沪深300ETF华夏.csv")
    dataname2 = os.path.join(data_path, "159934_黄金ETF.csv")
    data1 = ETFData(
        dataname=dataname1,
        timeframe=bt.TimeFrame.Days,
        fromdate=datetime.datetime(2014, 9, 1),
        todate=datetime.datetime(2025, 6, 1),
        # encoding="utf-8-sig",
    )
    data2 = ETFData(
        dataname=dataname2,
        timeframe=bt.TimeFrame.Days,
        fromdate=datetime.datetime(2014, 9, 1),
        todate=datetime.datetime(2025, 6, 1),
        # encoding="utf-8-sig",
    )
    cerebro.adddata(data1)
    cerebro.adddata(data2)
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.005)
    cerebro.addstrategy(UnpopularStrategy)
    logging.info("正在启动回测...")
    cerebro.run()
    final_cash = cerebro.broker.getvalue()
    print(f"最终资金: {final_cash:.2f} 元")
    # cerebro.plot(style="candlestick")
    print("end")
