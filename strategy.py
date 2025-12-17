import backtrader as bt
import logging

logging.basicConfig(level=logging.INFO)


class SimpleStrategy(bt.Strategy):
    def __init__(self):
        super().__init__()
        self.order = None
        self.buyprice = None
        self.buycomm = None
        self.stop_loss = None
        self.take_profit = None
        logging.info("策略初始化完成")

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            if order.isbuy():
                logging.info(f"买入 {self.data0} 100 股, 价格: {order.executed.price}")
            elif order.issell():
                logging.info(f"卖出 {self.data0} 100 股, 价格: {order.executed.price}")

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            logging.info("订单 Canceled/Margin/Rejected")

        self.order = None

    def next(self):
        logging.info(
            f"{self.data.datetime.date(0)}, 当前价格: {self.data.close[0]:.2f}, 当前持仓: {self.position.size} 股, 可用资金: {self.broker.getcash():.2f} 元"
        )

        # 必须添加：检查是否有未完成订单
        if self.order:
            return

        if not self.position:
            size = self.broker.getcash() // self.data.close[0]
            if size > 100:
                self.order = self.buy(size=100)
        else:
            self.order = self.sell(size=100)
