import backtrader as bt
import logging
import math

logging.basicConfig(level=logging.INFO)


class UnpopularStrategy(bt.Strategy):
    def __init__(self):
        super().__init__()
        self.order = None

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

    def next(self):
        logging.info("Executing next step")
        rates = []
        for stock_volumes_history in stocks_volumes_history:
            stock_volumes_history = get_all_volume()
            avg_volume = math.mean(stock_volumes_history)
            rate = stock_volume_today() / avg_volume
            rates.append((stock_name, rate))

        rates.sort(key=lambda x: x[1])

        for idx, _ in enumerate(rates):
            if idx < n_buy_num:
                stock_i = get_stock_idx(_[0])
                self.order = self.buy()
