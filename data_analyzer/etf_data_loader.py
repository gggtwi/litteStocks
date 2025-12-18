import os
import logging
import datetime
import pandas as pd
import backtrader as bt

from etf_data import ETFData

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class EefDataLoader:
    def __init__(
        self,
        cerebro: bt.Cerebro,
        data_source="download/etfs/daily",
        min_days=180,
        start_date="2014-09-01",
        end_date=datetime.datetime.now().strftime("%Y-%m-%d"),
        valid_ratio=0.2,
        test_ratio=None,
    ):
        self.data_source = data_source
        self.data_path_list = os.listdir(self.data_source)
        self.min_days = min_days
        self.start_date = start_date
        self.end_date = end_date
        self.valid_ratio = valid_ratio
        self.test_ratio = test_ratio
        self.cerebro = cerebro

        if self.test_ratio:
            self.test_ratio = valid_ratio
            self.test_data_start_day = self.get_test_data_start_day()
        else:
            self.test_data_start_day = end_date
        self.valid_data_start_day = self.get_valid_data_start_day()

    def get_valid_data_start_day(self):
        datetime_format = "%Y-%m-%d"
        start_dt = datetime.datetime.strptime(self.start_date, datetime_format)
        end_dt = datetime.datetime.strptime(self.end_date, datetime_format)
        total_days = (end_dt - start_dt).days
        valid_days = int(total_days * self.valid_ratio)
        valid_start_date = end_dt - datetime.timedelta(days=valid_days)
        return valid_start_date

    def get_test_data_start_day(self):
        datetime_format = "%Y-%m-%d"
        start_dt = datetime.datetime.strptime(self.start_date, datetime_format)
        end_dt = datetime.datetime.strptime(self.end_date, datetime_format)
        total_days = (end_dt - start_dt).days
        test_days = int(total_days * self.test_ratio)
        test_start_date = end_dt - datetime.timedelta(days=test_days)
        return test_start_date

    def train_data_load(self):
        logging.info("Loading train data from source: %s", self.data_source)

        for file_name in self.data_path_list:
            file_path = os.path.join(self.data_source, file_name)
            logging.info(f"正在加载训练数据: {file_path}")

            data = ETFData(
                dataname=file_path,
                timeframe=bt.TimeFrame.Days,
                fromdate=self.start_date,
                todate=self.valid_data_start_day - datetime.timedelta(days=1),
                encoding="utf-8-sig",
            )
            self.cerebro.adddata(data=data)

    def valid_data_load(self):
        logging.info("Loading valid data from source: %s", self.data_source)

        for file_name in self.data_path_list:
            file_path = os.path.join(self.data_source, file_name)
            logging.info(f"正在加载验证数据: {file_path}")

            data = ETFData(
                dataname=file_path,
                timeframe=bt.TimeFrame.Days,
                fromdate=self.valid_data_start_day - datetime.timedelta(days=1),
                todate=self.test_data_start_day,
                encoding="utf-8-sig",
            )
            self.cerebro.adddata(data=data)


if __name__ == "__main__":
    data_loader = EefDataLoader()
    data_loader.train_data_load()
