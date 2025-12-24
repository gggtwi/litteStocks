import os
import logging
import datetime
import pandas as pd
import backtrader as bt

from .etf_data import ETFData

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class EefDataLoader:
    def __init__(
        self,
        data_source: str | os.PathLike,
        min_days=180,
        start_date=datetime.datetime(2014, 9, 1),
        end_date=datetime.datetime.now(),
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

        if self.test_ratio is not None:
            self.test_ratio = valid_ratio
            self.test_data_start_day = self.get_test_data_start_day()
        else:
            self.test_data_start_day = end_date
        self.valid_data_start_day = self.get_valid_data_start_day()

    def get_valid_data_start_day(self):
        total_days = (self.end_date - self.start_date).days
        valid_days = int(total_days * self.valid_ratio)
        valid_start_date = self.end_date - datetime.timedelta(days=valid_days)
        return valid_start_date

    def get_test_data_start_day(self):
        total_days = (self.end_date - self.start_date).days
        test_days = int(total_days * self.test_ratio)
        test_start_date = self.end_date - datetime.timedelta(days=test_days)
        return test_start_date

    def data_load(self, mode="train"):
        data_list = []
        logging.info(f"Loading {mode} data from source: {self.data_source}")

        for file_name in self.data_path_list:
            file_path = self.data_source + "/" + file_name
            logging.info(f"正在加载 {mode} 数据: {file_path}")
            if not file_name.endswith(".csv"):
                logging.warning(f"文件 {file_path} 不是 CSV 格式，跳过加载。")
                continue
            df = pd.read_csv(file_path)
            if len(df) < self.min_days:
                logging.warning(
                    f"文件 {file_path} 的数据天数不足: {len(df)} < {self.min_days}"
                )
                continue
            if mode == "train":
                start_date = self.start_date
                end_date = self.valid_data_start_day - datetime.timedelta(days=1)
            elif mode == "valid":
                start_date = self.valid_data_start_day - datetime.timedelta(days=1)
                end_date = self.test_data_start_day
            elif mode == "test":
                start_date = self.test_data_start_day
                end_date = self.end_date
            else:
                logging.warning(f"Unknown mode: {mode}. Skipping file: {file_path}")
                continue

            data = ETFData(
                dataname=file_path,
                timeframe=bt.TimeFrame.Days,
                fromdate=start_date,
                todate=end_date,
            )
            data_list.append(data)
        logging.info(f"{mode} data loading completed.")
        return data_list


if __name__ == "__main__":
    data_loader = EefDataLoader(data_source="D:/stocks/download/etfs/daily")
    cerebro1 = data_loader.data_load(mode="train")
    cerebro2 = data_loader.data_load(mode="valid")
    cerebro3 = data_loader.data_load(mode="test")
    cerebro1.broker.setcash(100000.0)
    cerebro1.broker.setcommission(commission=0.005)

    pass
