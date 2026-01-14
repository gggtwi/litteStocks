#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Date: 2026/1/10
"""

import os
import time
import logging
import akshare as ak
import pandas as pd
from litteStocks.utils.logger_utils import setup_logger
from tenacity import retry, stop_after_attempt, wait_fixed
from litteStocks.utils.date_utils import dateUtils


class stocksDataDownload:
    def __init__(
        self,
        period="daily",
        adjust="hfq",
        start_date="19700101",
        end_date="20500101",
        save_path="download/stocks",
        log_level=logging.INFO,
        mode="update",
    ):
        # 数据周期：daily（日线）、weekly（周线）、monthly（月线）
        self.period = period

        # 复权方式：qfq（前复权）、hfq（后复权）、none（不复权）
        self.adjust = adjust
        self.start_date = start_date
        self.end_date = end_date
        self.save_path = os.path.join(os.getcwd(), save_path).replace("\\", "/")
        self.log_level = log_level
        self.logger = setup_logger("stocksDataDownload", self.log_level)
        if mode == "update":
            self.date_utils = dateUtils()

    # 获取symbol列表
    def load_symbol_list(self, all=False):
        self.logger.info(f"正在查找A股代码...")

        symbols = ak.stock_info_a_code_name()

        if all:
            symbols_ls = symbols.values.tolist()
            return symbols_ls

        existed_codes = ()
        self.logger.info(f"正在检测已下载内容...")
        if os.path.exists(self.save_path):
            file_names = os.listdir(self.save_path)
            for file_name in file_names:
                if ".csv" in file_name:
                    existed_codes = existed_codes + (file_name.split("_")[0],)

        symbols_clean = symbols[~symbols["code"].astype(str).isin(set(existed_codes))]

        symbols_ls = symbols_clean.values.tolist()
        return symbols_ls

    # 下载一个stock
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def download_stock(self, symbol, name):
        stock_data = ak.stock_zh_a_hist(
            symbol=symbol,
            period=self.period,
            adjust=self.adjust,
            start_date=self.start_date,
            end_date=self.end_date,
        )
        return stock_data

    # 下载所有stocks
    def download_all_stocks(self, update_all_symbols=False):
        symbols = self.load_symbol_list(all=update_all_symbols)

        for i, symbol in enumerate(symbols):
            code, name = symbol
            self.logger.info(f"[{i}/{len(symbols)}] 开始下载: {code}({name})")
            stock_data_df = self.download_stock(symbol=code, name=name)

            # 创建保存目录
            if not os.path.exists(self.save_path):
                os.makedirs(self.save_path)

            # 保存为csv文件
            stock_data_df.to_csv(
                f"{self.save_path}/{code}_{name.replace('*','')}.csv",
                index=False,
                encoding="utf-8",
            )
            self.logger.info(f"成功下载 {code}({name}) - 共{len(stock_data_df)}条记录")
            time.sleep(0.8)  # 等待0.8秒再下载下一个防止被ban

    # 将数据更新到今日
    def update_stocks_data(self):
        # 由于更新也需要遍历每一个stocks，故直接全部重新下载。
        self.logger.info(f"启动更新任务...")
        self.download_all_stocks(update_all_symbols=True)


if __name__ == "__main__":
    downloader = stocksDataDownload()
    downloader.download_all_stocks()
