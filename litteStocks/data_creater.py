import os
import logging
import pandas as pd


from .factor import RelativeRatioVolumeFactorForGet
from .utils import setup_logger

"""
创建可直接使用的数据
1、将所有股票数据合并为一个DataFrame，包含股票代码、名称、日期、因子值等信息
"""


class DataCreater:
    def __init__(self, root_path=os.getcwd().replace("\\", "/")):
        self.logger = setup_logger("DataCreater", log_level=logging.DEBUG)
        self.root_path = root_path

    def _merge_all_stock_data(self):
        """将所有股票数据合并为一个DataFrame（内存安全）"""
        all_dfs = []
        stocks_name_list = os.listdir(self.root_path + "/download/stocks")
        self.logger.info("开始合并所有股票数据...")
        for index, stock_file in enumerate(stocks_name_list):
            if not stock_file.endswith(".csv"):
                continue
            stock_code = stock_file.split("_")[0]
            stock_name = stock_file.split("_")[1].split(".")[0]
            stock_df = RelativeRatioVolumeFactorForGet(
                root_path=self.root_path
            ).get_code_factor_merge_by_code(stock_code)

            if stock_df is not None:
                if len(stock_df) < 360:
                    self.logger.warning(
                        f"股票上市不足一年, 不采用: {stock_code} - {stock_name}，数据量: {len(stock_df)}"
                    )
                    continue

                stock_df["stock_code"] = stock_code
                stock_df["stock_name"] = stock_name

                all_dfs.append(stock_df)
                self.logger.info(
                    f"[{index + 1}/{len(stocks_name_list)}] 已处理股票: {stock_code} - {stock_name}"
                )
            else:
                self.logger.warning(f"未找到股票因子数据: {stock_code}")

        # 按日期和股票代码排序（关键优化）
        all_df = pd.concat(all_dfs, ignore_index=True)
        all_df = all_df.sort_values(["日期", "stock_code"])  # 排序加速groupby

        # 保存合并结果（仅需一次）
        if not os.path.exists(f"{self.root_path}/download/parquet"):
            os.makedirs(f"{self.root_path}/download/parquet")
        all_df.to_parquet(
            f"{self.root_path}/download/parquet/all_stock_data.parquet",
            engine="auto",
            index=False,
        )
        self.logger.info(
            f"合并结果已保存至: {self.root_path}/download/parquet/all_stock_data.parquet"
        )
        return all_df

    def create_data(self):
        """创建数据主函数"""
        if os.path.exists(f"{self.root_path}/download/parquet/all_stock_data.parquet"):
            self.logger.info("已存在合并数据文件，直接加载...")
            merged_df = pd.read_parquet(
                f"{self.root_path}/download/parquet/all_stock_data.parquet"
            )
            return merged_df

        self.logger.info("合并数据文件不存在，开始合并股票数据...")
        merged_df = self._merge_all_stock_data()
        return merged_df
