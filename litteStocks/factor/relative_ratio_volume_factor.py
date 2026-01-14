import logging
import math
import os
import pandas as pd
from ..utils.logger_utils import setup_logger


class RelativeRatioVolumeFactor:
    def __init__(
        self,
        root_path="",
        n_days=20,  # 在n日中计算
    ):
        self.root_path = root_path
        self.n_days = n_days
        self.factor_name = f"{self.n_days}日成交量相对比率"
        self.logger = setup_logger("RelativeRatioVolumeFactor", logging.INFO)
        self.factor_path = (
            f"{self.root_path}/download/factor/stocks/{self.n_days}_relative_volume"
        )
        if not os.path.exists(self.factor_path):
            self.logger.info(f"{self.factor_name} 因子文件不存在，开始计算因子...")
            self.make_factor_csv()

    # 因子计算
    def factor_formula(self, data_df):
        df = pd.DataFrame(columns=[self.factor_name, "日期"])
        df["日期"] = data_df["日期"]
        df[self.factor_name] = (
            data_df["成交量"] / data_df["成交量"].rolling(window=self.n_days).mean()
        )
        return df

    # 计算因子并保存为csv文件
    def make_factor_csv(self):
        data_names = os.listdir(
            self.root_path + "/download/stocks"
        )  # 获取股票数据文件列表
        csv_path = f"{self.root_path}/download/factor/stocks/{self.n_days}_relative_volume"  # 因子文件保存路径
        if not os.path.exists(csv_path):
            os.makedirs(csv_path)
        self.logger.info(f"开始计算并保存 {self.factor_name} 因子文件...")
        for index, data_name in enumerate(data_names):
            data_df = pd.read_csv(f"{self.root_path}/download/stocks/{data_name}")
            df = self.factor_formula(data_df)
            df.to_csv(f"{csv_path}/{data_name.split('_')[0]}.csv", index=False)
            self.logger.info(
                f"[{index}/{len(data_names)}]已保存因子文件: {data_name.split('_')[0]}.csv"
            )
        self.logger.info(f"{self.factor_name} 因子文件计算并保存完成。")

    # 获取某代码股票的因子数据
    def get_factor_by_code(self, code):
        csv_path = f"{self.root_path}/download/factor/stocks/{self.n_days}_relative_volume/{code}.csv"
        if not os.path.exists(csv_path):
            self.logger.error(f"因子文件不存在: {csv_path}")
            return None
        df = pd.read_csv(csv_path)
        return df

    # 获取某代码股票和因子数据的合并
    def get_code_factor_merge_by_code(self, code):
        stock_csv_path = f"{self.root_path}/download/stocks/{code}.csv"
        factor_csv_path = f"{self.root_path}/download/factor/stocks/{self.n_days}_relative_volume/{code}.csv"
        if not os.path.exists(stock_csv_path):
            self.logger.error(f"股票文件不存在: {stock_csv_path}")
            return None
        if not os.path.exists(factor_csv_path):
            self.logger.error(f"因子文件不存在: {factor_csv_path}")
            return None
        stock_df = pd.read_csv(stock_csv_path)
        factor_df = pd.read_csv(factor_csv_path)
        merged_df = pd.merge(stock_df, factor_df, on="日期", how="inner")

        merged_df["因子与成交量比"] = merged_df[self.factor_name] / merged_df["成交量"]
        return merged_df


class RelativeRatioVolumeFactorForGet(RelativeRatioVolumeFactor):
    def __init__(
        self, root_path="download", n_days=20, symbol="000001", factor_only=False
    ):
        super().__init__(root_path, n_days)
        if factor_only:
            self.factor_df = self.get_factor_by_code(symbol)
        else:
            self.factor_df = self.get_code_factor_merge_by_code(symbol)

    def get_single_day_code_factor(self, day) -> float:
        if self.factor_df is None or self.factor_df.empty:
            self.logger.error("因子数据不存在，无法获取单日因子值")
            raise ValueError("因子数据不存在，无法获取单日因子值")

        if self.factor_df.loc[self.factor_df["日期"] == day, self.factor_name].empty():
            self.logger.error(f"指定日期 {day} 的因子值不存在")
            raise ValueError(f"指定日期 {day} 的因子值不存在")

        ans = self.factor_df.loc[
            self.factor_df["日期"] == day, self.factor_name
        ].values[0]

        return float(ans)
