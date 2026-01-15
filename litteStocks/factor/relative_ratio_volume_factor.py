import logging
import math
import os
import pandas as pd
from ..utils import setup_logger, JsonSaveLoadUtils


class RelativeRatioVolumeFactor:
    def __init__(
        self,
        root_path="",
        n_days=20,  # 在n日中计算
        code_name_dict_path="download/stocks/stocks_code_name_dict.json",
    ):
        self.logger = setup_logger("RelativeRatioVolumeFactor", logging.INFO)
        self.root_path = root_path
        self.n_days = n_days
        self.factor_name = f"{self.n_days}日成交量相对比率"
        self.code_name_dict = JsonSaveLoadUtils().load_dict_from_json(
            f"{self.root_path}/{code_name_dict_path}"
        )
        self.logger.info(
            f"成功加载股票代码与名称字典: {self.root_path}/{code_name_dict_path}"
        )
        self.factor_path = (
            f"{self.root_path}/download/factor/stocks/{self.n_days}_relative_volume"
        )
        self.logger.info(f"因子文件保存路径: {self.factor_path}")
        if not os.path.exists(self.factor_path):
            self.logger.info(f"{self.factor_name} 因子文件不存在，开始计算因子...")
            self._make_factor_csv()
            self.logger.info(f"{self.factor_name} 因子文件计算完成。")

    # 因子计算
    def _factor_formula(self, data_df):
        df = pd.DataFrame(columns=[self.factor_name, "日期"])
        df["日期"] = data_df["日期"]
        df[self.factor_name] = (
            data_df["成交量"] / data_df["成交量"].rolling(window=self.n_days).mean()
        )
        return df

    # 计算因子并保存为csv文件
    def _make_factor_csv(self):
        data_names = os.listdir(
            self.root_path + "/download/stocks"
        )  # 获取股票数据文件列表
        csv_path = f"{self.root_path}/download/factor/stocks/{self.n_days}_relative_volume"  # 因子文件保存路径
        if not os.path.exists(csv_path):
            os.makedirs(csv_path)
        self.logger.info(f"开始计算并保存 {self.factor_name} 因子文件...")
        for index, data_name in enumerate(data_names):
            data_df = pd.read_csv(f"{self.root_path}/download/stocks/{data_name}")
            df = self._factor_formula(data_df)
            df.to_csv(f"{csv_path}/{data_name.split('_')[0]}.csv", index=False)
            self.logger.info(
                f"[{index}/{len(data_names)}]已保存因子文件: {data_name.split('_')[0]}.csv"
            )
        self.logger.info(f"{self.factor_name} 因子文件计算并保存完成。")

    # 获取某代码股票的因子数据
    def _get_factor_by_code(self, code):
        csv_path = f"{self.root_path}/download/factor/stocks/{self.n_days}_relative_volume/{code}.csv"
        if not os.path.exists(csv_path):
            self.logger.error(f"因子文件不存在: {csv_path}")
            return None
        df = pd.read_csv(csv_path)
        return df

    # 获取某代码股票和因子数据的合并
    def _get_code_factor_merge_by_code(self, code):
        stock_csv_path = (
            f"{self.root_path}/download/stocks/{code}_{self.code_name_dict[code]}.csv"
        )
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

        return merged_df


class RelativeRatioVolumeFactorForGet:
    def __init__(
        self,
        root_path="",
        n_days=20,
        factor_only=False,
        code_name_dict_path="download/stocks/stocks_code_name_dict.json",
    ):
        self.root_path = root_path
        self.n_days = n_days
        self.factor_only = factor_only
        self.code_name_dict_path = code_name_dict_path

    def get_code_factor_merge_by_code(self, code) -> pd.DataFrame:
        return RelativeRatioVolumeFactor(
            root_path=self.root_path,
            n_days=self.n_days,
            code_name_dict_path=self.code_name_dict_path,
        )._get_code_factor_merge_by_code(code)
