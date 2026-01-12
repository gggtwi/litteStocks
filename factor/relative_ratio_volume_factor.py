import logging
import math
import os
import pandas as pd


class RelativeRatioVolumeFactor:
    def __init__(
        self,
        data_path="download",
        n_days=20,  # 在n日中计算
    ):
        self.data_path = data_path
        self.n_days = n_days

    # 因子计算
    def factor_formula(self, data_df):
        df = pd.DataFrame(columns=[f"{self.n_days}日成交量相对比率"])
        df[f"{self.n_days}日成交量相对比率"] = (
            data_df["成交量"] / data_df["成交量"].rolling(window=self.n_days).mean()
        )
        return df
    
    # 计算因子并保存为csv文件
    def get_data(self, csv=False):
        data_names = os.listdir(f"{self.data_path}/stocks")
        csv_path = f"{self.data_path}/factor/stocks/{self.n_days}_relative_volume"
        if not os.path.exists(csv_path):
            os.makedirs(csv_path)
        for data_name in data_names:
            data_df = pd.read_csv(f"{self.data_path}/stocks/{data_name}")
            df = self.factor_formula(data_df)
            df.to_csv(f"{csv_path}/{data_name.split('_')[0]}.csv", index=False)

    # 获取某代码股票的某天因子数据

    # 获取某代码股票的因子数据

    # 获取某代码股票和因子数据的合并

if __name__ == "__main__":
    ins = RelativeRatioVolumeFactor()
    ins.get_data()
