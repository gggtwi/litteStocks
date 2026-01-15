import sys
import os

run_dir = os.getcwd().replace("\\", "/").replace("/tests", "")
sys.path.insert(0, run_dir)

import pandas as pd

parquet_path = f"{run_dir}/download/parquet/all_stock_data.parquet"

df = pd.read_parquet(parquet_path, engine="auto")
df = df[df["stock_name"].str.contains("ST") == False].reset_index(drop=True)
df = df.dropna().reset_index(drop=True)

# 剔除2010年之前的日期
df = df[df["日期"] >= "2010-01-01"].reset_index(drop=True)


# 将每个日期下的股票按因子值排序，并添加排名列
df["factor_rank"] = df.groupby("日期")["20日成交量相对比率"].rank(ascending=True)

# TODO: 进一步处理数据，将因子排序转为百分位数等

print(df.head(5))
