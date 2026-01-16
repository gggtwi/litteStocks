import sys
import os

run_dir = os.getcwd().replace("\\", "/").replace("/tests", "")
sys.path.insert(0, run_dir)

from litteStocks.data_creater import DataCreater
from litteStocks.factor import RelativeRatioVolumeFactorForGet
import pandas as pd


parquet_path = f"{run_dir}/download/parquet/all_stock_data.parquet"
# RelativeRatioVolumeFactorForGet(root_path=run_dir).make_factor_csv()
if not os.path.exists(parquet_path):
    DataCreater().get_parquet_data()

df = pd.read_parquet(parquet_path, engine="auto")
df = df[df["stock_name"].str.contains("ST") == False].reset_index(drop=True)
df = df.dropna().reset_index(drop=True)

# 剔除2010年之前的日期
df = df[df["日期"] >= "2010-01-01"].reset_index(drop=True)


# 将每个日期下的股票按因子值排序，并添加排名列
df["factor_rank"] = df.groupby("日期")["20日成交量相对比率"].rank(ascending=True)

# 进一步处理数据，将因子排序转为百分位数等
df["factor_percentile"] = df.groupby("日期")["20日成交量相对比率"].rank(pct=True, ascending=True)

# TODO: 记录股票20个交易日后的开盘加
df["next_month_open"] = df.groupby("股票代码")["开盘"].shift(20)

# 筛选出因子百分位数高于90%的股票
df = df[df["factor_percentile"] >= 0.9].reset_index(drop=True)

# TODO: 计算收益

print(df.head(5))
