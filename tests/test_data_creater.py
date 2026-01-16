import sys
import os

run_dir = os.getcwd().replace("\\", "/").replace("/tests", "")
sys.path.insert(0, run_dir)
from litteStocks.data_creater import DataCreater


def test_data_creater():
    data_creater = DataCreater(root_path=run_dir)
    merged_data = data_creater.get_parquet_data()
    print(merged_data.head(5))


if __name__ == "__main__":
    test_data_creater()
