import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
current_dir_parent = os.path.dirname(current_dir)
sys.path.insert(0, current_dir_parent)

from litteStocks.factor.relative_ratio_volume_factor import (
    RelativeRatioVolumeFactorForGet,
)


def test_relative_ratio_volume_factor(code: str):
    ins = RelativeRatioVolumeFactorForGet(
        root_path=current_dir_parent.replace("\\", "/"),
        symbol=code,
        code_name_dict_path="download/stocks/stocks_code_name_dict.json",
    )
    factor_value = ins.get_single_code_factor_by_day(day="2026-01-08")
    assert factor_value is not None, "Factor value should not be None"
    print(factor_value)


if __name__ == "__main__":
    test_relative_ratio_volume_factor(code="000001")
