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
        data_path=current_dir_parent.replace("\\", "/"), symbol=code
    )
    factor_value = ins.get_single_day_code_factor(day="2023-01-01")
    assert factor_value is not None, "Factor value should not be None"
    print(factor_value)


if __name__ == "__main__":
    test_relative_ratio_volume_factor(code="000001")
