import os
import json


class JsonSaveLoadUtils:
    """保存与加载工具类"""

    @staticmethod
    def save_dict_to_json(data_dict, file_path):
        """将字典保存为JSON文件"""
        with open(file_path, "w", encoding="utf-8") as json_file:
            json.dump(data_dict, json_file, ensure_ascii=False, indent=4)

    @staticmethod
    def load_dict_from_json(file_path):
        """从JSON文件加载字典"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"{file_path} not found.")
        with open(file_path, "r", encoding="utf-8") as json_file:
            data_dict = json.load(json_file)
        return data_dict
