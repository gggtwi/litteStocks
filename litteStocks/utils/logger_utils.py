import os
import logging
from datetime import datetime


# TODO: 为logger添加颜色
def setup_logger(
    logger_name="ETFDownloader", log_level: int = logging.INFO
) -> logging.Logger:
    """配置日志记录器"""
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    # 避免重复添加处理器
    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 文件处理器
    log_dir = os.path.join("download", "logs", logger_name)
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.FileHandler(
        os.path.join(
            log_dir, f"download_{logger_name}_{datetime.now().strftime('%Y%m%d')}.log"
        ),
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
