#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Date: 2025/3/22
Desc: ETF分钟级数据下载工具 - 基于akshare
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Tuple, Set, Union
import pandas as pd
import akshare as ak

# 添加模块搜索路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)


# 配置日志
def setup_logger(log_level: int = logging.INFO) -> logging.Logger:
    """配置日志记录器"""
    logger = logging.getLogger("ETFMinuteDownloader")
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
    log_dir = os.path.join("download", "min", "logs")
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.FileHandler(
        os.path.join(
            log_dir, f"minute_download_{datetime.now().strftime('%Y%m%d')}.log"
        ),
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def backup_progress_file(progress_file: str) -> None:
    """备份进度文件"""
    if not os.path.exists(progress_file):
        return

    backup_file = f"{progress_file}.bak"
    try:
        import shutil

        shutil.copy2(progress_file, backup_file)
        logging.info(f"📁 已备份进度文件到: {backup_file}")
    except Exception as e:
        logging.warning(f"⚠️ 备份进度文件失败: {str(e)}")


def clear_progress_file(progress_file: str) -> None:
    """清空进度文件，用于强制重新下载所有ETF"""
    try:
        # 先备份
        backup_progress_file(progress_file)

        # 创建空的进度文件
        with open(progress_file, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "downloaded": {},
                    "failed": {},
                    "last_update": "",
                    "last_update_start": "",
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        logging.info("🧹 已清空进度文件，将重新下载所有ETF分钟数据")
    except Exception as e:
        logging.error(f"❌ 清空进度文件失败: {str(e)}")
        raise


class ETFMinuteDataDownloader:
    """ETF分钟级数据下载工具类"""

    def __init__(
        self,
        download_dir: str = "download/min",
        progress_file: str = "download/min/etf_minute_progress.json",
        max_retries: int = 3,
        retry_delay: float = 2.0,
        log_level: int = logging.INFO,
        periods: List[str] = ["1", "5", "15"],  # 支持的分钟周期
        days_to_download: int = 30,  # 默认下载最近30天数据
    ):
        """
        初始化分钟级数据下载器

        Args:
            download_dir: 数据下载目录
            progress_file: 进度记录文件路径
            max_retries: 最大重试次数
            retry_delay: 基础重试延迟(秒)
            log_level: 日志级别
            periods: 要下载的分钟周期列表
            days_to_download: 要下载的历史天数
        """
        self.download_dir = download_dir
        self.progress_file = progress_file
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.periods = periods
        self.days_to_download = days_to_download

        # 创建下载目录
        os.makedirs(self.download_dir, exist_ok=True)
        for period in self.periods:
            os.makedirs(os.path.join(self.download_dir, f"{period}min"), exist_ok=True)

        # 配置日志
        self.logger = setup_logger(log_level)

        # 加载进度
        self.progress = self._load_progress()
        self.downloaded_etfs = self.progress.get("downloaded", {})
        self.failed_etfs = self.progress.get("failed", {})

        self.logger.info(f"ETF分钟级数据下载工具初始化完成")
        self.logger.info(f"下载目录: {os.path.abspath(self.download_dir)}")
        self.logger.info(f"分钟周期: {', '.join(self.periods)}")
        self.logger.info(f"历史天数: {self.days_to_download}天")

        # 同步已有文件
        self._sync_existing_files()

    def _load_progress(self) -> Dict:
        """加载下载进度"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"加载进度文件失败: {str(e)}，使用默认进度")

        return {
            "downloaded": {},
            "failed": {},
            "last_update": "",
            "last_update_start": "",
        }

    def _save_progress(self) -> None:
        """保存下载进度"""
        os.makedirs(os.path.dirname(self.progress_file), exist_ok=True)
        try:
            with open(self.progress_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "downloaded": self.downloaded_etfs,
                        "failed": self.failed_etfs,
                        "last_update": self.progress.get("last_update", ""),
                        "last_update_start": self.progress.get("last_update_start", ""),
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )
            self.logger.debug("进度保存成功")
        except Exception as e:
            self.logger.error(f"保存进度失败: {str(e)}")

    def _sync_existing_files(self) -> None:
        """同步已有文件到进度记录"""
        for period in self.periods:
            period_dir = os.path.join(self.download_dir, f"{period}min")
            if not os.path.exists(period_dir):
                continue

            for filename in os.listdir(period_dir):
                if filename.endswith(".csv"):
                    parts = filename.split("_")
                    if len(parts) >= 2:
                        symbol = parts[0]
                        if symbol.replace(".", "").isdigit():  # 确保是有效的ETF代码
                            if symbol not in self.downloaded_etfs:
                                self.downloaded_etfs[symbol] = {}
                            self.downloaded_etfs[symbol][period] = {
                                "last_date": datetime.now().strftime("%Y%m%d"),
                                "status": "completed",
                            }
        self._save_progress()

    def _get_etf_list(self) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """获取ETF列表"""
        try:
            etf_df = ak.fund_etf_spot_em()
            etf_names = dict(zip(etf_df["代码"], etf_df["名称"]))
            self.logger.info(f"成功获取 {len(etf_df)} 只ETF基础信息")
            return etf_df, etf_names
        except Exception as e:
            self.logger.error(f"获取ETF列表失败: {str(e)}")
            raise

    def _generate_filename(self, symbol: str, name: str, period: str) -> str:
        """生成标准化的文件名"""
        clean_name = name
        for char in '*\\/:"?<>|':
            clean_name = clean_name.replace(char, "_")
        return f"{symbol}_{clean_name}_{period}min.csv"

    def _get_date_range(self) -> Tuple[str, str]:
        """获取要下载的日期范围"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.days_to_download)
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    def get_existing_etfs(self) -> List[str]:
        """获取已下载的ETF代码列表"""
        etf_symbols = set()

        for period in self.periods:
            period_dir = os.path.join(self.download_dir, f"{period}min")
            if not os.path.exists(period_dir):
                continue

            for filename in os.listdir(period_dir):
                if filename.endswith(".csv"):
                    symbol = filename.split("_")[0]
                    if symbol.replace(".", "").isdigit():
                        etf_symbols.add(symbol)

        return sorted(list(etf_symbols))

    def _download_single_etf_minute(
        self, symbol: str, name: str, period: str, start_date: str, end_date: str
    ) -> Tuple[bool, Optional[pd.DataFrame]]:
        """
        下载单个ETF的分钟级数据

        Args:
            symbol: ETF代码
            name: ETF名称
            period: 分钟周期
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            (成功状态, 数据DataFrame)
        """
        # 生成文件路径
        filename = self._generate_filename(symbol, name, period)
        filepath = os.path.join(self.download_dir, f"{period}min", filename)

        self.logger.debug(
            f"开始下载 {symbol}({name}) {period}分钟数据: {start_date} 至 {end_date}"
        )

        for retry in range(self.max_retries):
            try:
                # 调用akshare接口获取分钟数据
                df = ak.fund_etf_hist_min_em(
                    symbol=symbol,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                    adjust="",
                )

                if df.empty:
                    self.logger.warning(f"ETF {symbol}({name}) {period}分钟数据为空")
                    return False, None

                # 保存数据
                df.to_csv(filepath, index=False, encoding="utf_8_sig")
                self.logger.info(
                    f"成功下载 {symbol}({name}) {period}分钟数据 - {len(df)}条记录，保存至 {filename}"
                )
                return True, df

            except Exception as e:
                wait_time = self.retry_delay * (retry + 1)
                self.logger.warning(
                    f"下载 {symbol}({name}) {period}分钟数据失败 [{retry+1}/{self.max_retries}]: {str(e)}，等待 {wait_time:.1f}秒后重试"
                )
                if retry < self.max_retries - 1:
                    time.sleep(wait_time)

        self.logger.error(
            f"ETF {symbol}({name}) {period}分钟数据下载失败，已达最大重试次数"
        )
        return False, None

    def download_all_etfs(self, symbols: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        下载所有ETF的分钟级数据

        Args:
            symbols: 要下载的ETF代码列表，None表示下载所有

        Returns:
            结果统计字典
        """
        start_time = time.time()
        self.progress["last_update_start"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        self._save_progress()

        # 获取ETF列表
        etf_df, etf_names = self._get_etf_list()
        all_symbols = etf_df["代码"].tolist() if symbols is None else symbols

        # 获取日期范围
        start_date, end_date = self._get_date_range()
        self.logger.info(f"下载日期范围: {start_date} 至 {end_date}")

        # 统计
        total_etfs = len(all_symbols)
        total_periods = len(self.periods)
        total_tasks = total_etfs * total_periods

        success_count = 0
        fail_count = 0
        failed_details = []

        self.logger.info(
            f"开始下载 {total_etfs} 只ETF的分钟数据，共 {total_tasks} 个任务"
        )

        # 逐个ETF下载
        for i, symbol in enumerate(all_symbols, 1):
            name = etf_names.get(symbol, symbol)
            self.logger.info(f"[{i}/{total_etfs}] 处理ETF: {symbol}({name})")

            # 每个ETF的每个周期
            for period in self.periods:
                # 检查是否已下载
                if (
                    symbol in self.downloaded_etfs
                    and period in self.downloaded_etfs[symbol]
                ):
                    self.logger.debug(f"  跳过 {symbol} {period}分钟数据 (已下载)")
                    continue

                # 下载数据
                self.logger.debug(f"  下载 {symbol} {period}分钟数据")
                success, _ = self._download_single_etf_minute(
                    symbol, name, period, start_date, end_date
                )

                # 更新进度
                if symbol not in self.downloaded_etfs:
                    self.downloaded_etfs[symbol] = {}

                if success:
                    success_count += 1
                    self.downloaded_etfs[symbol][period] = {
                        "last_date": end_date.replace("-", ""),
                        "status": "completed",
                        "download_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    if symbol in self.failed_etfs and period in self.failed_etfs.get(
                        symbol, {}
                    ):
                        del self.failed_etfs[symbol][period]
                        if not self.failed_etfs[symbol]:
                            del self.failed_etfs[symbol]
                else:
                    fail_count += 1
                    failed_details.append(f"{symbol}_{period}min")
                    if symbol not in self.failed_etfs:
                        self.failed_etfs[symbol] = {}
                    self.failed_etfs[symbol][period] = {
                        "last_attempt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "reason": "download_failed",
                    }

                # 保存进度
                self._save_progress()

                # 遵守请求频率限制
                time.sleep(1.5)

            # 每完成一个ETF，更新总进度
            self.progress["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._save_progress()

        # 总结
        elapsed = time.time() - start_time
        self.logger.info(
            f"下载完成 - 成功: {success_count}, 失败: {fail_count}, 耗时: {elapsed:.1f}秒"
        )

        return {
            "success_count": success_count,
            "fail_count": fail_count,
            "failed_details": failed_details,
            "total_time": elapsed,
            "total_etfs": total_etfs,
            "total_periods": total_periods,
        }


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="ETF分钟级数据下载工具")
    parser.add_argument(
        "--full", action="store_true", help="强制全量下载（忽略已有数据）"
    )
    parser.add_argument(
        "--full-history", action="store_true", help="下载完整历史数据（清空进度记录）"
    )
    parser.add_argument(
        "--update", action="store_true", help="仅更新已有ETF数据（增量更新）"
    )
    parser.add_argument("--symbol", type=str, help="指定单个ETF代码，例如: 513500")
    parser.add_argument(
        "--symbols-file", type=str, help="从文件读取ETF代码列表，每行一个代码"
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="日志级别",
    )
    parser.add_argument(
        "--download-dir", type=str, default="download/min", help="下载目录路径"
    )
    parser.add_argument(
        "--progress-file",
        type=str,
        default="download/min/etf_minute_progress.json",
        help="进度文件路径",
    )
    parser.add_argument("--max-retries", type=int, default=3, help="最大重试次数")
    parser.add_argument(
        "--retry-delay", type=float, default=2.0, help="基础重试延迟(秒)"
    )
    parser.add_argument("--days", type=int, default=60, help="要下载的历史天数")
    parser.add_argument(
        "--periods", type=str, default="1,5,15", help="要下载的分钟周期，用逗号分隔"
    )

    args = parser.parse_args()

    # 配置日志
    log_level = getattr(logging, args.log_level.upper())
    logger = setup_logger(log_level)

    # 显示启动信息
    logger.info("=" * 60)
    logger.info("🚀 ETF分钟级数据下载工具")
    logger.info(f"版本: 1.0")
    logger.info(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"下载目录: {os.path.abspath(args.download_dir)}")
    logger.info(f"进度文件: {os.path.abspath(args.progress_file)}")
    logger.info(f"历史天数: {args.days}天")
    logger.info(f"分钟周期: {args.periods}")
    logger.info("=" * 60)

    # 处理参数
    periods = [p.strip() for p in args.periods.split(",")]
    symbols = None

    if args.symbol:
        symbols = [args.symbol.strip()]
        logger.info(f"🎯 指定ETF: {args.symbol}")
    elif args.symbols_file and os.path.exists(args.symbols_file):
        with open(args.symbols_file, "r", encoding="utf-8") as f:
            symbols = [
                line.strip() for line in f if line.strip() and not line.startswith("#")
            ]
        logger.info(f"📋 从文件加载 {len(symbols)} 个ETF代码")

    # 如果需要下载完整历史，清空进度文件
    if args.full and args.full_history:
        if os.path.exists(args.progress_file):
            clear_progress_file(args.progress_file)
        else:
            logger.info("ℹ️ 进度文件不存在，将创建新文件")

    try:
        # 创建下载器
        downloader = ETFMinuteDataDownloader(
            download_dir=args.download_dir,
            progress_file=args.progress_file,
            max_retries=args.max_retries,
            retry_delay=args.retry_delay,
            log_level=log_level,
            periods=periods,
            days_to_download=args.days,
        )

        # 确定运行模式
        mode = "full" if args.full else "update"
        if not args.full and not args.update:
            mode = "auto"
            existing_etfs = downloader.get_existing_etfs()
            if not existing_etfs:
                mode = "full"
                logger.info("未发现已有数据，自动切换到全量下载模式")
            else:
                mode = "update"
                logger.info(
                    f"发现 {len(existing_etfs)} 个已有ETF数据，自动切换到增量更新模式"
                )

        # 执行下载
        logger.info("\n" + "=" * 60)
        logger.info(f"⚡ 开始执行 {mode} 操作")
        logger.info("=" * 60)

        start_time = time.time()
        result = downloader.download_all_etfs(symbols)
        elapsed_time = time.time() - start_time

        # 显示结果
        logger.info("\n" + "=" * 60)
        logger.info("✅ 任务完成!")
        logger.info(f"成功: {result['success_count']}, 失败: {result['fail_count']}")
        logger.info(f"耗时: {elapsed_time:.1f} 秒")
        logger.info(f"进度已保存到: {args.progress_file}")
        logger.info("=" * 60)

        # 显示失败列表
        if result.get("fail_count", 0) > 0:
            failed_details = result.get("failed_details", [])
            logger.error(f"\n❌ {len(failed_details)} 个ETF分钟数据下载失败:")
            for i, detail in enumerate(failed_details[:10], 1):
                logger.error(f" {i}. {detail}")
            if len(failed_details) > 10:
                logger.error(f" ... 还有 {len(failed_details)-10} 个失败的任务")

        # 显示最终统计
        existing_etfs = downloader.get_existing_etfs()
        logger.info(f"\n📊 最终统计:")
        logger.info(f" 总ETF数量: {len(existing_etfs)}")
        logger.info(f" 本次成功: {result['success_count']}")

        return 0

    except KeyboardInterrupt:
        logger.warning("\n\n⚠️ 用户中断操作，进度已自动保存")
        logger.warning("下次运行将自动从断点继续")
        return 1
    except Exception as e:
        logger.exception(f"\n❌ 严重错误: {str(e)}")
        logger.error("进度已保存，修复问题后可继续运行")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
