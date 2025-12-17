#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2025/3/21
Desc: ETF数据下载工具类 - 支持断点续传与增量更新
"""

import os
import json
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Set, Tuple
import akshare as ak
from typing import List, Dict, Optional, Union, Set, Tuple, Any, Literal, Mapping


class ETFDataDownloader:
    """ETF数据下载工具类，支持断点续传和增量更新"""

    def __init__(
        self,
        download_dir: str = "download",
        progress_file: str = "download/etf_download_progress.json",
        max_retries: int = 3,
        retry_delay: float = 2.0,
        log_level: int = logging.INFO,
    ):
        """
        初始化下载器

        Args:
            download_dir: 数据下载目录
            progress_file: 进度记录文件路径
            max_retries: 单个ETF最大重试次数
            retry_delay: 重试基础延迟(秒)，实际使用指数退避
            log_level: 日志级别
        """
        self.download_dir = download_dir
        self.progress_file = progress_file
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # 创建下载目录
        os.makedirs(self.download_dir, exist_ok=True)

        # 配置日志
        self._setup_logger(log_level)

        # 加载进度
        self.progress = self._load_progress()
        self.downloaded_etfs = set(self.progress.get("downloaded", []))
        self.failed_etfs = set(self.progress.get("failed", []))

        self.logger.info(f"ETF数据下载工具初始化完成")
        self.logger.info(f"下载目录: {os.path.abspath(self.download_dir)}")
        self.logger.info(f"已下载ETF数量: {len(self.downloaded_etfs)}")
        self.logger.info(f"失败ETF数量: {len(self.failed_etfs)}")

        self._sync_existing_files_to_progress()

    def _sync_existing_files_to_progress(self):
        """将已存在的ETF文件同步到进度记录"""
        existing_files = self.get_existing_etfs()
        newly_added = 0
        
        for symbol in existing_files:
            if symbol not in self.downloaded_etfs:
                self.downloaded_etfs.add(symbol)
                if symbol in self.failed_etfs:
                    self.failed_etfs.remove(symbol)
                newly_added += 1
        
        if newly_added > 0:
            self.logger.info(f"自动同步 {newly_added} 个新发现的ETF文件到进度记录")
            self._save_progress()

    def _setup_logger(self, log_level: int) -> None:
        """配置日志记录器"""
        self.logger = logging.getLogger("ETFDataDownloader")
        self.logger.setLevel(log_level)

        # 避免重复添加处理器
        if not self.logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )

            # 控制台处理器
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

            # 文件处理器
            log_dir = os.path.join(self.download_dir, "logs")
            os.makedirs(log_dir, exist_ok=True)
            file_handler = logging.FileHandler(
                os.path.join(
                    log_dir, f"etf_downloader_{datetime.now().strftime('%Y%m%d')}.log"
                ),
                encoding="utf-8",
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def _load_progress(self) -> Dict:
        """加载下载进度"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"加载进度文件失败: {str(e)}，使用默认进度")
        return {"downloaded": [], "last_update": "", "failed": []}

    def _save_progress(self) -> None:
        """保存下载进度"""
        os.makedirs(os.path.dirname(self.progress_file), exist_ok=True)
        try:
            with open(self.progress_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "downloaded": list(self.downloaded_etfs),
                        "failed": list(self.failed_etfs),
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

    def get_existing_etfs(self) -> List[str]:
        """
        获取已下载的ETF代码列表

        Returns:
            ETF代码列表
        """
        etf_symbols = []
        if not os.path.exists(self.download_dir):
            return etf_symbols

        for filename in os.listdir(self.download_dir):
            if filename.endswith(".csv") and "_" in filename:
                symbol = filename.split("_")[0]
                if symbol.replace(".", "").isdigit():  # 确保是有效的ETF代码
                    etf_symbols.append(symbol)
        return etf_symbols

    def get_last_date_from_file(self, filepath: str) -> Optional[str]:
        """
        从CSV文件获取最后一条数据的日期

        Args:
            filepath: CSV文件路径

        Returns:
            最后一条数据的日期(YYYYMMDD格式)，若失败返回None
        """
        try:
            # 读取最后一行
            df = pd.read_csv(filepath, usecols=["日期"])
            if not df.empty:
                last_date = df["日期"].iloc[-1]
                # 标准化日期格式
                if isinstance(last_date, str):
                    # 尝试多种日期格式
                    for fmt in ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"]:
                        try:
                            dt = datetime.strptime(last_date, fmt)
                            return dt.strftime("%Y%m%d")
                        except ValueError:
                            continue
                elif isinstance(last_date, pd.Timestamp):
                    return last_date.strftime("%Y%m%d")
        except Exception as e:
            self.logger.warning(f"读取文件日期失败 {filepath}: {str(e)}")
        return None

    def _get_etf_spot_data(self) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """
        获取ETF实时行情数据

        Returns:
            (etf_df, etf_names_dict) - ETF数据框和代码-名称映射字典
        """
        try:
            etf_df = ak.fund_etf_spot_em()
            etf_names = dict(zip(etf_df["代码"], etf_df["名称"]))
            self.logger.info(f"成功获取 {len(etf_df)} 只ETF基础信息")
            return etf_df, etf_names
        except Exception as e:
            self.logger.error(f"获取ETF列表失败: {str(e)}")
            raise

    def _generate_filename(self, symbol: str, name: str) -> str:
        """
        生成标准化的文件名

        Args:
            symbol: ETF代码
            name: ETF名称

        Returns:
            标准化文件名
        """
        # 清理文件名中的非法字符
        clean_name = name.replace("*", "_").replace("/", "_").replace("\\", "_")
        clean_name = clean_name.replace(":", "_").replace('"', "_").replace("?", "_")
        clean_name = clean_name.replace("<", "_").replace(">", "_").replace("|", "_")
        return f"{symbol}_{clean_name}.csv"

    def _download_single_etf(
        self,
        symbol: str,
        name: str,
        is_update: bool = False,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Tuple[bool, Optional[pd.DataFrame]]:
        """
        下载单个ETF数据

        Args:
            symbol: ETF代码
            name: ETF名称
            is_update: 是否为更新模式
            start_date: 开始日期(YYYYMMDD格式)，可选
            end_date: 结束日期(YYYYMMDD格式)，可选

        Returns:
            (成功状态, 数据DataFrame)
        """
        filename = self._generate_filename(symbol, name)
        filepath = os.path.join(self.download_dir, filename)

        for retry in range(self.max_retries):
            try:
                # 确定日期范围
                if is_update and start_date:
                    actual_start = start_date
                    actual_end = end_date or datetime.now().strftime("%Y%m%d")
                    self.logger.debug(
                        f"增量下载 {symbol} 范围: {actual_start} 至 {actual_end}"
                    )
                else:
                    actual_start = "19700101"
                    actual_end = "20500101"
                    self.logger.debug(f"全量下载 {symbol}")

                # 获取数据
                hist_df = ak.fund_etf_hist_em(
                    symbol=symbol,
                    period="daily",
                    start_date=actual_start,
                    end_date=actual_end,
                    adjust="",
                )

                if hist_df.empty:
                    self.logger.warning(f"ETF {symbol}({name}) 无有效数据")
                    return False, None

                # 保存数据
                hist_df.to_csv(filepath, index=False, encoding="utf_8_sig")
                self.logger.info(f"成功下载 {symbol}({name}) - {len(hist_df)}条记录")
                return True, hist_df

            except Exception as e:
                wait_time = self.retry_delay * (retry + 1)
                self.logger.warning(
                    f"下载 {symbol}({name}) 失败 [{retry+1}/{self.max_retries}]: {str(e)}，等待 {wait_time:.1f}秒后重试"
                )
                if retry < self.max_retries - 1:
                    time.sleep(wait_time)

        self.logger.error(f"ETF {symbol}({name}) 下载失败，已达最大重试次数")
        return False, None

    def _update_single_etf(self, symbol: str, name: str) -> Tuple[bool, int]:
        """
        更新单个ETF的增量数据

        Args:
            symbol: ETF代码
            name: ETF名称

        Returns:
            (成功状态, 新增数据条数)
        """
        filename = self._generate_filename(symbol, name)
        filepath = os.path.join(self.download_dir, filename)

        if not os.path.exists(filepath):
            self.logger.warning(f"文件不存在，无法更新: {filepath}")
            return False, 0

        # 获取最后日期
        last_date = self.get_last_date_from_file(filepath)
        if not last_date:
            self.logger.warning(f"无法获取最后日期，使用默认日期: {symbol}")
            last_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

        # 计算更新范围
        try:
            start_date = (
                datetime.strptime(last_date, "%Y%m%d") + timedelta(days=1)
            ).strftime("%Y%m%d")
        except ValueError:
            self.logger.warning(f"日期格式错误，使用默认起始日期: {symbol}")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

        end_date = datetime.now().strftime("%Y%m%d")

        # 检查是否需要更新
        if datetime.strptime(start_date, "%Y%m%d") >= datetime.strptime(
            end_date, "%Y%m%d"
        ):
            self.logger.info(f"ETF {symbol}({name}) 已是最新数据，无需更新")
            return True, 0

        # 下载增量数据
        success, new_data = self._download_single_etf(
            symbol, name, True, start_date, end_date
        )
        if not success or new_data is None or new_data.empty:
            return False, 0

        try:
            # 读取现有数据
            existing_data = pd.read_csv(filepath, parse_dates=["日期"])
            existing_data["日期"] = pd.to_datetime(existing_data["日期"]).dt.strftime(
                "%Y-%m-%d"
            )

            # 处理新数据
            new_data["日期"] = pd.to_datetime(new_data["日期"]).dt.strftime("%Y-%m-%d")

            # 筛选真正的新数据
            existing_dates = set(existing_data["日期"])
            truly_new_data = new_data[~new_data["日期"].isin(existing_dates)]

            if truly_new_data.empty:
                self.logger.info(f"ETF {symbol}({name}) 无新数据")
                return True, 0

            # 合并数据
            updated_data = pd.concat([existing_data, truly_new_data], ignore_index=True)
            updated_data["日期"] = pd.to_datetime(updated_data["日期"])
            updated_data = updated_data.sort_values("日期").reset_index(drop=True)
            updated_data["日期"] = updated_data["日期"].dt.strftime("%Y-%m-%d")

            # 保存更新
            updated_data.to_csv(filepath, index=False, encoding="utf_8_sig")
            self.logger.info(
                f"成功更新 {symbol}({name}) - 新增 {len(truly_new_data)}条数据，总计 {len(updated_data)}条"
            )

            return True, len(truly_new_data)

        except Exception as e:
            self.logger.error(f"更新ETF {symbol}({name}) 时出错: {str(e)}")
            return False, 0

    def download_full_data(
        self, symbols: Optional[List[str]] = None, skip_downloaded: bool = True
    ) -> Dict[str, Union[int, float, List[str]]]:  # 确保这里包含float
        """
        全量下载ETF数据

        Args:
            symbols: 要下载的ETF代码列表，None表示下载所有
            skip_downloaded: 是否跳过已下载的ETF

        Returns:
            结果统计字典，包含成功/失败数量和列表
        """
        start_time = time.time()
        self.progress["last_update_start"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        self._save_progress()

        # 获取ETF列表
        etf_df, etf_names = self._get_etf_spot_data()
        all_symbols = etf_df["代码"].tolist() if symbols is None else symbols

        # 过滤已下载
        if skip_downloaded:
            symbols_to_download = [
                s for s in all_symbols if s not in self.downloaded_etfs
            ]
            self.logger.info(
                f"需要下载 {len(symbols_to_download)}/{len(all_symbols)} 只ETF (跳过 {len(self.downloaded_etfs)} 只已下载)"
            )
        else:
            symbols_to_download = all_symbols
            self.logger.info(f"强制重新下载 {len(symbols_to_download)} 只ETF")

        # 统计
        success_count = 0
        fail_count = 0
        failed_symbols: List[str] = []  # 修正2: 明确类型

        # 逐个下载
        for i, symbol in enumerate(symbols_to_download, 1):
            name = etf_names.get(symbol, symbol)
            self.logger.info(f"[{i}/{len(symbols_to_download)}] 开始下载: {symbol}({name})")

            success, _ = self._download_single_etf(symbol, name)
            if success:
                success_count += 1
                self.downloaded_etfs.add(symbol)
                if symbol in self.failed_etfs:
                    self.failed_etfs.remove(symbol)
            else:
                fail_count += 1
                failed_symbols.append(symbol)
                self.failed_etfs.add(symbol)

            # 更新进度和记录
            self.progress["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._save_progress()

            # 遵守请求频率限制
            time.sleep(0.8)

        # 总结
        elapsed = time.time() - start_time
        self.logger.info(
            f"全量下载完成 - 成功: {success_count}, 失败: {fail_count}, 耗时: {elapsed:.1f}秒"
        )

        return {
            "success_count": success_count,
            "fail_count": fail_count,
            "failed_symbols": failed_symbols,
            "total_time": elapsed,
        }

    def update_existing_data(
        self, symbols: Optional[List[str]] = None
    ) -> Dict[str, Union[int, float, List[str]]]:  # 修正3: 同样扩展返回类型
        """
        增量更新已有ETF数据

        Args:
            symbols: 要更新的ETF代码列表，None表示更新所有已有数据

        Returns:
            结果统计字典，包含成功/失败数量和列表
        """
        start_time = time.time()
        self.progress["last_update_start"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        self._save_progress()

        # 获取需要更新的ETF列表
        existing_etfs = self.get_existing_etfs()
        if symbols is None:
            symbols_to_update = existing_etfs
        else:
            symbols_to_update = [s for s in symbols if s in existing_etfs]

        # 获取最新ETF信息
        try:
            _, etf_names = self._get_etf_spot_data()
        except Exception as e:
            self.logger.warning(f"无法获取最新ETF信息，使用本地名称: {str(e)}")
            etf_names = {}

        self.logger.info(f"需要更新 {len(symbols_to_update)} 只ETF")

        # 统计
        success_count = 0
        fail_count = 0
        failed_symbols: List[str] = []  # 修正4: 明确类型
        total_new_records = 0

        # 逐个更新
        for i, symbol in enumerate(symbols_to_update, 1):
            name = etf_names.get(symbol, self._get_name_from_filename(symbol))
            self.logger.info(f"[{i}/{len(symbols_to_update)}] 开始更新: {symbol}({name})")

            success, new_count = self._update_single_etf(symbol, name)
            if success:
                success_count += 1
                total_new_records += new_count
                self.downloaded_etfs.add(symbol)
                if symbol in self.failed_etfs:
                    self.failed_etfs.remove(symbol)
            else:
                fail_count += 1
                failed_symbols.append(symbol)
                self.failed_etfs.add(symbol)

            # 更新进度
            self.progress["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._save_progress()

            # 遵守请求频率限制
            time.sleep(1.0)

        # 总结
        elapsed = time.time() - start_time
        self.logger.info(
            f"增量更新完成 - 成功: {success_count}, 失败: {fail_count}, 新增记录: {total_new_records}, 耗时: {elapsed:.1f}秒"
        )

        return {
            "success_count": success_count,
            "fail_count": fail_count,
            "failed_symbols": failed_symbols,
            "total_new_records": total_new_records,
            "total_time": elapsed,
        }

    def _get_name_from_filename(self, symbol: str) -> str:
        """从文件名获取ETF名称"""
        for filename in os.listdir(self.download_dir):
            if filename.startswith(f"{symbol}_") and filename.endswith(".csv"):
                return filename.split("_", 1)[1].rsplit(".", 1)[0]
        return symbol

    def run(
        self, mode: str = "auto", symbols: Optional[List[str]] = None
    ) -> Dict[str, Union[int, float, List[str], str]]:  # 修正1: 扩展返回类型包含所有可能的值类型
        """
        执行下载/更新任务

        Args:
            mode: 运行模式 - "full"(全量下载), "update"(增量更新), "auto"(自动判断)
            symbols: 要处理的ETF代码列表，None表示处理所有

        Returns:
            结果统计字典
        """
        if mode == "auto":
            existing_count = len(self.get_existing_etfs())
            if existing_count == 0:
                mode = "full"
                self.logger.info("未发现已有数据，自动切换到全量下载模式")
            else:
                mode = "update"
                self.logger.info(f"发现 {existing_count} 个已有ETF数据，自动切换到增量更新模式")

        if mode == "full":
            return self.download_full_data(symbols)  # type: ignore
        elif mode == "update":
            return self.update_existing_data(symbols)  # type: ignore
        else:
            raise ValueError(f"不支持的模式: {mode}，支持: full, update, auto")


# 命令行接口 (保持与工具类解耦)
def main():
    """命令行入口函数"""
    import argparse
    from tqdm import tqdm

    parser = argparse.ArgumentParser(description="ETF数据下载管理器")
    parser.add_argument("--update", action="store_true", help="仅更新已有ETF数据（增量更新）")
    parser.add_argument("--full", action="store_true", help="强制全量下载（忽略已有数据）")
    parser.add_argument("--symbol", type=str, help="指定下载/更新单个ETF代码，例如: 513500")
    parser.add_argument("--symbols-file", type=str, help="从文件读取ETF代码列表，每行一个代码")
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="日志级别",
    )
    args = parser.parse_args()

    # 确定运行模式
    mode = "auto"
    if args.full:
        mode = "full"
    elif args.update:
        mode = "update"

    # 确定ETF列表
    symbols = None
    if args.symbol:
        symbols = [args.symbol]
    elif args.symbols_file and os.path.exists(args.symbols_file):
        with open(args.symbols_file, "r", encoding="utf-8") as f:
            symbols = [line.strip() for line in f if line.strip()]
        print(f"从文件加载 {len(symbols)} 个ETF代码")

    # 配置日志级别
    log_level = getattr(logging, args.log_level)

    # 创建下载器
    downloader = ETFDataDownloader(log_level=log_level)

    try:
        # 执行任务
        result = downloader.run(mode=mode, symbols=symbols)
        print("\n" + "=" * 60)
        print(f"任务完成! 成功: {result['success_count']}, 失败: {result['fail_count']}")
        if "total_new_records" in result:
            print(f"新增数据记录: {result['total_new_records']}")
        print(f"耗时: {result['total_time']:.1f} 秒")
        print("=" * 60)

        # 显示失败列表 - 修正5: 修复类型问题
        fail_count = result.get("fail_count", 0)
        failed_symbols = result.get("failed_symbols", [])

        if (
            isinstance(fail_count, int)
            and fail_count > 0
            and isinstance(failed_symbols, list)
        ):
            print("\n失败的ETF代码:")
            for symbol in failed_symbols:
                print(f"  - {symbol}")
            print(f"\n可在进度文件中查看详细信息: {downloader.progress_file}")

    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断操作，进度已自动保存")
        print("下次运行将自动从断点继续")
    except Exception as e:
        print(f"\n❌ 严重错误: {str(e)}")
        import traceback

        traceback.print_exc()
        print("进度已保存，修复问题后可继续运行")


if __name__ == "__main__":
    main()



