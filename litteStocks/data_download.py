#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Date: 2025/3/21
Desc: ETF数据下载脚本 - 基于etf_data_manager工具
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime
from typing import List, Optional, Dict, Any

# 添加模块搜索路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

try:
    # 从上传的文件导入ETFDataDownloader
    from litteStocks.etf_data_manager import ETFDataDownloader
except ImportError as e:
    print(f"❌ 导入错误: {str(e)}")
    print("请确保 etf_data_manager.py 与本脚本在同一目录")
    sys.exit(1)

def setup_logger(log_level: int = logging.INFO) -> logging.Logger:
    """配置日志记录器"""
    logger = logging.getLogger("ETFDownloader")
    logger.setLevel(log_level)
    
    # 避免重复添加处理器
    if logger.handlers:
        return logger
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件处理器
    log_dir = os.path.join("download", "logs")
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.FileHandler(
        os.path.join(log_dir, f"download_{datetime.now().strftime('%Y%m%d')}.log"),
        encoding="utf-8"
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
        logging.warning(f"⚠️  备份进度文件失败: {str(e)}")

def clear_progress_file(progress_file: str) -> None:
    """清空进度文件，用于强制重新下载所有ETF"""
    try:
        # 先备份
        backup_progress_file(progress_file)
        
        # 创建空的进度文件
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump({
                "downloaded": [],
                "failed": [],
                "last_update": "",
                "last_update_start": ""
            }, f, ensure_ascii=False, indent=2)
        
        logging.info("🧹 已清空进度文件，将重新下载所有ETF数据")
    except Exception as e:
        logging.error(f"❌ 清空进度文件失败: {str(e)}")
        raise

def validate_etf_list(downloader: ETFDataDownloader, symbols: Optional[List[str]] = None) -> List[str]:
    """验证ETF代码列表的有效性"""
    try:
        # 获取所有ETF列表
        etf_df, _ = downloader._get_etf_spot_data()
        all_valid_symbols = set(etf_df["代码"].tolist())
        
        if symbols is None:
            return sorted(all_valid_symbols)
        
        # 验证指定的ETF代码
        valid_symbols = []
        invalid_symbols = []
        
        for symbol in symbols:
            if symbol in all_valid_symbols:
                valid_symbols.append(symbol)
            else:
                invalid_symbols.append(symbol)
        
        if invalid_symbols:
            logging.warning(f"⚠️  {len(invalid_symbols)} 个无效的ETF代码:")
            for symbol in invalid_symbols:
                logging.warning(f"  - {symbol}")
        
        if not valid_symbols:
            logging.error("❌ 没有有效的ETF代码可供下载")
            sys.exit(1)
        
        return valid_symbols
        
    except Exception as e:
        logging.error(f"❌ 获取ETF列表失败: {str(e)}")
        sys.exit(1)

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='ETF数据下载工具')
    parser.add_argument('--full', action='store_true', help='强制全量下载（忽略已有数据）')
    parser.add_argument('--full-history', action='store_true', help='下载完整历史数据（清空进度记录）')
    parser.add_argument('--update', action='store_true', help='仅更新已有ETF数据（增量更新）')
    parser.add_argument('--symbol', type=str, help='指定单个ETF代码，例如: 513500')
    parser.add_argument('--symbols-file', type=str, help='从文件读取ETF代码列表，每行一个代码')
    parser.add_argument('--log-level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='日志级别')
    parser.add_argument('--download-dir', type=str, default='download',
                        help='下载目录路径')
    parser.add_argument('--progress-file', type=str, default='download/etf_download_progress.json',
                        help='进度文件路径')
    parser.add_argument('--max-retries', type=int, default=3, help='最大重试次数')
    parser.add_argument('--retry-delay', type=float, default=2.0, help='基础重试延迟(秒)')
    
    args = parser.parse_args()
    
    # 配置日志
    log_level = getattr(logging, args.log_level.upper())
    logger = setup_logger(log_level)
    
    # 显示启动信息
    logger.info("=" * 60)
    logger.info("🚀 ETF数据下载工具")
    logger.info(f"版本: 1.0")
    logger.info(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"下载目录: {os.path.abspath(args.download_dir)}")
    logger.info(f"进度文件: {os.path.abspath(args.progress_file)}")
    logger.info("=" * 60)
    
    # 确定运行模式
    mode = "auto"
    if args.full:
        mode = "full"
        logger.info(f"🔄 运行模式: 全量下载（{'包含完整历史' if args.full_history else '跳过已下载'}）")
    elif args.update:
        mode = "update"
        logger.info("🔄 运行模式: 增量更新（只更新已有ETF）")
    else:
        logger.info("🔄 运行模式: 自动（根据已有数据判断）")
    
    # 处理ETF列表
    symbols = None
    if args.symbol:
        symbols = [args.symbol.strip()]
        logger.info(f"🎯 指定ETF: {args.symbol}")
    elif args.symbols_file and os.path.exists(args.symbols_file):
        with open(args.symbols_file, 'r', encoding='utf-8') as f:
            symbols = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        logger.info(f"📋 从文件加载 {len(symbols)} 个ETF代码")
    
    try:
        # 如果需要下载完整历史，清空进度文件
        if args.full and args.full_history:
            if os.path.exists(args.progress_file):
                clear_progress_file(args.progress_file)
            else:
                logger.info("ℹ️  进度文件不存在，将创建新文件")
        
        # 创建下载器
        downloader = ETFDataDownloader(
            download_dir=args.download_dir,
            progress_file=args.progress_file,
            max_retries=args.max_retries,
            retry_delay=args.retry_delay,
            log_level=log_level
        )
        
        # 验证ETF列表
        if symbols:
            symbols = validate_etf_list(downloader, symbols)
        
        # 执行下载/更新
        logger.info("\n" + "=" * 60)
        logger.info(f"⚡ 开始执行 {mode} 操作")
        logger.info("=" * 60)
        
        start_time = time.time()
        result = downloader.run(mode=mode, symbols=symbols)
        elapsed_time = time.time() - start_time
        
        # 显示结果
        logger.info("\n" + "=" * 60)
        logger.info("✅ 任务完成!")
        logger.info(f"成功: {result['success_count']}, 失败: {result['fail_count']}")
        
        if 'total_new_records' in result:
            logger.info(f"新增记录: {result['total_new_records']}")
        
        logger.info(f"耗时: {elapsed_time:.1f} 秒")
        logger.info(f"进度已保存到: {args.progress_file}")
        logger.info("=" * 60)
        
        # 显示失败列表
        if result.get('fail_count', 0) > 0: # type: ignore
            failed_symbols = result.get('failed_symbols', [])
            logger.error(f"\n❌ {len(failed_symbols)} 个ETF下载失败:") # type: ignore
            for i, symbol in enumerate(failed_symbols[:10], 1): # type: ignore
                logger.error(f"  {i}. {symbol}")
             
            if len(failed_symbols) > 10: # type: ignore
                logger.error(f"  ... 还有 {len(failed_symbols)-10} 个失败的ETF") # type: ignore
        
        # 显示最终统计
        existing_etfs = downloader.get_existing_etfs()
        logger.info(f"\n📊 最终统计:")
        logger.info(f"  总ETF数量: {len(existing_etfs)}")
        logger.info(f"  本次成功: {result['success_count']}")
        
        # 快速入门提示
        if result.get('success_count', 0) > 0: # type: ignore
            logger.info("\n💡 快速入门提示:")
            logger.info("  - 要更新已有数据: python data_download.py --update")
            logger.info("  - 要下载所有ETF: python data_download.py --full")
            logger.info("  - 要下载完整历史: python data_download.py --full --full-history")
            logger.info("  - 要下载特定ETF: python data_download.py --symbol 513500")
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("\n\n⚠️  用户中断操作，进度已自动保存")
        logger.warning("下次运行将自动从断点继续")
        return 1
    except Exception as e:
        logger.exception(f"\n❌ 严重错误: {str(e)}")
        logger.error("进度已保存，修复问题后可继续运行")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)