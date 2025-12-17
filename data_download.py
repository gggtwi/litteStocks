#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Date: 2025/3/21
Desc: ETF数据下载脚本 - 智能处理已存在数据
"""

import os
import sys
import json
import time
from datetime import datetime
import logging
import argparse
from typing import List, Tuple, Any, Optional

# 添加模块搜索路径（根据实际结构调整）
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'litteStocks'))

try:
    from litteStocks.etf_data_manager import ETFDataDownloader
except ImportError as e:
    print(f"❌ 模块导入失败: {str(e)}")
    print("请确保 etf_data_manager.py 位于 litteStocks 目录下")
    sys.exit(1)

def setup_global_logger(log_level=logging.INFO):
    """配置全局日志"""
    logger = logging.getLogger()
    logger.setLevel(log_level)
    
    # 避免重复添加处理器
    if logger.handlers:
        return logger
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def backup_existing_data(download_dir: str, backup_dir: Optional[str] = None) -> Optional[str]:
    """备份已存在的ETF数据"""
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        return None
    
    if backup_dir is None:
        backup_dir = os.path.join(download_dir, 'backups')
    
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    
    # 创建带时间戳的备份目录
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_subdir = os.path.join(backup_dir, f'backup_{timestamp}')
    os.makedirs(backup_subdir)
    
    # 备份CSV文件
    backed_up = 0
    for filename in os.listdir(download_dir):
        if filename.endswith('.csv'):
            src_path = os.path.join(download_dir, filename)
            dst_path = os.path.join(backup_subdir, filename)
            try:
                import shutil
                shutil.copy2(src_path, dst_path)
                backed_up += 1
            except Exception as e:
                logging.warning(f"备份文件失败 {filename}: {str(e)}")
    
    if backed_up > 0:
        logging.info(f"✅ 已备份 {backed_up} 个ETF文件到: {backup_subdir}")
        # 保留最近5个备份
        backup_dirs = sorted([
            d for d in os.listdir(backup_dir) 
            if os.path.isdir(os.path.join(backup_dir, d)) and d.startswith('backup_')
        ], reverse=True)
        
        for old_backup in backup_dirs[5:]:
            old_backup_path = os.path.join(backup_dir, old_backup)
            try:
                import shutil
                shutil.rmtree(old_backup_path)
                logging.debug(f"🧹 已清理旧备份: {old_backup_path}")
            except Exception as e:
                logging.warning(f"清理旧备份失败 {old_backup_path}: {str(e)}")
    
    return backup_subdir if backed_up > 0 else None

def verify_existing_data(downloader: Any) -> List[Tuple[str, str]]:
    """验证已存在数据的完整性"""
    existing_etfs = downloader.get_existing_etfs()
    logging.info(f"🔍 正在验证 {len(existing_etfs)} 个已存在ETF文件的完整性...")
    
    # 初始化为空列表，确保类型一致
    invalid_files: List[Tuple[str, str]] = []
    
    for i, symbol in enumerate(existing_etfs, 1):
        filename = [f for f in os.listdir(downloader.download_dir) 
                   if f.startswith(f"{symbol}_") and f.endswith('.csv')]
        if not filename:
            continue
        
        filepath = os.path.join(downloader.download_dir, filename[0])
        try:
            # 尝试读取文件
            last_date = downloader.get_last_date_from_file(filepath)
            if not last_date:
                invalid_files.append((symbol, "无法获取最后日期"))
            else:
                # 检查文件大小
                file_size = os.path.getsize(filepath)
                if file_size < 1000:  # 小于1KB可能有问题
                    invalid_files.append((symbol, f"文件过小 ({file_size}字节)"))
        except Exception as e:
            invalid_files.append((symbol, f"读取错误: {str(e)}"))
        
        if i % 10 == 0:
            logging.debug(f"  已验证 {i}/{len(existing_etfs)} 个文件...")
    
    # 确保 invalid_files 始终是列表类型
    if not isinstance(invalid_files, list):
        invalid_files = []
    
    if len(invalid_files) > 0:
        logging.warning(f"⚠️  发现 {len(invalid_files)} 个可能有问题的ETF文件:")
        # 确保我们只迭代列表类型
        for i, item in enumerate(invalid_files[:10], 1):
            # 额外保护：确保item是元组
            if isinstance(item, tuple) and len(item) >= 2:
                symbol, reason = item[0], item[1]
            else:
                symbol, reason = str(item), "格式错误"
            logging.warning(f"  {i}. {symbol}: {reason}")
        
        if len(invalid_files) > 10:
            logging.warning(f"  ... 还有 {len(invalid_files) - 10} 个文件有问题")

    return invalid_files

def main():
    """主函数"""
    # 配置命令行参数
    parser = argparse.ArgumentParser(description='ETF数据下载工具')
    parser.add_argument('--full', action='store_true', help='强制全量下载（忽略已有数据）')
    parser.add_argument('--update', action='store_true', help='仅更新已有ETF（增量更新）')
    parser.add_argument('--symbol', type=str, help='指定单个ETF代码下载/更新，例如: 513500')
    parser.add_argument('--symbols-file', type=str, help='从文件读取ETF代码列表，每行一个代码')
    parser.add_argument('--backup', action='store_true', help='在操作前备份现有数据')
    parser.add_argument('--verify', action='store_true', help='验证现有数据完整性')
    parser.add_argument('--log-level', type=str, default='INFO', 
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='日志级别')
    parser.add_argument('--download-dir', type=str, default='download',
                        help='下载目录路径')
    parser.add_argument('--progress-file', type=str, 
                        default='litteStocks/etf_download_progress.json',
                        help='进度文件路径')
    
    args = parser.parse_args()
    
    # 配置日志
    log_level = getattr(logging, args.log_level.upper())
    logger = setup_global_logger(log_level)
    
    logging.info("=" * 60)
    logging.info("🚀 ETF数据下载工具启动")
    logging.info(f"当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"下载目录: {os.path.abspath(args.download_dir)}")
    logging.info(f"进度文件: {os.path.abspath(args.progress_file)}")
    logging.info("=" * 60)
    
    # 备份现有数据
    if args.backup:
        backup_dir = backup_existing_data(args.download_dir)
        if backup_dir:
            logging.info(f"💾 备份已完成，备份位置: {backup_dir}")
    
    # 验证现有数据
    if args.verify:
        downloader = ETFDataDownloader(
            download_dir=args.download_dir,
            progress_file=args.progress_file,
            log_level=log_level
        )
        invalid_files = verify_existing_data(downloader)
        if invalid_files:
            logging.warning(f"建议修复或删除 {len(invalid_files)} 个有问题的文件")
        else:
            logging.info("✅ 所有现有ETF文件验证通过")
    
    # 确定运行模式
    mode = "auto"
    if args.full:
        mode = "full"
        logging.info("🔄 将执行全量下载（忽略已有数据）")
    elif args.update:
        mode = "update"
        logging.info("🔄 将执行增量更新（只更新已有ETF）")
    else:
        logging.info("🔄 将自动判断运行模式")
    
    # 确定ETF列表
    symbols = None
    if args.symbol:
        symbols = [args.symbol.strip()]
        logging.info(f"🎯 将处理指定ETF: {args.symbol}")
    elif args.symbols_file and os.path.exists(args.symbols_file):
        with open(args.symbols_file, 'r', encoding='utf-8') as f:
            symbols = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        logging.info(f"📋 从文件加载 {len(symbols)} 个ETF代码")
    
    try:
        # 初始化下载器
        downloader = ETFDataDownloader(
            download_dir=args.download_dir,
            progress_file=args.progress_file,
            max_retries=3,
            retry_delay=2.0,
            log_level=log_level
        )
        
        # 同步已有文件到进度记录
        if not args.full:  # 全量下载时不需要同步
            existing_files = downloader.get_existing_etfs()
            newly_added = 0
            for symbol in existing_files:
                if symbol not in downloader.downloaded_etfs:
                    downloader.downloaded_etfs.add(symbol)
                    if symbol in downloader.failed_etfs:
                        downloader.failed_etfs.remove(symbol)
                    newly_added += 1
            
            if newly_added > 0:
                downloader._save_progress()
                logging.info(f"✅ 自动同步 {newly_added} 个新发现的ETF文件到进度记录")
        
        # 执行下载/更新
        logging.info("\n" + "=" * 60)
        logging.info(f"⚡ 开始执行 {mode} 操作...")
        logging.info("=" * 60)
        
        start_time = time.time()
        result = downloader.run(mode=mode, symbols=symbols)
        elapsed_time = time.time() - start_time
        
        # 显示结果
        logging.info("\n" + "=" * 60)
        logging.info("✅ 任务完成!")
        logging.info(f"成功: {result['success_count']}, 失败: {result['fail_count']}")
        
        if 'total_new_records' in result:
            logging.info(f"新增记录: {result['total_new_records']}")
        
        logging.info(f"耗时: {elapsed_time:.1f} 秒")
        logging.info(f"进度已保存到: {args.progress_file}")
        logging.info("=" * 60)
        
        # 显示失败列表
        if result.get('fail_count', 0) > 0 and result.get('failed_symbols'): # type: ignore
            logging.error("\n❌ 失败的ETF代码:")
            for i, symbol in enumerate(result['failed_symbols'], 1):  # type: ignore 
                logging.error(f"  {i}. {symbol}")
            
            logging.error(f"\n可在日志文件中查看详细错误信息")
            logging.error(f"进度文件位置: {os.path.abspath(args.progress_file)}")
        
        # 统计最终状态
        final_existing = downloader.get_existing_etfs()
        logging.info(f"\n📊 最终状态:")
        logging.info(f"  总ETF文件数: {len(final_existing)}")
        logging.info(f"  本次成功处理: {result['success_count']}")
        
        # 保存最终进度
        downloader._save_progress()
        
        return 0
    
    except KeyboardInterrupt:
        logging.warning("\n\n⚠️  用户中断操作，进度已自动保存")
        logging.warning("下次运行将自动从断点继续")
        return 1
    except Exception as e:
        logging.exception(f"\n❌ 严重错误: {str(e)}")
        logging.error("进度已保存，修复问题后可继续运行")
        return 1

def quick_start_example():
    """快速入门示例"""
    print("\n" + "=" * 60)
    print("💡 快速入门示例:")
    print("=" * 60)
    print("1. 增量更新所有已有ETF数据:")
    print("   python data_download.py --update")
    print("")
    print("2. 全量下载所有ETF数据（忽略已有）:")
    print("   python data_download.py --full")
    print("")
    print("3. 下载/更新指定ETF:")
    print("   python data_download.py --symbol 159001")
    print("")
    print("4. 从文件批量下载ETF:")
    print("   # 创建symbols.txt，每行一个代码，支持注释")
    print("   echo 159001 > symbols.txt")
    print("   echo 510300 >> symbols.txt")
    print("   echo #512880 >> symbols.txt  # 注释行会被忽略")
    print("   python data_download.py --symbols-file symbols.txt")
    print("")
    print("5. 带备份的安全更新:")
    print("   python data_download.py --update --backup --verify")
    print("=" * 60)

if __name__ == "__main__":
    exit_code = main()
    
    # 显示快速入门提示
    if exit_code == 0:
        quick_start_example()
    
    sys.exit(exit_code)