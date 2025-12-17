import os
import json
import logging
import itertools
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
from datetime import datetime
import backtrader as bt
import backtrader.analyzers as btanalyzers
from collections import defaultdict
import warnings

# 设置Matplotlib中文字体支持
mpl.rcParams["font.sans-serif"] = [
    "SimHei",
    "Microsoft YaHei",
    "KaiTi",
    "Arial Unicode MS",
]  # 添加多种中文字体选项
mpl.rcParams["axes.unicode_minus"] = False  # 修复负号显示问题

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("etf_backtest.log"), logging.StreamHandler()],
)
logger = logging.getLogger("ETFBacktestManager")


class ETFDataFeed(bt.feeds.PandasData):
    """自定义数据源，处理可能的数据问题"""

    params = (
        ("datetime", None),
        ("open", "open"),
        ("high", "high"),
        ("low", "low"),
        ("close", "close"),
        ("volume", "volume"),
        ("openinterest", -1),
    )


class BasicETFStrategy(bt.Strategy):
    """改进的ETF交易策略，包含多重信号过滤"""

    params = (
        ("ma_short", 5),
        ("ma_long", 20),
        ("rsi_period", 14),
        ("rsi_overbought", 70),
        ("rsi_oversold", 30),
        ("stop_loss_pct", 0.02),
        ("take_profit_pct", 0.05),
        ("printlog", False),
    )

    def __init__(self):
        # 保存收盘价的引用
        self.dataclose = self.datas[0].close

        # 订单状态跟踪
        self.order = None
        self.buyprice = None
        self.buycomm = None

        # 技术指标
        self.sma_short = bt.indicators.SMA(self.datas[0], period=self.params.ma_short)
        self.sma_long = bt.indicators.SMA(self.datas[0], period=self.params.ma_long)
        self.rsi = bt.indicators.RSI(self.datas[0], period=self.params.rsi_period)

        # 交叉信号
        self.crossover = bt.indicators.CrossOver(self.sma_short, self.sma_long)

        # 跟踪止损价格
        self.stop_loss_price = None
        self.take_profit_price = None

    def log(self, txt, dt=None, doprint=False):
        """日志记录功能"""
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            logger.info(f"{dt.isoformat()}, {txt}")

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # 订单已提交/接受 - 无操作
            return

        # 检查订单是否已完成
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    f"BUY EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm {order.executed.comm:.2f}"
                )
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm

                # 设置止损和止盈价格
                self.stop_loss_price = self.buyprice * (1.0 - self.params.stop_loss_pct)
                self.take_profit_price = self.buyprice * (
                    1.0 + self.params.take_profit_pct
                )

            else:  # Sell
                self.log(
                    f"SELL EXECUTED, Price: {order.executed.price:.2f}, Cost: {order.executed.value:.2f}, Comm {order.executed.comm:.2f}"
                )

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("Order Canceled/Margin/Rejected")

        # 重置订单状态
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log(f"OPERATION PROFIT, GROSS {trade.pnl:.2f}, NET {trade.pnlcomm:.2f}")

    def next(self):
        # 记录当前价格
        self.log(f"Close, {self.dataclose[0]:.2f}", doprint=False)

        # 检查是否有挂单
        if self.order:
            return

        # 检查是否持有仓位
        if not self.position:
            # 买入条件
            if (
                self.crossover > 0  # 短期均线上穿长期均线
                and self.rsi[0] < self.params.rsi_oversold  # RSI处于超卖区
                and self.dataclose[0] > self.sma_long[0]
            ):  # 价格在长期均线上方

                self.log(f"BUY CREATE, {self.dataclose[0]:.2f}")
                # 跟踪止损价格
                self.stop_loss_price = self.dataclose[0] * (
                    1.0 - self.params.stop_loss_pct
                )
                self.take_profit_price = self.dataclose[0] * (
                    1.0 + self.params.take_profit_pct
                )

                # 买入（默认全仓）
                self.order = self.buy()

        else:
            # 卖出条件
            if (
                self.crossover < 0  # 短期均线下穿长期均线
                or self.rsi[0] > self.params.rsi_overbought  # RSI处于超买区
                or self.dataclose[0] < self.stop_loss_price  # 触及止损价
                or self.dataclose[0] > self.take_profit_price
            ):  # 触及止盈价

                self.log(f"SELL CREATE, {self.dataclose[0]:.2f}")
                # 卖出所有持仓
                self.order = self.sell()

    def stop(self):
        self.log(
            f"(MA Short {self.params.ma_short}, MA Long {self.params.ma_long}, RSI OB {self.params.rsi_overbought}, RSI OS {self.params.rsi_oversold}) Ending Value {self.broker.getvalue():.2f}",
            doprint=True,
        )


class ETFBacktestManager:
    """ETF回测管理器"""

    def __init__(self, data_dir="download", results_dir="results", plots_dir="plots"):
        self.data_dir = data_dir
        self.results_dir = results_dir
        self.plots_dir = plots_dir
        self.etf_data = {}

        # 创建结果和图表目录
        os.makedirs(self.results_dir, exist_ok=True)
        os.makedirs(self.plots_dir, exist_ok=True)

        logger.info(
            f"ETF回测管理器初始化完成，数据目录: {data_dir}, 结果目录: {results_dir}, 图表目录: {plots_dir}"
        )

    def load_etf_data(self):
        """加载所有ETF数据"""
        logger.info("开始加载ETF数据...")

        # 获取所有CSV文件
        etf_files = [f for f in os.listdir(self.data_dir) if f.endswith(".csv")]
        logger.info(f"自动检测到 {len(etf_files)} 个ETF文件")

        count = 0
        for file in etf_files:
            symbol = file.split(".")[0]
            file_path = os.path.join(self.data_dir, file)

            try:
                # 读取CSV，不自动解析日期
                df = pd.read_csv(file_path)

                # 检查是否有'日期'列
                if "日期" not in df.columns:
                    logger.error(f"文件 {file} 缺少'日期'列，跳过")
                    continue

                # 转换日期列
                df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
                df = df.set_index("日期")

                # 删除含无效日期的行
                df = df[df.index.notna()]

                if len(df) == 0:
                    logger.warning(f"文件 {file} 没有有效日期数据，跳过")
                    continue

                # 重命名列以符合backtrader要求
                column_mapping = {
                    "开盘": "open",
                    "收盘": "close",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume",
                }

                # 检查必要列是否存在
                missing_cols = [
                    col for col in column_mapping.keys() if col not in df.columns
                ]
                if missing_cols:
                    logger.error(f"文件 {file} 缺少必要列: {missing_cols}，跳过")
                    continue

                # 重命名列
                df = df.rename(columns=column_mapping)

                # 确保只保留需要的列
                required_columns = ["open", "high", "low", "close", "volume"]
                df = df[required_columns]

                # 按日期排序
                df = df.sort_index()

                # 保存数据
                self.etf_data[symbol] = df
                count += 1

            except Exception as e:
                logger.error(f"加载 {symbol} 数据失败: {str(e)}")

        logger.info(f"成功加载 {count}/{len(etf_files)} 个ETF数据")
        return count

    def get_available_etfs(self):
        """获取可用的ETF列表"""
        return list(self.etf_data.keys())

    def split_data(self, symbol, train_ratio=0.6, val_ratio=0.2):
        """按比例将数据分为训练、验证和测试集"""
        if symbol not in self.etf_data:
            logger.error(f"ETF {symbol} 不存在")
            return None, None, None

        data = self.etf_data[symbol].copy()

        # 检查数据量
        if len(data) < 10:
            logger.error(f"ETF {symbol} 数据量太少 ({len(data)} 条)，无法分割")
            return None, None, None

        # 按比例分割
        train_end_idx = int(len(data) * train_ratio)
        val_end_idx = int(len(data) * (train_ratio + val_ratio))

        train_data = data.iloc[:train_end_idx]
        val_data = data.iloc[train_end_idx:val_end_idx]
        test_data = data.iloc[val_end_idx:]

        # 记录日期范围
        logger.info(f"ETF {symbol} 数据集拆分完成:")
        logger.info(
            f"  - 训练集: {len(train_data)} 条, {train_data.index.min().date()} 至 {train_data.index.max().date()}"
        )
        logger.info(
            f"  - 验证集: {len(val_data)} 条, {val_data.index.min().date()} 至 {val_data.index.max().date()}"
        )
        logger.info(
            f"  - 测试集: {len(test_data)} 条, {test_data.index.min().date()} 至 {test_data.index.max().date()}"
        )

        return train_data, val_data, test_data

    def _check_time_gaps(self, data, symbol):
        """检查数据中的时间缺口"""
        dates = data.index
        gaps = []

        # 检查连续性（不包括周末）
        for i in range(1, len(dates)):
            gap_days = (dates[i] - dates[i - 1]).days
            # 超过3天的缺口（排除周末）
            if gap_days > 3:
                gaps.append((dates[i - 1].date(), dates[i].date(), gap_days - 1))

        if gaps:
            logger.warning(f"ETF {symbol} 有 {len(gaps)} 个显著时间缺口:")
            for i, (start, end, gap_days) in enumerate(gaps[:5]):
                logger.warning(f"  - 从 {start} 到 {end}, 缺口 {gap_days} 天")
            if len(gaps) > 5:
                logger.warning(f"  - 还有 {len(gaps)-5} 个缺口")

    def run_backtest(
        self,
        symbol,
        data,
        strategy_class=BasicETFStrategy,
        strategy_params=None,
        commission=0.001,
        start_cash=100000.0,
        plot=False,
        dataset_type="test",
        analyzers=None,
        **kwargs,
    ):
        """运行单个ETF回测"""
        if symbol not in self.etf_data:
            logger.error(f"ETF {symbol} 不存在")
            return None

        if strategy_params is None:
            strategy_params = {
                "ma_short": 5,
                "ma_long": 20,
                "rsi_period": 14,
                "rsi_overbought": 70,
                "rsi_oversold": 30,
                "stop_loss_pct": 0.02,
                "take_profit_pct": 0.05,
            }

        logger.info(f"策略设置为: {strategy_class.__name__}")
        logger.info(f"策略参数: {strategy_params}")

        # 创建cerebro引擎
        cerebro = bt.Cerebro()

        # 添加数据
        data_feed = ETFDataFeed(dataname=data)
        cerebro.adddata(data_feed, name=symbol)

        # 设置初始资金
        cerebro.broker.setcash(start_cash)

        # 设置佣金
        cerebro.broker.setcommission(commission=commission)

        # 添加策略
        cerebro.addstrategy(strategy_class, **strategy_params)

        # 添加分析器
        cerebro.addanalyzer(btanalyzers.SharpeRatio, _name="sharpe", riskfreerate=0.0)
        cerebro.addanalyzer(btanalyzers.DrawDown, _name="drawdown")
        cerebro.addanalyzer(btanalyzers.Returns, _name="returns", tann=252)
        cerebro.addanalyzer(btanalyzers.TradeAnalyzer, _name="trades")
        cerebro.addanalyzer(btanalyzers.VWR, _name="vwr", timeframe=bt.TimeFrame.Years)
        cerebro.addanalyzer(btanalyzers.Calmar, _name="calmar")

        # 运行回测
        logger.info(f"开始回测 ETF {symbol}...")
        results = cerebro.run()
        strat = results[0] if results else None

        if strat is None:
            logger.error("回测失败，没有返回结果")
            return None

        # 获取最终价值
        final_value = cerebro.broker.getvalue()
        total_return = (final_value - start_cash) / start_cash * 100

        logger.info(
            f"回测完成，最终资金: {final_value:,.2f}, 收益率: {total_return:.2f}%"
        )

        # 保存结果
        result_data = {
            "symbol": symbol,
            "start_date": str(data.index.min().date()),
            "end_date": str(data.index.max().date()),
            "initial_cash": start_cash,
            "final_value": final_value,
            "total_return_pct": total_return,
            "strategy_params": strategy_params,
            "dataset_type": dataset_type,
            "metrics": {},
        }

        # 提取分析器结果
        for name, analyzer in strat.analyzers.getitems():
            try:
                analysis = analyzer.get_analysis()
                if hasattr(analysis, "items") and callable(getattr(analysis, "items")):
                    for key, value in analysis.items():
                        result_key = f"{name}_{key}"
                        # 转换特殊类型为基本类型
                        if isinstance(value, (np.floating, np.integer)):
                            result_data["metrics"][result_key] = float(value)
                        elif isinstance(value, (pd.Timestamp, datetime)):
                            result_data["metrics"][result_key] = str(value)
                        else:
                            result_data["metrics"][result_key] = value
                else:
                    result_data["metrics"][name] = float(analysis)
            except Exception as e:
                logger.warning(f"提取分析器 {name} 结果失败: {str(e)}")
                result_data["metrics"][name] = None

        # 保存结果到文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_file = os.path.join(
            self.results_dir, f"backtest_{symbol}_{dataset_type}_{timestamp}.json"
        )
        try:
            with open(result_file, "w", encoding="utf-8") as f:
                json.dump(result_data, f, indent=4, ensure_ascii=False, default=str)
            logger.info(f"回测结果已保存至: {result_file}")
        except Exception as e:
            logger.error(f"保存回测结果失败: {str(e)}")

        # 生成图表
        if plot:
            plot_path = os.path.join(
                self.plots_dir, f"backtest_plot_{symbol}_{dataset_type}_{timestamp}.png"
            )
            try:
                figs = cerebro.plot(
                    style="candlestick",
                    barup="red",
                    bardown="green",
                    volume=True,
                    iplot=False,
                )
                if figs:
                    fig = figs[0][0]  # 获取第一个图表
                    fig.savefig(plot_path, dpi=150, bbox_inches="tight")
                    plt.close(fig)
                    logger.info(f"回测图表已保存至: {plot_path}")
            except Exception as e:
                logger.error(f"保存回测图表失败: {str(e)}")

            # 显示图表（可选）
            if "show_plot" in kwargs and kwargs["show_plot"]:
                plt.show()

        return result_data

    def parameter_optimization(
        self,
        symbol,
        data,
        strategy_class=BasicETFStrategy,
        param_ranges=None,
        metric_name="sharpe",
        maximize=True,
        max_evals=10,
        start_cash=100000.0,
        commission=0.001,
    ):
        """参数优化"""
        if symbol not in self.etf_data:
            logger.error(f"ETF {symbol} 不存在")
            return None

        if param_ranges is None:
            param_ranges = {
                "ma_short": [3, 5, 8],
                "ma_long": [15, 20, 25],
                "rsi_overbought": [65, 70, 75],
                "rsi_oversold": [25, 30, 35],
            }

        logger.info(f"开始参数优化 for ETF {symbol}...")
        logger.info(f"参数范围: {param_ranges}")

        # 生成所有参数组合
        keys = param_ranges.keys()
        values = param_ranges.values()
        param_combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]

        logger.info(
            f"参数组合数量 ({len(param_combinations)}) 超过 max_evals ({max_evals})，将只评估前 {max_evals} 个组合"
        )
        param_combinations = param_combinations[:max_evals]

        best_params = None
        best_value = -float("inf") if maximize else float("inf")
        all_results = []

        # 评估每个参数组合
        for i, params in enumerate(param_combinations, 1):
            logger.info(f"评估参数组合 {i}/{len(param_combinations)}: {params}")

            # 运行回测
            result = self.run_backtest(
                symbol=symbol,
                data=data,
                strategy_class=strategy_class,
                strategy_params=params,
                start_cash=start_cash,
                commission=commission,
                plot=False,
            )

            if result is None:
                continue

            # 获取评估指标
            metric_value = result["metrics"].get(f"{metric_name}_sharperatio", None)
            if metric_value is None:
                metric_value = result["metrics"].get(metric_name, None)

            # 处理无效指标值
            if metric_value is None or not isinstance(metric_value, (int, float)):
                logger.warning(
                    f"指标 {metric_name} 无效 (值: {metric_value})，使用默认低分"
                )
                metric_value = -1e9 if maximize else 1e9

            # 更新最佳参数
            if (maximize and metric_value > best_value) or (
                not maximize and metric_value < best_value
            ):
                best_value = metric_value
                best_params = params.copy()

            # 保存结果
            result["params"] = params
            result["evaluation_metric"] = {
                "name": metric_name,
                "value": metric_value,
                "maximize": maximize,
            }
            all_results.append(result)

            logger.info(
                f"参数组合评估完成，{metric_name}: {metric_value:.4f}, 当前最佳: {best_value:.4f}"
            )

        # 保存优化结果
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        optimization_file = os.path.join(
            self.results_dir, f"optimization_{symbol}_{timestamp}.json"
        )
        optimization_results = {
            "symbol": symbol,
            "start_date": str(data.index.min().date()),
            "end_date": str(data.index.max().date()),
            "strategy": strategy_class.__name__,
            "param_ranges": param_ranges,
            "metric_name": metric_name,
            "maximize": maximize,
            "best_params": best_params,
            "best_value": best_value,
            "all_evaluations": all_results,
        }

        try:
            with open(optimization_file, "w", encoding="utf-8") as f:
                json.dump(
                    optimization_results, f, indent=4, ensure_ascii=False, default=str
                )
            logger.info(f"参数优化结果已保存至: {optimization_file}")
            logger.info(f"最佳参数: {best_params}, {metric_name}: {best_value:.4f}")
        except Exception as e:
            logger.error(f"保存参数优化结果失败: {str(e)}")

        return optimization_results


def main():
    """主函数"""
    # 初始化回测管理器
    manager = ETFBacktestManager(
        data_dir="download", results_dir="results", plots_dir="plots"
    )

    # 加载数据
    manager.load_etf_data()

    # 显示可用ETF
    available_etfs = manager.get_available_etfs()
    logger.info(
        f"可用ETF: {available_etfs[:10]}..."
        if len(available_etfs) > 10
        else f"可用ETF: {available_etfs}"
    )

    if not available_etfs:
        logger.error("没有可用的ETF数据，程序退出")
        return

    # 选择第一个ETF进行测试
    symbol = available_etfs[0]  # 例如 '159001_货币ETF'
    logger.info(f"选择ETF {symbol} 进行回测")

    # 获取完整数据
    full_data = manager.etf_data[symbol]

    # 运行回测
    logger.info(f"开始回测 for {symbol}")
    result = manager.run_backtest(
        symbol=symbol,
        data=full_data,
        strategy_class=BasicETFStrategy,
        strategy_params={
            "ma_short": 5,
            "ma_long": 20,
            "rsi_period": 14,
            "rsi_overbought": 70,
            "rsi_oversold": 30,
            "stop_loss_pct": 0.02,
            "take_profit_pct": 0.05,
        },
        start_cash=100000.0,
        commission=0.001,
        plot=True,
        dataset_type="full",
        show_plot=True,
    )

    if result:
        logger.info("回测完成！")


if __name__ == "__main__":
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        main()
