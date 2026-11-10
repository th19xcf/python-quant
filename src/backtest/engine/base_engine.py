#!/usr/bin/env python3

"""
回测引擎基类，提供数据转换和通用组件初始化的统一逻辑
"""

import itertools
from collections.abc import Mapping
from typing import Any, Dict, List, Union

import pandas as pd
import polars as pl
from loguru import logger

from src.backtest.costs.cost_model import CostModel
from src.backtest.engine.execution_core import DEFAULT_BLOCKED_SELL_TTL_DAYS
from src.backtest.engine.holdout import FrameSplit, split_frame_with_purge
from src.backtest.engine.input_contract import (
    BAR_CONTRACT,
    FACTOR_CONTRACT,
    ROLE_ATR,
    ROLE_CLOSE,
    ROLE_DATE,
    ROLE_HIGH,
    ROLE_LOW,
    ROLE_MACD,
    ROLE_MACD_HIST,
    ROLE_MACD_SIGNAL,
    ROLE_RSI,
    BacktestInputContractError,
    FrameContract,
    ma_column,
    rsi_column,
)
from src.backtest.performance.performance_analyzer import PerformanceAnalyzer
from src.backtest.strategies.base_strategy import BaseStrategy
from src.backtest.visualization.backtest_visualizer import BacktestVisualizer
from src.risk import RiskEngine, single_asset_risk_engine
from src.tech_analysis.indicators.formulas import atr_expr, macd_exprs, rsi_expr

DataFrameType = Union[pl.DataFrame, pl.LazyFrame, pd.DataFrame, Any]


class BaseBacktestEngine:
    """
    回测引擎基类，封装数据转换、组件初始化等通用逻辑。
    子类仅需实现各自的回测主体逻辑。
    """

    # 以下四项同时作为类级默认值：允许 `Engine.__new__(Engine)` 直接调用
    # 指标预计算等纯函数（单测常用），不依赖 __init__ 是否执行过。
    risk_gate_policy: str = 'adv_gate'
    input_contract: FrameContract = BAR_CONTRACT
    price_basis: str = 'auto'
    blocked_sell_ttl_days: int = DEFAULT_BLOCKED_SELL_TTL_DAYS

    def __init__(self) -> None:
        """初始化通用组件（不含数据）。子类应先调用此方法，再处理数据。"""
        self.strategy: BaseStrategy | None = None
        self.cost_model = CostModel()
        self.performance_analyzer = PerformanceAnalyzer()
        self.visualizer = BacktestVisualizer()
        self.backtest_results: Dict[str, Any] = {}
        self.trades: List[Dict[str, Any]] = []
        self.equity_curve: List[Dict[str, Any]] = []
        self.ts_code = ''
        # 最近一次回测的执行上下文：参数优化必须复用同一 ts_code / 行业 / ST 状态，
        # 否则优化子回测会按主板 10% 涨跌停、空行业、ST 未知的口径运行，
        # 与主回测不可比
        self.industry = ''
        self.is_st: bool | None | List[Dict[str, Any]] = None

        # 风控层：None 表示未显式装配规则。A1 起不再「静默全放行」：
        # risk_gate_policy='adv_gate'（默认）会由内核自动装配 ADV 容量规则，
        # 'fail_closed' 直接抛错，'off' 才允许显式关闭对照实验。
        self.risk_engine: RiskEngine | None = None
        #: 未装配规则时 RiskGate 的处置策略：'adv_gate'（默认，自动装 ADV 闸门）、
        #: 'fail_closed'（直接抛错）、'off'（显式关闭，仅对照实验）
        self.risk_gate_policy: str = 'adv_gate'
        #: 行情列契约：换数据源只改 input_contract，缺必需列即抛错
        self.input_contract: FrameContract = BAR_CONTRACT
        #: 价格口径：'auto' / 'adjusted' / 'raw'，见 input_contract
        self.price_basis: str = 'auto'
        #: 跌停/停牌卖不出时保留卖单 intent 的最大重试天数（3b）
        self.blocked_sell_ttl_days: int = DEFAULT_BLOCKED_SELL_TTL_DAYS
        self.rejected_orders: List[Dict[str, Any]] = []   # 风控拒单日志
        self.expired_orders: List[Dict[str, Any]] = []    # 挂单作废日志
        self.overwritten_orders: List[Dict[str, Any]] = []  # 被新信号覆盖的挂单
        self.filtered_signals: List[Dict[str, Any]] = []    # 被持仓状态过滤的信号
        self.positions: Dict[str, int] = {}               # 回测结束的最终持仓（账本真源）
        # 除权除息（附加账本层）：dividends 为「标的 → 分红记录」，None 表示
        # 本次回测不记账分红/送转/配股（与修复前行为一致）
        self.dividends: Mapping[str, pl.DataFrame] | None = None
        self.dividend_tax_rate: float = 0.0
        self.rights_policy: str = 'partial'
        # P0-4：一次参数优化中所有失败组合的台账与尝试总数。
        # 早期实现把失败的 future 只写日志后丢弃，在残缺网格上照常选最优，
        # 结果无法反映"这次优化其实只跑了 8/10 组"。
        self._failed_combinations: List[Dict[str, Any]] = []
        self._optimization_attempts: int = 0

    @staticmethod
    def convert_to_polars(data: DataFrameType) -> pl.DataFrame:
        """
        统一的数据转换方法：接受多种类型并返回 Polars DataFrame。

        Args:
            data: Polars DataFrame / LazyFrame / Pandas DataFrame / 其他可转换对象

        Returns:
            pl.DataFrame: 转换后的 Polars DataFrame
        """
        if isinstance(data, pd.DataFrame):
            return pl.from_pandas(data)
        if isinstance(data, pl.LazyFrame):
            return data.collect()
        if isinstance(data, pl.DataFrame):
            return data
        return pl.DataFrame(data)

    def init_common_components(self) -> None:
        """初始化通用组件（供子类在 __init__ 末尾调用）。"""
        self.cost_model = CostModel()
        self.performance_analyzer = PerformanceAnalyzer()
        self.visualizer = BacktestVisualizer()
        self.backtest_results = {}
        self.trades = []
        self.equity_curve = []

    def set_strategy(self, strategy: BaseStrategy, code: str = '') -> None:
        """
        设置策略（基类 1 参数契约：只传 strategy）。
        内部委托 set_strategies，便于子类统一主入口。
        """
        self.set_strategies({code: strategy} if code else {'__default__': strategy})

    def set_strategies(self, mapping: Dict[str, BaseStrategy]) -> None:
        """
        主入口：一次性设置多标的策略映射。
        单标的引擎取 mapping 中唯一或首个值存入 self.strategy；
        组合引擎按 code 存入 self.strategies。
        """
        if not mapping:
            self.strategy = None
            return
        # 单标的场景：只关心策略对象本身，code 在 run_backtest 里由 ts_code 补位
        self.strategy = next(iter(mapping.values()))

    def enable_default_risk_gate(self) -> RiskEngine:
        """装配与撮合内核同源的最小安全规则集（涨跌停/停牌/回撤/现金/流动性）。

        裸用 ``BacktestEngine(data)`` 时风控默认为 None；生产路径应要么走
        ``src.core.usecases.run_backtest``（默认装配），要么显式调用本方法，
        要么把 ``risk_gate_policy`` 设为 ``'fail_closed'``，避免"无风控却不自知"。
        """
        self.risk_engine = single_asset_risk_engine()
        return self.risk_engine

    def set_cost_model(self, cost_model: CostModel) -> None:
        """
        设置成本模型

        Args:
            cost_model: 成本模型对象
        """
        self.cost_model = cost_model

    def get_backtest_results(self) -> Dict[str, Any]:
        """
        获取回测结果

        Returns:
            Dict[str, Any]: 回测结果
        """
        return self.backtest_results

    def visualize_backtest(self) -> Any:
        """
        可视化回测结果

        Returns:
            Any: 可视化结果
        """
        if not self.backtest_results:
            logger.error("尚未运行回测")
            return None

        return self.visualizer.visualize_backtest(self.backtest_results)

    # ------------------------------------------------------------------
    # 指标预计算：根据策略参数一次性向量化算好，避免策略内 O(n²) 循环
    # ------------------------------------------------------------------

    def _precompute_indicators(self, df: pl.DataFrame, strategy: BaseStrategy) -> pl.DataFrame:
        """
        向量化预计算策略会用到的指标列，结果写入 DataFrame 供策略按列名读取

        列名一律取自 ``input_contract.FACTOR_CONTRACT``（换源/改命名只改契约）：
            ma_{short_window}, ma_{long_window}     趋势跟踪 / 多因子 MA
            macd, macd_signal, macd_hist            多因子 / MACD 策略
            rsi, rsi_{period}                       均值回归 / 多因子
            atr                                     多因子

        算完立即自检必需因子列；缺失即抛 ``BacktestInputContractError``，
        避免策略读到 None 后恒返回 hold、回测静默空跑。
        """
        close_col = self.input_contract.column(ROLE_CLOSE)
        close = pl.col(close_col)

        short_w = int(strategy.params.get('short_window', 10))
        long_w = int(strategy.params.get('long_window', 20))
        rsi_w = int(strategy.params.get('rsi_period', 14))

        exprs = []

        # 移动平均线（多策略共用），min_samples=window 使窗口未满的前 window-1 根
        # 为 null，策略读到 None 即 hold，避免 ma_20 = close[0] 之类的暖机污染值产生虚假信号
        exprs.append(close.rolling_mean(window_size=short_w, min_samples=short_w).alias(ma_column(short_w)))
        exprs.append(close.rolling_mean(window_size=long_w, min_samples=long_w).alias(ma_column(long_w)))

        # EMA / MACD：统一走共享公式层（标准 EMA 口径）
        macd_dict = macd_exprs(close_col, 12, 26, 9)
        exprs.extend([
            macd_dict['macd'].alias(FACTOR_CONTRACT.column(ROLE_MACD)),
            macd_dict['signal'].alias(FACTOR_CONTRACT.column(ROLE_MACD_SIGNAL)),
            macd_dict['hist'].alias(FACTOR_CONTRACT.column(ROLE_MACD_HIST)),
        ])

        # RSI（支持自定义 period）：统一走共享公式层（Wilder 标准口径）
        rsi = rsi_expr(close_col, rsi_w)
        exprs.extend([
            rsi.alias(FACTOR_CONTRACT.column(ROLE_RSI)),
            rsi.alias(rsi_column(rsi_w)),
        ])

        # ATR（多因子可能用到）：统一走共享公式层
        exprs.append(atr_expr(
            self.input_contract.column(ROLE_HIGH),
            self.input_contract.column(ROLE_LOW),
            close_col, 14,
        ).alias(FACTOR_CONTRACT.column(ROLE_ATR)))

        result = df.with_columns(exprs)
        self._require_factor_columns(
            result, (ma_column(short_w), ma_column(long_w)),
            context=getattr(strategy, 'name', type(strategy).__name__),
        )
        return result

    @staticmethod
    def _require_factor_columns(df: pl.DataFrame, extra: tuple[str, ...],
                                *, context: str) -> None:
        """因子契约自检：必需因子列必须真的算出来了。"""
        required = extra + tuple(
            FACTOR_CONTRACT.column(role)
            for role in (ROLE_MACD, ROLE_MACD_SIGNAL, ROLE_MACD_HIST,
                         ROLE_RSI, ROLE_ATR)
        )
        missing = [name for name in required if name not in df.columns]
        if missing:
            raise BacktestInputContractError(
                f'{context} 指标预计算缺少因子列 {missing}：'
                '策略会读到 None 并恒返回 hold，回测将静默空跑'
            )

    def _generate_param_combinations(self, param_ranges: Dict[str, List[float]]) -> List[Dict[str, float]]:
        """
        生成参数组合

        Args:
            param_ranges: 参数范围

        Returns:
            List[Dict[str, float]]: 参数组合列表
        """
        param_names = list(param_ranges.keys())
        param_values = [param_ranges[name] for name in param_names]

        combinations = list(itertools.product(*param_values))

        param_combinations = []
        for combo in combinations:
            param_dict = {}
            for i, name in enumerate(param_names):
                param_dict[name] = combo[i]
            param_combinations.append(param_dict)

        return param_combinations

    def _select_best_params(self, results: List[Dict[str, Any]], objective: str) -> tuple:
        """
        选择最佳参数

        Args:
            results: 回测结果列表
            objective: 优化目标

        Returns:
            tuple: (最佳参数, 最佳值)
        """
        if not results:
            return {}, 0

        best_params = {}
        if objective == 'total_return':
            best_value = -float('inf')
            for result in results:
                value = result.get('total_return', -float('inf'))
                if value > best_value:
                    best_value = value
                    best_params = result.get('params', {})
        elif objective == 'sharpe_ratio':
            best_value = -float('inf')
            for result in results:
                sharpe = result.get('performance', {}).get('sharpe_ratio', -float('inf'))
                if sharpe > best_value:
                    best_value = sharpe
                    best_params = result.get('params', {})
        elif objective == 'max_drawdown':
            best_value = float('inf')
            for result in results:
                drawdown = result.get('performance', {}).get('max_drawdown', float('inf'))
                if drawdown < best_value:
                    best_value = drawdown
                    best_params = result.get('params', {})
        else:
            best_value = -float('inf')
            for result in results:
                value = result.get('total_return', -float('inf'))
                if value > best_value:
                    best_value = value
                    best_params = result.get('params', {})

        return best_params, best_value

    # ------------------------------------------------------------------
    # 样本外（OOS）/ walk-forward 参数验证
    # ------------------------------------------------------------------

    @staticmethod
    def _objective_value(result: Dict[str, Any], objective: str) -> float:
        """从单次回测结果中取目标函数值（与 _select_best_params 口径一致）。"""
        if objective == 'sharpe_ratio':
            return float(result.get('performance', {}).get('sharpe_ratio', 0.0) or 0.0)
        if objective == 'max_drawdown':
            return float(result.get('performance', {}).get('max_drawdown', 0.0) or 0.0)
        return float(result.get('total_return', 0.0) or 0.0)

    @staticmethod
    def _metric_snapshot(result: Dict[str, Any]) -> Dict[str, float]:
        """抽取对外展示的核心指标快照。"""
        performance = result.get('performance', {}) or {}
        return {
            'total_return': float(result.get('total_return', 0.0) or 0.0),
            'sharpe_ratio': float(performance.get('sharpe_ratio', 0.0) or 0.0),
            'max_drawdown': float(performance.get('max_drawdown', 0.0) or 0.0),
            'winning_rate': float(performance.get('winning_rate', 0.0) or 0.0),
            'trade_count': len(result.get('trades', []) or []) // 2,
            'final_equity': float(result.get('final_equity', 0.0) or 0.0),
        }

    @staticmethod
    def _split_out_of_sample(df: pl.DataFrame, oos_ratio: float,
                             min_rows: int = 20, horizon: int = 10,
                             embargo: int = 20) -> FrameSplit | None:
        """按时间顺序切分样本内 / 样本外，并施加 purge + embargo。

        口径见 ``src.backtest.engine.holdout``（与学习层 splits 同源）：
        训练窗末尾按标签跨度 purge，测试窗从 ``train_end + embargo`` 开始。
        行数不足以切出两段时返回 None，由调用方标记 OOS 未启用。
        """
        return split_frame_with_purge(
            df, oos_ratio, horizon=horizon, embargo=embargo, min_rows=min_rows,
        )

    def _build_stability_report(self, results: List[Dict[str, Any]],
                                objective: str) -> Dict[str, Any]:
        """参数稳定性报告：同一参数取值在网格中的平均目标值。

        只要某个取值的平均表现仍接近最优值（≥ 最优的 90%），
        说明最优点不是孤立尖峰，参数在该方向上"平坦"，实盘更耐受。
        """
        if not results:
            return {'param_stability': [], 'stable_ratio': 0.0, 'best_value': 0.0}

        values = [self._objective_value(r, objective) for r in results]
        best_value = max(values) if objective != 'max_drawdown' else min(values)

        param_names = sorted({name for r in results for name in (r.get('params') or {})})
        stability = []
        stable_count = 0
        for name in param_names:
            buckets: Dict[Any, List[float]] = {}
            for result, value in zip(results, values, strict=True):
                params = result.get('params') or {}
                if name not in params:
                    continue
                buckets.setdefault(params[name], []).append(value)

            entry = {'param': name, 'values': [], 'stable': True}
            for value, bucket_values in sorted(buckets.items(), key=lambda item: item[0]):
                mean_value = sum(bucket_values) / len(bucket_values)
                if objective == 'max_drawdown':
                    # 回撤越小越好：允许比最优值高 10% 以内
                    is_stable = mean_value <= best_value * 1.1 if best_value > 0 else mean_value <= best_value
                else:
                    # 收益类目标越大越好：允许平均表现比最优值低 10% 以内
                    is_stable = mean_value >= best_value * 0.9
                entry['values'].append({
                    'value': value,
                    'mean_objective': mean_value,
                    'stable': is_stable,
                })
                if not is_stable:
                    entry['stable'] = False
            if entry['stable']:
                stable_count += 1
            stability.append(entry)

        return {
            'param_stability': stability,
            'stable_ratio': (stable_count / len(stability)) if stability else 0.0,
            'best_value': best_value,
            'objective': objective,
        }

    def _slice_label(self, df: pl.DataFrame, column: str | None = None) -> str:
        """取 DataFrame 日期列的起止标签（日期列名取自输入契约）。"""
        column = column or self.input_contract.column(ROLE_DATE)
        if df.is_empty() or column not in df.columns:
            return ''
        first, last = df[column][0], df[column][-1]
        return f'{first} ~ {last}'
