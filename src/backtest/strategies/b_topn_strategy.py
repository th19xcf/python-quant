#!/usr/bin/env python3

"""
股东盈余比 B TopN 组合策略（横截面、月频调仓）。

选股逻辑（与 strategy_lab stock_select 规则同源，但作为 portfolio_strategy
接口独立跑通，不依赖 strategy_lab 的 FieldPanels 框架）：

1. 调仓频率：月频（每月第一个交易日，可通过 params['rebalance_month_offset']
   调整——0=月初、10=月中、-1=月末最后一天）；
2. 行业分层：每个调仓日按申万一级行业映射（industry_map）分组，
   行业内取 B 值前 ``ceil(top_n / n_industries)`` 只，
   合并后再按 B 值全市场排名取前 ``top_n`` 只（默认 50）；
3. 等权：调仓日所有入选标的给 ``buy``，不在目标里的持仓标的给 ``sell``，
   其余日期一律 ``hold``。

依赖输入列（由数据层注入或引擎默认占位）：
    - cfp_ttm （ROLE_CFP_TTM）：TTM 口径经营现金流
    - total_mv（BAR_CONTRACT 可选列）：总市值
    - industry_map（run_backtest 参数）：{ts_code: 申万一级行业}

B 值（股东盈余比）= cfp_ttm / total_mv；缺失或 total_mv <= 0 的标的当天
视为不可用，不参与排名。
"""

from __future__ import annotations

import math
from typing import Any, Dict, List

import polars as pl

from src.backtest.strategies.base_strategy import BaseStrategy


def _monthly_rebalance_dates(dates: List[Any], offset: int = 0) -> set:
    """从交易日序列中取月频调仓日（每月 offset 个交易日）。

    offset=0 → 每月第一个交易日；offset=-1 → 每月最后一个交易日。
    """
    out: Dict[tuple, Any] = {}
    for d in dates:
        y_m = (d.year, d.month)
        out.setdefault(y_m, []).append(d)
    result = set()
    for y_m, month_dates in out.items():
        if offset >= 0 and offset < len(month_dates):
            result.add(month_dates[offset])
        elif offset < 0:
            idx = len(month_dates) + offset
            if idx >= 0:
                result.add(month_dates[idx])
    return result


class BTopNStrategy(BaseStrategy):
    """股东盈余比 B TopN 月频组合策略（横截面选股）。

    作为 ``portfolio_strategy`` 使用：
        engine.set_portfolio_strategy(BTopNStrategy(top_n=50))
        engine.run_backtest(..., industry_map={...})

    不继承单标的逐行接口——横截面策略在 ``generate_signals_vectorized``
    里一次性看到全部标的，返回 {ts_code: 信号列}。
    """

    def __init__(self, top_n: int = 50, n_industries: int = 31,
                 industry_top_k: int | None = None,
                 rebalance_offset: int = 0):
        super().__init__(name="B-TopN")
        self.params['top_n'] = top_n
        # 行业分层：ceil(top_n / n_industries) 或显式指定
        self.params['industry_top_k'] = industry_top_k or math.ceil(top_n / max(1, n_industries))
        self.params['rebalance_offset'] = rebalance_offset

    def generate_signal(self, row: Dict[str, Any], index: int) -> str:
        """逐行接口：横截面策略不使用（保留 BaseStrategy 抽象签名）。"""
        return 'hold'

    def generate_signals_vectorized(self, universe: Dict[str, pl.DataFrame]) -> Dict[str, pl.Series]:
        """横截面信号生成：每个调仓日按 B 排名选股，其余日期 hold。

        Args:
            universe: {ts_code: 单标的 DataFrame}，每帧需含 date / cfp_ttm / total_mv
        Returns:
            {ts_code: pl.Series('signal', ['buy'|'sell'|'hold', ...])}，
            序列长度与该标的行数一致。
        """
        # 取行业映射（组合引擎 run_backtest 会把 industry_map 挂到策略上）
        industry_map: Dict[str, str] = getattr(self, '_industry_map', {})
        top_n = int(self.params['top_n'])
        industry_top_k = int(self.params['industry_top_k'])
        rebalance_offset = int(self.params['rebalance_offset'])

        # 收集所有日期 + 构建全局面板（date × ts_code × {B, industry}）
        all_dates_set: set = set()
        per_code_dates: Dict[str, List[Any]] = {}
        per_code_b: Dict[str, Dict[Any, float]] = {}
        per_code_industry: Dict[str, str] = {}

        for code, df in universe.items():
            per_code_industry[code] = industry_map.get(code, '')
            dates = df['date'].to_list()
            per_code_dates[code] = dates
            all_dates_set.update(dates)

            # B = cfp_ttm / total_mv（total_mv <= 0 或 cfp_ttm null 视为不可用）
            if 'cfp_ttm' in df.columns and 'total_mv' in df.columns:
                b_vals = {
                    d: float(cfp / mv)
                    for d, cfp, mv in zip(dates, df['cfp_ttm'].to_list(),
                                          df['total_mv'].to_list(), strict=True)
                    if cfp is not None and mv is not None and mv > 0
                }
            else:
                b_vals = {}
            per_code_b[code] = b_vals

        sorted_dates = sorted(all_dates_set)
        rebalance_dates = _monthly_rebalance_dates(sorted_dates, rebalance_offset)

        # 逐日选股：调仓日更新目标，非调仓日沿用目标（但信号为 hold，
        # 撮合内核只在调仓日产生 buy/sell，其余标的维持原状——等权不主动再平衡）
        # 简化实现：每个调仓日统一对 universe 产出 buy/sell，其余日期全 hold
        target_tracker: set = set()

        # 先为每个 code 建全 hold 列，再在调仓日写入 buy/sell
        signals_out: Dict[str, List[str]] = {code: ['hold'] * len(dates)
                                             for code, dates in per_code_dates.items()}

        for day in rebalance_dates:
            # 1) 行业内取 B 前 industry_top_k
            industry_pool: Dict[str, List[tuple]] = {}  # industry -> [(code, B), ...]
            for code in universe:
                if day not in per_code_b.get(code, {}):
                    continue
                b = per_code_b[code][day]
                ind = per_code_industry.get(code, '')
                if not ind:
                    continue
                industry_pool.setdefault(ind, []).append((code, b))

            industry_picks: List[str] = []
            for ind, items in industry_pool.items():
                items.sort(key=lambda x: (-x[1], x[0]))
                industry_picks.extend(code for code, _ in items[:industry_top_k])

            # 2) 合并后全市场排名取前 top_n
            all_items = [(code, per_code_b[code][day])
                         for code in industry_picks
                         if day in per_code_b.get(code, {})]
            all_items.sort(key=lambda x: (-x[1], x[0]))
            new_target = {code for code, _ in all_items[:top_n]}

            # 3) 旧目标 → sell，新目标 → buy
            sold = target_tracker - new_target
            bought = new_target - target_tracker

            for code in sold:
                if code in universe:
                    idx_list = [i for i, d in enumerate(per_code_dates[code]) if d == day]
                    for i in idx_list:
                        signals_out[code][i] = 'sell'

            for code in bought:
                if code in universe:
                    idx_list = [i for i, d in enumerate(per_code_dates[code]) if d == day]
                    for i in idx_list:
                        signals_out[code][i] = 'buy'

            target_tracker = new_target

        return {code: pl.Series('signal', sigs, dtype=pl.Utf8)
                for code, sigs in signals_out.items()}


def attach_industry_map(strategy: BTopNStrategy, industry_map: Dict[str, str]) -> None:
    """把行业映射挂到策略实例上（portfolio_backtest_engine.run_backtest
    的 industry_map 参数不会自动注入到 strategy，需调用方手动挂载）。"""
    strategy._industry_map = dict(industry_map or {})
