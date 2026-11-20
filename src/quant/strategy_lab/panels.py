"""PIT 字段面板：把 ``fields`` 里的每个字段算成「调仓日 → 代码 → 数值」。

三条口径与 :mod:`fields` 的 ``pit`` 标签一一对应：

1. ``bar``：只用信号日及之前的 bar（滚动窗口末端必须恰好是信号日）；
2. ``asof_trade_date``：按 ``trade_date`` 取不晚于信号日的最近一条快照；
3. ``asof_ann_date``：按 ``ann_date`` 取不晚于信号日的最近一期财报——
   绝不按 ``end_date``（报告期）对齐，否则会提前看到尚未公告的财务数据。

只读本地正式数据，不做任何写操作；面板按字段懒加载并在一次运行内缓存。
"""
from __future__ import annotations

import math
from bisect import bisect_right
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Set

import numpy as np
import polars as pl
from loguru import logger

from src.backtest.runner.compute import Context

ROOT = Path(__file__).resolve().parents[3]
VALUATION = ROOT / 'data/local_store/meta/financials/valuation_daily.parquet'
FINANCIAL_TABLES: Dict[str, Path] = {
    'income': ROOT / 'data/local_store/meta/financials/income.parquet',
    'balance': ROOT / 'data/local_store/meta/financials/balance.parquet',
    'cashflow': ROOT / 'data/local_store/meta/financials/cashflow.parquet',
}
#: 每个财务字段来自哪张表（``roe`` 需要两表，单独处理）
FINANCIAL_COLUMNS: Dict[str, str] = {
    'revenue_yoy': 'income',
    'net_profit_yoy': 'income',
    'parent_net_profit': 'income',
    'total_assets': 'balance',
    'total_equity': 'balance',
    'debt_ratio': 'balance',
    'operating_cf': 'cashflow',
}
VALUATION_COLUMNS = ('pe', 'pb', 'ps', 'dv_ratio', 'total_mv', 'circ_mv')
BAR_COLUMNS = ('close', 'pct_chg', 'volume', 'amount')

#: 正式日频因子面板（1990-12-19 ~ 2026-11-19）
FACTOR_PANEL = ROOT / 'data/local_store/factors/factor_panel.parquet'
INDUSTRY_PIT = ROOT / 'data/local_store/meta/industry_pit/industry_pit_sw.parquet'
#: 字段 key → factor_panel 列名
SF_COLUMNS: Dict[str, str] = {
    'ff_bp': 'bp',
    'ff_ep': 'ep',
    'ff_dy': 'dy',
    'ff_roe_ttm': 'roe_ttm',
    'ff_accruals': 'accruals',
    'ff_gross_margin': 'gross_margin',
    'cfp_ttm': 'b_ocf_ttm',
}
#: 合成分：价值三项与质量三项（质量里应计利润取负向 = 1 − 分位）
VALUE_FACTORS = ('ff_bp', 'ff_ep', 'ff_dy')
QUALITY_FACTORS = ('ff_roe_ttm', 'ff_accruals', 'ff_gross_margin')
QUALITY_FLIP = frozenset({'ff_accruals'})


# ---------------------------------------------------------------------------
# as-of 工具（单测直接覆盖这两处，保证 PIT 口径不靠端到端碰运气）
# ---------------------------------------------------------------------------

def build_asof_frame(df: pl.DataFrame, value_col: str, date_col: str = 'ann_date'
                     ) -> Dict[str, tuple[np.ndarray, np.ndarray]]:
    """把 (ts_code, 时点列, value) 长表拆成按代码的 as-of 序列。

    ``date_col`` 必须是「信息可得日」：财报用 ``ann_date``，估值用 ``trade_date``。
    同一时点有多期时按 ``end_date`` 升序保留最后一期（最新报告期），因此本函数
    先按 ``['ts_code', 时点列, 'end_date']`` 排序，避免上游顺序变化静默改口径。
    """
    if df.is_empty():
        return {}
    cols = ['ts_code', date_col, value_col]
    if 'end_date' in df.columns and 'end_date' not in cols:
        cols.append('end_date')
    frame = df.select(cols)
    sort_cols = ['ts_code', date_col] + (['end_date'] if 'end_date' in cols else [])
    frame = frame.sort(sort_cols)
    codes = frame['ts_code'].to_numpy()
    dates = frame[date_col].to_numpy().astype('datetime64[D]')
    values = frame[value_col].to_numpy().astype('float64')
    starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
    ends = np.r_[starts[1:], len(codes)]
    return {str(codes[a]): (dates[a:b], values[a:b])
            for a, b in zip(starts, ends, strict=True)}


def asof_lookup(pair: tuple[np.ndarray, np.ndarray] | None, day: date) -> float | None:
    """取「日期 <= day 的最近一条」；没有任何更早记录时返回 ``None``。"""
    if pair is None:
        return None
    dates, values = pair
    k = bisect_right(dates, np.datetime64(day)) - 1
    if k < 0:
        return None
    value = float(values[k])
    return value if math.isfinite(value) else None


def pct_rank(values: Dict[str, float]) -> Dict[str, float]:
    """组内升序分位（等价 pandas ``rank(pct=True)``，同值取平均秩）。"""
    if not values:
        return {}
    items = sorted(values.items(), key=lambda kv: kv[1])
    n = len(items)
    out: Dict[str, float] = {}
    i = 0
    while i < n:
        j = i
        while j + 1 < n and items[j + 1][1] == items[i][1]:
            j += 1
        rank = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            out[items[k][0]] = rank / n
        i = j + 1
    return out


# ---------------------------------------------------------------------------
# 面板
# ---------------------------------------------------------------------------

class FieldPanels:
    """一次运行内的字段面板缓存。"""

    def __init__(self, ctx: Context):
        self.ctx = ctx
        self._cache: Dict[str, Dict[date, Dict[str, Any]]] = {}
        self._valuation: Dict[str, Dict[str, tuple[np.ndarray, np.ndarray]]] | None = None
        self._financial: Dict[str, Dict[str, tuple[np.ndarray, np.ndarray]]] = {}
        self._base: Dict[str, Dict[str, tuple[np.ndarray, np.ndarray]]] | None = None
        self._industry: Dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]] | None = None
        self._composite: Dict[str, Dict[date, Dict[str, float]]] | None = None

    # -- 对外 --------------------------------------------------------------
    def values(self, key: str, days: List[date] | None = None
               ) -> Dict[date, Dict[str, Any]]:
        if key not in self._cache:
            self._cache[key] = self._build(key)
        if days is None:
            return self._cache[key]
        return {d: self._cache[key].get(d, {}) for d in days}

    # -- 构造 --------------------------------------------------------------
    def _build(self, key: str) -> Dict[date, Dict[str, Any]]:
        if key in BAR_COLUMNS:
            return self._bar_panel(key)
        if key.startswith('vol_'):
            return self._vol_panel(int(key.split('_')[1]))
        if key.startswith('mom_'):
            return self._mom_panel(int(key.split('_')[1]))
        if key == 'avg_amount_20':
            return self._avg_amount_panel(20)
        if key in VALUATION_COLUMNS:
            return self._valuation_panel(key)
        if key in ('ep', 'bp'):
            return self._inverse_valuation_panel('pe' if key == 'ep' else 'pb')
        if key in FINANCIAL_COLUMNS:
            return self._financial_panel(key)
        if key == 'roe':
            return self._roe_panel()
        if key == 'industry':
            return self._industry_panel()
        if key in SF_COLUMNS:
            return self._base_factor_panel(key)
        if key in ('vq_value', 'vq_quality', 'vq_score'):
            return self._composite_panel(key)
        raise ValueError(f'字段 {key} 没有面板实现（fields.py 与 panels.py 不同步）')

    def _empty(self) -> Dict[date, Dict[str, float]]:
        return {d: {} for d in self.ctx.days}

    def _bar_panel(self, column: str) -> Dict[date, Dict[str, float]]:
        panel = self._empty()
        me = np.array(self.ctx.days, dtype='datetime64[D]')
        for code, df in self.ctx.data.items():
            if column not in df.columns:
                continue
            dates = df['date'].to_numpy().astype('datetime64[D]')
            values = df[column].to_numpy().astype('float64')
            pos = np.searchsorted(dates, me)
            idx = np.minimum(pos, max(dates.size - 1, 0))
            hit = np.nonzero((pos < dates.size) & (dates[idx] == me))[0]
            for j in hit:
                value = float(values[idx[j]])
                if math.isfinite(value):
                    panel[self.ctx.days[j]][code] = value
        logger.info('面板 {}：{} 个调仓日', column, len(panel))
        return panel

    def _vol_panel(self, window: int) -> Dict[date, Dict[str, float]]:
        panel = self._empty()
        me = np.array(self.ctx.days, dtype='datetime64[D]')
        for code, df in self.ctx.data.items():
            dates = df['date'].to_numpy().astype('datetime64[D]')
            close = df['close'].to_numpy().astype('float64')
            m = close.size
            if m <= window:
                continue
            with np.errstate(divide='ignore', invalid='ignore'):
                ret = np.diff(np.log(np.where(close > 0, close, np.nan)))
            bad = ~np.isfinite(ret)
            r = np.nan_to_num(ret, nan=0.0, posinf=0.0, neginf=0.0)
            cs1 = np.concatenate(([0.0], np.cumsum(r)))
            cs2 = np.concatenate(([0.0], np.cumsum(r * r)))
            csb = np.concatenate(([0], np.cumsum(bad)))
            pos = np.searchsorted(dates, me)
            idx = np.minimum(pos, m - 1)
            hit = np.nonzero((pos < m) & (dates[idx] == me))[0]
            for j in hit:
                k = int(idx[j])
                a = k - window
                if a < 0 or csb[k] - csb[a] != 0:
                    continue
                mean = (cs1[k] - cs1[a]) / window
                var = (cs2[k] - cs2[a] - window * mean * mean) / (window - 1)
                if var > 0:
                    panel[self.ctx.days[j]][code] = float(np.sqrt(var))
        logger.info('面板 vol_{}：{} 个调仓日', window, len(panel))
        return panel

    def _mom_panel(self, window: int) -> Dict[date, Dict[str, float]]:
        panel = self._empty()
        me = np.array(self.ctx.days, dtype='datetime64[D]')
        for code, df in self.ctx.data.items():
            dates = df['date'].to_numpy().astype('datetime64[D]')
            close = df['close'].to_numpy().astype('float64')
            pos = np.searchsorted(dates, me)
            idx = np.minimum(pos, max(close.size - 1, 0))
            hit = np.nonzero((pos < close.size) & (dates[idx] == me))[0]
            for j in hit:
                k = int(idx[j])
                a = k - window
                if a < 0:
                    continue
                base, now = close[a], close[k]
                if base > 0 and now > 0:
                    panel[self.ctx.days[j]][code] = float(now / base - 1.0)
        logger.info('面板 mom_{}：{} 个调仓日', window, len(panel))
        return panel

    def _avg_amount_panel(self, window: int) -> Dict[date, Dict[str, float]]:
        panel = self._empty()
        me = np.array(self.ctx.days, dtype='datetime64[D]')
        for code, df in self.ctx.data.items():
            if 'amount' not in df.columns:
                continue
            dates = df['date'].to_numpy().astype('datetime64[D]')
            amount = df['amount'].to_numpy().astype('float64')
            pos = np.searchsorted(dates, me)
            idx = np.minimum(pos, max(amount.size - 1, 0))
            hit = np.nonzero((pos < amount.size) & (dates[idx] == me))[0]
            for j in hit:
                k = int(idx[j])
                if k < window - 1:
                    continue
                window_values = amount[k - window + 1:k + 1]
                if np.all(np.isfinite(window_values)):
                    panel[self.ctx.days[j]][code] = float(window_values.mean())
        logger.info('面板 avg_amount_{}：{} 个调仓日', window, len(panel))
        return panel

    # -- 估值 --------------------------------------------------------------
    def _ensure_valuation(self) -> Dict[str, Dict[str, tuple[np.ndarray, np.ndarray]]]:
        if self._valuation is None:
            lo = min(self.ctx.days) - timedelta(days=30)
            hi = max(self.ctx.days)
            df = pl.scan_parquet(VALUATION).select(
                ['ts_code', 'trade_date', *VALUATION_COLUMNS]).filter(
                (pl.col('trade_date') >= lo) & (pl.col('trade_date') <= hi)).collect()
            df = df.sort(['ts_code', 'trade_date'])
            codes = df['ts_code'].to_numpy()
            starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
            ends = np.r_[starts[1:], len(codes)]
            dates = df['trade_date'].to_numpy().astype('datetime64[D]')
            per_code: Dict[str, Dict[str, tuple[np.ndarray, np.ndarray]]] = {}
            for column in VALUATION_COLUMNS:
                values = df[column].to_numpy().astype('float64')
                for a, b in zip(starts, ends, strict=True):
                    per_code.setdefault(str(codes[a]), {})[column] = (dates[a:b], values[a:b])
            self._valuation = per_code
            logger.info('估值快照：{} 只 / {} 行（{}~{}）', len(per_code), df.height, lo, hi)
        return self._valuation

    def _valuation_panel(self, column: str) -> Dict[date, Dict[str, float]]:
        data = self._ensure_valuation()
        panel = self._empty()
        for day in self.ctx.days:
            for code in self.ctx.quoted[day]:
                pair = data.get(code, {}).get(column)
                value = asof_lookup(pair, day)
                if value is not None:
                    panel[day][code] = value
        return panel

    def _inverse_valuation_panel(self, column: str) -> Dict[date, Dict[str, float]]:
        raw = self._valuation_panel(column)
        return {day: {code: float(1.0 / value) for code, value in values.items()
                      if value > 0}
                for day, values in raw.items()}

    # -- 财务 --------------------------------------------------------------
    def _financial_series(self, column: str
                          ) -> Dict[str, tuple[np.ndarray, np.ndarray]]:
        if column in self._financial:
            return self._financial[column]
        table = FINANCIAL_COLUMNS.get(column)
        if table is None:
            raise ValueError(f'财务字段 {column} 未登记来源表')
        path = FINANCIAL_TABLES[table]
        df = pl.scan_parquet(path).select(['ts_code', 'ann_date', 'end_date', column]).filter(
            pl.col('ann_date').is_not_null()).collect()
        series = build_asof_frame(df, column, date_col='ann_date')
        self._financial[column] = series
        logger.info('财务序列 {}（{}）：{} 只 / {} 行', column, table, len(series), df.height)
        return series

    def _financial_panel(self, column: str) -> Dict[date, Dict[str, float]]:
        series = self._financial_series(column)
        panel = self._empty()
        for day in self.ctx.days:
            for code in self.ctx.quoted[day]:
                value = asof_lookup(series.get(code), day)
                if value is not None:
                    panel[day][code] = value
        return panel

    def _roe_panel(self) -> Dict[date, Dict[str, float]]:
        profit = self._financial_series('parent_net_profit')
        equity = self._financial_series('total_equity')
        panel = self._empty()
        for day in self.ctx.days:
            for code in self.ctx.quoted[day]:
                p = asof_lookup(profit.get(code), day)
                e = asof_lookup(equity.get(code), day)
                if p is None or e is None or e <= 0:
                    continue
                panel[day][code] = float(p / e * 100.0)
        return panel

    # -- 正式日频因子面板 / 行业 PIT / 合成分位 --------------------------------
    def _ensure_base(self) -> Dict[str, Dict[str, tuple[np.ndarray, np.ndarray]]]:
        """正式日频因子面板（只读，一次运行只读一次）。"""
        if self._base is None:
            factor_cols = list(SF_COLUMNS.values())
            base = pl.scan_parquet(FACTOR_PANEL).select(
                [pl.col('signal_date').cast(pl.Date).alias('date'), pl.col('ts_code'),
                 *[pl.col(c).cast(pl.Float64) for c in factor_cols]]).collect()
            self._base = {col: build_asof_frame(base.select(['ts_code', 'date', col]), col,
                                                date_col='date')
                          for col in factor_cols}
            logger.info('因子面板：{} 行 / {} 只', base.height, base['ts_code'].n_unique())
        return self._base

    def _base_factor_panel(self, key: str) -> Dict[date, Dict[str, float]]:
        series = self._ensure_base()[SF_COLUMNS[key]]
        panel = self._empty()
        for day in self.ctx.days:
            for code in self.ctx.quoted[day]:
                value = asof_lookup(series.get(code), day)
                if value is not None:
                    panel[day][code] = value
        logger.info('面板 {}：{} 个调仓日', key, len(panel))
        return panel

    def _ensure_industry(self) -> Dict[str, tuple[np.ndarray, np.ndarray, np.ndarray]]:
        """申万一级 PIT：按代码存 (in_date, out_date, 行业名)，同代码取 in_date 最晚的有效段。"""
        if self._industry is None:
            ip = pl.read_parquet(INDUSTRY_PIT, columns=[
                'ts_code', 'industry_l1', 'in_date', 'out_date']).sort(['ts_code', 'in_date'])
            codes = ip['ts_code'].to_numpy()
            in_dates = ip['in_date'].to_numpy().astype('datetime64[D]')
            out_dates = ip['out_date'].to_numpy().astype('datetime64[D]')
            names = ip['industry_l1'].to_numpy()
            starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
            ends = np.r_[starts[1:], len(codes)]
            self._industry = {str(codes[a]): (in_dates[a:b], out_dates[a:b], names[a:b])
                              for a, b in zip(starts, ends, strict=True)}
            logger.info('行业 PIT：{} 只 / {} 行', ip['ts_code'].n_unique(), ip.height)
        return self._industry

    def _industry_panel(self) -> Dict[date, Dict[str, str]]:
        series = self._ensure_industry()
        panel: Dict[date, Dict[str, str]] = {d: {} for d in self.ctx.days}
        for day in self.ctx.days:
            me = np.datetime64(day)
            for code in self.ctx.quoted[day]:
                pair = series.get(code)
                if pair is None:
                    continue
                in_dates, out_dates, names = pair
                k = bisect_right(in_dates, me) - 1
                if k < 0:
                    continue
                out = out_dates[k]
                if not np.isnat(out) and out < me:
                    continue
                panel[day][code] = str(names[k])
        return panel

    def _composite_panel(self, key: str) -> Dict[date, Dict[str, float]]:
        if self._composite is None:
            value = self._cross_panel(VALUE_FACTORS, frozenset())
            quality = self._cross_panel(QUALITY_FACTORS, QUALITY_FLIP)
            score: Dict[date, Dict[str, float]] = {}
            for day, vals in value.items():
                q = quality.get(day, {})
                score[day] = {c: 0.5 * (v + q[c]) for c, v in vals.items() if c in q}
            self._composite = {'vq_value': value, 'vq_quality': quality, 'vq_score': score}
        return self._composite[key]

    def _cross_panel(self, fields: tuple, flip: frozenset) -> Dict[date, Dict[str, float]]:
        """多个因子各自按行业内分位后取均值（skipna）；行业缺失的代码不参与。"""
        industry = self._industry_panel()
        factor_panels = [self.values(f) for f in fields]
        out = self._empty()
        for day in self.ctx.days:
            groups: Dict[str, List[str]] = {}
            for code, ind in industry.get(day, {}).items():
                groups.setdefault(ind, []).append(code)
            per_field: List[Dict[str, float]] = []
            for key, panel in zip(fields, factor_panels, strict=True):
                values = panel.get(day, {})
                ranked: Dict[str, float] = {}
                for codes in groups.values():
                    sub = {c: values[c] for c in codes if c in values}
                    for code, pct in pct_rank(sub).items():
                        ranked[code] = 1.0 - pct if key in flip else pct
                per_field.append(ranked)
            for code in set().union(*[set(x) for x in per_field]):
                nums = [x[code] for x in per_field if code in x]
                out[day][code] = float(np.mean(nums))
        return out


def required_columns(rule: Dict[str, Any]) -> Set[str]:
    """规则实际用到的字段集合，供测试与文档核对（不参与运行）。"""
    fields = {c['field'] for c in rule['conditions']}
    stages = rule['sort'] if isinstance(rule['sort'], list) else [rule['sort']]
    fields.update(st['field'] for st in stages)
    if any(c['operand'].get('group') == 'industry' for c in rule['conditions']):
        fields.add('industry')
    return fields
