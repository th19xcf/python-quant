#!/usr/bin/env python3

"""统一排除层：三条「新建仓拦截」规则，各自独立开关，默认全关。

规则与口径（不重新定义，沿用已有研究）：

- ``rev5``：近 N 日复权收益最高分位剔除，见
  ``work/REV5与异常换手_2026-11-22/factor_definition.md``；
- ``ato``：当日换手 ÷ 过去 N 日均换手（不含当日、≥15 个观测）最高分位剔除，
  同一份研究；
- ``margin``：融资余额 N 日增速最高分位剔除，直接复用
  :class:`src.advice.margin_filter.MarginMomentumFilter`。

约束：

- 只在新建仓时候选被拦截，不改动已有持仓（调用方只把结果用于买入候选）；
- 关闭某条规则时该规则不读取任何数据；三条全关时本模块无任何 IO；
- 数据缺失或当日有效截面过小时**整体跳过该规则**并 warning，不静默剔除。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from src.advice.config import ExclusionLayerConfig
from src.advice.margin_filter import MarginMomentumFilter

RULE_REV5 = 'rev5'
RULE_ATO = 'ato'
RULE_MARGIN = 'margin'

_TURNOVER_REL = Path('factors') / 'stock_turnover.parquet'


@dataclass(frozen=True)
class RuleStatus:
    """单条规则在某个交易日的执行状态。"""

    rule: str
    applied: bool
    reason: str = ''
    universe: int = 0
    excluded: int = 0
    threshold: float | None = None
    top_pct: float = 0.0
    lookback: int = 0


@dataclass(frozen=True)
class ExclusionResult:
    """单个交易日的排除结果（合并 + 逐规则）。"""

    day: date
    codes: frozenset[str]
    by_rule: dict[str, frozenset[str]]
    statuses: tuple[RuleStatus, ...]

    def notes(self) -> list[str]:
        """转成推荐层备注：命中写数量，跳过写原因。"""
        return [_status_note(status) for status in self.statuses]


def _status_note(status: RuleStatus) -> str:
    if status.rule == RULE_MARGIN:
        if status.applied:
            return (f"两融剔除：融资余额{status.lookback}日增速最高"
                    f"{status.top_pct:.0%} 剔除 {status.excluded}/"
                    f"{status.universe} 只（仅新建仓）")
        return f'两融剔除跳过（{status.reason}）'
    if status.rule == RULE_REV5:
        if status.applied:
            return (f"REV5剔除：近{status.lookback}日复权收益最高"
                    f"{status.top_pct:.0%} 剔除 {status.excluded}/"
                    f"{status.universe} 只（仅新建仓）")
        return f'REV5剔除跳过（{status.reason}）'
    if status.applied:
        return (f"ATO剔除：当日换手/过去{status.lookback}日均换手最高"
                f"{status.top_pct:.0%} 剔除 {status.excluded}/"
                f"{status.universe} 只（仅新建仓）")
    return f'ATO剔除跳过（{status.reason}）'


class ExclusionLayer:
    """按交易日给出新建仓排除名单。

    ``history`` 为已加载的行情/因子面板（至少含 ``date/ts_code/close``，
    ``close`` 为后复权价），用于计算 REV5；ATO 的换手数据在启用时懒加载。
    """

    def __init__(self, config: ExclusionLayerConfig, store_root: Any,
                 history: pl.DataFrame | None = None) -> None:
        self.config = config
        self.enabled = config.any_enabled()
        self._store_root = Path(store_root)
        self._cache: dict[date, ExclusionResult] = {}
        self._rev5_frame: pl.DataFrame | None = None
        self._rev5_reason = ''
        self._ato_frame: pl.DataFrame | None = None
        self._ato_reason = ''
        self._margin: MarginMomentumFilter | None = None
        if not self.enabled:
            return
        if config.rev5.enabled:
            self._prepare_rev5(history)
        if config.ato.enabled:
            self._prepare_ato(history)
        if config.margin.enabled:
            self._margin = MarginMomentumFilter(config.margin, self._store_root)

    # ------------------------------------------------------------------
    # 数据准备（只在对应规则启用时执行）
    # ------------------------------------------------------------------
    def _prepare_rev5(self, history: pl.DataFrame | None) -> None:
        if history is None or history.is_empty():
            self._rev5_reason = '缺少行情面板，无法计算 REV5'
            logger.warning(f'[advice.exclusion] {self._rev5_reason}，该规则整体跳过')
            return
        missing = {'date', 'ts_code', 'close'} - set(history.columns)
        if missing:
            self._rev5_reason = f'行情面板缺少列 {sorted(missing)}'
            logger.warning(f'[advice.exclusion] {self._rev5_reason}，该规则整体跳过')
            return
        window = self.config.rev5.window
        frame = history.select(
            pl.col('date'),
            pl.col('ts_code').cast(pl.Utf8),
            pl.col('close').cast(pl.Float64, strict=False),
        ).sort(['ts_code', 'date'])
        frame = frame.with_columns(
            (pl.col('close') / pl.col('close').shift(window).over('ts_code') - 1.0)
            .alias('value'))
        frame = frame.filter(
            pl.col('value').is_not_null() & pl.col('value').is_finite())
        self._rev5_frame = frame.select('date', 'ts_code', 'value')

    def _prepare_ato(self, history: pl.DataFrame | None) -> None:
        path = self._store_root / _TURNOVER_REL
        if not path.is_file():
            self._ato_reason = f'缺少 {_TURNOVER_REL.as_posix()}'
            logger.warning(f'[advice.exclusion] {self._ato_reason}，该规则整体跳过')
            return
        cfg = self.config.ato
        scan = pl.scan_parquet(path).select(
            pl.col('trade_date').alias('date'),
            pl.col('ts_code').cast(pl.Utf8),
            pl.col('turn').cast(pl.Float64, strict=False),
        )
        if history is not None and not history.is_empty():
            codes = history['ts_code'].unique().to_list()
            scans = [scan.filter(pl.col('ts_code').is_in(codes))]
            if 'date' in history.columns:
                start = history['date'].min()
                if isinstance(start, date):
                    scans[0] = scans[0].filter(
                        pl.col('date') >= start - timedelta(days=60))
            scan = scans[0]
        try:
            frame = scan.collect()
        except Exception as exc:  # noqa: BLE001 - 数据文件损坏按缺失处理并跳过
            self._ato_reason = f'换手表读取失败: {exc}'
            logger.warning(f'[advice.exclusion] {self._ato_reason}，该规则整体跳过')
            return
        if frame.is_empty():
            self._ato_reason = '换手表为空'
            logger.warning(f'[advice.exclusion] {self._ato_reason}，该规则整体跳过')
            return
        frame = frame.sort(['ts_code', 'date']).with_columns(
            pl.col('turn').shift(1)
            .rolling_mean(window_size=cfg.lookback_days,
                          min_samples=cfg.min_obs)
            .over('ts_code').alias('base'))
        frame = frame.with_columns(
            (pl.col('turn') / pl.col('base')).alias('value'))
        frame = frame.filter(
            pl.col('value').is_not_null() & pl.col('value').is_finite()
            & (pl.col('base') > 0))
        self._ato_frame = frame.select('date', 'ts_code', 'value')

    # ------------------------------------------------------------------
    # 主入口
    # ------------------------------------------------------------------
    def excluded_codes(self, day: date) -> set[str]:
        return set(self.status_for(day).codes)

    def status_for(self, day: date) -> ExclusionResult:
        cached = self._cache.get(day)
        if cached is not None:
            return cached
        result = self._compute(day)
        self._cache[day] = result
        return result

    def _compute(self, day: date) -> ExclusionResult:
        if not self.enabled:
            return ExclusionResult(day, frozenset(), {}, ())
        statuses: list[RuleStatus] = []
        by_rule: dict[str, frozenset[str]] = {}
        if self.config.rev5.enabled:
            rev5 = self.config.rev5
            codes, status = self._cross_section(
                self._rev5_frame, day, RULE_REV5, rev5.exclude_top_pct,
                rev5.min_universe, rev5.window, self._rev5_reason)
            by_rule[RULE_REV5] = codes
            statuses.append(status)
        if self.config.ato.enabled:
            ato = self.config.ato
            codes, status = self._cross_section(
                self._ato_frame, day, RULE_ATO, ato.exclude_top_pct,
                ato.min_universe, ato.lookback_days, self._ato_reason)
            by_rule[RULE_ATO] = codes
            statuses.append(status)
        if self.config.margin.enabled and self._margin is not None:
            margin = self.config.margin
            margin_codes, margin_status = self._margin.status_for(day)
            by_rule[RULE_MARGIN] = frozenset(margin_codes)
            statuses.append(RuleStatus(
                RULE_MARGIN, margin_status.applied, margin_status.reason,
                universe=margin_status.universe,
                excluded=margin_status.excluded,
                threshold=margin_status.threshold,
                top_pct=margin.exclude_top_pct, lookback=margin.lookback_days))
        union: set[str] = set()
        for codes in by_rule.values():
            union.update(codes)
        result = ExclusionResult(day, frozenset(union), by_rule,
                                 tuple(statuses))
        self._log(day, result)
        return result

    def _cross_section(self, frame: pl.DataFrame | None, day: date, rule: str,
                       top_pct: float, min_universe: int, lookback: int,
                       unavailable: str
                       ) -> tuple[frozenset[str], RuleStatus]:
        if frame is None:
            return frozenset(), RuleStatus(
                rule, False, unavailable or '数据不可用',
                top_pct=top_pct, lookback=lookback)
        part = frame.filter(pl.col('date') == day).drop_nulls(['value'])
        universe = part.height
        if universe < min_universe:
            reason = f'当日有效截面仅 {universe} 只，低于 {min_universe}'
            return frozenset(), RuleStatus(
                rule, False, reason, universe=universe,
                top_pct=top_pct, lookback=lookback)
        cutoff = 1.0 - top_pct
        rank_pct = pl.col('value').rank('average') / universe
        codes = frozenset(
            part.filter(rank_pct > cutoff)['ts_code'].to_list())
        threshold = float(part.select(
            pl.col('value').quantile(cutoff, 'linear')).item())
        return codes, RuleStatus(
            rule, True, 'applied', universe=universe, excluded=len(codes),
            threshold=threshold, top_pct=top_pct, lookback=lookback)

    def _log(self, day: date, result: ExclusionResult) -> None:
        for status in result.statuses:
            if status.applied:
                logger.info(
                    f'[advice.exclusion] {day} {status.rule} 剔除最高'
                    f'{status.top_pct:.0%}：{status.excluded}/{status.universe} 只')
            elif status.reason and status.reason != 'disabled':
                logger.warning(
                    f'[advice.exclusion] {day} {status.rule} 跳过：{status.reason}')
