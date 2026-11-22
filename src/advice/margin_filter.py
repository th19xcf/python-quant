#!/usr/bin/env python3

"""融资余额 5 日增速分位剔除（选股侧可选闸门，默认关闭）。

依据 ``work/个股两融买点_2026-11-22/结论.md``：24 个「信号×分档×持有期」
组合相对全 A 等权超额全为负，唯一跨市值稳健的效应是**反向**的——
融资余额 5 日增速（``rzye_chg5``）最高十分位在 5/20 日均显著跑输，
小/中/大市值三组都为负。本模块只把这一个反向效应做成可选的剔除闸门，
不引入任何做多信号，也不改动撮合、ST 与涨跌停判定。

口径：

- 横截面：当日两融明细中 ``rzye`` 有效的个股；``rzye_chg5 =
  rzye(t)/rzye(t-5) - 1``，t-5 取**完整性表里的第 5 个更早交易日**；
- 阈值用分位表达：按当日横截面排名，剔除 ``rank/n > 1 - exclude_top_pct``
  的个股（默认最高 10%）；
- 数据缺口：``completeness.parquet`` 标记当日不完整、无记录、缺 t-5 或
  有效截面过小时，**整体跳过过滤**并记日志，绝不按「无数据」误剔除。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl
from loguru import logger

from src.advice.config import MarginFilterConfig
from src.data.margin_quality import load_completeness

REL_DIR = Path('meta') / 'margin_detail'


@dataclass(frozen=True)
class MarginFilterStatus:
    """单个交易日的过滤执行状态，供日志与清单说明使用。"""

    applied: bool
    reason: str
    day: date | None = None
    prev_day: date | None = None
    universe: int = 0
    excluded: int = 0
    threshold: float | None = None


class MarginMomentumFilter:
    """按日给出「融资余额 5 日增速最高分位」的剔除名单。

    只读 ``meta/margin_detail`` 与 ``completeness.parquet``；结果按日缓存，
    同一次推荐/回放内重复查询不重复读盘。
    """

    def __init__(self, config: MarginFilterConfig, store_root: Path | str) -> None:
        self.config = config
        self._dir = Path(store_root) / REL_DIR
        self._completeness: pl.DataFrame | None = None
        self._months: dict[str, pl.DataFrame] = {}
        self._cache: dict[date, tuple[set[str], MarginFilterStatus]] = {}

    # ------------------------------------------------------------------
    # 数据读取（带缓存）
    # ------------------------------------------------------------------
    def _complete_days(self) -> list[date]:
        if self._completeness is None:
            self._completeness = load_completeness(self._dir.parent.parent)
        frame = self._completeness
        if frame is None or frame.height == 0:
            return []
        return (frame.filter(pl.col('is_complete'))
                .sort('trade_date')['trade_date'].to_list())

    def _day_frame(self, day: date) -> pl.DataFrame | None:
        month = f'{day:%Y%m}'
        frame = self._months.get(month)
        if frame is None:
            path = self._dir / f'margin_detail_{month}.parquet'
            if not path.is_file():
                return None
            frame = pl.read_parquet(path, columns=['trade_date', 'ts_code', 'rzye'])
            self._months[month] = frame
        return frame.filter(pl.col('trade_date') == day).select('ts_code', 'rzye')

    # ------------------------------------------------------------------
    # 主入口
    # ------------------------------------------------------------------
    def excluded_codes(self, day: date) -> set[str]:
        return self.status_for(day)[0]

    def status_for(self, day: date) -> tuple[set[str], MarginFilterStatus]:
        cached = self._cache.get(day)
        if cached is not None:
            return cached
        result = self._compute(day)
        self._cache[day] = result
        return result

    def _skip(self, day: date, reason: str) -> tuple[set[str], MarginFilterStatus]:
        status = MarginFilterStatus(False, reason, day=day)
        logger.warning(f'[advice.margin_filter] {day} 跳过两融剔除：{reason}')
        return set(), status

    def _compute(self, day: date) -> tuple[set[str], MarginFilterStatus]:
        cfg = self.config
        if not cfg.enabled:
            return set(), MarginFilterStatus(False, 'disabled', day=day)
        days = self._complete_days()
        if not days:
            return self._skip(day, '无完整性标记（completeness.parquet 缺失或为空）')
        try:
            index = days.index(day)
        except ValueError:
            return self._skip(day, '当日不在完整性标记中或标记为不完整')
        if index < cfg.lookback_days:
            return self._skip(day, f'缺少 {cfg.lookback_days} 个交易日前两融数据')
        prev_day = days[index - cfg.lookback_days]
        current = self._day_frame(day)
        previous = self._day_frame(prev_day)
        if current is None or current.height == 0:
            return self._skip(day, '当日两融明细文件缺失或为空')
        if previous is None or previous.height == 0:
            return self._skip(day, f'{prev_day} 两融明细缺失或为空')

        joined = (current.rename({'rzye': 'rzye_now'})
                  .join(previous.rename({'rzye': 'rzye_prev'}),
                        on='ts_code', how='inner')
                  .filter((pl.col('rzye_prev') > 0) & (pl.col('rzye_now') > 0)))
        universe = joined.height
        if universe < cfg.min_universe:
            return self._skip(day, f'有效两融截面仅 {universe} 只，低于 {cfg.min_universe}')

        chg = pl.col('rzye_now') / pl.col('rzye_prev') - 1
        rank_pct = chg.rank('average') / universe
        cutoff = 1.0 - cfg.exclude_top_pct
        excluded = set(joined.filter(rank_pct > cutoff)['ts_code'].to_list())
        threshold = float(joined.select(chg.quantile(cutoff, 'linear')).item())
        status = MarginFilterStatus(
            True, 'applied', day=day, prev_day=prev_day, universe=universe,
            excluded=len(excluded), threshold=threshold)
        logger.info(
            f'[advice.margin_filter] {day} 剔除融资余额 {cfg.lookback_days} 日增速'
            f'最高 {cfg.exclude_top_pct:.0%}：{len(excluded)}/{universe} 只'
            f'（阈值 {threshold:.2%}，基准 {prev_day}）')
        return excluded, status
