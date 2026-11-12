#!/usr/bin/env python3

"""用行情数据反推「疑似 ST 区间」，并与名称变更源得到的「确证区间」对拍。

为什么能做
----------
主板（含原中小板、B 股）与 2020-08-24 前的创业板，风险警示股票涨跌幅收窄
到 5%（:meth:`ExecutionRules.resolve_limit_pct` 已是项目内唯一真源）。若一段
时间内的日收益被持续压在 ±5% 以内、且反复触到 4.5%~5.5% 这个「贴着 5% 上限」
的区间，同时振幅不是横盘级别的，就可以反推该段处于 ST。

反之，改革后创业板（20%）、科创板（20%）、北交所（30%）的 ST 股票涨跌幅
与普通股票一致，行情里**没有**可用于区分 ST 的信号——这些标的不做推断，
记为 ``not_inferable``，绝不硬凑。

判定规则（逐条独立，不做加权）
------------------------------
对某标的、某 120 个连续交易日窗口（全部落在常规上限 10% 的制度段内）：

1. 上限证据：窗口内 ``max|日收益| ≤ 5.6%``（若存在 >5.6% 的日子，说明没有被
   5% 限制住）。120 日（约半年）内一次都没有突破，是比 20 日窗口强得多的约束：
   实测在从未 ST 的大盘股上，20 日窗口误报率约 5%，120 日窗口接近 0；
2. 触碰证据：窗口内至少 10 天 ``|日收益| ≥ 4.5%``（证明确实贴着 5% 在跑，
   而不是波动本来就小）。取 6 天时误报高 4 倍、召回只多 2.5 个百分点，
   实测后定为 10 天；
3. 非横盘证据：窗口内振幅中位数 ``≥ 2%``；
4. 流动性证据：窗口内有效成交日（成交量>0 且成交额>0）占比 ``≥ 80%``。

四条全中 → 该窗口判「疑似 ST」。连续窗口合并为区间，最短区间 = 40 个交易日
（短于此仅记录在窗口统计里，不生成区间）。区间两端再用「首次 |日收益| > 5.6%
的日子」收紧，减少窗口带来的外延。

必须防的误判（各自独立计量）
----------------------------
- 连续跌停/连环一字板：主板一字板是 ±10%（>5.5%），第 1 条直接排除；对
  「触板日全为负向」的疑似区间单独计数，作为该类误判的量化口径。
- 除权跳空：全部收益用**后复权**价（``close × hfq_factor``）计算，除权日
  跳空不计入真实涨跌；另计「原始价超限、复权后不超限」的天数，作为被消除的
  误判量。
- 长期横盘：第 3 条独立拦截，计入 ``rejected_low_amp``。
- 创业板 2020-08-24 板幅切换：窗口不允许跨越制度段，制度段按
  ``resolve_limit_pct`` 逐日推导后切分。
- 停牌/长期无成交：第 4 条独立拦截，计入 ``rejected_illiquid``。

落盘
----
``{store_root}/meta/st/st_price_inferred.parquet``（``flag='suspected_st'``）+
``price_inference.json``（规则参数、命中统计、与确证区间的对拍结果）。
确证区间表 ``st_intervals.parquet`` 只读，不修改。

查询
----
:func:`suspected_status_at` / :func:`suspected_intervals_for_backtest` 单独返回
疑似标记；:meth:`src.data.st_history.status_at` 默认**不**采信疑似区间
（``include_suspected=False``），语义红线不变：无任何记录 → ``unknown``。
"""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.data import st_history
from src.data.atomic_write import write_parquet_atomic
from src.risk.execution_rules import ExecutionRules

FLAG_SUSPECTED = 'suspected_st'
SOURCE_PRICE = 'price_inference'
CONF_HIGH = 'high'
CONF_LOW = 'low'

WINDOW = 120
CAP_TOL = 0.056          # 5% 上限 + 0.6% 容差
TOUCH_MIN = 0.045        # 贴近 5% 的「触碰」阈值
TOUCH_NEED = 10          # 窗口内最少触碰天数
AMP_MIN = 0.02           # 振幅中位数下限（排除横盘）
VALID_MIN = 0.80         # 有效成交日占比下限
MIN_RUN = 40             # 最短区间（交易日）：太短的波动窗口不算
MERGE_GAP = 10           # 相邻疑似区间合并的最大自然日间隔
EDGE_LOOKBACK = 120      # 端点收紧时向前/向后搜索的天数

_PRICE_SCHEMA = {
    'ts_code': pl.Utf8, 'start_date': pl.Date, 'end_date': pl.Date,
    'flag': pl.Utf8, 'source': pl.Utf8, 'confidence': pl.Utf8,
    'evidence': pl.Utf8, 'fetched_at': pl.Datetime,
}
_PRICE_CACHE: dict[str, tuple[int, pl.DataFrame]] = {}


def price_path(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'st' / 'st_price_inferred.parquet'


def summary_path(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'st' / 'price_inference.json'


def manifest_path(store_root: Path | str) -> Path:
    return Path(store_root) / 'meta' / 'st' / 'manifest.json'


def _read_cached(path: Path, cache: dict[str, tuple[int, pl.DataFrame]],
                 schema: dict[str, Any]) -> pl.DataFrame | None:
    if not path.is_file():
        return None
    mtime = path.stat().st_mtime_ns
    cached = cache.get(str(path))
    if cached is not None and cached[0] == mtime:
        return cached[1]
    frame = pl.read_parquet(path)
    if frame.columns:
        frame = frame.with_columns(
            pl.col(key).cast(dtype) for key, dtype in schema.items() if key in frame.columns)
    cache[str(path)] = (mtime, frame)
    return frame


def load_price_inferred(store_root: Path | str) -> pl.DataFrame | None:
    return _read_cached(price_path(store_root), _PRICE_CACHE, _PRICE_SCHEMA)


# ----------------------------------------------------------------------
# 逐标的推断
# ----------------------------------------------------------------------
def _regular_limit(ts_code: str, day: date | None = None) -> float:
    """常规涨跌幅上限（不带 ST 状态），复用项目内唯一真源。"""
    return ExecutionRules.resolve_limit_pct(ts_code, None, trade_date=day)


def _limit_runs(ts_code: str, dates: list[date]) -> list[tuple[int, int]]:
    """把交易日切成「常规上限 = 10%（可推断）」的连续制度段。"""
    code = ts_code.split('.')[0]
    if code.startswith(('300', '301')):
        runs: list[tuple[int, int]] = []
        start: int | None = None
        for index, day in enumerate(dates):
            inferable = _regular_limit(ts_code, day) == 0.10
            if inferable and start is None:
                start = index
            elif not inferable and start is not None:
                runs.append((start, index - 1))
                start = None
        if start is not None:
            runs.append((start, len(dates) - 1))
        return runs
    return [(0, len(dates) - 1)] if _regular_limit(ts_code) == 0.10 else []


def _prepare(frame: pl.DataFrame | None) -> pl.DataFrame | None:
    if frame is None or frame.height < WINDOW + 2:
        return None
    required = {'date', 'high', 'low', 'close', 'pre_close', 'volume', 'amount'}
    if not required.issubset(set(frame.columns)):
        return None
    columns = list(required)
    if 'hfq_factor' in frame.columns:
        columns.append('hfq_factor')
    df = (frame.select(columns)
          .filter((pl.col('close') > 0) & (pl.col('pre_close') > 0))
          .sort('date'))
    if df.height < WINDOW + 2:
        return None
    factor = (pl.when(pl.col('hfq_factor') > 0).then(pl.col('hfq_factor')).otherwise(1.0)
              if 'hfq_factor' in df.columns else pl.lit(1.0))
    df = df.with_columns([
        (pl.col('close') * factor).alias('hfq'),
        ((pl.col('high') - pl.col('low')) / pl.col('pre_close')).alias('amp'),
        ((pl.col('volume') > 0) & (pl.col('amount') > 0)).cast(pl.Float64).alias('valid'),
        (pl.col('close') / pl.col('pre_close') - 1.0).alias('raw_ret'),
    ]).with_columns([
        (pl.col('hfq') / pl.col('hfq').shift(1) - 1.0).alias('ret'),
    ]).with_columns([
        pl.col('ret').abs().alias('absret'),
        (pl.col('raw_ret').abs() > CAP_TOL).alias('raw_breach'),
        (pl.col('ret').abs() > CAP_TOL).alias('breach'),
    ])
    return df


def _spans(flags: list[bool]) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    index = 0
    total = len(flags)
    while index < total:
        if not flags[index]:
            index += 1
            continue
        end = index
        while end + 1 < total and flags[end + 1]:
            end += 1
        start = max(0, index - (WINDOW - 1))
        if out and start <= out[-1][1] + 1:
            out[-1] = (out[-1][0], end)
        else:
            out.append((start, end))
        index = end + 1
    return out


def _infer_code(ts_code: str, frame: pl.DataFrame | None, counters: dict[str, int]
                ) -> list[dict[str, Any]]:
    df = _prepare(frame)
    if df is None:
        counters['no_quotes'] += 1
        return []
    dates: list[date] = []
    for value in df['date'].to_list():
        day = value if isinstance(value, date) else st_history._as_date(value)  # noqa: SLF001
        dates.append(day if day is not None else date(1990, 1, 1))
    runs = _limit_runs(ts_code, dates)
    if not runs:
        counters['not_inferable'] += 1
        return []

    rows: list[dict[str, Any]] = []
    for run_start, run_end in runs:
        length = run_end - run_start + 1
        if length < WINDOW:
            continue
        sub = df.slice(run_start, length).with_columns([
            pl.col('absret').fill_null(0.0).rolling_max(window_size=WINDOW).alias('w_max'),
            (pl.col('absret').fill_null(0.0) >= TOUCH_MIN).cast(pl.Int32)
            .rolling_sum(window_size=WINDOW).alias('w_touch'),
            pl.col('amp').fill_null(0.0).rolling_median(window_size=WINDOW).alias('w_amp'),
            pl.col('valid').fill_null(0.0).rolling_mean(window_size=WINDOW).alias('w_valid'),
            pl.col('breach').cast(pl.Int32).alias('breach_int'),
            pl.col('raw_breach').cast(pl.Int32).alias('raw_breach_int'),
        ]).with_columns([
            (pl.col('w_max').is_not_null() & (pl.col('w_max') <= CAP_TOL)
             & (pl.col('w_touch') >= TOUCH_NEED) & (pl.col('w_amp') >= AMP_MIN)
             & (pl.col('w_valid') >= VALID_MIN)).fill_null(False).alias('flag'),
        ])

        stats = sub.select([
            pl.len().alias('windows'),
            pl.col('flag').sum().alias('flagged'),
            ((pl.col('w_max') > CAP_TOL) & pl.col('flag').not_()).sum().alias('breach'),
            ((pl.col('w_max') <= CAP_TOL) & (pl.col('w_touch') < TOUCH_NEED)
             & pl.col('flag').not_()).sum().alias('few_touch'),
            ((pl.col('w_max') <= CAP_TOL) & (pl.col('w_touch') >= TOUCH_NEED)
             & (pl.col('w_amp') < AMP_MIN) & pl.col('flag').not_()).sum().alias('low_amp'),
            ((pl.col('w_max') <= CAP_TOL) & (pl.col('w_touch') >= TOUCH_NEED)
             & (pl.col('w_amp') >= AMP_MIN) & (pl.col('w_valid') < VALID_MIN)
             & pl.col('flag').not_()).sum().alias('illiquid'),
            (((pl.col('raw_breach_int') == 1) & (pl.col('breach_int') == 0))
             .sum()).alias('exdiv'),
        ]).to_dicts()[0]
        counters['windows_total'] += int(stats['windows'] or 0)
        counters['windows_flagged'] += int(stats['flagged'] or 0)
        counters['rejected_breach'] += int(stats['breach'] or 0)
        counters['rejected_few_touch'] += int(stats['few_touch'] or 0)
        counters['rejected_low_amp'] += int(stats['low_amp'] or 0)
        counters['rejected_illiquid'] += int(stats['illiquid'] or 0)
        counters['exdiv_false_removed'] += int(stats['exdiv'] or 0)

        absret = sub['absret'].fill_null(0.0).to_list()
        breach = sub['breach'].to_list()
        local_dates = sub['date'].to_list()
        flags = sub['flag'].to_list()

        for span_start, span_end in _spans(flags):
            new_start, new_end = _refine(span_start, span_end, breach)
            new_end = _trim_tail(new_start, new_end, absret)
            if new_end - new_start + 1 < MIN_RUN:
                continue
            rows.append({
                'ts_code': ts_code,
                'start_date': local_dates[new_start],
                'end_date': None if new_end >= len(local_dates) - 1 else local_dates[new_end],
                'flag': FLAG_SUSPECTED,
                'source': SOURCE_PRICE,
                'confidence': '',
                'evidence': '',
                'fetched_at': datetime.now(),
            })
    merged = _merge_rows(rows)
    # evidence 必须在合并之后按最终区间重算：合并会把相邻段接起来、改写 end_date，
    # 若沿用合并前的段内统计，就会出现 evidence['end'] 早于 end_date 的矛盾证据。
    for row in merged:
        evidence = _evidence_frame(df, row['start_date'], row['end_date'])
        row['evidence'] = json.dumps(evidence, ensure_ascii=False, default=str)
        if evidence['touch_neg'] >= 3 and evidence['touch_pos'] == 0:
            counters['neg_only_intervals'] += 1
    return merged


def _refine(span_start: int, span_end: int, breach: list[bool]) -> tuple[int, int]:
    """用「超限日」收紧区间两端，抵消 20 日窗口带来的外延。"""
    begin = span_start
    for offset in range(1, EDGE_LOOKBACK + 1):
        index = span_start - offset
        if index < 0:
            break
        if breach[index]:
            begin = index + 1
            break
    end = span_end
    for offset in range(1, EDGE_LOOKBACK + 1):
        index = span_end + offset
        if index >= len(breach):
            break
        if breach[index]:
            end = index - 1
            break
    return begin, end


def _trim_tail(begin: int, end: int, absret: list[float]) -> int:
    """尾部收口：ST 结束后若一直没有「贴 5%」的日子，窗口仍会短暂成立。

    以最后一次触碰日为锚，其后留 3 个交易日缓冲（不超过原区间终点）。
    """
    last_touch: int | None = None
    for index in range(end, begin - 1, -1):
        if absret[index] >= TOUCH_MIN:
            last_touch = index
            break
    if last_touch is not None and end - last_touch > 5:
        return min(end, last_touch + 3)
    return end


def _evidence_frame(df: pl.DataFrame, start: date,
                    end: date | None) -> dict[str, Any]:
    """按最终区间（合并 / 收尾之后）从整段行情重算 evidence。

    ``end`` 为空表示「延伸到本地最后一根 bar」：统计窗口取到最后一根 bar，
    证据里的 ``end`` 同为空，保证 ``evidence['end'] == end_date`` 恒成立。
    """
    stop = end if end is not None else df['date'][-1]
    window = df.filter((pl.col('date') >= start) & (pl.col('date') <= stop))
    absret = window['absret'].fill_null(0.0).to_list()
    ret = window['ret'].fill_null(0.0).to_list()
    amp = window['amp'].fill_null(0.0).to_list()
    valid = window['valid'].fill_null(0.0).to_list()
    amounts = window['amount'].fill_null(0.0).to_list()
    breach = window['breach'].to_list()
    touch_indices = [index for index, value in enumerate(absret)
                     if TOUCH_MIN <= value < CAP_TOL]
    return {
        'days': len(absret),
        'max_absret': round(max(absret), 4),
        'touch_days': len(touch_indices),
        'touch_neg': sum(1 for index in touch_indices if ret[index] < 0),
        'touch_pos': sum(1 for index in touch_indices if ret[index] > 0),
        'amp_median': round(statistics.median(amp), 4),
        'valid_ratio': round(statistics.mean(valid), 4),
        'amount_median': round(statistics.median(amounts), 2),
        'breach_inside': sum(1 for value in breach if value),
        'start': str(start), 'end': None if end is None else str(end),
    }


def _merge_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """同标的相邻/重叠区间合并（自然日间隔 ≤ MERGE_GAP）。"""
    rows.sort(key=lambda item: item['start_date'])
    merged: list[dict[str, Any]] = []
    for row in rows:
        if merged:
            last = merged[-1]
            last_end = last['end_date'] or date(2100, 1, 1)
            if row['start_date'] <= last_end + timedelta(days=MERGE_GAP):
                limit = row['end_date'] or date(2100, 1, 1)
                last['end_date'] = None if max(last_end, limit) >= date(2100, 1, 1) \
                    else max(last_end, limit)
                continue
        merged.append(dict(row))
    return merged


# ----------------------------------------------------------------------
# 全量构建 + 对拍
# ----------------------------------------------------------------------
def _confirmed_by_code(intervals: pl.DataFrame) -> dict[str, list[tuple[date, date | None, str]]]:
    """确证区间按标的聚合，并把重叠区间合并（保证互不重叠、便于按日对拍）。"""
    far = date(2100, 1, 1)
    out: dict[str, list[tuple[date, date | None, str]]] = {}
    for row in intervals.to_dicts():
        out.setdefault(row['ts_code'], []).append(
            (row['start_date'], row['end_date'], row['flag']))
    merged: dict[str, list[tuple[date, date | None, str]]] = {}
    for code, items in out.items():
        items.sort(key=lambda item: item[0])
        result: list[tuple[date, date | None, str]] = []
        for start, end, flag in items:
            finish = end or far
            if result and start <= (result[-1][1] or far):
                prev_start, prev_end, prev_flag = result[-1]
                merged_end = max(prev_end or far, finish)
                result[-1] = (prev_start, None if merged_end >= far else merged_end,
                              prev_flag)
            else:
                result.append((start, None if finish >= far else finish, flag))
        merged[code] = result
    return merged


def _overlap(a_start: date, a_end: date | None,
             b_start: date, b_end: date | None) -> int:
    end_a = min(a_end or date.today(), date.today())
    end_b = min(b_end or date.today(), date.today())
    return max(0, (min(end_a, end_b) - max(a_start, b_start)).days + 1)


def _annotate(suspected: pl.DataFrame,
              confirmed: dict[str, list[tuple[date, date | None, str]]],
              coverage: dict[str, tuple[date, date]]) -> tuple[pl.DataFrame, dict[str, Any]]:
    """对拍：只在该标的可信覆盖窗口内判定行情推断的对错。

    覆盖窗口外的疑似区间（如沪市 2005 年前的推测）无法用名称源验证，单独计数，
    不计入误判率——否则会把「无法验证」当成「误判」。
    """
    rows = suspected.to_dicts()
    classes: dict[str, Any] = {'both': [], 'confirmed_only': [],
                               'suspected_only': [], 'unverified': []}
    suspected_total = suspected_overlap = suspected_validated = 0
    suspected_validated_overlap = 0
    for row in rows:
        code = row['ts_code']
        items = confirmed.get(code, [])
        overlap = sum(_overlap(row['start_date'], row['end_date'], start, end)
                      for start, end, _flag in items)
        row['confidence'] = CONF_HIGH if overlap else CONF_LOW
        start_day = row['start_date']
        end_day = min(row['end_date'] or date.today(), date.today())
        days = (end_day - start_day).days + 1
        suspected_total += days
        suspected_overlap += overlap
        window = coverage.get(code)
        if window is None:
            if not overlap:
                classes['unverified'].append({
                    'ts_code': code, 'start': str(start_day),
                    'flag': row['flag'], 'reason': 'no_coverage'})
        else:
            cov_start, cov_end = window
            inside = max(0, (min(end_day, cov_end) - max(start_day, cov_start)).days + 1)
            suspected_validated += inside
            if inside:
                in_start, in_end = max(start_day, cov_start), min(end_day, cov_end)
                inner_overlap = sum(
                    _overlap(in_start, in_end, start, end)
                    for start, end, _flag in items)
                suspected_validated_overlap += inner_overlap
            if not overlap:
                classes['suspected_only' if inside else 'unverified'].append({
                    'ts_code': code, 'start': str(start_day), 'flag': row['flag'],
                    'reason': 'no_overlap' if inside else 'outside_coverage'})
        if overlap:
            classes['both'].append({'ts_code': code, 'start': str(start_day),
                                    'overlap_days': overlap})

    matched_confirmed = 0
    for code, items in confirmed.items():
        for start, end, flag in items:
            hits = [row for row in rows if row['ts_code'] == code
                    and _overlap(row['start_date'], row['end_date'], start, end)]
            if hits:
                matched_confirmed += 1
            else:
                classes['confirmed_only'].append(
                    {'ts_code': code, 'start': str(start),
                     'end': str(end) if end else '', 'flag': flag})
    metrics = {
        'suspected_intervals': len(rows),
        'confirmed_intervals': matched_confirmed + len(classes['confirmed_only']),
        'both_intervals': len(classes['both']),
        'confirmed_only_intervals': len(classes['confirmed_only']),
        'suspected_only_intervals': len(classes['suspected_only']),
        'unverified_intervals': len(classes['unverified']),
        'suspected_days': suspected_total,
        'suspected_overlap_days': suspected_overlap,
        'suspected_day_precision': (suspected_overlap / suspected_total
                                    if suspected_total else 0.0),
        'validated_days': suspected_validated,
        'validated_overlap_days': suspected_validated_overlap,
        'validated_day_precision': (suspected_validated_overlap / suspected_validated
                                     if suspected_validated else 0.0),
        'confirmed_recall': (matched_confirmed / (matched_confirmed
                                                 + len(classes['confirmed_only']))
                             if matched_confirmed + len(classes['confirmed_only']) else 0.0),
    }
    frame = pl.DataFrame(rows, schema=_PRICE_SCHEMA) if rows else pl.DataFrame(
        schema=_PRICE_SCHEMA)
    return frame, {'metrics': metrics, 'classes': classes}


def _miss_reasons(missed: list[dict[str, Any]], store: Any) -> dict[str, int]:
    """给「仅名称记录有」的区间归因（每类独立计数，不做加权）。"""
    counts = {'board_not_inferable': 0, 'no_quotes': 0, 'short_interval': 0,
              'breach_inside': 0, 'few_touch': 0, 'low_amp': 0, 'no_window': 0}
    cache: dict[str, pl.DataFrame | None] = {}
    for item in missed:
        code = item['ts_code']
        start = st_history._as_date(item['start'])  # noqa: SLF001
        end = st_history._as_date(item['end']) or date.today()  # noqa: SLF001
        if start is None:
            continue
        if _regular_limit(code, start) != 0.10:
            counts['board_not_inferable'] += 1
            continue
        if code not in cache:
            cache[code] = _prepare(store.read_file('stock', code))
        frame = cache[code]
        if frame is None:
            counts['no_quotes'] += 1
            continue
        window = frame.filter((pl.col('date') >= start) & (pl.col('date') <= end))
        if window.height < WINDOW:
            counts['short_interval'] += 1
            continue
        absret = window['absret'].fill_null(0.0).to_list()
        amps = window['amp'].fill_null(0.0).to_list()
        if max(absret) > CAP_TOL:
            counts['breach_inside'] += 1
        elif sum(1 for value in absret if value >= TOUCH_MIN) < TOUCH_NEED:
            counts['few_touch'] += 1
        elif statistics.median(amps) < AMP_MIN:
            counts['low_amp'] += 1
        else:
            counts['no_window'] += 1
    return counts


def _write_atomic(frame: pl.DataFrame, path: Path) -> None:
    write_parquet_atomic(frame, path)


def run(store_root: Path | str, *, codes: Iterable[str] | None = None,
        window: int = WINDOW, touch_need: int = TOUCH_NEED) -> dict[str, Any]:
    """全量推断 + 对拍 + 落盘，返回摘要。"""
    global WINDOW, TOUCH_NEED
    WINDOW = int(window)
    TOUCH_NEED = int(touch_need)
    root = Path(store_root)
    from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415

    store = LocalQuoteStore(root)
    code_list = list(codes) if codes is not None else store.codes('stock')
    intervals = st_history.load_intervals(root)
    coverage = st_history.load_coverage(root)
    if intervals is None:
        raise RuntimeError('确证区间表不存在，先执行 python -m src.data.st_history build')
    coverage_map = {row['ts_code']: (row['coverage_start'], row['coverage_end'])
                    for row in coverage.to_dicts()} if coverage is not None else {}

    counters = {
        'codes_total': len(code_list), 'no_quotes': 0, 'not_inferable': 0,
        'windows_total': 0, 'windows_flagged': 0, 'rejected_breach': 0,
        'rejected_few_touch': 0, 'rejected_low_amp': 0, 'rejected_illiquid': 0,
        'exdiv_false_removed': 0, 'neg_only_intervals': 0,
    }
    rows: list[dict[str, Any]] = []
    for index, code in enumerate(code_list):
        try:
            frame = store.read_file('stock', code)
        # 兜底理由：单只标的行情损坏不应中断全市场推断
        except Exception as exc:  # noqa: BLE001
            logger.warning(f'[st-price] 读取行情失败 {code}: {exc}')
            frame = None
        rows.extend(_infer_code(code, frame, counters))
        if (index + 1) % 500 == 0:
            logger.info(f'[st-price] 已处理 {index + 1}/{len(code_list)}，'
                        f'疑似区间 {len(rows)}')

    suspected = pl.DataFrame(rows, schema=_PRICE_SCHEMA) if rows else pl.DataFrame(
        schema=_PRICE_SCHEMA)
    confirmed = _confirmed_by_code(intervals)
    annotated, compare = _annotate(suspected, confirmed, coverage_map)
    _write_atomic(annotated, price_path(root))

    misses = _miss_reasons(compare['classes']['confirmed_only'], store)
    fills = _early_fill(annotated, confirmed)
    summary = {
        'built_at': datetime.now().isoformat(timespec='seconds'),
        'window': WINDOW, 'cap_tol': CAP_TOL, 'touch_min': TOUCH_MIN,
        'touch_need': TOUCH_NEED, 'amp_min': AMP_MIN, 'valid_min': VALID_MIN,
        'min_run': MIN_RUN,
        'counters': counters,
        'metrics': compare['metrics'],
        'miss_reasons': misses,
        'classes': {
            'both': len(compare['classes']['both']),
            'confirmed_only': len(compare['classes']['confirmed_only']),
            'suspected_only': len(compare['classes']['suspected_only']),
            'unverified': len(compare['classes']['unverified']),
            'confirmed_only_samples': compare['classes']['confirmed_only'][:60],
            'suspected_only_samples': compare['classes']['suspected_only'][:60],
        },
        'early_fill': fills,
        'suspected_codes': annotated['ts_code'].n_unique() if annotated.height else 0,
    }
    summary_path(root).write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    # 写盘后按磁盘实测回写同目录 manifest；required=True 要求两个文件都已登记，
    # 漏登记时报错而不是静默跳过（DG-3）。
    from src.data.manifest_refresh import refresh_manifest_files  # noqa: PLC0415

    refresh_manifest_files(
        manifest_path(root), root,
        [price_path(root), summary_path(root)], required=True)
    logger.info(f'[st-price] 疑似区间 {annotated.height} 段 / '
                f'{summary["suspected_codes"]} 只；日级验证精确率 '
                f'{compare["metrics"]["validated_day_precision"]:.3f}')
    return summary


def _early_fill(suspected: pl.DataFrame,
                confirmed: dict[str, list[tuple[date, date | None, str]]]) -> dict[str, Any]:
    """统计「行情推断比名称源多补出的早年标的」。"""
    cutoff = st_history.CNINFO_SAFE_START
    codes = suspected['ts_code'].n_unique() if suspected.height else 0
    no_record = set()
    earlier = set()
    early_codes = set()
    for row in suspected.to_dicts():
        items = confirmed.get(row['ts_code'], [])
        if not items:
            no_record.add(row['ts_code'])
        elif row['start_date'] < min(item[0] for item in items) - timedelta(days=60):
            earlier.add(row['ts_code'])
        if row['start_date'] < cutoff:
            early_codes.add(row['ts_code'])
    return {'suspected_codes': codes, 'codes_without_confirmed': len(no_record),
            'codes_earlier_than_confirmed': len(earlier),
            'codes_starting_before_2005': len(early_codes)}


def suspected_status_at(store_root: Path | str, code: str, as_of: Any) -> st_history.StHistoryResult | None:
    """疑似区间命中返回 ST 结果（``source=price_inference``），否则 ``None``。"""
    day = st_history._as_date(as_of)  # noqa: SLF001
    frame = load_price_inferred(store_root)
    if day is None or frame is None or frame.height == 0:
        return None
    target = st_history._normalize(code)  # noqa: SLF001
    hits = frame.filter(
        (pl.col('ts_code') == target) & (pl.col('start_date') <= day)
        & ((pl.col('end_date').is_null()) | (pl.col('end_date') >= day)))
    if hits.height == 0:
        return None
    row = hits.sort('start_date').to_dicts()[-1]
    return st_history.StHistoryResult(
        st_history.STATUS_ST, flag=FLAG_SUSPECTED, source=SOURCE_PRICE,
        start=row['start_date'], end=row['end_date'], reason='suspected_interval_hit')


def suspected_intervals_for_backtest(store_root: Path | str, codes: Iterable[str],
                                     start: Any, end: Any) -> dict[str, list[dict[str, Any]]]:
    """导出疑似区间（``is_suspected=True``），与确证区间结构同构、互不混用。"""
    start_day = st_history._as_date(start) or date(1990, 1, 1)  # noqa: SLF001
    end_day = st_history._as_date(end) or date.today()  # noqa: SLF001
    frame = load_price_inferred(store_root)
    if frame is None or frame.height == 0:
        return {}
    wanted = {st_history._normalize(code) for code in codes}  # noqa: SLF001
    rows = frame.filter(
        pl.col('ts_code').is_in(list(wanted))
        & (pl.col('start_date') <= end_day)
        & ((pl.col('end_date').is_null()) | (pl.col('end_date') >= start_day)))
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows.sort('start_date').to_dicts():
        out.setdefault(row['ts_code'], []).append({
            'start_date': max(start_day, row['start_date']).isoformat(),
            'end_date': min(end_day, row['end_date'] or end_day).isoformat(),
            'is_suspected': True,
            'confidence': row['confidence'],
            'source': row['source'],
        })
    return out


# ----------------------------------------------------------------------
# 验证：已知 ST 票逐只对照 + 负样本
# ----------------------------------------------------------------------
def _trading_gap(dates: list[date], first: date, second: date) -> int:
    if not dates:
        return 0
    ordered = sorted(dates)
    low, high = sorted((first, second))
    return sum(1 for day in ordered if low <= day < high)


def samples(store_root: Path | str, pairs: list[tuple[str, str, str]] | None = None,
            negatives: list[str] | None = None) -> dict[str, Any]:
    """逐只对照疑似区间与确证区间的起止日误差（交易日），并跑负样本。"""
    root = Path(store_root)
    from src.data.local_store.store import LocalQuoteStore  # noqa: PLC0415

    store = LocalQuoteStore(root)
    intervals = st_history.load_intervals(root)
    suspected = load_price_inferred(root)
    confirmed = _confirmed_by_code(intervals) if intervals is not None else {}
    rows: list[dict[str, Any]] = []
    default_pairs = _pick_pairs(confirmed)
    for code, c_start, c_end in (pairs or default_pairs):
        frame = store.read_file('stock', code)
        dates: list[date] = []
        if frame is not None and 'date' in frame.columns:
            values = [value if isinstance(value, date) else
                      st_history._as_date(value) for value in frame['date'].to_list()]  # noqa: SLF001
            dates = [day for day in values if day is not None]
        c_start_day = st_history._as_date(c_start)  # noqa: SLF001
        c_end_day = st_history._as_date(c_end)  # noqa: SLF001
        hits = [] if suspected is None else [
            row for row in suspected.filter(pl.col('ts_code') == code).to_dicts()
            if c_start_day and _overlap(row['start_date'], row['end_date'],
                                        c_start_day, c_end_day or date.today())]
        if not hits:
            rows.append({'ts_code': code, 'confirmed_start': c_start,
                         'confirmed_end': c_end, 'inferred_start': '',
                         'inferred_end': '', 'start_error_td': None,
                         'end_gap_td': None, 'overrun_td': None, 'matched': False})
            continue
        earliest = min(item['start_date'] for item in hits)
        latest = max(item['end_date'] or date.today() for item in hits)
        start_error = _trading_gap(dates, earliest, c_start_day) if c_start_day else None
        end_gap: int | None = None
        overrun: int | None = None
        if c_end_day is not None:
            end_day = c_end_day
            covered_until = max(min(item['end_date'] or end_day, end_day)
                                for item in hits)
            end_gap = _trading_gap(dates, covered_until, end_day)
            if latest > end_day:
                ranges = [(start, end or date.today())
                          for start, end, _flag in confirmed.get(code, [])]
                overrun = sum(
                    1 for day in dates
                    if end_day <= day <= latest
                    and not any(start <= day <= end for start, end in ranges))
        rows.append({
            'ts_code': code, 'confirmed_start': c_start, 'confirmed_end': c_end,
            'inferred_start': str(earliest), 'inferred_end': str(latest),
            'start_error_td': start_error, 'end_gap_td': end_gap,
            'overrun_td': overrun, 'matched': True,
        })
    negatives_out: list[dict[str, Any]] = []
    for code in negatives or ['600000.SH', '600519.SH', '000333.SZ']:
        neg_hits = 0 if suspected is None else suspected.filter(
            pl.col('ts_code') == code).height
        negatives_out.append({'ts_code': code, 'suspected_intervals': neg_hits})
    errors = [row['start_error_td'] for row in rows if row.get('start_error_td') is not None]
    gaps = [row['end_gap_td'] for row in rows if row.get('end_gap_td') is not None]
    overruns = [row['overrun_td'] for row in rows if row.get('overrun_td') is not None]
    return {'samples': rows, 'negatives': negatives_out,
            'start_error_median_td': statistics.median(errors) if errors else None,
            'start_error_max_td': max(errors) if errors else None,
            'end_gap_median_td': statistics.median(gaps) if gaps else None,
            'end_gap_max_td': max(gaps) if gaps else None,
            'overrun_median_td': statistics.median(overruns) if overruns else None,
            'overrun_max_td': max(overruns) if overruns else None,
            'matched_samples': sum(1 for row in rows if row['matched'])}


def _pick_pairs(confirmed: dict[str, list[tuple[date, date | None, str]]]
                ) -> list[tuple[str, str, str]]:
    """自动挑 6 只：含 *ST 与已摘帽（区间已闭合）的代表。"""
    picked: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for want_star in (True, False):
        for code, items in sorted(confirmed.items()):
            if code in seen or len(items) < 2:
                continue
            star = any(flag == st_history.FLAG_STAR for _s, _e, flag in items)
            closed = all(end is not None for _s, end, _f in items)
            if star == want_star and closed:
                start, end, _flag = items[0]
                picked.append((code, str(start), str(end) if end else ''))
                seen.add(code)
            if len(picked) >= 6:
                return picked
    return picked


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------
def _cmd_run(args: argparse.Namespace) -> int:
    from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
    from src.utils.config import get_config  # noqa: PLC0415

    summary = run(resolve_store_root(get_config()), window=args.window,
                  touch_need=args.touch_need)
    logger.info(f'[st-price] 落盘: {json.dumps({key: summary[key] for key in
                                                ("counters", "metrics", "early_fill")},
                                               ensure_ascii=False)}')
    return 0


def _cmd_samples(args: argparse.Namespace) -> int:
    from src.data.local_store.store import resolve_store_root  # noqa: PLC0415
    from src.utils.config import get_config  # noqa: PLC0415

    result = samples(resolve_store_root(get_config()))
    logger.info(f'[st-price] 样本对照: {json.dumps(result, ensure_ascii=False, default=str)}')
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='行情反推疑似 ST 区间')
    sub = parser.add_subparsers(dest='command', required=True)
    run_cmd = sub.add_parser('run', help='全量推断并落盘')
    run_cmd.add_argument('--window', type=int, default=WINDOW)
    run_cmd.add_argument('--touch-need', type=int, default=TOUCH_NEED)
    run_cmd.set_defaults(func=_cmd_run)
    sample_cmd = sub.add_parser('samples', help='已知 ST 票逐只对照 + 负样本')
    sample_cmd.set_defaults(func=_cmd_samples)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    raise SystemExit(main())
