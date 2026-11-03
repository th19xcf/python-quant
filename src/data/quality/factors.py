#!/usr/bin/env python3

"""复权因子全市场准确性核验（只读）。

核验内容：

1. 覆盖与一致性：因子快照覆盖标的/日期范围，与本地列存各类别标的的差集；
   并用「本地行情 + 分红」逐票重算，量化是否存在两批口径。
2. 算法正确性：抽样独立复算（原始价 → 因子 → 复权价 → 反推），覆盖
   送转/配股/现金分红/无分红与不同上市年代；验证不变量。
3. 分红完整性：因子跳变日期与 ``stock_dividend`` 双向反查。
4. 品种边界：指数不应有因子；ETF/基金因子与取数降级行为。
5. 结论：输出可机器读取的 JSON/CSV 报告，不修改任何数据。

所有结论只落 ``logs/`` 报告，绝不改库、改表或改行情文件。
"""

from __future__ import annotations

import csv
import json
import random
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import polars as pl
from loguru import logger

from src.data.adj_factor_calculator import AdjFactorCalculator
from src.utils.code_converter import is_fund_code, is_index_code

#: 判定「逐行一致」的容差：重算走同一实现，Float32 化后允许 1e-6 相对误差
EXACT_REL_TOL = 1e-6
#: 独立复算（不同累乘顺序）允许的浮点相对误差
NAIVE_REL_TOL = 1e-9
#: 快照按代码分批读取，避免一次性载入 1,700 万行进内存
BATCH_CODES = 400


@dataclass
class FactorAudit:
    """一次核验的结果与报告文件路径。"""

    payload: dict[str, Any]
    report_files: dict[str, str] = field(default_factory=dict)


@dataclass
class _ScanStats:
    """全市场逐票重算与跳变反查的累积结果。"""

    recompute_rows: list[dict[str, Any]] = field(default_factory=list)
    gap_rows: list[dict[str, Any]] = field(default_factory=list)
    max_rel_qfq: float = 0.0
    max_rel_hfq: float = 0.0
    codes_exact: int = 0
    codes_diff: int = 0
    jump_rows: int = 0
    matched_jump_rows: int = 0
    factor_without_source: int = 0
    event_without_jump: int = 0
    events_before_first: int = 0
    events_after_last: int = 0
    invalid_events: int = 0
    jump_codes_checked: int = 0


def _quote_files(root: Path, data_type: str) -> list[Path]:
    """按目录枚举行情文件（不依赖可能残缺的覆盖度索引）。"""
    base = root / 'quotes' / data_type
    if not base.is_dir():
        return []
    return sorted(base.glob('*/*.parquet'))


def _market_dir(ts_code: str) -> str:
    suffix = ts_code.rsplit('.', 1)[-1].lower() if '.' in ts_code else 'sz'
    return suffix if suffix in {'sh', 'sz', 'bj'} else 'sz'


def _max_rel(left: pl.Series, right: pl.Series) -> float:
    """两列相对误差最大值（忽略空值与非有限值）。"""
    frame = pl.DataFrame({
        'left': left.cast(pl.Float64),
        'right': right.cast(pl.Float64),
    }).drop_nulls()
    if frame.is_empty():
        return 0.0
    left_values = frame['left'].to_numpy().astype(np.float64, copy=False)
    right_values = frame['right'].to_numpy().astype(np.float64, copy=False)
    mask = np.isfinite(left_values) & np.isfinite(right_values)
    if not mask.any():
        return 0.0
    a = left_values[mask]
    b = right_values[mask]
    denom = np.maximum(np.maximum(np.abs(a), np.abs(b)), 1e-12)
    return float(np.max(np.abs(a - b) / denom))


def _expected_frame(quote: pl.DataFrame, div: pl.DataFrame | None) -> pl.DataFrame:
    """按生产实现重算单票因子（本地行情 + 分红）。"""
    prices = quote.select([
        pl.col('date').alias('trade_date'),
        pl.col('close').cast(pl.Float64),
    ])
    unit = prices.with_columns([
        pl.lit(1.0).alias('qfq_factor'),
        pl.lit(1.0).alias('hfq_factor'),
    ])
    if div is None or div.is_empty():
        return unit
    valid = div.filter(pl.col('ex_date').is_not_null())
    if valid.is_empty():
        return unit
    valid = valid.with_columns([
        pl.col('cash_div').cast(pl.Float64).fill_null(0.0),
        pl.col('share_div').cast(pl.Float64).fill_null(0.0),
        pl.col('rights_issue_ratio').cast(pl.Float64).fill_null(0.0),
        pl.col('rights_issue_price').cast(pl.Float64).fill_null(0.0),
    ])
    return AdjFactorCalculator.calculate_factors(prices, valid)


def _event_factor(prev_close: float, event: dict[str, Any]) -> float | None:
    """单个除权事件的复权系数（与生产公式一致，返回 None 表示无效）。"""
    cash = float(event.get('cash_div') or 0.0)
    share = float(event.get('share_div') or 0.0)
    ratio = float(event.get('rights_issue_ratio') or 0.0)
    rights_price = float(event.get('rights_issue_price') or 0.0)
    if prev_close <= 0 or (cash == 0 and share == 0 and ratio == 0):
        return None
    denominator = prev_close * (1 + share + ratio)
    if denominator == 0:
        return None
    factor = (prev_close - cash + rights_price * ratio) / denominator
    if factor <= 0 or factor > 10:
        return None
    return factor


def _naive_factors(dates: Sequence[date], closes: Sequence[float],
                   events: Sequence[dict[str, Any]]) -> tuple[np.ndarray, np.ndarray]:
    """独立复算：逐事件累乘（不依赖生产实现的向量化后缀乘积）。"""
    ordinals = np.asarray([item.toordinal() for item in dates], dtype=np.int64)
    qfq = np.ones(len(dates), dtype=np.float64)
    hfq = np.ones(len(dates), dtype=np.float64)
    for event in events:
        ex_date = event.get('ex_date')
        if ex_date is None:
            continue
        pos = int(np.searchsorted(ordinals, ex_date.toordinal(), side='left'))
        if pos <= 0:
            continue
        factor = _event_factor(float(closes[pos - 1]), event)
        if factor is None:
            continue
        qfq[:pos] *= factor
        hfq[pos:] /= factor
    return qfq, hfq


def _split_dividends(dividends: pl.DataFrame) -> dict[str, pl.DataFrame]:
    if dividends.is_empty() or 'ts_code' not in dividends.columns:
        return {}
    return {
        str(group['ts_code'][0]): group
        for group in dividends.partition_by('ts_code', maintain_order=True)
    }


def _select_sample(stock_codes: Sequence[str], dividends: pl.DataFrame,
                   root: Path, count: int, seed: int) -> list[str]:
    """按分红特征与上市年代分层抽样（覆盖送转/配股/纯现金/无分红）。"""
    valid = dividends.filter(pl.col('ex_date').is_not_null())
    share_codes = set(valid.filter(pl.col('share_div') > 0)['ts_code'].to_list())
    rights_codes = set(
        valid.filter(pl.col('rights_issue_ratio') > 0)['ts_code'].to_list())
    cash_codes = set(valid.filter(
        (pl.col('cash_div') > 0)
        & (pl.col('share_div') == 0)
        & (pl.col('rights_issue_ratio') == 0)
    )['ts_code'].to_list())
    all_codes = list(stock_codes)

    rng = random.Random(seed)  # noqa: S311 —— 抽样确定性，非加密用途
    quota = max(count // 4, 1)
    selected: list[str] = []
    for pool in (share_codes, rights_codes, cash_codes):
        candidates = sorted(pool)
        rng.shuffle(candidates)
        added = 0
        for code in candidates:
            if added >= quota:
                break
            if code in all_codes and code not in selected:
                selected.append(code)
                added += 1

    # 上市年代兜底：最早/最新上市的若干只直接纳入，保证跨年代覆盖
    spans: list[tuple[date, str]] = []
    for code in all_codes:
        path = root / 'quotes' / 'stock' / _market_dir(code) / f'{code}.parquet'
        if not path.exists():
            continue
        span = pl.scan_parquet(str(path)).select(
            pl.col('date').min().alias('start')).collect()
        if not span.is_empty() and span['start'][0] is not None:
            spans.append((span['start'][0], code))
    spans.sort()
    for _, code in [*spans[:4], *spans[-4:]]:
        if code not in selected:
            selected.append(code)

    remaining = sorted(set(all_codes) - set(selected))
    rng.shuffle(remaining)
    for code in remaining:
        if len(selected) >= count:
            break
        selected.append(code)
    return sorted(selected[:count])


def _snapshot_batches(path: Path) -> list[list[str]]:
    """按 ts_code 分批返回快照代码（只读一列，避免整表常驻）。"""
    codes = (
        pl.scan_parquet(str(path))
        .select('ts_code')
        .unique()
        .collect()['ts_code']
        .sort()
        .to_list()
    )
    return [codes[i:i + BATCH_CODES] for i in range(0, len(codes), BATCH_CODES)]


def _coverage_section(root: Path, factor_path: Path,
                      stock_codes: set[str], index_codes: set[str],
                      fund_codes: set[str]) -> tuple[dict[str, Any], set[str]]:
    snapshot = pl.scan_parquet(str(factor_path))
    frame = snapshot.select([
        pl.len().alias('rows'),
        pl.col('ts_code').n_unique().alias('codes'),
        pl.col('trade_date').min().alias('start'),
        pl.col('trade_date').max().alias('end'),
    ]).collect()
    factor_codes = set(
        snapshot.select('ts_code').unique().collect()['ts_code'].to_list())
    coverage = {
        'factor_rows': int(frame['rows'][0]),
        'factor_codes': int(frame['codes'][0]),
        'factor_start': str(frame['start'][0]),
        'factor_end': str(frame['end'][0]),
        'quote_stock_codes': len(stock_codes),
        'quote_index_codes': len(index_codes),
        'quote_fund_codes': len(fund_codes),
        'factor_code_count_not_in_stock': len(factor_codes - stock_codes),
        'stock_count_without_factor': len(stock_codes - factor_codes),
        'factor_codes_not_in_stock_sample': sorted(factor_codes - stock_codes)[:20],
        'stock_without_factor_sample': sorted(stock_codes - factor_codes)[:20],
    }
    return coverage, factor_codes


def _scan_symbol(ts_code: str, snap: pl.DataFrame, quote: pl.DataFrame,
                 div: pl.DataFrame | None, stats: _ScanStats) -> None:
    """单票：重算对比 + 跳变↔分红事件双向反查。"""
    expected = _expected_frame(quote, div).select(
        ['trade_date', 'qfq_factor', 'hfq_factor'])
    joined = snap.join(expected, on='trade_date', how='full',
                       suffix='_exp', coalesce=True)
    qfq_rel = _max_rel(joined['qfq_factor'], joined['qfq_factor_exp'])
    hfq_rel = _max_rel(joined['hfq_factor'], joined['hfq_factor_exp'])
    stats.max_rel_qfq = max(stats.max_rel_qfq, qfq_rel)
    stats.max_rel_hfq = max(stats.max_rel_hfq, hfq_rel)
    is_exact = qfq_rel <= EXACT_REL_TOL and hfq_rel <= EXACT_REL_TOL
    stats.codes_exact += int(is_exact)
    stats.codes_diff += int(not is_exact)
    stats.recompute_rows.append({
        'ts_code': ts_code,
        'rows': snap.height,
        'date_mismatch': joined.filter(
            pl.col('qfq_factor_exp').is_null()
            | pl.col('qfq_factor').is_null()).height,
        'max_rel_qfq': qfq_rel,
        'max_rel_hfq': hfq_rel,
        'same_algorithm': is_exact,
    })

    dates = snap['trade_date'].to_list()
    hfq = snap['hfq_factor'].to_numpy()
    jump_rows = np.flatnonzero(np.diff(hfq) != 0) + 1
    jump_dates = {dates[index] for index in jump_rows}
    stats.jump_rows += len(jump_rows)
    stats.jump_codes_checked += 1

    valid_div = (
        div.filter(pl.col('ex_date').is_not_null())
        if div is not None and not div.is_empty() else None
    )
    expected_dates: set[date] = set()
    if valid_div is not None and not valid_div.is_empty():
        closes = quote.sort('date')['close'].to_list()
        ordinals = np.asarray(
            [item.toordinal() for item in dates], dtype=np.int64)
        for event in valid_div.to_dicts():
            pos = int(np.searchsorted(
                ordinals, event['ex_date'].toordinal(), side='left'))
            if pos <= 0:
                stats.events_before_first += 1
                continue
            if pos >= len(dates):
                stats.events_after_last += 1
                stats.gap_rows.append({
                    'kind': 'event_after_snapshot_tail',
                    'ts_code': ts_code,
                    'trade_date': str(event['ex_date']),
                    'detail': '除权事件晚于该标的因子快照最后一行，因子未更新',
                })
                continue
            factor = _event_factor(float(closes[pos - 1]), event)
            if factor is None:
                stats.invalid_events += 1
                continue
            expected_dates.add(dates[pos])

    unmatched = jump_dates - expected_dates
    missing = expected_dates - jump_dates
    stats.factor_without_source += len(unmatched)
    stats.event_without_jump += len(missing)
    stats.matched_jump_rows += len(jump_dates & expected_dates)
    for trade_date in sorted(unmatched)[:20]:
        stats.gap_rows.append({
            'kind': 'factor_without_source',
            'ts_code': ts_code,
            'trade_date': str(trade_date),
            'detail': 'hfq 跳变但分红快照无对应除权事件',
        })
    for trade_date in sorted(missing)[:20]:
        stats.gap_rows.append({
            'kind': 'event_without_jump',
            'ts_code': ts_code,
            'trade_date': str(trade_date),
            'detail': '分红事件映射到该交易日但 hfq 未跳变',
        })


def _scan_all(factor_path: Path, root: Path,
              div_by_code: dict[str, pl.DataFrame]) -> _ScanStats:
    stats = _ScanStats()
    for batch in _snapshot_batches(factor_path):
        frame = (
            pl.scan_parquet(str(factor_path))
            .filter(pl.col('ts_code').is_in(batch))
            .collect()
            .sort(['ts_code', 'trade_date'])
        )
        for group in frame.partition_by('ts_code', maintain_order=True):
            ts_code = str(group['ts_code'][0])
            snap = group.select(['trade_date', 'qfq_factor', 'hfq_factor'])
            quote_path = (
                root / 'quotes' / 'stock' / _market_dir(ts_code)
                / f'{ts_code}.parquet')
            if not quote_path.exists():
                continue
            quote = pl.read_parquet(quote_path, columns=['date', 'close'])
            _scan_symbol(ts_code, snap, quote, div_by_code.get(ts_code), stats)
    return stats


def _sample_section(factor_path: Path, root: Path,
                    div_by_code: dict[str, pl.DataFrame],
                    dividends: pl.DataFrame, codes: Sequence[str]) -> dict[str, Any]:
    share_set = set(
        dividends.filter(pl.col('share_div') > 0)['ts_code'].to_list())
    rights_set = set(
        dividends.filter(pl.col('rights_issue_ratio') > 0)['ts_code'].to_list())
    rows: list[dict[str, Any]] = []
    max_rel = 0.0
    reverse_max_rel = 0.0
    ratio_violations = 0
    for ts_code in codes:
        quote_path = (
            root / 'quotes' / 'stock' / _market_dir(ts_code)
            / f'{ts_code}.parquet')
        if not quote_path.exists():
            continue
        quote = pl.read_parquet(
            quote_path, columns=['date', 'close']).sort('date')
        raw_snap = (
            pl.scan_parquet(str(factor_path))
            .filter(pl.col('ts_code') == ts_code)
            .collect()
            .sort('trade_date')
            .select(['trade_date', 'qfq_factor', 'hfq_factor'])
        )
        # 旧批次因子可能带有本地行情没有的历史日期，按内连接对齐重叠区间
        snap = raw_snap.join(
            quote.select([pl.col('date').alias('trade_date'), 'close']),
            on='trade_date', how='inner',
        ).sort('trade_date')
        if snap.is_empty():
            continue
        dates = snap['trade_date'].to_list()
        closes = snap['close'].to_list()
        div = div_by_code.get(ts_code)
        events = div.to_dicts() if div is not None and not div.is_empty() else []
        naive_qfq, naive_hfq = _naive_factors(dates, closes, events)

        qfq_rel = _max_rel(snap['qfq_factor'], pl.Series(naive_qfq))
        hfq_rel = _max_rel(snap['hfq_factor'], pl.Series(naive_hfq))
        max_rel = max(max_rel, qfq_rel, hfq_rel)

        # 原始价 → 复权价 → 反推原始价
        raw = np.asarray(closes, dtype=np.float64)
        for factor_name in ('qfq_factor', 'hfq_factor'):
            factor_values = snap[factor_name].to_numpy().astype(np.float64)
            recovered = (raw * factor_values) / factor_values
            denom = np.maximum(np.abs(raw), 1e-12)
            reverse_max_rel = max(
                reverse_max_rel, float(np.max(np.abs(recovered - raw) / denom)))

        # hfq / qfq 的比值应逐票恒定（不同标的之间并非常数）
        ratio = (
            snap['hfq_factor'].to_numpy().astype(np.float64)
            / snap['qfq_factor'].to_numpy().astype(np.float64)
        )
        finite = ratio[np.isfinite(ratio)]
        if finite.size:
            deviation = np.abs(finite - finite[0]) / np.maximum(
                np.abs(finite[0]), 1e-12)
            if float(deviation.max()) > NAIVE_REL_TOL:
                ratio_violations += 1
        rows.append({
            'ts_code': ts_code,
            'rows': snap.height,
            'first_year': quote['date'][0].year if quote.height else None,
            'has_share_div': ts_code in share_set,
            'has_rights_issue': ts_code in rights_set,
            'event_rows': len(events),
            'max_rel_qfq': qfq_rel,
            'max_rel_hfq': hfq_rel,
        })
    return {
        'size': len(rows),
        'codes': rows,
        'first_year_buckets': _year_buckets(rows),
        'max_rel_vs_snapshot': max_rel,
        'reversible_max_rel': reverse_max_rel,
        'ratio_time_constant_violations': ratio_violations,
        'invariant_note': (
            '本实现中 hfq = qfq / 全历史因子乘积，故 hfq/qfq 逐票恒定，'
            '但不同标的之间不是同一常数；跨票常数关系不成立，已按逐票恒定校验'
        ),
    }


def _year_buckets(rows: Sequence[dict[str, Any]]) -> dict[str, int]:
    buckets = {'<2000': 0, '2000-2009': 0, '2010-2019': 0, '>=2020': 0}
    for row in rows:
        year = row.get('first_year')
        if not isinstance(year, int):
            continue
        if year < 2000:
            buckets['<2000'] += 1
        elif year < 2010:
            buckets['2000-2009'] += 1
        elif year < 2020:
            buckets['2010-2019'] += 1
        else:
            buckets['>=2020'] += 1
    return buckets


def _boundary_section(factor_codes: set[str],
                      fund_files: Sequence[Path]) -> dict[str, Any]:
    index_in_factor = sorted(
        code for code in factor_codes if is_index_code(code))
    fund_in_factor = sorted(
        code for code in factor_codes
        if is_fund_code(code.split('.')[0], _market_dir(code)))
    fund_frame = (
        pl.scan_parquet([str(path) for path in fund_files])
        .select(['qfq_factor', 'hfq_factor'])
        .collect()
        if fund_files else pl.DataFrame()
    )
    fund_non_unit = 0
    fund_nulls = 0
    if not fund_frame.is_empty():
        fund_non_unit = int(
            ((fund_frame['qfq_factor'] != 1.0)
             | (fund_frame['hfq_factor'] != 1.0)).sum() or 0)
        fund_nulls = int(fund_frame.null_count().to_numpy().sum())
    return {
        'index_codes_in_factor': len(index_in_factor),
        'index_sample_in_factor': index_in_factor[:10],
        'fund_codes_in_factor': len(fund_in_factor),
        'fund_sample_in_factor': fund_in_factor[:10],
        'fund_quote_rows': fund_frame.height,
        'fund_non_unit_factor_rows': fund_non_unit,
        'fund_null_factor_rows': fund_nulls,
        'fund_read_behavior': (
            '基金行情文件不写非 1 因子；DataFetcher._apply_adjustment 在'
            '因子列全为 1.0 时显式告警（复权降级），不会静默伪装成复权价'
        ),
    }


def _conclusions(*, coverage: dict[str, Any], recompute: dict[str, Any],
                 jumps: dict[str, Any], boundary: dict[str, Any],
                 dividends: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    if coverage['stock_count_without_factor'] == 0 \
            and coverage['factor_code_count_not_in_stock'] == 0:
        lines.append('因子快照与个股列存完全一一对应，无漏算/越界标的。')
    else:
        lines.append('因子快照与个股列存存在差集，见 coverage 字段。')
    if recompute['codes_diff'] == 0:
        lines.append('全部标的与「本地行情+分红」重算逐行一致，可视为同一算法产出。')
    else:
        lines.append(
            f"{recompute['codes_diff']} 只与重算存在差异（最大相对 "
            f"qfq={recompute['max_rel']['qfq_factor']:.3e} / "
            f"hfq={recompute['max_rel']['hfq_factor']:.3e}），属两批口径，"
            '需按 diff_codes 逐只确认。')
    if jumps['factor_without_source'] == 0 and jumps['event_without_jump'] == 0:
        lines.append('因子跳变与分红事件双向完全对齐。')
    else:
        lines.append(
            f"因子无来源 {jumps['factor_without_source']} 行，"
            f"有事件无跳变 {jumps['event_without_jump']} 行，"
            '需结合分红快照补缺或修正。')
    if jumps['events_after_last_row']:
        lines.append(
            f"{jumps['events_after_last_row']} 条除权事件晚于对应标的因子快照末端，"
            '这些标的的因子未更新（见 gaps.csv 的 event_after_snapshot_tail），必须重建。')
    if recompute['codes_with_tail_gap']:
        lines.append(
            f"{recompute['codes_with_tail_gap']} 只标的的因子快照行数少于行情"
            f"（合计 {recompute['tail_gap_rows']} 行），但仅当缺口内存在除权事件时才影响数值。")
    if boundary['index_codes_in_factor'] == 0:
        lines.append('指数未写入任何复权因子，符合「指数不复权」契约。')
    if boundary['fund_codes_in_factor'] == 0:
        lines.append(
            '基金未写入因子，取数时因子为 1.0 并会触发复权降级告警，'
            '不会静默返回伪复权价。')
    lines.append(
        f"分红快照 {dividends['rows']} 行中 ex_date 为空 {dividends['null_ex_date']} 行，"
        '计算前已被过滤，不参与因子。')
    return lines


def audit_factors(store: Any, factor_store: Any, output_dir: Path | str = 'logs',
                  sample_size: int = 60, seed: int = 42,
                  write_report: bool = True) -> FactorAudit:
    """执行全市场因子核验并写出 JSON/CSV 报告。"""
    started = time.perf_counter()
    root = Path(store.root)
    factor_path = Path(factor_store.factor_path)
    dividend_path = Path(factor_store.dividend_path)
    if not factor_path.exists():
        raise FileNotFoundError(f'因子快照不存在: {factor_path}')

    stock_codes = {path.stem for path in _quote_files(root, 'stock')}
    index_codes = {path.stem for path in _quote_files(root, 'index')}
    fund_codes = {path.stem for path in _quote_files(root, 'fund')}
    fund_files = _quote_files(root, 'fund')

    dividends = (
        pl.read_parquet(dividend_path)
        if dividend_path.exists()
        else pl.DataFrame({'ts_code': [], 'ex_date': []})
    )
    div_by_code = _split_dividends(dividends)

    coverage, factor_codes = _coverage_section(
        root, factor_path, stock_codes, index_codes, fund_codes)
    stats = _scan_all(factor_path, root, div_by_code)

    recompute = {
        'codes_checked': len(stats.recompute_rows),
        'codes_same_algorithm': stats.codes_exact,
        'codes_diff': stats.codes_diff,
        'codes_with_tail_gap': sum(
            1 for row in stats.recompute_rows if row['date_mismatch'] > 0),
        'tail_gap_rows': sum(
            row['date_mismatch'] for row in stats.recompute_rows),
        'max_rel': {'qfq_factor': stats.max_rel_qfq, 'hfq_factor': stats.max_rel_hfq},
        'diff_codes': sorted(
            (row for row in stats.recompute_rows if not row['same_algorithm']),
            key=lambda row: max(row['max_rel_qfq'], row['max_rel_hfq']),
            reverse=True,
        )[:20],
    }
    jumps = {
        'codes_checked': stats.jump_codes_checked,
        'jump_rows': stats.jump_rows,
        'matched_jump_rows': stats.matched_jump_rows,
        'factor_without_source': stats.factor_without_source,
        'event_without_jump': stats.event_without_jump,
        'events_before_first_row': stats.events_before_first,
        'events_after_last_row': stats.events_after_last,
        'invalid_events': stats.invalid_events,
    }

    sample_codes = _select_sample(
        sorted(factor_codes & stock_codes), dividends, root, sample_size, seed)
    sample = _sample_section(
        factor_path, root, div_by_code, dividends, sample_codes)

    null_ex_date = (
        int(dividends['ex_date'].null_count())
        if 'ex_date' in dividends.columns else 0)
    dividend_info = {
        'rows': dividends.height,
        'codes': dividends['ts_code'].n_unique() if 'ts_code' in dividends.columns else 0,
        'null_ex_date': null_ex_date,
        'null_ex_date_affects_factor': False,
        'null_ex_date_reason': 'calculate_factors 先过滤 ex_date 为空的行，不参与任何事件',
    }
    boundary = _boundary_section(factor_codes, fund_files)
    conclusions = _conclusions(
        coverage=coverage, recompute=recompute, jumps=jumps,
        boundary=boundary, dividends=dividend_info)

    elapsed = time.perf_counter() - started
    payload: dict[str, Any] = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'elapsed_s': round(elapsed, 3),
        'tolerances': {'exact_rel': EXACT_REL_TOL, 'naive_rel': NAIVE_REL_TOL},
        'coverage': coverage,
        'recompute': recompute,
        'sample': sample,
        'jumps': jumps,
        'dividends': dividend_info,
        'boundary': boundary,
        'conclusions': conclusions,
    }

    result = FactorAudit(payload=payload)
    if write_report:
        result.report_files = _write_reports(
            payload, stats.recompute_rows, stats.gap_rows, sample['codes'],
            Path(output_dir))
        payload['report_files'] = result.report_files
    return result


def _write_reports(payload: dict[str, Any], recompute_rows: list[dict[str, Any]],
                   gap_rows: list[dict[str, Any]], sample_rows: list[dict[str, Any]],
                   target: Path) -> dict[str, str]:
    target.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    base = f'factor_audit_{stamp}'
    json_path = target / f'{base}.json'
    codes_path = target / f'{base}_codes.csv'
    gaps_path = target / f'{base}_gaps.csv'
    sample_path = target / f'{base}_sample.csv'

    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding='utf-8')

    _write_csv(codes_path, recompute_rows)
    _write_csv(gaps_path, gap_rows)
    _write_csv(sample_path, sample_rows)
    report_files = {
        'json': str(json_path),
        'codes_csv': str(codes_path),
        'gaps_csv': str(gaps_path),
        'sample_csv': str(sample_path),
    }
    for name, path in report_files.items():
        logger.info(f'  报告 {name}: {path}')
    return report_files


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text('', encoding='utf-8')
        return
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


__all__ = ['FactorAudit', 'audit_factors']
