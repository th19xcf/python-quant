#!/usr/bin/env python3

"""相似态回溯检索（analog lookup）：给定指标读数，找回历史相近时点并统计后续走势。

定位
----
本工具回答一个问题：**当某指标出现某个读数时，历史上同一票（或同批票）出现过
相似读数之后，未来 N 个交易日的走势分布是什么样，和「什么都不看」的基线相比
是否可区分。**

不另起一套数据与指标实现，全部复用既有模块：

- 行情与复权：``src.quant.factor_lib.data.load_panel``（hfq 价 + 原始价 + ret + vwap）；
- 指标计算：``src.advice.factor_panel.compute_factor_panel``（与因子评估、回测同源）；
- 换手率等外部指标：直接读 ``data/local_store/factors/stock_turnover.parquet``；
- 流动性票池：``src.quant.factor_lib.data.pick_liquid_universe``；
- 前向收益口径：与 ``src.advice.factor_eval.add_forward_returns`` 一致（hfq 收盘到收盘）。

四个结论成立的前提，本工具逐一处理（报告里也会显式写出）：

1. **前视污染**：指标只用 t 日及以前的数据（rolling 窗口以 t 收尾），相似态检索
   的候选日期上限为查询日（``--query-date``）或面板末日；后续收益只用 t 之后的数据。
2. **样本重叠**：相邻交易日会同时命中，报告同时给「命中次数」和「去重叠独立样本数」
   （同一 ``ts_code`` 内命中间隔 < horizon 视为同一段，只保留首次）。
3. **基线对照**：同批票、同一时间段、同一 horizon 的**无条件**收益分布（另给剔除命中
   后的基线，避免命中集污染基线）。
4. **样本量与不确定性**：bootstrap 置信区间 + 与基线的 Mann-Whitney 检验；独立样本
   不足 ``--min-independent``（默认 30）时显式标注「样本不足，结论不可用」。

输出
----
``reports/analog_lookup_<YYYYMMDD>/`` 下：

- ``analog_lookup.md``      markdown 报告
- ``hits.csv``              命中明细（含是否计入独立样本）
- ``params.json``           本次运行参数与相似度定义（复现用）

用法示例
--------
单票、绝对带宽（20 日动量取 0.15 附近 ±0.02）::

    .venv\\Scripts\\python.exe tools/research/analog_lookup.py \\
        --codes 600519.SH --column ret_20 --mode abs --target 0.15 --band 0.02

单票、分位带宽（读该票自身近 252 日历史分位，取 90 分位 ±5 分位点）::

    .venv\\Scripts\\python.exe tools/research/analog_lookup.py \\
        --codes 600519.SH --column ret_20 --mode pct --target 90 --band 5

由「日期 + 票」自动取读数（当前读数场景）::

    .venv\\Scripts\\python.exe tools/research/analog_lookup.py \\
        --codes 600519.SH --column ret_20 --mode pct \\
        --query-code 600519.SH --query-date 2026-11-06

全市场换手率高分位态（外部 parquet 源）::

    .venv\\Scripts\\python.exe tools/research/analog_lookup.py \\
        --source parquet --panel-path data/local_store/factors/stock_turnover.parquet \\
        --value-col turn --universe liquid:300 --column turn --mode pct --target 90 --band 5
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from loguru import logger

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.quant.factor_lib.data import (  # noqa: E402
    PanelRequest,
    load_panel,
    pick_liquid_universe,
)

DATA_DIR = REPO_ROOT / 'data' / 'local_store'
DEFAULT_OUT_ROOT = REPO_ROOT / 'reports'

#: 指标来源枚举
SOURCES = ('panel', 'parquet')
#: 相似度定义枚举
MODES = ('abs', 'pct', 'z')
#: 分位/标准化窗口不足时，``rolling_rank`` 与 ``rolling_mean`` 返回 null，
#: 因此这两个模式天然不会被因子库的 0 填充占位污染；``abs`` 模式靠
#: ``min_history`` 兜底（因子库对窗口不足的列填 0）。
MODE_DOC = {
    'abs': '绝对值带宽：|value - target| <= band（band 与指标同量纲）',
    'pct': '历史分位带宽：pct = 该票近 lookback 个交易日中的分位(0~100)，'
           '|pct - target| <= band（band 单位为分位点）',
    'z': '标准化带宽：z = (value - 近 lookback 日均值) / 近 lookback 日标准差，'
         '|z - target| <= band',
}


# ----------------------------------------------------------------------
# 参数
# ----------------------------------------------------------------------
@dataclass
class LookupConfig:
    """一次检索的全部参数。"""

    source: str = 'panel'
    panel_path: Path | None = None
    value_col: str | None = None
    date_col: str = 'trade_date'
    code_col: str = 'ts_code'

    columns: list[str] = field(default_factory=list)
    codes: list[str] | None = None
    universe: str = 'explicit'

    start: str = '2015-01-01'
    end: str | None = None
    warmup_days: int = 260
    batch: int = 400

    mode: str = 'pct'
    target: float | None = None
    band: float = 5.0
    lookback: int = 252
    min_history: int = 120

    query_code: str | None = None
    query_date: str | None = None

    horizons: tuple[int, ...] = (5, 10, 20, 60)
    exclude_window: int = 20
    min_independent: int = 30
    bootstrap: int = 2000
    seed: int = 42
    max_csv_rows: int = 100_000

    out_dir: Path | None = None
    command_line: str = ''


# ----------------------------------------------------------------------
# 票池
# ----------------------------------------------------------------------
def _resolve_universe(cfg: LookupConfig) -> list[str]:
    """解析检索范围：显式列表 / 全市场 / 流动性前 N。"""
    if cfg.codes:
        return list(dict.fromkeys(cfg.codes))
    if cfg.universe == 'all':
        from src.data.local_store.store import get_local_quote_store
        from src.utils.config import get_config

        codes = get_local_quote_store(get_config()).codes('stock')
        if not codes:
            raise SystemExit('全市场票池为空：请检查 data/local_store 行情元数据')
        return codes
    if cfg.universe.startswith('liquid:'):
        size = int(cfg.universe.split(':', 1)[1])
        codes = pick_liquid_universe(size)
        if not codes:
            raise SystemExit('流动性票池为空：请检查本地列存')
        return codes
    raise SystemExit(f'未知票池: {cfg.universe!r}，可用 explicit/all/liquid:N')


def _load_indicator_panel(codes: list[str], columns: list[str],
                          cfg: LookupConfig) -> pl.DataFrame:
    """从行情计算指标面板（复用 load_panel + compute_factor_panel）。

    分批处理以控制内存：全市场 5,800 只 × 11 年若一次 concat 再算 79 列因子，
    常驻内存会到 10GB 量级。

    Returns:
        ``[ts_code, date, close, *columns]`` 的长表（close 为 hfq 价）。
    """
    from src.advice.factor_panel import compute_factor_panel

    start = date.fromisoformat(cfg.start)
    end = date.fromisoformat(cfg.end) if cfg.end else date.today()
    warmup_start = (start - timedelta(days=cfg.warmup_days)).isoformat()

    parts: list[pl.DataFrame] = []
    missing: dict[str, int] = {}
    for i in range(0, len(codes), cfg.batch):
        chunk = codes[i:i + cfg.batch]
        try:
            panel = load_panel(
                PanelRequest(start=warmup_start, end=end.isoformat(), use_cache=False,
                             universe_size=len(chunk)),
                codes=chunk,
            )
        except RuntimeError as exc:
            logger.warning(f'[analog] 批次 {i}~{i + len(chunk)} 无行情，已跳过: {exc}')
            continue
        # compute_factor_panel 的派生列需要原始成交额列（load_panel 的 amount 即原始额）
        panel = panel.with_columns(pl.col('amount').alias('raw_amount'))
        # extended=True 才不会把 score 列 fill_null(0)，缺失值保留为 null；
        # 0 填充会让「窗口不足」的占位值看起来像真实读数。
        factor = compute_factor_panel(panel, extended=True)
        absent = [name for name in columns if name not in factor.columns]
        for name in absent:
            missing[name] = missing.get(name, 0) + 1
        keep = ['ts_code', 'date', 'close', *[c for c in columns if c in factor.columns]]
        parts.append(factor.select(keep))
        logger.info(f'[analog] 指标面板批次 {i // cfg.batch + 1}: '
                    f'{len(chunk)} 只 / {parts[-1].height} 行')

    if missing:
        raise SystemExit(
            f'指标列不存在: {sorted(missing)}。'
            f'可用列见 src/advice/factor_panel.py 的 SCORE_COLUMNS / 扩展因子列')
    if not parts:
        raise SystemExit('指标面板为空：所选票池在目标区间均无行情')

    frame = pl.concat(parts, how='vertical_relaxed').sort(['ts_code', 'date'])
    # 保留 warmup 段：``bar_no`` 必须反映「已积累多少个交易日」，
    # 否则 min_history 守卫会把起点之后的头 120 个交易日误判为历史不足。
    return frame


def _load_close_panel(codes: list[str], cfg: LookupConfig) -> pl.DataFrame:
    """只读取 hfq 收盘价（外部 parquet 指标源做前向收益用）。"""
    from src.data.local_store.store import get_local_quote_store
    from src.data.managers.data_fetcher import DataFetcher
    from src.utils.config import get_config

    store = get_local_quote_store(get_config())
    start = date.fromisoformat(cfg.start)
    end = date.fromisoformat(cfg.end) if cfg.end else date.today()
    warmup_start = (start - timedelta(days=cfg.warmup_days)).isoformat()

    parts: list[pl.DataFrame] = []
    for code in codes:
        raw = store.read_file('stock', code, with_factors=True)
        if raw is None or raw.is_empty():
            continue
        adjusted = DataFetcher._apply_adjustment(raw, 'hfq')
        sliced = adjusted.filter(
            (pl.col('date') >= pl.lit(warmup_start).str.to_date())
            & (pl.col('date') <= pl.lit(end.isoformat()).str.to_date())
        ).select(['ts_code', 'date', pl.col('close').alias('close')])
        if not sliced.is_empty():
            parts.append(sliced)
    if not parts:
        raise SystemExit('收盘价面板为空：所选票池在目标区间均无行情')
    return pl.concat(parts, how='vertical_relaxed').sort(['ts_code', 'date'])


def _load_parquet_indicator(codes: list[str], cfg: LookupConfig,
                            column: str) -> pl.DataFrame:
    """从外部 parquet 读取已算好的指标（只返回 ``[ts_code, date, column]``）。"""
    path = Path(cfg.panel_path) if cfg.panel_path else None
    if path is None:
        raise SystemExit('--source parquet 需要 --panel-path')
    if not path.exists():
        raise SystemExit(f'指标面板不存在: {path}')
    if cfg.value_col:
        raw = pl.read_parquet(
            path, columns=[cfg.code_col, cfg.date_col, cfg.value_col])
        raw = raw.rename({cfg.date_col: 'date', cfg.value_col: column})
    else:
        if column not in pl.read_parquet_schema(path):
            raise SystemExit(f'{path} 中没有列 {column}')
        raw = pl.read_parquet(path, columns=[cfg.code_col, cfg.date_col, column])
        raw = raw.rename({cfg.date_col: 'date'})
    start = date.fromisoformat(cfg.start)
    end = date.fromisoformat(cfg.end) if cfg.end else date.today()
    return raw.filter(
        pl.col(cfg.code_col).is_in(codes)
        & (pl.col('date') >= start) & (pl.col('date') <= end)
    ).rename({cfg.code_col: 'ts_code'}).sort(['ts_code', 'date'])


# ----------------------------------------------------------------------
# 相似度与收益
# ----------------------------------------------------------------------
def _add_sequence(frame: pl.DataFrame) -> pl.DataFrame:
    """补交易日序号（去重叠按交易日间隔判断，不能按自然日）。"""
    return frame.sort(['ts_code', 'date']).with_columns(
        pl.int_range(pl.len()).over('ts_code').alias('bar_no'))


def _add_mode_metric(frame: pl.DataFrame, column: str, mode: str,
                     lookback: int) -> pl.DataFrame:
    """按所选相似度定义生成匹配空间的度量列 ``metric``。

    ``pct``/``z`` 的窗口一律以 t 收尾（含 t），因此只用当日及以前信息。
    """
    if mode == 'abs':
        return frame.with_columns(pl.col(column).alias('metric'))

    expr = pl.col(column).cast(pl.Float64)
    if mode == 'pct':
        rank = expr.rolling_rank(window_size=lookback, method='max',
                                 min_samples=lookback).over('ts_code')
        count = expr.is_not_null().cast(pl.Int64).rolling_sum(
            window_size=lookback, min_samples=lookback).over('ts_code')
        # 分位取 0~100：窗口内不高于当前值的占比
        metric = pl.when(count > 1).then((rank - 1) / (count - 1) * 100.0).otherwise(None)
        return frame.with_columns(metric.alias('metric'))

    mean = expr.rolling_mean(window_size=lookback, min_samples=lookback).over('ts_code')
    std = expr.rolling_std(window_size=lookback, min_samples=lookback).over('ts_code')
    metric = pl.when(std.is_not_null() & (std > 1e-12)).then(
        (expr - mean) / std).otherwise(None)
    return frame.with_columns(metric.alias('metric'))


def _add_forward_returns(frame: pl.DataFrame, horizons: tuple[int, ...]) -> pl.DataFrame:
    """前向收益：hfq 收盘到收盘，t 收盘买入、t+h 收盘卖出（与 factor_eval 同口径）。"""
    exprs = [
        (pl.col('close').shift(-h).over('ts_code') / pl.col('close') - 1.0)
        .alias(f'fwd_{h}')
        for h in horizons
    ]
    return frame.with_columns(exprs)


def _add_benchmark(frame: pl.DataFrame, horizons: tuple[int, ...]) -> pl.DataFrame:
    """等权基准：同一交易日全票池的横截面均值（同 horizon）。"""
    exprs = [
        (pl.col(f'fwd_{h}') - pl.col(f'fwd_{h}').mean().over('date')).alias(f'exc_{h}')
        for h in horizons
    ]
    return frame.with_columns(exprs)


def _dedupe_overlap(indices: np.ndarray, bars: np.ndarray,
                    codes: list[str], gap: int) -> np.ndarray:
    """同一标的命中间隔 < gap 个交易日视为同一段，只保留首次。

    Args:
        indices: 候选行下标（应已按 (ts_code, bar_no) 升序）
        bars: 与候选全表对齐的 ``bar_no`` 数组（按行下标取）
        codes: 与候选全表对齐的 ``ts_code`` 列表（按行下标取）
        gap: 重叠阈值（通常取 horizon）

    Returns:
        计入独立样本的行下标子集（保持升序）。
    """
    keep: list[int] = []
    last_by_code: dict[str, int] = {}
    for pos in indices:
        index = int(pos)
        code = codes[index]
        bar = int(bars[index])
        last = last_by_code.get(code)
        if last is not None and bar - last < gap:
            continue
        last_by_code[code] = bar
        keep.append(index)
    return np.asarray(keep, dtype=np.int64)


def _bootstrap_ci(values: np.ndarray, stat: str, iterations: int,
                  seed: int) -> tuple[float, float]:
    """bootstrap 95% 置信区间（百分位法）。"""
    if values.size == 0:
        return (float('nan'), float('nan'))
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, values.size, size=(iterations, values.size))
    samples = values[idx]
    point = samples.mean(axis=1) if stat == 'mean' else np.median(samples, axis=1)
    low, high = np.percentile(point, [2.5, 97.5])
    return (float(low), float(high))


def _win_rate(values: np.ndarray) -> float:
    return float(np.mean(values > 0)) if values.size else float('nan')


def _mannwhitney(hit: np.ndarray, base: np.ndarray) -> float:
    """命中集 vs 基线的 Mann-Whitney U 双侧 p 值（基线过大时抽样）。"""
    if hit.size < 2 or base.size < 2:  # noqa: PLR2004
        return float('nan')
    from scipy import stats

    sample = base
    if base.size > 50_000:
        rng = np.random.default_rng(7)
        sample = base[rng.integers(0, base.size, size=50_000)]
    try:
        return float(stats.mannwhitneyu(hit, sample, alternative='two-sided').pvalue)
    except ValueError:
        return float('nan')


# ----------------------------------------------------------------------
# 单指标检索
# ----------------------------------------------------------------------
@dataclass
class ColumnResult:
    """单个指标列的一次检索结果。"""

    column: str
    target: float
    target_source: str
    metric_kind: str
    rows_total: int
    rows_valid: int
    hits_raw: int
    per_horizon: list[dict[str, Any]]
    hit_table: pl.DataFrame
    coverage_codes: int
    hit_codes: int
    query_row: dict[str, Any] | None = None


def _pick_target(frame: pl.DataFrame, column: str, cfg: LookupConfig) -> tuple[float, str, dict | None]:
    """确定查询读数：显式 ``--target`` 优先，其次由 ``--query-code/--query-date`` 取。"""
    if cfg.query_code and cfg.query_date:
        qd = date.fromisoformat(cfg.query_date)
        row = frame.filter(
            (pl.col('ts_code') == cfg.query_code) & (pl.col('date') == qd))
        if row.is_empty():
            raise SystemExit(
                f'查询点无数据: {cfg.query_code} @ {cfg.query_date}（指标 {column}）')
        metric = row['metric'][0]
        if metric is None or not np.isfinite(float(metric)):
            raise SystemExit(
                f'查询点指标无效: {cfg.query_code} @ {cfg.query_date}（{column}，'
                f'可能历史不足 {cfg.lookback} 日或该列缺失）')
        detail = {
            'ts_code': cfg.query_code,
            'date': cfg.query_date,
            'value': float(row[column][0]) if row[column][0] is not None else None,
            'metric': float(metric),
        }
        return float(metric), 'query_point', detail
    if cfg.target is None:
        raise SystemExit(
            f'指标 {column} 未给出查询读数：请给 --target，或给 --query-code 与 --query-date')
    return float(cfg.target), 'explicit', None


def _analyze_column(frame: pl.DataFrame, column: str, cfg: LookupConfig) -> ColumnResult:
    """单列检索：匹配 → 去重叠 → 基线 → 统计。"""
    scored = _add_mode_metric(frame, column, cfg.mode, cfg.lookback)
    scored = _add_forward_returns(scored, cfg.horizons)
    scored = _add_benchmark(scored, cfg.horizons)

    # 有效性：分位/标准化模式由 rolling 的 min_samples 保证窗口填满；
    # abs 模式靠 min_history 剔除因子库对「窗口不足」的 0 填充占位。
    valid = (
        pl.col('metric').is_not_null()
        & pl.col(column).is_not_null()
        & (pl.col('bar_no') + 1 >= cfg.min_history)
        & (pl.col('close') > 0)
    )
    candidates = scored.filter(valid)
    if candidates.is_empty():
        raise SystemExit(f'指标 {column} 在目标区间没有有效读数（检查 min_history/lookback）')

    target, target_source, query_row = _pick_target(candidates, column, cfg)

    # 检索日期上限：给了查询日就只用查询日及以前（当前读数场景，杜绝拿未来当历史）
    search_end = date.fromisoformat(cfg.query_date) if cfg.query_code and cfg.query_date else None
    if search_end is not None:
        candidates = candidates.filter(pl.col('date') <= search_end)
        scored = scored.filter(pl.col('date') <= search_end)

    # 排除查询票自身在查询日附近的时点（否则「相似态」会命中自己）
    if cfg.query_code and cfg.query_date:
        qd = date.fromisoformat(cfg.query_date)
        guard = (
            (pl.col('ts_code') == cfg.query_code)
            & (pl.col('date') >= qd - timedelta(days=cfg.exclude_window * 2))
            & (pl.col('date') <= qd)
        )
        candidates = candidates.filter(~guard)

    hits = (
        candidates.with_columns((pl.col('metric') - target).abs().alias('distance'))
        .filter(pl.col('distance') <= cfg.band)
        .sort(['ts_code', 'bar_no'])
    )
    if hits.is_empty():
        raise SystemExit(
            f'指标 {column} 在 band={cfg.band:g}（mode={cfg.mode}，target={target:.4g}）'
            f'内没有命中；请放宽 --band 或检查 target 是否在样本取值范围内')

    table = hits.select([
        pl.lit(column).alias('indicator'),
        pl.lit(cfg.mode).alias('similarity_mode'),
        'ts_code', 'date',
        pl.col(column).alias('value'),
        pl.col('metric').alias('metric_value'),
        'distance',
        *[f'fwd_{h}' for h in cfg.horizons],
        *[f'exc_{h}' for h in cfg.horizons],
    ])

    hit_codes = hits['ts_code'].to_list()
    bars = hits['bar_no'].cast(pl.Int64).to_numpy()
    positions = np.arange(hits.height, dtype=np.int64)
    max_h = max(cfg.horizons)

    # 公开口径：用最长 horizon 去重叠（最保守）；每 horizon 再单独算一次
    fwd_max = hits[f'fwd_{max_h}'].cast(pl.Float64).to_numpy()
    indep_max = _dedupe_overlap(positions[np.isfinite(fwd_max)], bars, hit_codes, max_h)

    # 基线：同票池、同区间、同 horizon 的无条件分布；剔除命中行避免自污染
    hit_keys = hits.select(['ts_code', 'date']).unique()
    baseline_frame = scored.join(hit_keys, on=['ts_code', 'date'], how='anti')

    per_horizon: list[dict[str, Any]] = []
    for h in cfg.horizons:
        fwd = hits[f'fwd_{h}'].cast(pl.Float64).to_numpy()
        exc = hits[f'exc_{h}'].cast(pl.Float64).to_numpy()
        ok = np.isfinite(fwd)
        indep = _dedupe_overlap(positions[ok], bars, hit_codes, h)

        hit_indep = fwd[indep]
        base = baseline_frame[f'fwd_{h}'].cast(pl.Float64).to_numpy()
        base = base[np.isfinite(base)]
        base_all = scored[f'fwd_{h}'].cast(pl.Float64).to_numpy()
        base_all = base_all[np.isfinite(base_all)]

        mw_p = _mannwhitney(hit_indep, base)
        per_horizon.append({
            'horizon': h,
            'hits_valid': int(ok.sum()),
            'hits_independent': int(indep.size),
            'hits_independent_maxh': int(indep_max.size),
            'mean': float(np.mean(hit_indep)) if hit_indep.size else float('nan'),
            'median': float(np.median(hit_indep)) if hit_indep.size else float('nan'),
            'win_rate': _win_rate(hit_indep),
            'mean_ci': _bootstrap_ci(hit_indep, 'mean', cfg.bootstrap, cfg.seed),
            'median_ci': _bootstrap_ci(hit_indep, 'median', cfg.bootstrap, cfg.seed),
            'excess_mean': float(np.nanmean(exc[ok])) if ok.any() else float('nan'),
            'baseline_mean': float(np.mean(base)) if base.size else float('nan'),
            'baseline_median': float(np.median(base)) if base.size else float('nan'),
            'baseline_win_rate': _win_rate(base),
            'baseline_n': int(base.size),
            'baseline_all_mean': float(np.mean(base_all)) if base_all.size else float('nan'),
            'baseline_all_n': int(base_all.size),
            'mw_p': mw_p,
            'enough': bool(indep.size >= cfg.min_independent),
            'p_bonf': bool(np.isfinite(mw_p) and mw_p < 0.05 / max(len(cfg.horizons), 1)),
        })

    return ColumnResult(
        column=column,
        target=target,
        target_source=target_source,
        metric_kind=cfg.mode,
        rows_total=frame.height,
        rows_valid=candidates.height,
        hits_raw=hits.height,
        per_horizon=per_horizon,
        hit_table=table,
        coverage_codes=int(frame['ts_code'].n_unique()),
        hit_codes=int(hits['ts_code'].n_unique()),
        query_row=query_row,
    )


# ----------------------------------------------------------------------
# 报告
# ----------------------------------------------------------------------
def _fmt_pct(x: float, digits: int = 2) -> str:
    return 'n/a' if not np.isfinite(x) else f'{x * 100:.{digits}f}%'


def _fmt_num(x: float, digits: int = 4) -> str:
    return 'n/a' if not np.isfinite(x) else f'{x:.{digits}f}'


def _fmt_ci(ci: tuple[float, float]) -> str:
    return 'n/a' if not np.isfinite(ci[0]) else f'[{ci[0]:.4f}, {ci[1]:.4f}]'


def _verdict(result: ColumnResult, cfg: LookupConfig) -> str:
    """一句话结论：取「样本充足的最长 horizon」；最长 horizon 不足时显式说明。"""
    usable = [r for r in result.per_horizon if r['enough']]
    max_h = max(cfg.horizons)
    if not usable:
        detail = '、'.join(
            f'{r["horizon"]}日={r["hits_independent"]}' for r in result.per_horizon)
        return (f'全部 horizon 样本不足（独立样本 {detail}，阈值 {cfg.min_independent}），'
                '结论不可用')
    row = max(usable, key=lambda r: r['horizon'])
    p = row['mw_p']
    base = row['baseline_mean']
    diff = row['mean'] - base
    suffix = ''
    if row['horizon'] != max_h:
        suffix = (f'（{max_h} 日独立样本仅 '
                  f'{next(r["hits_independent"] for r in result.per_horizon if r["horizon"] == max_h)}'
                  f' < {cfg.min_independent}，故按 {row["horizon"]} 日口径判定）')
    if not np.isfinite(p):
        return f'{row["horizon"]} 日无法做基线检验（基线样本为空）{suffix}'
    head = f'{row["horizon"]} 日'
    if p < 0.05 and diff > 0:
        return (f'{head}后续收益显著高于基线（均值 {_fmt_pct(row["mean"])} vs '
                f'{_fmt_pct(base)}，MW p={p:.4f}），读数具备正向区分度{suffix}')
    if p < 0.05 and diff < 0:
        return (f'{head}后续收益显著低于基线（均值 {_fmt_pct(row["mean"])} vs '
                f'{_fmt_pct(base)}，MW p={p:.4f}），读数具备负向区分度{suffix}')
    return (f'{head}后续收益与基线不可区分（均值 {_fmt_pct(row["mean"])} vs '
            f'{_fmt_pct(base)}，MW p={p:.4f}），该读数不具备可区分的后续走势{suffix}')


def _render_report(results: list[ColumnResult], cfg: LookupConfig,
                   codes_count: int, out_dir: Path,
                   hits_total: int, hits_written: int) -> str:
    lines: list[str] = []
    add = lines.append

    add('# 相似态回溯检索报告')
    add('')
    add(f'- 生成时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    add(f'- 指标来源：`{cfg.source}`'
        + (f'（`{cfg.panel_path}`）' if cfg.panel_path else '（本地行情 + '
           '`src/advice/factor_panel.py::compute_factor_panel`）'))
    add(f'- 票池：`{cfg.universe}`，实际 {codes_count} 只')
    add(f'- 样本区间：{cfg.start} ~ {cfg.end or "最新"}（指标 warmup {cfg.warmup_days} 自然日）')
    add(f'- 相似度定义：**{cfg.mode}** —— {MODE_DOC[cfg.mode]}')
    add(f'- 前视窗口：{", ".join(str(h) for h in cfg.horizons)} 个交易日')
    add(f'- 输出目录：`{out_dir}`')
    add('')

    add('## 1. 结论摘要')
    add('')
    for res in results:
        add(f'- **{res.column}**（target={res.target:.4g}，来源：{res.target_source}）：'
            f'{_verdict(res, cfg)}')
    add('')

    add('## 2. 口径与前提（结论成立的条件）')
    add('')
    add('1. **前视污染**：指标只用 t 日及以前的数据——分位/标准化模式的滚动窗口以 t 收尾'
        f'（lookback={cfg.lookback} 个交易日，`min_samples` 要求窗口填满）；'
        '候选相似态日期上限为查询日（若给了 `--query-date`）或面板末日，'
        '不会把未来数据当作历史参照。')
    add('2. **样本重叠**：相邻交易日会同时命中。表中同时给「命中次数」与'
        f'「去重叠独立样本数」：同一 `ts_code` 内命中间隔 < horizon 视为同一段，只保留首次。'
        f'跨 horizon 的公开口径用最长 horizon（{max(cfg.horizons)} 日）去重叠，'
        '因此最保守。')
    add('3. **基线对照**：基线 = 同一批票、同一时间段、同一 horizon 的**无条件**收益分布'
        '（横截面等权，不做任何筛选）。检验用 Mann-Whitney U 双侧。')
    add('4. **样本量与不确定性**：均值/中位数给 bootstrap 95% 百分位置信区间'
        f'（{cfg.bootstrap} 次重采样，seed={cfg.seed}）；独立样本 < {cfg.min_independent} '
        '时显式标注「样本不足，结论不可用」。')
    add('5. **收益定义**：hfq 收盘到收盘（`close[t+h] / close[t] - 1`，含分红再投资），'
        '`excess` = 该收益 − 同日全票池等权均值（同 horizon）。未扣交易成本。')
    if cfg.universe.startswith('liquid:'):
        add(f'6. **票池偏差（重要）**：`liquid:{cfg.universe.split(":", 1)[1]}` 按**当前**'
            '流动性选票，属于事后选样，存在幸存者偏差与选择偏差。命中集与基线取自同一票池，'
            '「是否可区分」的对照关系不受影响；但**收益率水平不可外推到全市场**，'
            '要外推请改用 `--universe all` 或显式给出历史成分股。')
    add('')

    for index, res in enumerate(results, start=1):
        add(f'## 3.{index} 指标 `{res.column}`')
        add('')
        add(f'- 查询读数：target = **{res.target:.6g}**（{res.target_source}，'
            f'空间 = {cfg.mode}，band = ±{cfg.band:g}）')
        if res.query_row:
            add(f'- 查询点：`{res.query_row["ts_code"]}` @ {res.query_row["date"]}，'
                f'原值 {res.query_row["value"]}，度量值 {res.query_row["metric"]:.4f}')
        add(f'- 有效读数 {res.rows_valid:,} 行 / 覆盖 {res.coverage_codes} 只；'
            f'命中 {res.hits_raw:,} 行、涉及 {res.hit_codes} 只')
        add('')
        add('| horizon | 命中(有效) | 独立样本 | 独立样本(最长h口径) | 均值 | 中位数 | 胜率 | '
            '超额均值 | 基线均值 | 基线中位数 | 基线胜率 | 均值 CI95 | MW p | 判定 |')
        add('|---|---|---|---|---|---|---|---|---|---|---|---|---|---|')
        for row in res.per_horizon:
            flag = '可用' if row['enough'] else '**样本不足**'
            add(
                f'| {row["horizon"]} | {row["hits_valid"]} | {row["hits_independent"]} | '
                f'{row["hits_independent_maxh"]} | {_fmt_pct(row["mean"])} | '
                f'{_fmt_pct(row["median"])} | {_fmt_pct(row["win_rate"], 1)} | '
                f'{_fmt_pct(row["excess_mean"])} | '
                f'{_fmt_pct(row["baseline_mean"])} | {_fmt_pct(row["baseline_median"])} | '
                f'{_fmt_pct(row["baseline_win_rate"], 1)} | {_fmt_ci(row["mean_ci"])} | '
                f'{_fmt_num(row["mw_p"])} | {flag} |')
        add('')
        add('- 基线样本量（剔除命中行后）：'
            + '、'.join(f'{r["horizon"]}日 {r["baseline_n"]:,}' for r in res.per_horizon))
        add(f'- 「独立样本(最长h口径)」= 用最长 horizon（{max(cfg.horizons)} 日）去重叠后的'
            f'独立样本数，是该指标在全表中最保守的口径，不随 horizon 变化。')
        add('')
        add('命中时点抽样（按时间等距抽取，全量见 hits.csv）：')
        add('')
        ordered = res.hit_table.sort('date')
        step = max(1, ordered.height // 20)
        preview = (
            ordered.gather_every(step).head(20)
            .with_columns([
                pl.col('date').dt.strftime('%Y-%m-%d'),
                pl.col('value').round(4),
                pl.col('metric_value').round(2),
                pl.col('distance').round(2),
            ])
            .select(['ts_code', 'date', 'value', 'metric_value', 'distance'])
        )
        add(preview.to_pandas().to_markdown(index=False))
        add('')

    add('## 4. 多重比较与调参声明')
    add('')
    alpha_bonf = 0.05 / max(len(cfg.horizons), 1)
    add(f'- 同一指标对 {len(cfg.horizons)} 个 horizon 各做一次 Mann-Whitney 检验，'
        f'Bonferroni 校正阈值 α = 0.05 / {len(cfg.horizons)} = **{alpha_bonf:.4f}**；'
        '未通过校正的 p 值按不显著处理（下表给出结论，避免「挑一个显著的说」）。')
    add('- 本次运行涉及的全部指标、相似度定义与带宽均在下方参数中列明，报告给出**全部**'
        '组合结果，未做「换带宽/换指标直到显著」的择优报告。若读者只关心某一个组合，'
        '请以本表为全集自行判断，不要把其中最好的一组当作唯一结论。')
    add('')
    add('| 指标 | horizon | 独立样本 | MW p | 通过 Bonferroni |')
    add('|---|---|---|---|---|')
    for res in results:
        for row in res.per_horizon:
            add(f'| {res.column} | {row["horizon"]} | {row["hits_independent"]} | '
                f'{_fmt_num(row["mw_p"])} | {"是" if row["p_bonf"] else "否"} |')
    add('')
    add('本次运行的完整参数组合：')
    add('')
    add('| 指标 | 相似度 | target | band | lookback |')
    add('|---|---|---|---|---|')
    for res in results:
        add(f'| {res.column} | {cfg.mode} | {res.target:.6g} | ±{cfg.band:g} | {cfg.lookback} |')
    add('')

    add('## 5. 复现')
    add('')
    add('```powershell')
    add(cfg.command_line)
    add('```')
    add('')
    add('命中明细见 `hits.csv`，运行参数见 `params.json`。')
    add('')
    if hits_written < hits_total:
        add(f'> `hits.csv` 为**等距抽样**：全量命中 {hits_total:,} 行，'
            f'CSV 仅写入 {hits_written:,} 行（`--max-csv-rows`）。'
            '报告中所有统计量均基于**全量**命中集计算，未使用抽样后的子集。')
        add('')
    return '\n'.join(lines)


# ----------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------
def _parse_args(argv: list[str] | None = None) -> LookupConfig:
    p = argparse.ArgumentParser(
        description='相似态回溯检索：给定指标读数，统计历史相近时点之后的走势分布',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument('--source', choices=SOURCES, default='panel',
                   help='指标来源：panel=从行情现算；parquet=读已算好的指标表')
    p.add_argument('--panel-path', type=Path, default=None, help='parquet 指标表路径')
    p.add_argument('--value-col', default=None,
                   help='parquet 指标表的取值列（不填则按 --column 名直接取）')
    p.add_argument('--date-col', default='trade_date', help='parquet 指标表的日期列')
    p.add_argument('--code-col', default='ts_code', help='parquet 指标表的代码列')
    p.add_argument('--column', action='append', default=None,
                   help='指标列名，可重复；panel 源取 SCORE_COLUMNS 等已算列')
    p.add_argument('--codes', action='append', default=None,
                   help='ts_code 列表，可重复或逗号分隔')
    p.add_argument('--universe', default='explicit',
                   help='票池：explicit（默认，用 --codes）/ all / liquid:N')
    p.add_argument('--start', default='2015-01-01')
    p.add_argument('--end', default=None)
    p.add_argument('--warmup-days', type=int, default=260)
    p.add_argument('--batch', type=int, default=400)
    p.add_argument('--mode', choices=MODES, default='pct', help='相似度定义')
    p.add_argument('--target', type=float, default=None,
                   help='目标读数（abs=原值，pct=0~100 分位，z=z 分数）')
    p.add_argument('--band', type=float, default=None,
                   help='带宽（abs 同量纲；pct 分位点；z 标准差倍数）')
    p.add_argument('--lookback', type=int, default=252,
                   help='pct/z 模式的滚动窗口（交易日）')
    p.add_argument('--min-history', type=int, default=120,
                   help='abs 模式的最小历史长度，用于剔除 0 填充占位')
    p.add_argument('--query-code', default=None, help='查询票，配合 --query-date 自动取读数')
    p.add_argument('--query-date', default=None, help='查询日（同时作为检索日期上限）')
    p.add_argument('--horizons', default='5,10,20,60',
                   help='前视窗口，逗号分隔的交易日')
    p.add_argument('--exclude-window', type=int, default=20,
                   help='排除查询票自身在查询日附近的交易日数')
    p.add_argument('--min-independent', type=int, default=30,
                   help='独立样本低于该值标注「样本不足」')
    p.add_argument('--bootstrap', type=int, default=2000)
    p.add_argument('--seed', type=int, default=42)
    p.add_argument('--max-csv-rows', type=int, default=100_000,
                   help='hits.csv 最大行数，超出按时间等距抽样（统计仍用全量）')
    p.add_argument('--out-dir', type=Path, default=None)
    args = p.parse_args(argv)

    columns: list[str] = []
    for raw in args.column or []:
        columns.extend(x.strip() for x in raw.split(',') if x.strip())
    if not columns:
        raise SystemExit('必须用 --column 指定至少一个指标列')

    codes: list[str] = []
    for raw in args.codes or []:
        codes.extend(x.strip() for x in raw.split(',') if x.strip())

    horizons = tuple(int(x) for x in args.horizons.split(',') if x.strip())
    if not horizons or min(horizons) <= 0:
        raise SystemExit('--horizons 必须是正整数，逗号分隔')

    band = args.band
    if band is None:
        band = {'abs': None, 'pct': 5.0, 'z': 1.0}[args.mode]
        if band is None:
            raise SystemExit('--mode abs 必须显式给 --band（与指标同量纲）')

    cfg = LookupConfig(
        source=args.source, panel_path=args.panel_path, value_col=args.value_col,
        date_col=args.date_col, code_col=args.code_col,
        columns=columns, codes=codes or None,
        universe='explicit' if codes else args.universe,
        start=args.start, end=args.end, warmup_days=args.warmup_days, batch=args.batch,
        mode=args.mode, target=args.target, band=float(band),
        lookback=args.lookback, min_history=args.min_history,
        query_code=args.query_code, query_date=args.query_date,
        horizons=horizons, exclude_window=args.exclude_window,
        min_independent=args.min_independent, bootstrap=args.bootstrap, seed=args.seed,
        max_csv_rows=args.max_csv_rows, out_dir=args.out_dir,
    )
    cfg.command_line = ' '.join(
        ['.venv\\Scripts\\python.exe', 'tools/research/analog_lookup.py',
         *sys.argv[1:]])
    return cfg


def main(argv: list[str] | None = None) -> int:
    logger.remove()
    logger.add(sys.stderr, level='WARNING')
    cfg = _parse_args(argv)
    if cfg.out_dir is None:
        cfg.out_dir = DEFAULT_OUT_ROOT / f'analog_lookup_{date.today():%Y%m%d}'
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    codes = _resolve_universe(cfg)
    print(f'[analog] 票池 {len(codes)} 只，指标 {cfg.columns}，'
          f'相似度 {cfg.mode}，band={cfg.band}', flush=True)

    if cfg.source == 'panel':
        frame = _load_indicator_panel(codes, cfg.columns, cfg)
    else:
        # 外部 parquet 指标 + hfq 收盘价（前向收益）
        frame = _load_close_panel(codes, cfg)
        for column in cfg.columns:
            indicator = _load_parquet_indicator(codes, cfg, column)
            frame = frame.join(indicator, on=['ts_code', 'date'], how='left')

    frame = _add_sequence(frame)
    # 指标 warmup 段只服务于 rolling 窗口与 bar_no，统计窗口从 --start 起算
    frame = frame.filter(pl.col('date') >= date.fromisoformat(cfg.start))
    print(f'[analog] 指标面板 {frame.height:,} 行 / {frame["ts_code"].n_unique()} 只 / '
          f'{str(frame["date"].min())} ~ {str(frame["date"].max())}', flush=True)

    results: list[ColumnResult] = []
    for column in cfg.columns:
        print(f'[analog] 检索 {column} ...', flush=True)
        results.append(_analyze_column(frame, column, cfg))

    hits = pl.concat([r.hit_table for r in results], how='vertical_relaxed')
    hits_total = hits.height
    to_write = hits.sort(['indicator', 'ts_code', 'date'])
    if hits_total > cfg.max_csv_rows:
        step = max(1, hits_total // cfg.max_csv_rows + 1)
        to_write = to_write.gather_every(step)
    hits_path = cfg.out_dir / 'hits.csv'
    to_write.write_csv(hits_path)
    hits_written = to_write.height

    report = _render_report(results, cfg, frame['ts_code'].n_unique(), cfg.out_dir,
                            hits_total, hits_written)
    report_path = cfg.out_dir / 'analog_lookup.md'
    report_path.write_text(report, encoding='utf-8')

    params = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'source': cfg.source,
        'panel_path': str(cfg.panel_path) if cfg.panel_path else None,
        'value_col': cfg.value_col,
        'columns': cfg.columns,
        'universe': cfg.universe,
        'codes_count': frame['ts_code'].n_unique(),
        'start': cfg.start,
        'end': cfg.end or str(frame['date'].max()),
        'warmup_days': cfg.warmup_days,
        'similarity': {'mode': cfg.mode, 'band': cfg.band, 'lookback': cfg.lookback,
                       'definition': MODE_DOC[cfg.mode]},
        'targets': {r.column: {'target': r.target, 'source': r.target_source}
                    for r in results},
        'horizons': list(cfg.horizons),
        'query': {'code': cfg.query_code, 'date': cfg.query_date},
        'min_history': cfg.min_history,
        'min_independent': cfg.min_independent,
        'bootstrap': cfg.bootstrap,
        'seed': cfg.seed,
        'hits_total': hits_total,
        'hits_csv_rows': hits_written,
        'command': cfg.command_line,
        'outputs': {'report': str(report_path), 'hits': str(hits_path)},
    }
    (cfg.out_dir / 'params.json').write_text(
        json.dumps(params, ensure_ascii=False, indent=2), encoding='utf-8')

    # 控制台摘要
    for res in results:
        print(f'\n=== {res.column} (target={res.target:.6g}, {res.target_source}) ===')
        print(f'命中 {res.hits_raw} 行 / {res.hit_codes} 只')
        for row in res.per_horizon:
            flag = '' if row['enough'] else '  [样本不足]'
            print(f'  h={row["horizon"]:>3}: 独立样本={row["hits_independent"]:>4} '
                  f'均值={_fmt_pct(row["mean"]):>8} 中位数={_fmt_pct(row["median"]):>8} '
                  f'胜率={_fmt_pct(row["win_rate"], 1):>6} '
                  f'基线={_fmt_pct(row["baseline_mean"]):>8} '
                  f'MW p={_fmt_num(row["mw_p"], 4)}{flag}')
        print(f'  结论: {_verdict(res, cfg)}')
    print(f'\n[analog] 报告 {report_path}')
    print(f'[analog] 明细 {hits_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
