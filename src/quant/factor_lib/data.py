#!/usr/bin/env python3

"""
因子库取数层 —— 从本地列存构建横截面面板，并做量纲自检。

取数路径说明（重要）：
    本地列存 ``data/local_store/quotes/stock/{sh,sz,bj}/{ts_code}.parquet``
    是唯一可信来源。它绕开了 ``DataFetcher.get_stock_batch`` 的元数据路由：
    ``meta/daily_index.parquet`` 当前只登记了 fund，stock 元数据被覆盖丢失，
    走 ``get_stock_batch`` 会因覆盖度不足回退 TDX，而 TDX 路径不存在 → 空表。
    因此这里直接 ``LocalQuoteStore.read_file`` + ``DataFetcher._apply_adjustment``，
    复权仍走项目唯一出口，不自己乘因子。

面板口径：
    - 价格列（open/high/low/close）为后复权价（hfq），收益与因子在复权空间计算；
    - 额外保留 raw_open / raw_close / raw_high / raw_low（原始价），
      停牌与涨跌停判定必须以原始价进行；
    - volume 单位 = 手（100 股），amount 单位 = 元，
      vwap = amount / (volume * 100) 为当日成交均价（元/股，原始价空间），
      再乘 hfq_factor 转到复权空间；
    - ret = close / delay(close, 1) - 1（后复权收益，已含分红再投资效应）。
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import cast

import polars as pl
from loguru import logger

from src.data.local_store.store import LocalQuoteStore, get_local_quote_store
from src.data.managers.data_fetcher import DataFetcher
from src.data.quality.code_migration import load_entity_index
from src.utils.config import get_config

#: 面板必需列
PRICE_COLS = ['open', 'high', 'low', 'close']
BASE_COLS = ['date', 'ts_code', *PRICE_COLS, 'pre_close', 'volume', 'amount']
PANEL_COLS = [*BASE_COLS, 'hfq_factor', 'raw_open', 'raw_close', 'raw_high', 'raw_low']


@dataclass(frozen=True, slots=True)
class PanelRequest:
    """面板请求参数。"""

    start: str = '2021-01-01'
    end: str = '2026-10-31'
    universe_size: int = 80
    liquidity_lookback: int = 250
    use_cache: bool = True


def _cache_dir() -> Path:
    """缓存目录（放在 tmp/ 下，不进版本库）。"""
    path = Path(__file__).resolve().parents[3] / 'tmp' / 'factor_lib'
    path.mkdir(parents=True, exist_ok=True)
    return path


def _store() -> LocalQuoteStore:
    return get_local_quote_store(get_config())


def pick_liquid_universe(
    size: int = 80,
    lookback: int = 250,
    store: LocalQuoteStore | None = None,
    use_cache: bool = True,
) -> list[str]:
    """按近期成交额中位数挑选流动性最好的 N 只标的（默认剔除北交所）。

    口径：取每个标的最末 ``lookback`` 个交易日的 ``amount`` 中位数（元），
    降序取前 N。要求区间内至少有 ``lookback`` 行，避免新股/长期停牌标的
    因为一天巨额成交混进样本。结果缓存到 JSON，避免每次重扫 5,600 个文件。
    """
    store = store or _store()
    cache_path = _cache_dir() / f'universe_liquid_{size}_{lookback}.json'
    if use_cache and cache_path.exists():
        payload = json.loads(cache_path.read_text(encoding='utf-8'))
        return list(payload['codes'])

    started = time.perf_counter()
    rows: list[tuple[str, float, int]] = []
    for code in store.codes('stock'):
        if code.endswith('.BJ'):
            continue
        frame = store.read_file('stock', code)
        if frame is None or frame.height < lookback:
            continue
        tail = frame.tail(lookback)
        median_amount = float(cast(float, tail['amount'].median() or 0.0))
        if median_amount <= 0:
            continue
        rows.append((code, median_amount, frame.height))

    rows.sort(key=lambda item: item[1], reverse=True)
    codes = [code for code, _amt, _rows in rows[:size]]
    payload = {
        'codes': codes,
        'metrics': {code: {'median_amount': amt, 'rows': n} for code, amt, n in rows[:size]},
        'generated_at': date.today().isoformat(),
        'elapsed_sec': round(time.perf_counter() - started, 2),
    }
    cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info(f'流动性筛选完成：{len(codes)} 只，扫描 {len(rows)} 只，'
                f'耗时 {payload["elapsed_sec"]}s')
    return codes


def _adjust(frame: pl.DataFrame) -> pl.DataFrame:
    """后复权 + 补齐原始价列（停牌/涨跌停判定专用）。"""
    adjusted = DataFetcher._apply_adjustment(frame, 'hfq')
    factor = adjusted['hfq_factor']
    return adjusted.with_columns([
        (pl.col('high') / factor).alias('raw_high'),
        (pl.col('low') / factor).alias('raw_low'),
    ])


def load_panel(request: PanelRequest | None = None, codes: list[str] | None = None) -> pl.DataFrame:
    """构建横截面面板（后复权价 + 原始价 + 收益 + VWAP）。

    Args:
        request: 面板请求参数
        codes: 显式指定标的列表（为空则按流动性自动挑选）

    Returns:
        pl.DataFrame: 按 (ts_code, date) 升序，含 PANEL_COLS/ret/vwap 等列
    """
    request = request or PanelRequest()
    store = _store()
    codes = codes or pick_liquid_universe(
        request.universe_size, request.liquidity_lookback,
        store=store, use_cache=request.use_cache,
    )
    # 同一实体的旧码已被新码完整覆盖时只保留新码，避免横截面重复计样本
    codes = load_entity_index(store.root).dedupe(codes)

    factor_store = store.factor_snapshot_store()
    factor_version = '0'
    if factor_store.factor_path.exists():
        factor_version = str(factor_store.factor_path.stat().st_mtime_ns)
    cache_path = _cache_dir() / (
        f'panel_{len(codes)}_{request.start}_{request.end}_{factor_version}.parquet'
    )
    if request.use_cache and cache_path.exists():
        return _with_derived(pl.read_parquet(cache_path))

    frames: list[pl.DataFrame] = []
    missing: list[str] = []
    for code in codes:
        frame = store.read_file('stock', code, with_factors=True)
        if frame is None or frame.is_empty():
            missing.append(code)
            continue
        sliced = frame.filter(
            (pl.col('date') >= pl.lit(request.start).str.to_date())
            & (pl.col('date') <= pl.lit(request.end).str.to_date())
        )
        if sliced.is_empty():
            missing.append(code)
            continue
        frames.append(_adjust(sliced))

    if not frames:
        raise RuntimeError('面板为空：所选标的在目标区间均无行情，请检查本地列存')

    panel = pl.concat(frames, how='vertical_relaxed').sort(['ts_code', 'date'])
    panel = panel.with_columns([
        (pl.col('close') / pl.col('close').shift(1).over('ts_code') - 1.0).alias('ret'),
        (
            pl.col('amount') / (pl.col('volume') * 100.0) * pl.col('hfq_factor')
        ).alias('vwap'),
        (pl.col('volume') > 0).alias('tradable_bar'),
    ])
    if missing:
        logger.warning(f'{len(missing)} 只标的无行情被剔除：{missing[:5]}...')

    panel = _with_derived(panel)
    if request.use_cache:
        panel.write_parquet(cache_path)
    return panel


def _with_derived(panel: pl.DataFrame) -> pl.DataFrame:
    """补齐派生列：内部行号 ``seq`` 与「次日可否成交」标记。

    - ``seq``：Alpha158 的 BETA/RSQR/RESI 回归自变量，必须是真实列——
      polars 的 ``rolling_cov`` 不接受带 ``.over()`` 的入参；
    - ``next_tradable``：T+1 开盘建仓时点是否可成交。判定口径：
      次日成交量 > 0（非停牌），且次日开盘相对当日原始收盘的涨跌幅
      未触及涨跌停（主板 10%、创业板/科创板 20%）；北交所已在股票池中剔除。
      无 ST 标记，ST 股的 5% 限制未单独处理，属于近似口径。
    """
    panel = panel.sort(['ts_code', 'date'])
    derived = panel.with_columns(
        pl.int_range(0, pl.len()).over('ts_code').cast(pl.Float64).alias('seq'),
        pl.col('open').shift(-1).over('ts_code').alias('next_open'),
        pl.col('raw_open').shift(-1).over('ts_code').alias('next_raw_open'),
        pl.col('volume').shift(-1).over('ts_code').alias('next_volume'),
    )
    limit_rate = (
        pl.when(
            pl.col('ts_code').str.starts_with('688')
            | pl.col('ts_code').str.starts_with('300')
            | pl.col('ts_code').str.starts_with('301')
        )
        .then(0.20)
        .otherwise(0.10)
    )
    return derived.with_columns(limit_rate.alias('limit_rate')).with_columns(
        (
            (pl.col('next_volume') > 0)
            & (
                (pl.col('next_raw_open') / pl.col('raw_close') - 1.0).abs()
                <= pl.col('limit_rate') - 0.004
            )
        )
        .fill_null(False)
        .alias('next_tradable')
    )


def dimension_check(panel: pl.DataFrame) -> dict[str, object]:
    """量纲自检：``amount / (原始收盘价 × volume × 100)`` 应约等于 1。

    - volume 单位是「手」（1 手 = 100 股），amount 单位是「元」，
      两者相乘再乘原始价即当日名义成交额，与 amount 的比值应≈1；
    - 比值显著偏离 1 说明某个字段的单位口径被改动过（例如 volume 被
      提前换成股数、amount 被换成万元），此时所有依赖成交额/换手的
      因子全部失真，必须先修数据再接因子。

    Returns:
        dict: 样本数、分位数、落在 [0.9, 1.1] 的占比、是否通过
    """
    ratio = (
        panel.filter(pl.col('volume') > 1, pl.col('raw_close') > 0)
        .select(
            (pl.col('amount') / (pl.col('raw_close') * pl.col('volume') * 100.0)).alias('r')
        )['r'].drop_nulls()
    )
    if ratio.len() == 0:
        return {'passed': False, 'samples': 0, 'reason': '无有效样本'}
    quantiles = ratio.quantile(0.01), ratio.quantile(0.5), ratio.quantile(0.99)
    within = float(cast(float, ratio.is_between(0.9, 1.1).mean() or 0.0))
    p01 = float(cast(float, quantiles[0] or 0.0))
    median = float(cast(float, quantiles[1] or 0.0))
    p99 = float(cast(float, quantiles[2] or 0.0))
    result: dict[str, object] = {
        'samples': ratio.len(),
        'p01': round(p01, 4),
        'median': round(median, 4),
        'p99': round(p99, 4),
        'share_within_10pct': round(within, 4),
        'passed': bool(0.9 <= median <= 1.1 and within >= 0.9),
    }
    return result


def panel_summary(panel: pl.DataFrame) -> dict[str, object]:
    """面板概况（写入报告，便于复核股票池与区间）。"""
    return {
        'rows': panel.height,
        'codes': panel['ts_code'].n_unique(),
        'start': str(panel['date'].min()),
        'end': str(panel['date'].max()),
        'trading_days': panel['date'].n_unique(),
        'suspended_rows': int((panel['volume'] <= 0).sum()),
    }


__all__ = [
    'BASE_COLS',
    'PANEL_COLS',
    'PRICE_COLS',
    'PanelRequest',
    'dimension_check',
    'load_panel',
    'panel_summary',
    'pick_liquid_universe',
]
