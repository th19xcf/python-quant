#!/usr/bin/env python3

"""学习实验的本地数据加载。

读路径只使用本地列存（Parquet），不发起远程数据库请求；行情单位保持原样：
``volume`` 为「手」、``amount`` 为「元」。因子计算统一使用后复权 OHLC，
涨跌停判定仍使用原始价格。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Dict, List

import polars as pl
from loguru import logger

from src.alpha.calculator.alpha_calculator import calculate_factors_cross_sectional
from src.data.local_store.store import LocalQuoteStore
from src.data.quality.code_migration import load_entity_index
from src.quant.factor_lib.data import LIQUIDITY_LOOKBACK_DAYS
from src.quant.learning.config import ADV_COL, TOTAL_MV_YI_COL
from src.risk.execution_rules import ExecutionRules

PRICE_COLUMNS = ('open', 'high', 'low', 'close')
FACTOR_TYPES = ('price_momentum', 'volatility', 'volume', 'technical', 'risk')

#: 总市值的唯一正式来源（见 docs/数据权威路径清单.md「估值」）。
VALUATION_PATH = ('meta', 'financials', 'valuation_daily.parquet')

# 因子列：覆盖动量、波动、量能、技术形态、风险五类，保持报告可读性
DEFAULT_FACTOR_COLS: tuple[str, ...] = (
    'momentum_1m', 'momentum_3m', 'momentum_6m', 'momentum_12m',
    'volatility_20', 'volatility_60', 'rsi_14', 'rsi_21',
    'volume_momentum_20', 'bollinger_position', 'sharpe_ratio_20',
    'max_drawdown_20',
)


def _code_from_path(path: Path) -> str:
    """从文件名解析 ts_code，例如 ``600000.SH.parquet`` → ``600000.SH``。"""
    return path.name.split('.parquet')[0]


def _eligible_code(code: str) -> bool:
    """排除指数与板块代码，只保留可交易标的。"""
    numeric = code.split('.')[0]
    if not numeric.isdigit() or len(numeric) != 6:
        return False
    return numeric[:2] not in {'88', '89'} and not numeric.startswith('999')


def select_universe(root: Path, limit: int, asset: str = 'stock') -> List[str]:
    """按文件大小分层抽样选出一个初选股票池。

    文件大小与历史长度/数据完整度正相关，可低成本避开刚上市、只有几百行
    的新股；真正的流动性过滤在 ``load_panel`` 里用近期成交额完成。
    """
    store = root / 'data' / 'local_store' / 'quotes' / asset
    entity_index = load_entity_index(store.parents[1]) if asset == 'stock' else None
    files = sorted(store.glob('*/*.parquet'))
    candidates = [path for path in files
                  if _eligible_code(_code_from_path(path))
                  and (entity_index is None
                       or not entity_index.is_superseded(_code_from_path(path)))
                  and path.stat().st_size >= 50_000]
    if not candidates:
        return []
    candidates.sort(key=lambda path: path.stat().st_size, reverse=True)
    if limit >= len(candidates):
        return [_code_from_path(path) for path in candidates]
    step = len(candidates) / float(limit)
    picked = [candidates[int(index * step)] for index in range(limit)]
    return [_code_from_path(path) for path in picked]


def load_panel(root: Path, start: date, end: date,
               codes: Sequence[str] | None = None,
               limit: int = 600, min_bars: int = 500,
               liquidity_fraction: float = 0.7,
               asset: str = 'stock') -> pl.DataFrame:
    """从本地列存加载并拼装多标的面板。

    Args:
        root: 项目根目录（包含 ``data/local_store``）。
        start/end: 样本区间（含首尾）。
        codes: 指定标的；为空时按文件大小抽样 ``limit`` 只。
        limit: 抽样标的数。
        min_bars: 单标的在区间内最少交易日数。
        liquidity_fraction: 按区间末 250 日平均成交额保留的比例。
        asset: 资产类型（stock / fund），对应 ``quotes/<asset>`` 目录。

    Returns:
        Polars DataFrame，含原始 OHLC、后复权 OHLC、成交量/额与复权因子。
    """
    if codes is None:
        codes = select_universe(root, limit, asset=asset)
    if not codes:
        raise ValueError('未找到可用标的，请检查本地列存是否已构建')
    file_map = {_code_from_path(path): path
                for path in (root / 'data' / 'local_store' / 'quotes' / asset).glob('*/*.parquet')}
    missing = [code for code in codes if code not in file_map]
    if missing:
        logger.warning(f'{len(missing)} 只标的不在本地列存中，已跳过: {missing[:5]}')

    quote_store = LocalQuoteStore(root / 'data' / 'local_store')
    frames: List[pl.DataFrame] = []
    skipped = 0
    for code in codes:
        path = file_map.get(code)
        if path is None:
            continue
        try:
            frame = quote_store.read_file(asset, code, with_factors=True)
            if frame is None:
                raise FileNotFoundError(path)
        except Exception as exc:  # noqa: BLE001 - 单个坏文件不应中断整批加载
            logger.warning(f'读取 {code} 失败，已跳过: {exc}')
            skipped += 1
            continue
        if 'hfq_factor' not in frame.columns:
            logger.warning(f'{code} 缺少 hfq_factor 列，已跳过')
            skipped += 1
            continue
        frame = frame.filter(
            (pl.col('date') >= start) & (pl.col('date') <= end))
        if frame.height < min_bars:
            skipped += 1
            continue
        frames.append(frame)
    if not frames:
        raise ValueError('所选标的均无足够历史数据')
    logger.info(f'加载 {len(frames)} 只标的，跳过 {skipped} 只')

    panel = pl.concat(frames, how='vertical_relaxed')
    panel = panel.with_columns([
        pl.col(column).cast(pl.Float64)
        for column in ('open', 'high', 'low', 'close', 'pre_close',
                       'volume', 'amount', 'qfq_factor', 'hfq_factor')
        if column in panel.columns
    ])
    panel = panel.sort(['ts_code', 'date'])
    panel = panel.unique(subset=['ts_code', 'date'], keep='last')
    panel = panel.sort(['ts_code', 'date'])
    panel = _filter_liquidity(panel, liquidity_fraction)
    return panel


def _filter_liquidity(panel: pl.DataFrame, fraction: float) -> pl.DataFrame:
    """按近期平均成交额剔除流动性最差的尾部标的。"""
    if not 0 < fraction <= 1 or panel.is_empty() or 'amount' not in panel.columns:
        return panel
    recent = (
        panel.group_by('ts_code')
        .agg(pl.col('amount').tail(LIQUIDITY_LOOKBACK_DAYS).mean().alias('recent_amount'))
        .sort('recent_amount', descending=True)
    )
    keep = max(1, int(round(recent.height * fraction)))
    codes = recent.head(keep)['ts_code'].to_list()
    return panel.filter(pl.col('ts_code').is_in(codes))


def compute_factors(panel: pl.DataFrame,
                    factor_cols: Sequence[str] = DEFAULT_FACTOR_COLS,
                    factor_types: Sequence[str] = FACTOR_TYPES) -> pl.DataFrame:
    """用后复权 OHLC 计算因子，再把因子列并回原面板。

    原始 OHLC 始终保留，因子计算用临时视图替换为后复权价，保证量化口径
    与回测/因子模块一致，同时不破坏涨跌停判定所需的原始价格。

    缺失一律保留 null（``fill_missing=False``）：预热期样本若填 0，会在
    截面排序里被当成极端因子值而不是「没有数据」。
    """
    if panel.is_empty():
        return panel
    adjusted = panel.with_columns([
        pl.col('open_hfq').alias('open'),
        pl.col('high_hfq').alias('high'),
        pl.col('low_hfq').alias('low'),
        pl.col('close_hfq').alias('close'),
    ])
    result = calculate_factors_cross_sectional(
        adjusted, list(factor_types), code_col='ts_code',
        fill_missing=False)
    available = [col for col in factor_cols if col in result.columns]
    absent = [col for col in factor_cols if col not in result.columns]
    if absent:
        logger.warning(f'以下因子列未生成，将从特征集中剔除: {absent}')
    return panel.with_columns([result[col].cast(pl.Float64) for col in available])


def attach_cost_inputs(panel: pl.DataFrame, root: Path) -> pl.DataFrame:
    """并入分档滑点所需的两列：``adv20``（成交额，元）与 ``total_mv_yi``（亿元）。

    口径与回测内核 ``execution_core._prepare_universe`` 一致，一律只使用
    **t-1 及之前**的信息：订单在 t 日开盘成交，而 t 日成交额与收盘市值要等
    当日收盘才知道，直接用当日值等于给成本定价时偷看未来。

    - ``adv20`` = 前一交易日起算的 20 日成交额滚动均值（``min_samples=1``，
      只让上市首日为空）；
    - ``total_mv_yi`` = 前一交易日的 ``total_mv``（元）/ 1e8，来源为权威估值
      表 ``meta/financials/valuation_daily.parquet``。

    估值表缺失、或某标的某日无估值行时保留 null，由 ``CostModel`` 按小盘
    30bp 兜底——不插值、不前填、不用行情反推市值。
    """
    if panel.is_empty():
        return panel
    frame = panel.sort(['ts_code', 'date'])
    if 'amount' in frame.columns:
        frame = frame.with_columns(
            pl.col('amount').cast(pl.Float64).shift(1)
            .rolling_mean(window_size=ExecutionRules.ADV_WINDOW, min_samples=1)
            .over('ts_code').alias(ADV_COL))

    path = root.joinpath('data', 'local_store', *VALUATION_PATH)
    if not path.is_file():
        logger.warning(f'估值表缺失，total_mv_yi 全部留空并按小盘档计费: {path}')
        return frame
    valuation = (
        pl.scan_parquet(path)
        .select(['ts_code', 'trade_date', 'total_mv'])
        .filter(pl.col('ts_code').is_in(frame['ts_code'].unique().to_list()))
        .collect()
        .rename({'trade_date': 'date'})
        .with_columns(
            pl.when(pl.col('total_mv') > 0)
            .then(pl.col('total_mv') / 1e8)
            .otherwise(pl.lit(None, dtype=pl.Float64))
            .alias(TOTAL_MV_YI_COL))
        .select(['ts_code', 'date', TOTAL_MV_YI_COL])
    )
    joined = frame.join(valuation, on=['ts_code', 'date'], how='left')
    return joined.with_columns(
        pl.col(TOTAL_MV_YI_COL).shift(1).over('ts_code')
        .alias(TOTAL_MV_YI_COL))


def cost_input_coverage(panel: pl.DataFrame) -> Dict[str, int]:
    """成本分档输入的非空行数，用于报告里说明「按档位计费 vs 兜底档」。"""
    return {
        'total_mv_yi_notnull': (int(panel[TOTAL_MV_YI_COL].is_not_null().sum())
                                if TOTAL_MV_YI_COL in panel.columns else 0),
        'adv20_notnull': (int(panel[ADV_COL].is_not_null().sum())
                          if ADV_COL in panel.columns else 0),
    }


def load_learning_dataset(root: Path, start: date, end: date,
                          limit: int = 600,
                          factor_cols: Sequence[str] = DEFAULT_FACTOR_COLS,
                          asset: str = 'stock') -> tuple[pl.DataFrame, Dict[str, object]]:
    """加载行情 → 计算后复权价与因子 → 返回 (面板, 数据范围说明)。"""
    panel = load_panel(root, start, end, limit=limit, asset=asset)
    panel = panel.with_columns([
        (pl.col('open') * pl.col('hfq_factor')).alias('open_hfq'),
        (pl.col('high') * pl.col('hfq_factor')).alias('high_hfq'),
        (pl.col('low') * pl.col('hfq_factor')).alias('low_hfq'),
        (pl.col('close') * pl.col('hfq_factor')).alias('close_hfq'),
    ])
    panel = compute_factors(panel, factor_cols)
    panel = attach_cost_inputs(panel, root)
    scope: Dict[str, object] = {
        'asset': asset,
        'codes': panel['ts_code'].n_unique(),
        'rows': panel.height,
        'start': str(panel['date'].min()),
        'end': str(panel['date'].max()),
        'factors': [col for col in factor_cols if col in panel.columns],
        **cost_input_coverage(panel),
    }
    logger.info(
        f'学习数据集就绪: {scope["codes"]} 只标的 / {scope["rows"]} 行 / '
        f'{scope["start"]} → {scope["end"]}')
    return panel, scope
