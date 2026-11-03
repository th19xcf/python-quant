#!/usr/bin/env python3

"""短线档位参数校准：训练段调参、测试段只验证，外加 ±20% 邻域敏感性。

口径与 ``src.advice.replay`` 完全一致（T+1 开盘成交、涨跌停/停牌剔除、
双边成本），本脚本只做参数搜索，不改撮合与成本模型。

阶段：
    python scripts/calibrate_advice.py --stage prepare      # 取股票池 + 因子缓存
    python scripts/calibrate_advice.py --stage baseline     # 三档原参数，两段回放
    python scripts/calibrate_advice.py --stage scan         # 换手网格 + 权重变体
    python scripts/calibrate_advice.py --stage refine       # ±20% 单变量邻域
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable

import polars as pl
from loguru import logger

from src.advice.config import (
    AdviceConfig,
    FactorSpec,
    RiskFilterConfig,
    TierConfig,
    TurnoverConfig,
    default_config,
)
from src.advice.data import LocalPanelLoader, panel_trading_dates
from src.advice.factor_panel import compute_factor_panel
from src.advice.pipeline import _StoreConfig
from src.advice.replay import _ReplayTier
from src.advice.scoring import attach_score
from src.advice.universe import build_meta, list_local_stock_codes, load_name_map
from src.utils.config import get_config

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE = PROJECT_ROOT / 'reports' / 'advice' / 'calibration'
TRAIN = (date(2024, 11, 1), date(2025, 10, 31))
TEST = (date(2025, 11, 1), date(2026, 11, 2))
DATA_END = TEST[1]
#: 股票池按训练段之前（2024-06~2024-10）的成交额排序，避免用未来数据选样本
LIQUIDITY_WINDOW = (date(2024, 6, 1), date(2024, 10, 31))
MAX_LOOKBACK = 500
EQUITY = 1_000_000.0
TURNOVER_TARGET = 4.0
#: 「不再显著为负」的判定线：训练段成本后年化 ≥ -5%
NET_FLOOR = -0.05


# ----------------------------------------------------------------------
# 数据准备
# ----------------------------------------------------------------------
def prepare(store_root: Path, cache_dir: Path, symbols: int) -> dict[str, Any]:
    """按训练段前流动性选股，加载全窗口行情并缓存因子面板。"""
    store_config = _StoreConfig(store_root)
    all_codes = list_local_stock_codes(store_config)
    logger.info(f"[calib] 本地股票 {len(all_codes)} 只，按 2024-06~2024-10 成交额排序")

    liq_loader = LocalPanelLoader(store_config, min_rows=40)
    liq_panel, liq_report = liq_loader.load(
        all_codes, LIQUIDITY_WINDOW[0], LIQUIDITY_WINDOW[1])
    liquidity = (
        liq_panel.group_by('ts_code')
        .agg(pl.col('amount').tail(60).mean().alias('avg_amount'),
             pl.len().alias('rows'))
        .filter(pl.col('rows') >= 40)
        .sort('avg_amount', descending=True)
    )
    if liquidity.is_empty():
        raise SystemExit('流动性统计为空，无法选股')
    picked = liquidity.head(symbols)
    codes = picked['ts_code'].to_list()
    avg_amounts = picked['avg_amount'].to_list()

    loader = LocalPanelLoader(store_config, min_rows=60)
    start = loader.default_start(TRAIN[0], MAX_LOOKBACK)
    panel, report = loader.load(codes, start, DATA_END)
    factors = compute_factor_panel(panel)
    cache_dir.mkdir(parents=True, exist_ok=True)
    factor_path = cache_dir / 'factor_panel.parquet'
    factors.write_parquet(factor_path)
    info = {
        'symbols_requested': symbols,
        'universe_size': len(codes),
        'universe_liquidity_window': [d.isoformat() for d in LIQUIDITY_WINDOW],
        'liquidity_report': liq_report.summary(),
        'panel_report': report.summary(),
        'panel_start': start.isoformat(),
        'panel_end': DATA_END.isoformat(),
        'factor_rows': factors.height,
        'avg_amount_min': min(avg_amounts) if avg_amounts else 0.0,
        'avg_amount_median': sorted(avg_amounts)[len(avg_amounts) // 2]
        if avg_amounts else 0.0,
        'codes': codes,
    }
    (cache_dir / 'universe.json').write_text(
        json.dumps(info, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info(
        f"[calib] 缓存完成: {len(codes)} 只, {factors.height} 行 → {factor_path}")
    return info


def load_cache(cache_dir: Path) -> tuple[pl.DataFrame, dict[str, Any], dict[str, Any]]:
    info = json.loads((cache_dir / 'universe.json').read_text(encoding='utf-8'))
    factors = pl.read_parquet(cache_dir / 'factor_panel.parquet')
    store_root = Path(get_config().data.local_store_path)
    names = load_name_map(store_root / 'meta' / 'names.json')
    meta = build_meta(factors['ts_code'].unique().to_list(), names, {})
    return factors, info, meta


def segment(factors: pl.DataFrame, window: tuple[date, date]) -> tuple[pl.DataFrame, list[date]]:
    start, end = window
    frame = factors.filter((pl.col('date') >= start) & (pl.col('date') <= end))
    dates = panel_trading_dates(frame)
    return frame, dates


def build_cache(scored: pl.DataFrame) -> dict[date, dict[str, dict]]:
    cache: dict[date, dict[str, dict]] = {}
    for key, part in scored.partition_by(['date'], as_dict=True).items():
        day = key[0]
        cache[day] = {str(row['ts_code']): row for row in part.to_dicts()}
    return cache


def prepare_scored(factors: pl.DataFrame, tier: TierConfig,
                   window: tuple[date, date]) -> tuple[pl.DataFrame, list[date], dict]:
    frame, dates = segment(factors, window)
    scored = attach_score(frame, tier)
    return scored, dates, build_cache(scored)


def run_combo(base: AdviceConfig, tier: TierConfig, scored: pl.DataFrame,
              dates: list[date], cache: dict, meta: dict,
              turnover: TurnoverConfig | None = None) -> dict[str, Any]:
    config = replace(base, tiers={**base.tiers, tier.key: tier},
                     turnover=turnover or base.turnover)
    runner = _ReplayTier(SimpleNamespace(config=config), tier, scored, dates,
                         meta, EQUITY)
    runner._rows = lambda day: cache.get(day, {})
    runner._sample_rows = lambda: []
    summary = runner.run().summary()
    summary.pop('sample_rows', None)
    return summary


def _calmar(summary: dict[str, Any]) -> float:
    drawdown = summary.get('max_drawdown') or 0.0
    if drawdown <= 1e-6:
        return float(summary.get('annualized_return') or 0.0)
    return float(summary.get('annualized_return') or 0.0) / drawdown


def _eligible(summary: dict[str, Any]) -> bool:
    return (summary['annual_turnover'] <= TURNOVER_TARGET
            and summary['annualized_return'] >= NET_FLOOR)


def _rank(summary: dict[str, Any]) -> tuple[float, float, float]:
    return (_calmar(summary), summary['annualized_return'],
            -summary['annual_turnover'])


# ----------------------------------------------------------------------
# 参数组合
# ----------------------------------------------------------------------
def tier_with(tier: TierConfig, **kwargs) -> TierConfig:
    return tier.with_overrides(**kwargs)


def factor_variants(tier: TierConfig) -> dict[str, TierConfig]:
    """动量/反转配比变体（其余字段沿用短线预设）。"""
    def specs(items: Iterable[tuple[str, str, float, float]]) -> tuple[FactorSpec, ...]:
        return tuple(FactorSpec(c, label, w, d) for c, label, w, d in items)

    return {
        'base': tier,
        'momentum_strong': tier_with(tier, factors=specs([
            ('momentum_1m', '1月动量', 0.45, 1.0),
            ('volume_momentum_20', '20日量能', 0.20, 1.0),
            ('ma20_gap', '偏离20日线', 0.20, 1.0),
            ('rsi_14', 'RSI(14)', 0.00, 1.0),
            ('atr_pct', 'ATR占比', 0.15, -1.0),
        ])),
        'no_overbought': tier_with(tier, factors=specs([
            ('momentum_1m', '1月动量', 0.35, 1.0),
            ('volume_momentum_20', '20日量能', 0.20, 1.0),
            ('ma20_gap', '偏离20日线', 0.20, 1.0),
            ('rsi_14', 'RSI(14)', 0.00, 1.0),
            ('atr_pct', 'ATR占比', 0.25, -1.0),
        ])),
        'reversal': tier_with(tier, factors=specs([
            ('momentum_1m', '1月动量', 0.30, -1.0),
            ('volume_momentum_20', '20日量能', 0.15, 1.0),
            ('ma20_gap', '偏离20日线', 0.20, -1.0),
            ('rsi_14', 'RSI(14)', 0.15, -1.0),
            ('atr_pct', 'ATR占比', 0.20, -1.0),
        ])),
        'balanced_reversal': tier_with(tier, factors=specs([
            ('momentum_1m', '1月动量', 0.15, 1.0),
            ('volume_momentum_20', '20日量能', 0.20, 1.0),
            ('ma20_gap', '偏离20日线', 0.10, 1.0),
            ('rsi_14', 'RSI(14)', 0.20, -1.0),
            ('atr_pct', 'ATR占比', 0.25, -1.0),
        ])),
        'jegadeesh_6m': tier_with(tier, factors=specs([
            ('momentum_3m', '3月动量', 0.40, 1.0),
            ('volume_momentum_20', '20日量能', 0.20, 1.0),
            ('ma20_gap', '偏离20日线', 0.20, 1.0),
            ('rsi_14', 'RSI(14)', 0.10, 1.0),
            ('atr_pct', 'ATR占比', 0.20, -1.0),
        ])),
    }


def turnover_combos() -> list[dict[str, Any]]:
    combos: list[dict[str, Any]] = []
    for min_hold in (10, 15, 20, 30):
        for confirm in (2, 3, 5):
            for cap in (0.05, 0.10, 0.15, 0.20):
                for cost_mult in (1.0, 2.0, 3.0):
                    combos.append({
                        'min_hold_days': min_hold,
                        'confirm_days': confirm,
                        'max_turnover_per_rebalance': cap,
                        'cost_threshold_multiple': cost_mult,
                    })
    return combos


def apply_combo(base_tier: TierConfig, combo: dict[str, Any],
                factor_tier: TierConfig | None = None) -> tuple[TierConfig, TurnoverConfig]:
    source = factor_tier or base_tier
    tier = tier_with(source,
                     min_hold_days=int(combo['min_hold_days']),
                     confirm_days=int(combo['confirm_days']))
    turnover = replace(
        base_config().turnover,
        max_turnover_per_rebalance=float(combo['max_turnover_per_rebalance']),
        cost_threshold_multiple=float(combo['cost_threshold_multiple']),
        min_entry_edge_multiple=float(combo['cost_threshold_multiple']),
    )
    return tier, turnover


def base_config() -> AdviceConfig:
    # 校准脚本只做参数搜索：关闭风险过滤，避免把 ST/行业/事件的效应混入参数比较
    return replace(default_config(), risk=RiskFilterConfig(
        st_enabled=False, industry_enabled=False, event_enabled=False))


# ----------------------------------------------------------------------
# 阶段实现
# ----------------------------------------------------------------------
def stage_baseline(cache_dir: Path, out: Path) -> dict[str, Any]:
    factors, info, meta = load_cache(cache_dir)
    base = base_config()
    payload: dict[str, Any] = {'universe': {k: v for k, v in info.items() if k != 'codes'}}
    for tier_key in ('short', 'mid', 'long'):
        tier = base.tier(tier_key)
        payload[tier_key] = {}
        for name, window in (('train', TRAIN), ('test', TEST)):
            scored, dates, cache = prepare_scored(factors, tier, window)
            summary = run_combo(base, tier, scored, dates, cache, meta)
            payload[tier_key][name] = summary
            logger.info(f"[baseline] {tier_key}/{name}: {summary}")
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def stage_scan(cache_dir: Path, out: Path) -> dict[str, Any]:
    factors, _info, meta = load_cache(cache_dir)
    base = base_config()
    short = base.tier('short')
    scored, dates, cache = prepare_scored(factors, short, TRAIN)

    grid: list[dict[str, Any]] = []
    for combo in turnover_combos():
        tier, turnover = apply_combo(short, combo)
        summary = run_combo(base, tier, scored, dates, cache, meta, turnover)
        grid.append({'params': combo, 'metrics': summary,
                     'eligible': _eligible(summary)})
    grid.sort(key=lambda item: _rank(item['metrics']), reverse=True)
    eligible_grid = [item for item in grid if item['eligible']]
    logger.info(
        f"[scan] 换手网格 {len(grid)} 组，达标 {len(eligible_grid)} 组，"
        f"达标中最优: {eligible_grid[0] if eligible_grid else '无'}")

    # 权重变体：取达标配置前 5 名；全不达标时退化为全网格前 5
    weight_runs: list[dict[str, Any]] = []
    for top in (eligible_grid or grid)[:5]:
        for weight_name, weight_tier in factor_variants(short).items():
            scored_w, dates_w, cache_w = prepare_scored(factors, weight_tier, TRAIN)
            tier, turnover = apply_combo(short, top['params'], weight_tier)
            summary = run_combo(base, tier, scored_w, dates_w, cache_w, meta, turnover)
            weight_runs.append({
                'weight_variant': weight_name,
                'turnover_params': top['params'],
                'metrics': summary,
                'eligible': _eligible(summary),
            })
    weight_runs.sort(key=lambda item: _rank(item['metrics']), reverse=True)
    eligible_weights = [item for item in weight_runs if item['eligible']]
    logger.info(
        f"[scan] 权重变体 {len(weight_runs)} 组，达标 {len(eligible_weights)} 组，"
        f"达标中最优: {eligible_weights[0] if eligible_weights else '无'}")

    candidates = [
        item for item in grid[:1] + weight_runs[:3]
        if item['eligible']
    ] or grid[:1] + weight_runs[:3]
    best = max(candidates, key=lambda item: _rank(item['metrics']))
    payload = {
        'train_window': [d.isoformat() for d in TRAIN],
        'turnover_grid': grid,
        'weight_variants': weight_runs,
        'best': {'params': best.get('params') or best.get('turnover_params'),
                 'weight_variant': best.get('weight_variant', 'base'),
                 'metrics': best['metrics']},
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def neighbourhood_combos(params: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """最优参数 ±20% 的单变量邻域（整数参数四舍五入且至少为 1）。"""
    combos: list[tuple[str, dict[str, Any]]] = []
    integer_fields = (('min_hold_days', 'min_hold'), ('confirm_days', 'confirm'))
    float_fields = (
        ('max_turnover_per_rebalance', 'turnover cap'),
        ('cost_threshold_multiple', 'cost multiple'),
    )
    for field, label in integer_fields:
        value = int(params[field])
        for sign, tag in ((-1, '-20%'), (1, '+20%')):
            combos.append((f'{label} {tag}',
                           dict(params, **{field: max(1, round(value * (1 + 0.2 * sign)))})))
    for field, label in float_fields:
        value = float(params[field])
        for sign, tag in ((-1, '-20%'), (1, '+20%')):
            combos.append((f'{label} {tag}',
                           dict(params, **{field: value * (1 + 0.2 * sign)})))
    return combos


def _weight_scale(tier: TierConfig, column: str, factor: float) -> TierConfig:
    return tier_with(tier, factors=tuple(
        replace(spec, weight=spec.weight * factor) if spec.column == column else spec
        for spec in tier.factors))


def stage_refine(cache_dir: Path, out: Path, frontier_path: Path) -> dict[str, Any]:
    """选定训练段最优可行组合后，做 ±20% 邻域并在测试段同步验证。"""
    factors, _info, meta = load_cache(cache_dir)
    base = base_config()
    short = base.tier('short')
    frontier = json.loads(frontier_path.read_text(encoding='utf-8'))
    train_rows = [row for row in frontier['rows'] if row['window'] == 'train']
    feasible = [row for row in train_rows
                if row['metrics']['annual_turnover'] <= TURNOVER_TARGET]
    # 选择准则只用训练段：换手达标者中取年化最高；无达标者退化为换手最低
    if feasible:
        picked = max(feasible, key=lambda row: row['metrics']['annualized_return'])
    else:
        picked = min(train_rows, key=lambda row: row['metrics']['annual_turnover'])

    params: dict[str, Any] = {
        'min_hold_days': picked['min_hold_days'],
        'confirm_days': 2,
        'max_turnover_per_rebalance': 0.05,
        'cost_threshold_multiple': 1.0,
        'validity_days': picked['validity_days'],
    }
    weight_name = picked['weights']
    weight_tier = factor_variants(short)[weight_name]
    evaluators: dict[tuple, Any] = {}

    def evaluate(tier_source: TierConfig, window: tuple[date, date],
                 combo: dict[str, Any]) -> dict[str, Any]:
        key = (window, tuple((s.column, s.weight, s.direction)
                             for s in tier_source.factors))
        if key not in evaluators:
            scored, dates, cache = prepare_scored(factors, tier_source, window)
            evaluators[key] = (scored, dates, cache)
        scored, dates, cache = evaluators[key]
        tier, turnover = apply_combo(short, combo, tier_source)
        tier = tier_with(tier, validity_days=int(combo['validity_days']))
        return run_combo(base, tier, scored, dates, cache, meta, turnover)

    def judge(combo: dict[str, Any], tier_source: TierConfig) -> dict[str, Any]:
        train = evaluate(tier_source, TRAIN, combo)
        test = evaluate(tier_source, TEST, combo)
        return {
            'train': train,
            'test': test,
            'platform': (_eligible(train)
                         and test['annual_turnover'] <= TURNOVER_TARGET
                         and test['annualized_return'] >= NET_FLOOR),
        }

    neighbours: list[dict[str, Any]] = [
        {'label': 'optimum', 'params': combo, **judge(combo, weight_tier)}
        for combo in [params]
    ]
    for label, combo in neighbourhood_combos(params):
        neighbours.append({'label': label, 'params': combo,
                           **judge(combo, weight_tier)})
    for label, delta in (('validity -20%', -0.2), ('validity +20%', 0.2)):
        combo = dict(params,
                     validity_days=max(int(params['min_hold_days']),
                                       round(params['validity_days'] * (1 + delta))))
        neighbours.append({'label': label, 'params': combo,
                           **judge(combo, weight_tier)})
    for column in ('momentum_1m', 'rsi_14', 'atr_pct'):
        for label, factor in ((f'{column} -20%', 0.8), (f'{column} +20%', 1.2)):
            scaled = _weight_scale(weight_tier, column, factor)
            neighbours.append({'label': label, 'params': params,
                               **judge(params, scaled)})

    payload = {
        'train_window': [d.isoformat() for d in TRAIN],
        'test_window': [d.isoformat() for d in TEST],
        'selection': '训练段换手≤4x 中年化最高；测试段只验证不重选',
        'weight_variant': weight_name,
        'params': params,
        'neighbours': neighbours,
        'platform_ok': sum(1 for item in neighbours if item['platform']),
        'platform_total': len(neighbours),
        'train': neighbours[0]['train'],
        'test': neighbours[0]['test'],
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def stage_frontier(cache_dir: Path, out: Path) -> dict[str, Any]:
    """持有期前沿：证明「换手≤4x」需要多长的持有期（训练+测试双段）。"""
    factors, _info, meta = load_cache(cache_dir)
    base = base_config()
    short = base.tier('short')
    variants = factor_variants(short)
    rows: list[dict[str, Any]] = []
    for weights in ('base', 'reversal', 'no_overbought'):
        weight_tier = variants[weights]
        for window_name, window in (('train', TRAIN), ('test', TEST)):
            scored, dates, cache = prepare_scored(factors, weight_tier, window)
            for min_hold in (10, 20, 30, 45):
                for validity in (20, 30, 45, 60, 90):
                    if validity < min_hold:
                        continue
                    combo = {
                        'min_hold_days': min_hold,
                        'confirm_days': 2,
                        'max_turnover_per_rebalance': 0.05,
                        'cost_threshold_multiple': 1.0,
                    }
                    tier, turnover = apply_combo(short, combo, weight_tier)
                    tier = tier_with(tier, validity_days=validity)
                    summary = run_combo(base, tier, scored, dates, cache, meta,
                                        turnover)
                    rows.append({
                        'window': window_name,
                        'weights': weights,
                        'min_hold_days': min_hold,
                        'validity_days': validity,
                        'metrics': summary,
                        'eligible': _eligible(summary),
                    })
                    logger.info(
                        f"[frontier] {window_name}/{weights} mh={min_hold} "
                        f"val={validity}: net={summary['net_return']:+.3f} "
                        f"to={summary['annual_turnover']:.2f}")
    payload = {
        'train_window': [d.isoformat() for d in TRAIN],
        'test_window': [d.isoformat() for d in TEST],
        'rows': rows,
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description='短线档位参数校准')
    parser.add_argument('--stage', required=True,
                        choices=['prepare', 'baseline', 'scan', 'refine', 'frontier'])
    parser.add_argument('--cache-dir', default=str(DEFAULT_CACHE))
    parser.add_argument('--symbols', type=int, default=1200)
    args = parser.parse_args()
    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    store_root = Path(get_config().data.local_store_path)

    if args.stage == 'prepare':
        info = prepare(store_root, cache_dir, args.symbols)
        payload = json.dumps({k: v for k, v in info.items() if k != 'codes'},
                             ensure_ascii=False, indent=2)
    elif args.stage == 'baseline':
        payload = json.dumps(stage_baseline(cache_dir, cache_dir / 'baseline.json'),
                             ensure_ascii=False, indent=2)
    elif args.stage == 'scan':
        payload = json.dumps(stage_scan(cache_dir, cache_dir / 'scan.json'),
                             ensure_ascii=False, indent=2)[:2000]
    elif args.stage == 'frontier':
        payload = json.dumps(stage_frontier(cache_dir, cache_dir / 'frontier.json'),
                             ensure_ascii=False, indent=2)[:2000]
    else:
        payload = json.dumps(stage_refine(cache_dir, cache_dir / 'refine.json',
                                          cache_dir / 'frontier.json'),
                             ensure_ascii=False, indent=2)
    sys.stdout.write(payload + '\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
