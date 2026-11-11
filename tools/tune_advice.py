#!/usr/bin/env python3

"""策略调优评测框架（口径冻结，可复现、可记账）。

只搭评测架子，不改任何权重取值、不动 ``src/advice/config.py``。三个子命令：

- ``baseline``：把当前 ``src/advice/config.py`` 的参数导出为带日期与 git commit
  的基线快照（docs/，**时点快照，不是新的配置真源**）；
- ``evaluate``：按写死的 v1 口径跑一次滚动前推评测，输出扣成本净收益、期胜率、
  年化单边换手、最大回撤，并给出相对基线的差值，同时追加一行到累计试验表；
- ``ledger``：打印累计试验次数（多重比较校正的分母）。

口径全部是模块常量 ``PROTOCOL``，命令行无法覆盖持有期以外的任何一项；
每次运行都会把完整口径打印到 stdout 并写进结果文件。

成本口径：**0.724%/期往返**（佣金 5bp×2 + 印花税 + 过户费 + 市值分档滑点），
与 ``docs/退市股回池对比实证_2026-11-11.md`` 同一常数，按每期一次性扣除，
不使用 ``CostModel`` 的逐笔费率——逐笔会随标的市值/日期变化，无法在参数之间
保持同一常数，破坏可比性。
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import subprocess
import sys
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.advice.config import (  # noqa: E402
    AdviceConfig,
    FactorSpec,
    PortfolioConstraints,
    RiskFilterConfig,
    SoftFilterConfig,
    TierConfig,
    TradingCosts,
    TurnoverConfig,
    default_config,
)
from src.advice.data import panel_trading_dates  # noqa: E402
from src.advice.factor_panel import compute_factor_panel  # noqa: E402
from src.advice.scoring import attach_score  # noqa: E402
from src.advice.soft_filters import apply_soft_filter  # noqa: E402
from src.quant.factor_lib.data import PanelRequest, load_panel  # noqa: E402
from src.risk.execution_rules import ExecutionRules  # noqa: E402

# ----------------------------------------------------------------------
# 冻结口径（v1）。改这里等于改口径，必须同时升 PROTOCOL_VERSION 并写新文档。
# ----------------------------------------------------------------------
PROTOCOL: dict[str, Any] = {
    'version': 'v1',
    'eval_start': '2022-01-01',
    'eval_end': '2026-10-31',
    'is_start': '2015-01-05',
    'is_end': '2021-12-31',
    'hold_main': 60,
    'hold_controls': [5, 20],
    'cost_round_trip': 0.00724,
    'cost_breakdown': ('佣金 5bp×2 + 印花税 5bp(卖出) + 过户费 0.2bp×2 + 市值分档滑点；'
                       '满仓全额往返 0.724%，按本期实际换手比例摊销'),
    'top_k': 15,
    'min_history_rows': 250,
    'universe': 800,
    'universe_seed': 20261111,
    'universe_note': ('全市场分层抽样 800 只（按成交额十分位等比例，固定 seed 20261111）；'
                      '股票池来自 list_local_stock_codes，已排除状态表登记的 236 只'
                      '停更/退市标的，v1 存在幸存者偏差，对基线与候选同等生效'),
    'rebalance': '每 hold 个交易日一期，非重叠，逐期复利',
    'entry': 'T 日收盘打分，T+1 开盘等权买入；涨停/停牌跳过（不顺延）',
    'exit': 'T+1+hold 开盘卖出；跌停/停牌跳过（不顺延）',
    'score': '各档 zscore 加权（config 权重×方向），缺失按剩余权重归一，不填 0',
    'risk_filters': 'v1 不含 ST/事件过滤（基线与候选同等生效，不引入相对偏差）',
}

LEDGER_DIR = ROOT / 'reports' / 'strategy_tuning'
LEDGER_PATH = LEDGER_DIR / 'experiments.csv'
CACHE_DIR = ROOT / 'tmp' / 'strategy_tuning'
HOLD_TO_TIER = {5: 'short', 20: 'mid', 60: 'long'}
LEDGER_COLUMNS = [
    'experiment_id', 'timestamp', 'label', 'protocol_version', 'code_commit',
    'hold', 'segment', 'params_delta', 'net_annualized', 'net_total',
    'win_rate', 'turnover_annual', 'max_drawdown',
    'delta_net_annualized', 'delta_win_rate', 'delta_turnover',
    'delta_max_drawdown', 'n_periods', 'cumulative_attempts', 'purpose',
]


# ----------------------------------------------------------------------
# 配置 <-> 快照字典
# ----------------------------------------------------------------------
def config_to_dict(config: AdviceConfig) -> dict[str, Any]:
    """把配置展开成可 diff / 可覆盖的纯字典（因子按 column 建键）。"""
    tiers: dict[str, Any] = {}
    for key, tier in config.tiers.items():
        tiers[key] = {
            'label': tier.label,
            'lookback_days': tier.lookback_days,
            'min_hold_days': tier.min_hold_days,
            'confirm_days': tier.confirm_days,
            'validity_days': tier.validity_days,
            'buy_percentile': tier.buy_percentile,
            'sell_percentile': tier.sell_percentile,
            'max_positions': tier.max_positions,
            'edge_scale': tier.edge_scale,
            'signal_decay': tier.signal_decay,
            'max_volatility': tier.max_volatility,
            'max_atr_pct': tier.max_atr_pct,
            'min_amount': tier.min_amount,
            'factors': {
                spec.column: {
                    'label': spec.label,
                    'weight': spec.weight,
                    'direction': spec.direction,
                }
                for spec in tier.factors
            },
        }
    constraints = config.constraints
    costs = config.costs
    turnover = config.turnover
    risk = config.risk
    soft = config.soft_filter
    return {
        'factor_set': config.factor_set,
        'normalization': config.normalization,
        'min_history_rows': config.min_history_rows,
        'min_price': config.min_price,
        'max_avoid_rows': config.max_avoid_rows,
        'tiers': tiers,
        'constraints': {
            'max_single_weight': constraints.max_single_weight,
            'max_industry_weight': constraints.max_industry_weight,
            'cash_buffer': constraints.cash_buffer,
            'lot_size': constraints.lot_size,
            'adv_pct': constraints.adv_pct,
            'normalize_iterations': constraints.normalize_iterations,
            'uncapped_industry_buckets': list(constraints.uncapped_industry_buckets),
        },
        'costs': {
            'commission_rate': costs.commission_rate,
            'stamp_duty_rate': costs.stamp_duty_rate,
            'impact_rate': costs.impact_rate,
            'min_commission': costs.min_commission,
        },
        'turnover': {
            'min_hold_days_override': turnover.min_hold_days_override,
            'max_turnover_per_rebalance': turnover.max_turnover_per_rebalance,
            'cost_threshold_multiple': turnover.cost_threshold_multiple,
            'min_entry_edge_multiple': turnover.min_entry_edge_multiple,
        },
        'risk': {
            'st_enabled': risk.st_enabled,
            'st_unknown_policy': risk.st_unknown_policy,
            'industry_enabled': risk.industry_enabled,
            'event_enabled': risk.event_enabled,
            'event_unknown_policy': risk.event_unknown_policy,
            'event_cooldown': [list(item) for item in risk.event_cooldown],
        },
        'soft_filter': {
            'enabled': soft.enabled,
            'ma_trend_required': soft.ma_trend_required,
            'volume_quantile': soft.volume_quantile,
            'atr_quantile': soft.atr_quantile,
            'rsi_low': soft.rsi_low,
            'rsi_high': soft.rsi_high,
        },
    }


def dict_to_config(payload: dict[str, Any]) -> AdviceConfig:
    """快照字典 → ``AdviceConfig``（评测只读这层，不读活配置）。"""
    tiers: dict[str, TierConfig] = {}
    for key, item in payload['tiers'].items():
        factors = tuple(
            FactorSpec(column=column,
                       label=spec.get('label', column),
                       weight=float(spec['weight']),
                       direction=float(spec.get('direction', 1.0)))
            for column, spec in item['factors'].items()
        )
        tier = TierConfig(
            key=key, label=item.get('label', key),
            lookback_days=int(item['lookback_days']),
            min_hold_days=int(item['min_hold_days']),
            confirm_days=int(item['confirm_days']),
            validity_days=int(item['validity_days']),
            buy_percentile=float(item['buy_percentile']),
            sell_percentile=float(item['sell_percentile']),
            max_positions=int(item['max_positions']),
            edge_scale=float(item['edge_scale']),
            signal_decay=float(item['signal_decay']),
            max_volatility=float(item['max_volatility']),
            max_atr_pct=float(item['max_atr_pct']),
            min_amount=float(item['min_amount']),
            factors=factors,
        )
        tiers[key] = tier
    cons = payload['constraints']
    costs = payload['costs']
    turn = payload['turnover']
    risk = payload['risk']
    soft = payload['soft_filter']
    return AdviceConfig(
        tiers=tiers,
        constraints=PortfolioConstraints(
            max_single_weight=cons['max_single_weight'],
            max_industry_weight=cons['max_industry_weight'],
            cash_buffer=cons['cash_buffer'],
            lot_size=cons['lot_size'],
            adv_pct=cons['adv_pct'],
            normalize_iterations=cons['normalize_iterations'],
            uncapped_industry_buckets=tuple(cons['uncapped_industry_buckets']),
        ),
        costs=TradingCosts(**costs),
        turnover=TurnoverConfig(
            min_hold_days_override=turn['min_hold_days_override'],
            max_turnover_per_rebalance=turn['max_turnover_per_rebalance'],
            cost_threshold_multiple=turn['cost_threshold_multiple'],
            min_entry_edge_multiple=turn['min_entry_edge_multiple'],
        ),
        risk=RiskFilterConfig(
            st_enabled=risk['st_enabled'],
            st_unknown_policy=risk['st_unknown_policy'],
            industry_enabled=risk['industry_enabled'],
            event_enabled=risk['event_enabled'],
            event_unknown_policy=risk['event_unknown_policy'],
            event_cooldown=tuple((str(k), int(v)) for k, v in risk['event_cooldown']),
        ),
        min_history_rows=int(payload['min_history_rows']),
        min_price=float(payload['min_price']),
        max_avoid_rows=int(payload['max_avoid_rows']),
        factor_set=payload['factor_set'],
        soft_filter=SoftFilterConfig(**soft),
        normalization=payload['normalization'],
    )


def _flatten(node: Any, prefix: str = '') -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(node, dict):
        for key, value in node.items():
            out.update(_flatten(value, f'{prefix}.{key}' if prefix else str(key)))
    else:
        out[prefix] = node
    return out


def diff_params(base: dict[str, Any], other: dict[str, Any]) -> dict[str, list]:
    """返回 {路径: [基线值, 新值]}，只含真正变化的键。"""
    left, right = _flatten(base), _flatten(other)
    return {key: [left.get(key), right.get(key)]
            for key in sorted(set(left) | set(right))
            if left.get(key) != right.get(key)}


def _set_dotted(payload: dict[str, Any], dotted: str, raw_value: str) -> None:
    """按 ``a.b.c=value`` 覆盖快照；值按 JSON 解析，失败则保留字符串。"""
    parts = dotted.split('.')
    node: Any = payload
    for part in parts[:-1]:
        if part not in node:
            raise KeyError(f'参数路径不存在: {dotted}（断在 {part}）')
        node = node[part]
    if parts[-1] not in node:
        raise KeyError(f'参数路径不存在: {dotted}')
    try:
        value: Any = json.loads(raw_value)
    except json.JSONDecodeError:
        value = raw_value
    node[parts[-1]] = value


def _git_commit() -> str:
    try:
        out = subprocess.run(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'],
                             capture_output=True, text=True, check=True)
        return out.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return 'unknown'


def _git_dirty() -> bool:
    try:
        out = subprocess.run(['git', '-C', str(ROOT), 'status', '--porcelain'],
                             capture_output=True, text=True, check=True)
        return bool(out.stdout.strip())
    except (subprocess.CalledProcessError, FileNotFoundError):
        return True


# ----------------------------------------------------------------------
# 基线快照
# ----------------------------------------------------------------------
def export_baseline(path: Path | None = None) -> Path:
    """导出当前配置为基线快照 JSON（文件头标注「时点快照，非配置真源」）。"""
    today = date.today().isoformat()
    path = path or ROOT / 'docs' / f'策略参数基线_{today}.json'
    payload = {
        '_notice': (
            '这是时点快照，不是新的配置真源。配置真源仍是 src/advice/config.py；'
            '本文件只用于冻结评测对照口径，任何参数改动必须改 config.py 后重新导出，'
            '禁止反向以本文件覆盖 config.py。'
        ),
        '_generated_at': datetime.now().isoformat(timespec='seconds'),
        '_git_commit': _git_commit(),
        '_git_dirty': _git_dirty(),
        '_protocol': PROTOCOL,
        'config': config_to_dict(default_config()),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding='utf-8', newline='\n')
    logger.info(f'[baseline] 已导出 {path}（commit {payload["_git_commit"][:12]}）')
    return path


def latest_baseline() -> Path:
    files = sorted((ROOT / 'docs').glob('策略参数基线_*.json'))
    if not files:
        raise FileNotFoundError(
            '未找到基线快照，先运行: python tools/tune_advice.py baseline')
    return files[-1]


# ----------------------------------------------------------------------
# 数据：面板与因子
# ----------------------------------------------------------------------
def _panel_cache(universe: int, start: str, end: str) -> Path:
    return CACHE_DIR / f'panel_u{universe}_{start}_{end}_ext.parquet'


def load_factor_panel(universe: int, start: str, end: str,
                      no_cache: bool = False) -> tuple[pl.DataFrame, dict[str, Any]]:
    """加载后复权面板并算 legacy 12 因子（与推荐链路同一套公式）。"""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache = _panel_cache(universe, start, end)
    if cache.is_file() and not no_cache:
        logger.info(f'[panel] 读缓存 {cache}')
        frame = pl.read_parquet(cache)
        info = {'cached': True, 'rows': frame.height,
                'symbols': frame['ts_code'].n_unique()}
    else:
        from tools.eval_daily_advice import strat_universe

        info = strat_universe(universe, start, end, PROTOCOL['universe_seed'])
        logger.info(f'[panel] 构建 {len(info["codes"])} 只 {start}~{end}')
        panel = load_panel(PanelRequest(start=start, end=end,
                                        universe_size=universe, use_cache=True),
                           codes=info['codes'])
        compat = panel.with_columns([
            pl.col('amount').alias('raw_amount'),
            pl.col('volume').alias('raw_volume'),
            pl.col('pre_close').alias('raw_prev_close')
            if 'pre_close' in panel.columns
            else pl.col('raw_close').shift(1).over('ts_code').alias('raw_prev_close'),
        ])
        frame = compute_factor_panel(compat, extended=True)
        frame.write_parquet(cache)
        info = {**{k: info[k] for k in ('eligible', 'delisted_in_period',
                                        'sampled', 'seed', 'rule')},
                'cached': False, 'rows': frame.height,
                'symbols': frame['ts_code'].n_unique()}
    return frame, info


# ----------------------------------------------------------------------
# 评测
# ----------------------------------------------------------------------
def _eligible_candidates(signal_rows: dict[str, dict], tier: TierConfig,
                         min_price: float) -> list[tuple[str, float]]:
    """信号日截面筛选：分数达标 + 硬门槛 + 软筛选（与实盘链路顺序一致）。"""
    candidates: list[tuple[str, float]] = []
    for code, row in signal_rows.items():
        percentile = row.get('score_percentile')
        if percentile is None or not np.isfinite(float(percentile)):
            continue
        if float(percentile) < tier.buy_percentile:
            continue
        if not row.get('raw_open') or not row.get('volume'):
            continue
        if float(row.get('raw_close') or 0.0) < min_price:
            continue
        amount = row.get('amount_ma20')
        if amount is None or float(amount) < tier.min_amount:
            continue
        volatility = row.get('volatility_20')
        if volatility is not None and float(volatility) > tier.max_volatility:
            continue
        atr = row.get('atr_pct')
        if atr is not None and float(atr) > tier.max_atr_pct:
            continue
        if row.get('soft_pass') is False:
            continue
        candidates.append((code, float(percentile)))
    candidates.sort(key=lambda item: item[1], reverse=True)
    return candidates


def _portfolio_gross(picks: list[str], base_open: dict[str, float],
                     price_of: dict[str, float], top_k: int) -> float:
    """等权 Top-K，未买满的部分留现金（0 收益）。"""
    if not picks:
        return 0.0
    ratios = [price_of[code] / base_open[code] for code in picks
              if price_of.get(code) and np.isfinite(price_of[code])]
    if not ratios:
        return 0.0
    return (float(np.mean(ratios)) - 1.0) * (len(ratios) / top_k)


def run_periods(scored: pl.DataFrame, dates: list[date], hold: int,
                top_k: int, tier: TierConfig,
                min_price: float) -> dict[str, Any]:
    """非重叠逐期模拟：T 收盘打分 → T+1 开盘买入 → T+1+hold 开盘卖出。

    返回逐期净收益（已扣 0.724% 往返）、逐日净值与汇总指标。
    """
    cost = float(PROTOCOL['cost_round_trip'])
    rules = ExecutionRules()
    keep = ['date', 'ts_code', 'score_percentile', 'open', 'close',
            'raw_open', 'raw_close', 'volume', 'amount_ma20', 'volatility_20',
            'atr_pct', 'soft_pass']
    slim = scored.select([column for column in keep if column in scored.columns])
    by_date: dict[date, dict[str, dict]] = {}
    for day, rows in slim.partition_by('date', as_dict=True).items():
        key = day[0] if isinstance(day, tuple) else day
        by_date[key] = {str(row['ts_code']): row for row in rows.iter_rows(named=True)}

    equity = 1.0
    curve: list[dict[str, Any]] = [{'date': dates[0].isoformat(), 'equity': equity}]
    periods: list[dict[str, Any]] = []
    skipped = 0
    filled_prev = 0
    traded_fraction = 0.0
    start_i = 0
    while start_i + hold + 1 < len(dates):
        signal_day = dates[start_i]
        entry_i = start_i + 1
        exit_i = entry_i + hold
        entry_day, exit_day = dates[entry_i], dates[exit_i]
        signal_rows = by_date.get(signal_day, {})
        candidates = _eligible_candidates(signal_rows, tier, min_price)

        picks: list[str] = []
        for code, _score in candidates:
            row = by_date.get(entry_day, {}).get(code)
            prev_row = signal_rows.get(code)
            if row is None or not row.get('raw_open') or not row.get('volume'):
                skipped += 1
                continue
            prev_close = (prev_row or {}).get('raw_close')
            if not prev_close:
                skipped += 1
                continue
            move = float(row['raw_open']) / float(prev_close) - 1.0
            if move >= float(rules.resolve_limit_pct(
                    code, is_st=None, trade_date=entry_day)) - 0.004:
                skipped += 1
                continue
            picks.append(code)
            if len(picks) >= top_k:
                break
        if not picks:
            filled_prev = 0
            periods.append({
                'signal_date': signal_day.isoformat(),
                'entry_date': entry_day.isoformat(),
                'exit_date': exit_day.isoformat(),
                'picks': 0, 'gross_return': 0.0, 'net_return': 0.0,
                'equity': equity,
            })
            curve.append({'date': exit_day.isoformat(), 'equity': equity})
            start_i = exit_i
            continue

        base_open = {code: float(by_date[entry_day][code]['open']) for code in picks}
        exit_row = by_date.get(exit_day, {})
        exit_prices = {code: (float(exit_row[code]['open'])
                              if code in exit_row and exit_row[code].get('open')
                              else np.nan) for code in picks}
        gross_period = _portfolio_gross(picks, base_open, exit_prices, top_k)

        # 成本按本期实际换手摊销：满仓全额换手 = 0.724%，半仓换手 = 0.362%
        period_fraction = (filled_prev + len(picks)) / (2.0 * top_k)
        net_period = (1.0 + gross_period) * (1.0 - cost * period_fraction) - 1.0
        period_equity = equity * (1.0 + net_period)
        for offset in range(1, hold):
            day = dates[entry_i + offset]
            day_rows = by_date.get(day, {})
            closes = {code: (float(day_rows[code]['close'])
                             if code in day_rows and day_rows[code].get('close')
                             else np.nan) for code in picks}
            gross = _portfolio_gross(picks, base_open, closes, top_k)
            curve.append({'date': day.isoformat(),
                          'equity': equity * (1.0 + gross)})
        curve.append({'date': exit_day.isoformat(), 'equity': period_equity})

        traded_fraction += period_fraction
        filled_prev = len(picks)
        periods.append({
            'signal_date': signal_day.isoformat(),
            'entry_date': entry_day.isoformat(),
            'exit_date': exit_day.isoformat(),
            'picks': len(picks),
            'gross_return': gross_period,
            'net_return': net_period,
            'equity': period_equity,
        })
        equity = period_equity
        start_i = exit_i

    values = np.array([item['equity'] for item in curve], dtype=float)
    peaks = np.maximum.accumulate(values)
    drawdowns = (values - peaks) / np.where(peaks > 0, peaks, np.nan)
    total_days = len(dates) - 1
    years = max(1e-9, total_days / 252.0)
    final = float(values[-1]) if len(values) else 1.0
    period_returns = np.array([item['net_return'] for item in periods], dtype=float)
    turnover = traded_fraction / years
    return {
        'hold': hold,
        'n_periods': len(periods),
        'net_total': final - 1.0,
        'net_annualized': final ** (1.0 / years) - 1.0 if final > 0 else -1.0,
        'win_rate': float(np.mean(period_returns > 0)) if len(period_returns) else 0.0,
        'turnover_annual': turnover,
        'avg_filled': float(np.mean([p['picks'] for p in periods])) if periods else 0.0,
        'avg_exposure': float(np.mean([p['picks'] for p in periods]) / top_k)
        if periods else 0.0,
        'max_drawdown': float(-np.nanmin(drawdowns)) if len(drawdowns) else 0.0,
        'avg_gross_per_period': float(np.mean([p['gross_return'] for p in periods]))
        if periods else 0.0,
        'skipped_untradable': skipped,
        'curve': curve,
        'periods': periods,
        'days': total_days,
    }


def evaluate(config: AdviceConfig, factors: pl.DataFrame, hold: int,
             segment: str, top_k: int) -> dict[str, Any]:
    """在指定区段上评测一个档位/持有期。"""
    start, end = ((PROTOCOL['is_start'], PROTOCOL['is_end'])
                  if segment == 'is'
                  else (PROTOCOL['eval_start'], PROTOCOL['eval_end']))
    if segment == 'all':
        start, end = PROTOCOL['is_start'], PROTOCOL['eval_end']
    lo, hi = date.fromisoformat(start), date.fromisoformat(end)
    tier_key = HOLD_TO_TIER[hold]
    tier = config.tier(tier_key)
    scored = attach_score(factors, tier,
                          min_history_rows=int(PROTOCOL['min_history_rows']))
    if config.soft_filter.enabled:
        scored = apply_soft_filter(scored, config.soft_filter)
    scored = scored.filter((pl.col('date') >= lo) & (pl.col('date') <= hi))
    dates = panel_trading_dates(scored)
    result = run_periods(scored, dates, hold, top_k, tier, config.min_price)
    result.update({'tier': tier_key, 'segment': segment,
                   'start': start, 'end': end})
    return result


def _metric_block(result: dict[str, Any]) -> dict[str, float]:
    return {key: round(float(result[key]), 6) for key in (
        'net_annualized', 'net_total', 'win_rate', 'turnover_annual',
        'max_drawdown')}


def _append_ledger(row: dict[str, Any]) -> int:
    """追加一行试验记录，返回累计尝试次数（含本行）。"""
    LEDGER_DIR.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, str]] = []
    if LEDGER_PATH.is_file():
        with LEDGER_PATH.open('r', encoding='utf-8', newline='') as handle:
            rows = list(csv.DictReader(handle))
    cumulative = len(rows) + 1
    record = {key: str(row.get(key, '')) for key in LEDGER_COLUMNS}
    record['experiment_id'] = str(row['experiment_id'])
    record['cumulative_attempts'] = str(cumulative)
    rows.append(record)
    with LEDGER_PATH.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=LEDGER_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return cumulative


def print_protocol() -> None:
    print('=' * 78)
    print('评测口径（冻结，不允许每次评测改动）')
    for key, value in PROTOCOL.items():
        print(f'  {key:<18} {value}')
    print('=' * 78)


def cmd_baseline(args: argparse.Namespace) -> int:
    path = export_baseline(Path(args.out) if args.out else None)
    print(f'基线快照: {path}')
    return 0


def cmd_ledger(_args: argparse.Namespace) -> int:
    if not LEDGER_PATH.is_file():
        print('累计试验表尚不存在（累计尝试次数 = 0）')
        return 0
    with LEDGER_PATH.open('r', encoding='utf-8', newline='') as handle:
        rows = list(csv.DictReader(handle))
    print(f'累计试验表: {LEDGER_PATH}')
    print(f'累计尝试次数: {len(rows)}（Bonferroni α=0.05/{len(rows)} = '
          f'{0.05 / max(len(rows), 1):.5f}）')
    print(f'{"id":<18}{"label":<40}{"hold":>5}{"净年化":>10}{"期胜率":>8}{"Δ净年化":>10}')
    for row in rows[-10:]:
        print(f'{row["experiment_id"]:<18}{row["label"][:38]:<40}'
              f'{row["hold"]:>5}{float(row["net_annualized"]):>10.2%}'
              f'{float(row["win_rate"]):>8.1%}'
              f'{float(row["delta_net_annualized"]):>10.2%}')
    return 0


def cmd_evaluate(args: argparse.Namespace) -> int:
    if args.hold is not None:
        holds = [args.hold]
    else:
        holds = [PROTOCOL['hold_main'], *PROTOCOL['hold_controls']]

    snapshot_path = Path(args.baseline) if args.baseline else latest_baseline()
    snapshot = json.loads(snapshot_path.read_text(encoding='utf-8'))
    base_payload = copy.deepcopy(snapshot['config'])
    candidate_payload = copy.deepcopy(base_payload)
    for item in args.set or []:
        dotted, _, value = item.partition('=')
        _set_dotted(candidate_payload, dotted, value)
    delta = diff_params(base_payload, candidate_payload)

    factors, panel_info = load_factor_panel(
        PROTOCOL['universe'],
        (date.fromisoformat(PROTOCOL['is_start']) - timedelta(days=400)).isoformat(),
        PROTOCOL['eval_end'], no_cache=args.no_cache)

    base_config = dict_to_config(base_payload)
    cand_config = dict_to_config(candidate_payload)
    commit = _git_commit()
    started = datetime.now()

    print_protocol()
    print(f'基线快照   : {snapshot_path.name}（commit {snapshot["_git_commit"][:12]}）')
    print(f'本次参数改动: {json.dumps(delta, ensure_ascii=False) if delta else "无（基线自评）"}')
    print(f'样本        : {panel_info["symbols"]} 只 / {panel_info["rows"]} 行')
    print()

    summary: dict[str, Any] = {'protocol': PROTOCOL, 'baseline': str(snapshot_path),
                               'params_delta': delta, 'code_commit': commit,
                               'panel': panel_info, 'results': {}}
    results = summary['results']
    for hold in holds:
        base = evaluate(base_config, factors, hold, args.segment, PROTOCOL['top_k'])
        cand = base if not delta else evaluate(
            cand_config, factors, hold, args.segment, PROTOCOL['top_k'])
        base_m, cand_m = _metric_block(base), _metric_block(cand)
        metrics = {
            'baseline': base_m,
            'candidate': cand_m,
            'delta': {key: round(cand_m[key] - base_m[key], 6) for key in base_m},
            'n_periods': base['n_periods'],
            'skipped_untradable': base['skipped_untradable'],
        }
        results[str(hold)] = metrics
        print(f'--- 持有 {hold} 日（{base["tier"]} 档，{base["start"]}~{base["end"]}，'
              f'{base["n_periods"]} 期）---')
        print(f'  基线净年化 {base_m["net_annualized"]:+.2%} | 期胜率 {base_m["win_rate"]:.1%} '
              f'| 年化单边换手 {base_m["turnover_annual"]:.2f}x '
              f'| 最大回撤 {base_m["max_drawdown"]:.2%} '
              f'| 均持仓 {base["avg_filled"]:.1f}/{PROTOCOL["top_k"]}')
        print(f'  候选净年化 {cand_m["net_annualized"]:+.2%} | 期胜率 {cand_m["win_rate"]:.1%} '
              f'| 年化单边换手 {cand_m["turnover_annual"]:.2f}x '
              f'| 最大回撤 {cand_m["max_drawdown"]:.2%}')
        print(f'  差值      Δ净年化 {metrics["delta"]["net_annualized"]:+.2%} '
              f'| Δ期胜率 {metrics["delta"]["win_rate"]:+.1%} '
              f'| Δ最大回撤 {metrics["delta"]["max_drawdown"]:+.2%}')
        print()

    if args.no_ledger:
        print('未写试验记账（--no-ledger）')
        return 0

    main = results[str(holds[0])]
    cumulative = _append_ledger({
        'experiment_id': started.strftime('%Y%m%dT%H%M%S'),
        'timestamp': started.isoformat(timespec='seconds'),
        'label': args.label,
        'protocol_version': PROTOCOL['version'],
        'code_commit': commit,
        'hold': holds[0],
        'segment': args.segment,
        'params_delta': json.dumps(delta, ensure_ascii=False),
        'net_annualized': main['candidate']['net_annualized'],
        'net_total': main['candidate']['net_total'],
        'win_rate': main['candidate']['win_rate'],
        'turnover_annual': main['candidate']['turnover_annual'],
        'max_drawdown': main['candidate']['max_drawdown'],
        'delta_net_annualized': main['delta']['net_annualized'],
        'delta_win_rate': main['delta']['win_rate'],
        'delta_turnover': main['delta']['turnover_annual'],
        'delta_max_drawdown': main['delta']['max_drawdown'],
        'n_periods': main['n_periods'],
    })
    print(f'已记入 {LEDGER_PATH}；累计尝试次数 = {cumulative}'
          f'（Bonferroni α=0.05/{cumulative} = {0.05 / cumulative:.5f}）')
    out = LEDGER_DIR / f'eval_{started.strftime("%Y%m%dT%H%M%S")}.json'
    summary['cumulative_attempts'] = cumulative
    summary['elapsed_sec'] = round((datetime.now() - started).total_seconds(), 1)
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2),
                   encoding='utf-8', newline='\n')
    print(f'明细: {out}')
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='策略调优评测（v1 冻结口径）')
    sub = parser.add_subparsers(dest='command', required=True)

    p_base = sub.add_parser('baseline', help='导出当前 config.py 为基线快照')
    p_base.add_argument('--out', default='', help='输出路径（默认 docs/策略参数基线_<日期>.json）')
    p_base.set_defaults(func=cmd_baseline)

    p_eval = sub.add_parser('evaluate', help='按冻结口径评测一组参数')
    p_eval.add_argument('--baseline', default='', help='基线快照路径（默认最新一份）')
    p_eval.add_argument('--set', action='append', default=[],
                        help='覆盖参数，如 --set tiers.long.buy_percentile=0.9')
    p_eval.add_argument('--label', default='baseline', help='本次试验标签')
    p_eval.add_argument('--hold', type=int, default=None,
                        help='只跑一个持有期；缺省跑 60 主口径 + 5/20 对照')
    p_eval.add_argument('--segment', choices=['is', 'oos', 'all'], default='oos',
                        help='评估区段；参数选择只允许看 is，结论必须报 oos')
    p_eval.add_argument('--no-ledger', action='store_true', help='不写试验记账')
    p_eval.add_argument('--no-cache', action='store_true', help='重建面板缓存')
    p_eval.set_defaults(func=cmd_evaluate)

    p_ledger = sub.add_parser('ledger', help='查看累计试验次数')
    p_ledger.set_defaults(func=cmd_ledger)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    raise SystemExit(main())
