#!/usr/bin/env python3

"""单窗超额归因诊断（只读分析，不改生产逻辑）。

输入：
- 因子矩阵 parquet（含 191 候选因子 + 行情 + 基本面因子 + legacy 因子）；
- 冻结窗口 JSON（每个测试窗的入选因子与权重）；
- 行业映射、指数行情（只读）。

输出（默认 ``reports/single_window_diag/<tag>/``）：
- ``window_summary.csv``：逐窗策略/基准收益、超额、Sharpe、换手、IC、beta；
- ``attribution_windows.csv``：逐窗行业暴露 + 市值/价值/投资/动量/波动/流动性 +
  个股 alpha 残差 + 现金/截距 + 交易执行残差；
- ``industry_windows.csv``：逐窗行业权重、行业收益、Brinson 配置/选股贡献；
- ``factor_ic_windows.csv``：逐窗每个入选因子的对齐后 RankIC；
- ``leaders_windows.csv``：逐窗领涨票捕获率；
- ``attribution_best_monthly.csv``：最优窗逐月归因；
- ``diagnostics.json``：汇总口径与自检结果。

口径说明：
- 组合收益用生产 ``simulate_fixed_hold`` 同一套撮合/成本规则复刻，并额外记录
  每日持仓权重；复刻结果与生产函数做逐窗自检；
- 归因以「等权全池基准」为参照：``r_b = mean(ret)``，与评估脚本一致；
- 行业使用本地 TDX 一级行业（13 类、静态快照），**不是申万一级**，报告中需标注；
- 每只股票日收益 ``ret`` 为后复权收益，归因为日频算术累计，复利净收益另列。
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date
from pathlib import Path
from typing import Any, cast

import numpy as np
import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.advice.factor_eval import (  # noqa: E402
    SimConfig,
    _curve_stats,
    add_forward_returns,
    build_composite,
    simulate_fixed_hold,
)
from src.alpha.cross_section.preprocess import standardize, winsorize_mad  # noqa: E402
from src.risk.execution_rules import ExecutionRules  # noqa: E402

MARKET_COLUMNS = (
    'date', 'ts_code', 'ret', 'close', 'volume', 'next_tradable',
    'open', 'raw_open', 'raw_close', 'history_rows', 'amount', 'pre_close',
)
STYLE_COLUMNS = (
    'style_size', 'style_value', 'style_invest', 'style_momentum',
    'style_volatility', 'style_liquidity',
)
STYLE_LABELS = {
    'style_size': '市值(size)',
    'style_value': '价值(value)',
    'style_invest': '投资/成长(invest)',
    'style_momentum': '动量(momentum)',
    'style_volatility': '波动(volatility)',
    'style_liquidity': '流动性(liquidity)',
}
VALUE_COMPONENTS = ('ff_value_bp', 'ff_value_ep', 'ff_sales_yield', 'ff_div_yield')
STYLE_SOURCE = {
    'ff_size': 'style_size',
    'ff_investment': 'style_invest',
    'momentum_3m': 'style_momentum',
    'volatility_20': 'style_volatility',
    'ext_turnover_20': 'style_liquidity',
}
MIN_IC_PAIRS = 30


def _as_date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(str(value)[:10])


def load_windows(path: Path) -> dict[str, Any]:
    """读取冻结窗口 JSON，校验矩阵哈希字段存在。"""
    payload = json.loads(path.read_text(encoding='utf-8'))
    if not payload.get('windows'):
        raise ValueError(f'{path} 不含 windows')
    return payload


def matrix_columns(windows: dict[str, Any]) -> list[str]:
    """按窗口权重 + 归因所需列做列投影，避免整表读入。"""
    factor_cols = {
        name for item in windows['windows'] for name in item['weights']
    }
    needed = set(MARKET_COLUMNS) | set(STYLE_SOURCE) | set(VALUE_COMPONENTS)
    return sorted(needed | factor_cols)


def load_matrix(path: Path, windows: dict[str, Any]) -> pl.DataFrame:
    """只读加载矩阵投影列，并附 ``fwd_h`` 标签。"""
    frame = pl.read_parquet(path, columns=matrix_columns(windows))
    return add_forward_returns(frame, (20,)).sort(['date', 'ts_code'])


def build_style_panel(frame: pl.DataFrame) -> pl.DataFrame:
    """构造 6 类风格暴露的截面 z 值（价值为 4 个分量的等权 z 均值）。

    预处理走唯一管线的两步：``winsorize_mad``（MAD k=3）+ ``standardize``
    （zscore）。缺失与常数截面一律保持 null，不再 ``otherwise(0.0)`` /
    ``fill_null(0.0)``：那会把「没有数据」当成截面均值参与归因回归。
    回归侧若拿到 null，按截面均值 0 处理（见 :func:`_day_data`），与
    :func:`_lookup_arrays` 的「缺失记 0」同口径。
    """
    sources = list(dict.fromkeys([*STYLE_SOURCE, *VALUE_COMPONENTS]))
    win, _ = winsorize_mad(frame, sources, by='date', suffix='__win')
    std, _ = standardize(win, [f'{name}__win' for name in sources], by='date',
                         suffix='__z')
    renamed = [(dst, f'{src}__win__z') for src, dst in STYLE_SOURCE.items()]
    renamed += [(f'__z_{name}', f'{name}__win__z') for name in VALUE_COMPONENTS]
    out = std.with_columns([pl.col(source).alias(target) for target, source in renamed])
    value = pl.mean_horizontal(
        *[f'__z_{name}' for name in VALUE_COMPONENTS])
    return out.with_columns(value.alias('style_value')).select(
        ['date', 'ts_code', *STYLE_COLUMNS])


def load_industry(path: Path) -> pl.DataFrame:
    """读取行业映射；列名统一为 ``ts_code`` / ``industry``。"""
    frame = pl.read_parquet(path).select(
        pl.col('code').alias('ts_code'), pl.col('industry_l1').alias('industry'))
    return frame.unique(subset=['ts_code'], keep='first')


def _design(industry: np.ndarray, z: np.ndarray) -> tuple[np.ndarray, list[str]]:
    """OLS 设计矩阵：截距 + 行业哑变量（去掉首类）+ 风格 z。"""
    industries = sorted({str(value) for value in industry})
    columns = [np.ones(len(industry))]
    for name in industries[1:]:
        columns.append((industry == name).astype(float))
    columns.append(z)
    return np.column_stack(columns), industries


def decompose_excess(
    weights: np.ndarray,
    bench_weights: np.ndarray,
    y: np.ndarray,
    z: np.ndarray,
    industry: np.ndarray,
) -> dict[str, float]:
    """把 ``(w-b)·y`` 精确分解为行业、风格、alpha 残差、现金/截距。

    恒等式：``(w-b)·y = (Σw-1)α + Σ_j(Σw_j-Σb_j)γ_j
    + Σ_k((w-b)·z_k)β_k + (w-b)·e``。返回按风格名的贡献字典。
    """
    design, industries = _design(industry, z)
    beta, *_ = np.linalg.lstsq(design, y, rcond=None)
    residual = y - design @ beta
    gamma = dict.fromkeys(industries, 0.0)
    for index, name in enumerate(industries[1:], start=1):
        gamma[name] = float(beta[index])
    n_style = z.shape[1]
    style_beta = beta[design.shape[1] - n_style:]
    contributions: dict[str, float] = {
        '现金/截距': float((weights.sum() - 1.0) * beta[0]),
    }
    industry_total = 0.0
    for name in industries:
        mask = industry == name
        industry_total += float((weights[mask].sum() - bench_weights[mask].sum()) * gamma[name])
    contributions['行业暴露'] = industry_total
    for index, name in enumerate(STYLE_COLUMNS):
        contributions[STYLE_LABELS[name]] = float(
            ((weights - bench_weights) * z[:, index]).sum() * style_beta[index])
    contributions['个股alpha残差'] = float(((weights - bench_weights) * residual).sum())
    contributions['_total'] = float((weights - bench_weights) @ y)
    contributions['_check'] = float(sum(
        value for key, value in contributions.items() if not key.startswith('_')))
    return contributions


def brinson_contrib(
    weights: np.ndarray,
    bench_weights: np.ndarray,
    y: np.ndarray,
    industry: np.ndarray,
) -> dict[str, float]:
    """Brinson：行业配置贡献 + 行业内选股贡献（两项之和 = 组合超额）。"""
    allocation = 0.0
    selection = 0.0
    for name in sorted({str(value) for value in industry}):
        mask = industry == name
        w_port = float(weights[mask].sum())
        w_bench = float(bench_weights[mask].sum())
        if w_bench <= 0:
            continue
        r_port = float(weights[mask] @ y[mask]) / w_port if w_port > 0 else 0.0
        r_bench = float(bench_weights[mask] @ y[mask]) / w_bench
        allocation += (w_port - w_bench) * r_bench
        selection += w_port * (r_port - r_bench)
    return {'行业配置': allocation, '行业选股': selection,
            '_total': float((weights - bench_weights) @ y)}


def style_exposures(weights: np.ndarray, bench_weights: np.ndarray,
                    z: np.ndarray) -> dict[str, float]:
    """组合相对基准的风格暴露差（组合减基准）。"""
    diff = weights - bench_weights
    return {f'暴露_{STYLE_LABELS[name]}': float(diff @ z[:, index])
            for index, name in enumerate(STYLE_COLUMNS)}


def simulate_holdings(frame: pl.DataFrame, config: SimConfig  # noqa: C901
                      ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """复刻 ``simulate_fixed_hold`` 的撮合/成本逻辑，并记录每日持仓权重。

    返回 ``(equity, daily, holdings)``：
    - equity：date/equity；
    - daily：date/equity/cash/positions/cost_buy/cost_sell；
    - holdings：date/ts_code/weight（权重以总权益为分母，含现金）。
    """
    from src.backtest.costs.cost_model import CostModel

    rules = ExecutionRules()
    cost_model = config.make_cost_model()
    dates = sorted(frame['date'].unique().to_list())
    by_date = {day: frame.filter(pl.col('date') == day).to_dicts() for day in dates}
    by_code_by_date = {
        day: {str(row['ts_code']): row for row in rows}
        for day, rows in by_date.items()
    }
    cash = 1_000_000.0
    initial = cash
    positions: list[dict[str, Any]] = []
    equity_rows: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    holding_rows: list[dict[str, Any]] = []
    cost_buy = 0.0
    cost_sell = 0.0

    for index in range(1, len(dates)):
        day = dates[index]
        prev = dates[index - 1]
        rows = by_date[prev]
        today = by_code_by_date[day]
        prev_rows = by_code_by_date[prev]

        still: list[dict[str, Any]] = []
        for pos in positions:
            pos['held'] += 1
            if pos['held'] < config.hold_days:
                still.append(pos)
                continue
            row = today.get(pos['ts_code'])
            prev_row = prev_rows.get(pos['ts_code'])
            if (row is None or not row.get('raw_open')
                    or row.get('volume', 0) <= 0 or prev_row is None):
                still.append(pos)
                continue
            limit = rules.resolve_limit_pct(pos['ts_code'], is_st=None, trade_date=day)
            move = float(row['raw_open']) / float(prev_row['raw_close']) - 1.0
            if move <= -limit + 0.004:
                still.append(pos)
                continue
            price = float(row['open'])
            proceeds = pos['shares'] * price
            cost = cost_model.calculate_sell_cost(
                pos['shares'], price, trade_date=day, ts_code=pos['ts_code'])
            cash += proceeds - cost
            cost_sell += cost
        positions = still

        ranked = [
            row for row in rows
            if row.get('__score') is not None
            and not row.get('__short_history')
            and row.get('raw_close')
            and row.get('volume', 0) > 0
        ]
        ranked.sort(key=lambda r: float(r['__score']), reverse=True)
        budget = max(0.0, cash - initial * config.cash_buffer)
        slots = max(0, config.top_k - len(positions))
        for row in ranked[:slots]:
            code = str(row['ts_code'])
            if any(pos['ts_code'] == code for pos in positions):
                continue
            nrow = today.get(code)
            if nrow is None or not nrow.get('raw_open') or nrow.get('volume', 0) <= 0:
                continue
            limit = rules.resolve_limit_pct(code, is_st=None, trade_date=day)
            move = abs(float(nrow['raw_open']) / float(row['raw_close']) - 1.0)
            if move >= limit - 0.004:
                continue
            weight = 1.0 / max(1, config.top_k)
            notional = min(budget, initial * weight)
            if notional < 1000:
                continue
            price = float(nrow['open'])
            shares = int(notional / price / 100) * 100
            if shares <= 0:
                continue
            cost = cost_model.calculate_buy_cost(
                shares, price, trade_date=day, ts_code=code)
            cash -= shares * price + cost
            budget -= shares * price
            cost_buy += cost
            positions.append({
                'ts_code': code, 'shares': shares, 'price': price, 'held': 0,
            })

        mark = cash
        marks: dict[str, float] = {}
        for pos in positions:
            row = today.get(pos['ts_code'])
            price = float(row['close']) if row else pos['price']
            value = pos['shares'] * price
            marks[pos['ts_code']] = value
            mark += value
        for code, value in marks.items():
            holding_rows.append({'date': day, 'ts_code': code,
                                 'weight': value / mark if mark else 0.0})
        equity_rows.append({'date': day, 'equity': mark})
        daily_rows.append({'date': day, 'equity': mark, 'cash': cash,
                           'positions': len(positions),
                           'cash_weight': cash / mark if mark else 0.0})

    equity = pl.DataFrame(equity_rows)
    daily = pl.DataFrame(daily_rows)
    holdings = pl.DataFrame(holding_rows)
    return equity, daily, holdings


def _day_data(frame: pl.DataFrame, styles: pl.DataFrame,
              industry: pl.DataFrame) -> dict[date, dict[str, Any]]:
    """把窗口数据整理成 ``date -> {codes, ret, z, industry}``，供逐日回归。"""
    merged = (
        frame.select('date', 'ts_code', 'ret')
        .join(styles, on=['date', 'ts_code'], how='left')
        .join(industry, on='ts_code', how='left')
        .with_columns(pl.col('industry').fill_null('未知'))
    )
    pdf = merged.to_pandas()
    out: dict[date, dict[str, Any]] = {}
    for day, group in pdf.groupby('date', sort=True):
        key = day.date() if hasattr(day, 'date') else _as_date(str(day))
        out[key] = {
            'codes': group['ts_code'].to_numpy(),
            'ret': group['ret'].to_numpy(dtype=float),
            # 风格 z 的 null（因子缺失/常数截面）在回归里按截面均值 0 处理，
            # 与 _lookup_arrays 的「缺失记 0」同口径
            'z': np.nan_to_num(group[list(STYLE_COLUMNS)].to_numpy(dtype=float),
                               nan=0.0, posinf=0.0, neginf=0.0),
            'industry': group['industry'].to_numpy(),
        }
    return out


def _lookup_arrays(codes: np.ndarray, source: dict[str, Any],
                   n_style: int) -> tuple[np.ndarray, np.ndarray]:
    """按代码对齐风格 z 与行业；缺失记 0 / 未知。"""
    z = np.zeros((len(codes), n_style))
    industry = np.array(['未知'] * len(codes), dtype=object)
    for index, code in enumerate(codes):
        row = source.get(code)
        if row is None:
            continue
        z[index] = row['z']
        industry[index] = row['industry']
    return z, industry


def attribute_window(
    frame: pl.DataFrame,
    styles: pl.DataFrame,
    industry: pl.DataFrame,
    equity: pl.DataFrame,
    holdings: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """逐日归因一个测试窗，返回日频明细与行业汇总。"""
    days = _day_data(frame, styles, industry)
    sorted_days = sorted(days)
    eq = {row['date']: float(row['equity']) for row in equity.iter_rows(named=True)}
    weights_by_day: dict[date, dict[str, float]] = {}
    for row in holdings.iter_rows(named=True):
        weights_by_day.setdefault(row['date'], {})[row['ts_code']] = float(row['weight'])

    detail: list[dict[str, Any]] = []
    industry_rows: list[dict[str, Any]] = []
    for index in range(len(sorted_days) - 1):
        day = sorted_days[index]
        nxt = sorted_days[index + 1]
        current = days[day]
        forward = days[nxt]
        ret = forward['ret']
        valid = np.isfinite(ret)
        if valid.sum() < MIN_IC_PAIRS or day not in eq or nxt not in eq:
            continue
        codes = forward['codes'][valid]
        y = ret[valid]
        source = {code: {'z': current['z'][pos], 'industry': current['industry'][pos]}
                  for pos, code in enumerate(current['codes'])}
        z, ind = _lookup_arrays(codes, source, len(STYLE_COLUMNS))
        held = weights_by_day.get(day, {})
        weights = np.array([held.get(code, 0.0) for code in codes])
        bench = np.full(len(codes), 1.0 / len(codes))
        contrib = decompose_excess(weights, bench, y, z, ind)
        brinson = brinson_contrib(weights, bench, y, ind)
        exposure = style_exposures(weights, bench, z)
        own = float(weights @ y)
        bench_ret = float(bench @ y)
        actual = eq[nxt] / eq[day] - 1.0
        detail_row: dict[str, Any] = {
            'date': day,
            'month': f'{day.year:04d}-{day.month:02d}',
            'port_gross': own,
            'bench': bench_ret,
            'gross_excess': own - bench_ret,
            'actual_return': actual,
            'exec_residual': actual - own,
            'cash_weight': 1.0 - float(weights.sum()),
            **contrib,
            **brinson,
            **exposure,
        }
        detail.append(detail_row)
        for name in sorted(set(ind.tolist())):
            mask = ind == name
            w_port = float(weights[mask].sum())
            w_bench = float(bench[mask].sum())
            r_port = float(weights[mask] @ y[mask]) / w_port if w_port > 0 else 0.0
            r_bench = float(bench[mask] @ y[mask]) / w_bench if w_bench > 0 else 0.0
            industry_rows.append({
                'date': day, 'industry': name,
                'w_port': w_port, 'w_bench': w_bench,
                'r_port': r_port, 'r_bench': r_bench,
                'alloc': (w_port - w_bench) * r_bench,
                'select': w_port * (r_port - r_bench),
            })
    return pl.DataFrame(detail), pl.DataFrame(industry_rows)


def ic_stats(frame: pl.DataFrame, score: str, label: str) -> dict[str, float]:
    """日频 RankIC 的均值 / IR / 正比例 / t。"""
    sub = frame.filter(pl.col(score).is_not_null() & pl.col(label).is_not_null())
    daily = (
        sub.group_by('date')
        .agg(pl.len().alias('n'),
             pl.corr(pl.col(score).rank(), pl.col(label).rank()).alias('ic'))
        .filter((pl.col('n') >= MIN_IC_PAIRS) & pl.col('ic').is_not_null())
    )
    values = daily['ic'].to_numpy()
    if len(values) < 2:
        return {'ic': 0.0, 'ir': 0.0, 'positive': 0.0, 'n': float(len(values))}
    mean = float(np.mean(values))
    std = float(np.std(values, ddof=1))
    return {'ic': mean, 'ir': mean / std if std > 1e-12 else 0.0,
            'positive': float(np.mean(values > 0)), 'n': float(len(values))}


def beta_stats(strategy: pl.DataFrame, benchmark: pl.DataFrame) -> dict[str, float]:
    """日频 ``r_p = α + β r_b`` 回归，返回 β / R² / 年化 α。"""
    joined = (
        strategy.with_columns(pl.col('equity').pct_change().alias('rp'))
        .select('date', 'rp')
        .join(benchmark.select('date', pl.col('ret').alias('rb')), on='date', how='inner')
        .drop_nulls()
    )
    if joined.height < 10:
        return {'beta': 0.0, 'r2': 0.0, 'alpha_annual': 0.0, 'n': float(joined.height)}
    y = joined['rp'].to_numpy()
    x = joined['rb'].to_numpy()
    design = np.column_stack([np.ones(len(x)), x])
    coef, *_ = np.linalg.lstsq(design, y, rcond=None)
    residual = y - design @ coef
    total = float(((y - y.mean()) ** 2).sum())
    r2 = 1.0 - float((residual ** 2).sum()) / total if total > 1e-12 else 0.0
    return {'beta': float(coef[1]), 'r2': r2, 'n': float(len(x)),
            'alpha_daily': float(coef[0]),
            'alpha_annual': float((1.0 + coef[0]) ** 252 - 1.0),
            'resid_vol_annual': float(np.std(residual, ddof=1) * math.sqrt(252))}


def leaders_stats(frame: pl.DataFrame, holdings: pl.DataFrame,
                  top_n: int = 20) -> dict[str, float]:
    """窗口领涨票捕获：全窗口收益前 N 名是否被持有、平均最高分位。"""
    prices = frame.select('date', 'ts_code', 'close', '__score')
    bounds = prices.group_by('ts_code').agg(
        pl.col('close').sort_by('date').first().alias('first'),
        pl.col('close').sort_by('date').last().alias('last'),
        pl.col('__score').max().alias('best_score'))
    bounds = bounds.with_columns((pl.col('last') / pl.col('first') - 1.0).alias('ret'))
    ranked = bounds.sort('ret', descending=True)
    top = ranked.head(top_n)['ts_code'].to_list()
    bottom = ranked.tail(top_n)['ts_code'].to_list()
    held = set(holdings['ts_code'].unique().to_list())
    top_held = [code for code in top if code in held]
    bottom_held = [code for code in bottom if code in held]
    scores = frame.select('date', 'ts_code', '__score').drop_nulls()
    counts = scores.group_by('date').len().rename({'len': 'n'})
    pct = (scores.join(counts, on='date', how='left')
           .with_columns((pl.col('__score').rank('average').over('date')
                          / pl.col('n') * 100.0).alias('pct')))
    best = pct.group_by('ts_code').agg(pl.col('pct').max().alias('best_pct'))
    top_pct = best.filter(pl.col('ts_code').is_in(top))['best_pct'].to_numpy()
    return {
        'n_pool': float(bounds.height),
        'top_capture': len(top_held) / len(top) if top else 0.0,
        'top_held_count': float(len(top_held)),
        'bottom_avoid': 1.0 - len(bottom_held) / len(bottom) if bottom else 0.0,
        'top_best_pct_median': float(np.median(top_pct)) if len(top_pct) else 0.0,
        'top_ret_mean': float(cast(float, ranked.head(top_n)['ret'].mean())),
        'pool_ret_mean': float(cast(float, bounds['ret'].mean())),
    }


def _log_shares(returns: list[float]) -> list[float]:
    logs = [math.log1p(value) for value in returns]
    total = sum(logs)
    return [value / total if abs(total) > 1e-12 else 0.0 for value in logs]


def _drop_one_annualized(returns: list[float], drop: int) -> float:
    """剔除一个窗口后按复利拼接的年化（每窗 125 交易日）。"""
    kept = [value for index, value in enumerate(returns) if index != drop]
    product = 1.0
    for value in kept:
        product *= 1.0 + value
    if product <= 0:
        return -1.0
    return product ** (252.0 / (125.0 * len(kept))) - 1.0


def _index_windows(paths: list[Path],
                   windows: list[dict[str, Any]]) -> dict[str, list[float | None]]:
    """指数在各测试窗的区间收益（只读，缺失指数跳过）。"""
    out: dict[str, list[float | None]] = {}
    for path in paths:
        if not path.is_file():
            continue
        frame = pl.read_parquet(path).select('date', 'close').sort('date')
        rows: list[float | None] = []
        for item in windows:
            chunk = frame.filter((pl.col('date') >= _as_date(item['test_start']))
                                 & (pl.col('date') < _as_date(item['test_end'])))
            if chunk.height < 2:
                rows.append(None)
                continue
            rows.append(float(chunk['close'][-1] / chunk['close'][0] - 1.0))
        out[path.stem] = rows
    return out


def run(args: argparse.Namespace) -> int:
    windows = load_windows(Path(args.windows))
    matrix = load_matrix(Path(args.matrix), windows)
    styles = build_style_panel(matrix)
    industry = load_industry(Path(args.industry))
    logger.info(f'[diag] 矩阵 {matrix.height} 行 × {len(matrix.columns)} 列，'
                f'{matrix["ts_code"].n_unique()} 只，{matrix["date"].n_unique()} 日')
    logger.info(f'[diag] 行业映射 {industry.height} 条')

    sim = SimConfig(top_k=args.top_k, hold_days=args.hold)
    window_rows: list[dict[str, Any]] = []
    attribution_rows: list[dict[str, Any]] = []
    industry_rows: list[dict[str, Any]] = []
    ic_rows: list[dict[str, Any]] = []
    leader_rows: list[dict[str, Any]] = []
    daily_rows_all: list[pl.DataFrame] = []
    detail_by_window: dict[str, pl.DataFrame] = {}
    verify: list[dict[str, Any]] = []

    for item in windows['windows']:
        start = _as_date(item['test_start'])
        end = _as_date(item['test_end'])
        chunk = matrix.filter((pl.col('date') >= start) & (pl.col('date') < end))
        composite = build_composite(chunk, item['weights'])
        score = ic_stats(composite, '__score', 'fwd_20')
        equity, daily, holdings = simulate_holdings(composite, sim)

        prod_curve, prod_stats = simulate_fixed_hold(composite, sim)
        replay_stats = _curve_stats(equity, 1_000_000.0, len(equity))
        diff = abs(prod_stats['net_return'] - replay_stats['net_return'])
        verify.append({'test': f'{start}~{end}', 'prod_net': prod_stats['net_return'],
                       'diag_net': replay_stats['net_return'], 'abs_diff': diff})
        if diff > 1e-9:
            logger.warning(f'[diag] {start}~{end} 复刻收益与生产偏差 {diff:.3e}')
        if equity.height != prod_curve.height:
            logger.warning(f'[diag] {start}~{end} 净值日数不一致')

        bench = chunk.select('date', 'ts_code', 'ret').drop_nulls().group_by('date').agg(
            pl.col('ret').mean().alias('ret')).sort('date')
        bench_equity = bench.with_columns((pl.col('ret') + 1.0).cum_prod().alias('equity'))
        bench_stats = _curve_stats(bench_equity, 1.0, bench.height)
        beta = beta_stats(equity, bench)
        attribution, industries = attribute_window(chunk, styles, industry, equity, holdings)
        detail_by_window[f'{start}~{end}'] = attribution
        daily_rows_all.append(
            attribution.with_columns(pl.lit(f'{start}~{end}').alias('test')))
        # 同起点基准：从策略开始持仓的 d1 收盘算起，剔除首日（d1）隔夜跳空
        bench_same = float((1.0 + attribution['bench']).product() - 1.0)
        strategy_same = float((1.0 + attribution['actual_return']).product() - 1.0)
        excess_same = (1.0 + strategy_same) / (1.0 + bench_same) - 1.0

        factor_ic = []
        for name, weight in item['weights'].items():
            if name not in chunk.columns or weight == 0:
                continue
            stats = ic_stats(chunk, name, 'fwd_20')
            direction = 1.0 if weight >= 0 else -1.0
            factor_ic.append({
                'test': f'{start}~{end}', 'factor': name, 'weight': float(weight),
                'ic_raw': stats['ic'], 'ic_aligned': direction * stats['ic'],
                'ir_aligned': direction * stats['ir'], 'positive': stats['positive'],
            })
        ic_rows.extend(factor_ic)

        leaders = leaders_stats(composite, holdings, top_n=20)
        leader_rows.append({'test': f'{start}~{end}', **leaders})

        contributions = {
            key: float(value) for key, value in attribution.select(
                [pl.col(name).sum() for name in
                 ['行业暴露', '市值(size)', '价值(value)', '投资/成长(invest)',
                  '动量(momentum)', '波动(volatility)', '流动性(liquidity)',
                  '个股alpha残差', '现金/截距', '行业配置', '行业选股',
                  'exec_residual', 'gross_excess']]
            ).row(0, named=True).items()
        }
        window_rows.append({
            'test': f'{start}~{end}',
            'net': replay_stats['net_return'],
            'bench_net': bench_stats['net_return'],
            'excess_compound': replay_stats['net_return'] - bench_stats['net_return'],
            'strategy_same': strategy_same,
            'bench_same': bench_same,
            'excess_same': excess_same,
            'bench_alignment_gap': (1.0 + bench_stats['net_return']) / (1.0 + bench_same) - 1.0,
            'strategy_start_gap': (1.0 + replay_stats['net_return'])
            / (1.0 + strategy_same) - 1.0,
            'gross_excess_sum': contributions['gross_excess'],
            'avg_positions': float(cast(float, daily['positions'].mean())),
            'sharpe': replay_stats['sharpe'],
            'mdd': replay_stats['max_drawdown'],
            'turnover': prod_stats['turnover_annual'],
            'trades': prod_stats['trades'],
            'skipped': prod_stats['skipped'],
            'days': prod_stats['days'],
            'ic': score['ic'], 'ic_ir': score['ir'], 'ic_positive': score['positive'],
            'beta': beta['beta'], 'r2': beta['r2'],
            'alpha_annual': beta['alpha_annual'],
            'avg_cash_weight': float(cast(float, attribution['cash_weight'].mean())),
            **{f'contrib_{key}': value for key, value in contributions.items()},
            **{f'leader_{key}': value for key, value in leaders.items()},
        })
        attr_row = {'test': f'{start}~{end}', **contributions}
        attribution_rows.append(attr_row)
        if not industries.is_empty():
            agg = industries.group_by('industry').agg(
                pl.col('w_port').mean(), pl.col('w_bench').mean(),
                pl.col('r_port').mean(), pl.col('r_bench').mean(),
                pl.col('alloc').sum(), pl.col('select').sum())
            for row in agg.iter_rows(named=True):
                industry_rows.append({'test': f'{start}~{end}', **row})
        logger.info(f'[diag] {start}~{end} net={replay_stats["net_return"]:.2%} '
                    f'bench={bench_stats["net_return"]:.2%} '
                    f'excess={replay_stats["net_return"] - bench_stats["net_return"]:.2%} '
                    f'IC={score["ic"]:.4f} beta={beta["beta"]:.2f} '
                    f'top20捕获={leaders["top_capture"]:.0%}')

    summary = pl.DataFrame(window_rows)
    returns = summary['net'].to_list()
    shares = _log_shares(returns)
    summary = summary.with_columns(pl.Series('log_share', shares))
    max_excess_index = int(np.argmax(summary['contrib_gross_excess'].to_numpy()))
    max_share_index = int(np.argmax(shares))
    prod_reported = (float((1.0 + summary['net']).product())
                     / float((1.0 + summary['bench_net']).product()))
    prod_same = (float((1.0 + summary['strategy_same']).product())
                 / float((1.0 + summary['bench_same']).product()))
    years = 125.0 * summary.height / 252.0
    reported_excess_annual = prod_reported ** (1.0 / years) - 1.0
    same_excess_annual = prod_same ** (1.0 / years) - 1.0
    max_share_row = summary.row(max_share_index, named=True)
    detail_best = detail_by_window[max_share_row['test']]
    monthly = detail_best.group_by('month').agg(
        pl.col('gross_excess').sum(), pl.col('exec_residual').sum(),
        pl.col('行业配置').sum(), pl.col('行业选股').sum(),
        *[pl.col(STYLE_LABELS[name]).sum() for name in STYLE_COLUMNS],
        pl.col('个股alpha残差').sum()).sort('month')
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    summary.write_csv(out_dir / 'window_summary.csv')
    pl.DataFrame(attribution_rows).write_csv(out_dir / 'attribution_windows.csv')
    pl.DataFrame(industry_rows).write_csv(out_dir / 'industry_windows.csv')
    pl.DataFrame(ic_rows).write_csv(out_dir / 'factor_ic_windows.csv')
    pl.DataFrame(leader_rows).write_csv(out_dir / 'leaders_windows.csv')
    monthly.write_csv(out_dir / 'attribution_best_monthly.csv')
    pl.concat(daily_rows_all).write_csv(out_dir / 'attribution_daily.csv')

    index_windows = _index_windows(
        [Path(value) for value in args.index], windows['windows'])
    diagnostics = {
        'matrix': str(args.matrix),
        'window_file': str(args.windows),
        'tag': args.tag,
        'industry_source': str(args.industry),
        'industry_note': 'TDX 一级行业（13 类，静态快照），非申万一级',
        'style_note': {
            'size': 'ff_size = ln(流通市值)',
            'value': 'BP/EP/营收收益率/股息率 四项 z 均值',
            'invest': 'ff_investment = 总资产同比（成长/投资代理）',
            'momentum': 'momentum_3m',
            'volatility': 'volatility_20',
            'liquidity': 'ext_turnover_20（20 日平均真实换手）',
        },
        'windows': window_rows,
        'max_excess_window': summary.row(max_excess_index, named=True)['test'],
        'max_log_share_window': max_share_row['test'],
        'max_log_share': shares[max_share_index],
        'drop_max_log_share_annualized': _drop_one_annualized(returns, max_share_index),
        'reported_excess_annualized': reported_excess_annual,
        'same_start_excess_annualized': same_excess_annual,
        'alignment_effect_annualized': reported_excess_annual - same_excess_annual,
        'max_same_start_excess_window': summary.row(
            int(np.argmax(summary['excess_same'].to_numpy())), named=True)['test'],
        'all_drop_annualized': [_drop_one_annualized(returns, index)
                                for index in range(len(returns))],
        'index_window_returns': index_windows,
        'sim_verify': verify,
        'attribution_identity_max_error': float(
            pl.DataFrame(attribution_rows).select(
                (pl.col('gross_excess') - pl.col('行业暴露') - pl.col('市值(size)')
                 - pl.col('价值(value)') - pl.col('投资/成长(invest)')
                 - pl.col('动量(momentum)') - pl.col('波动(volatility)')
                 - pl.col('流动性(liquidity)') - pl.col('个股alpha残差')
                 - pl.col('现金/截距')).abs().max()).item()),
        'brinson_identity_max_error': float(
            pl.DataFrame(attribution_rows).select(
                (pl.col('gross_excess') - pl.col('行业配置')
                 - pl.col('行业选股')).abs().max()).item()),
        'elapsed_sec': None,
    }
    (out_dir / 'diagnostics.json').write_text(
        json.dumps(diagnostics, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info(f'[diag] 输出 {out_dir}；最大贡献窗 {max_share_row["test"]}，'
                f'对数收益占比 {shares[max_share_index]:.1%}，'
                f'剔除后年化 {_drop_one_annualized(returns, max_share_index):.2%}')
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='单窗超额归因诊断（只读）')
    parser.add_argument('--matrix', default=str(
        ROOT / 'tmp/kelly_fixed_matrix/matrix_frozen_500.parquet'))
    parser.add_argument('--windows', default=str(
        ROOT / 'tmp/kelly_fixed_matrix/windows_frozen_500.json'))
    parser.add_argument('--industry', default=str(
        ROOT / 'data/local_store/meta/industry/industry_20261104.parquet'))
    parser.add_argument('--index', nargs='*', default=[
        str(ROOT / 'data/local_store/quotes/index/sh/000300.SH.parquet'),
        str(ROOT / 'data/local_store/quotes/index/sh/000905.SH.parquet')])
    parser.add_argument('--tag', default='frozen500')
    parser.add_argument('--out-dir', default=str(ROOT / 'reports/single_window_diag'))
    parser.add_argument('--top-k', type=int, default=20)
    parser.add_argument('--hold', type=int, default=20)
    args = parser.parse_args(argv)
    return run(args)


if __name__ == '__main__':
    raise SystemExit(main())
