#!/usr/bin/env python3

"""回测可置信性基准（改前 vs 改后）。

用途：
1. 单票 / 组合两条固定策略、固定区间的改前（2 条旧默认规则）与改后
   （补全后的默认规则集 + purge/embargo 样本外）对照；
2. 过户费默认值修正的独立成本影响；
3. ADV（20 日均成交额）容量：不同参与率下单票最多能承接多少资金；
4. 推荐侧成本口径统一前后的往返成本变化。

数据直接读取本地 parquet（hfq 口径），不访问数据库。运行：

    .venv\\Scripts\\python.exe tools/backtest_trust_benchmark.py
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import cast

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.backtest.costs.cost_model import COST_DEFAULTS, CostModel  # noqa: E402
from src.backtest.strategies.base_strategy import BaseStrategy  # noqa: E402
from src.backtest.strategies.ma_strategy import MAStrategy  # noqa: E402
from src.core.usecases import run_backtest  # noqa: E402
from src.risk import (  # noqa: E402
    NoSuspendedTrade,
    PriceLimitTradable,
    RiskEngine,
)

PRICE_COLS = ('open', 'high', 'low', 'close')
STORE = ROOT / 'data/local_store/quotes/stock'
START = date(2021, 1, 1)
END = date(2025, 12, 31)
INITIAL = 1_000_000.0

SINGLE_CODE = '600000.SH'
PORTFOLIO_MAP = {
    '600000.SH': '银行',
    '600036.SH': '银行',
    '601318.SH': '保险',
    '600519.SH': '食品饮料',
    '000858.SZ': '食品饮料',
    '300750.SZ': '电池',
    '000651.SZ': '家电',
}


def _path(code: str) -> Path:
    if code.endswith('.SH'):
        return STORE / 'sh' / f'{code}.parquet'
    if code.endswith('.BJ'):
        return STORE / 'bj' / f'{code}.parquet'
    return STORE / 'sz' / f'{code}.parquet'


def load_hfq(code: str, start: date, end: date) -> pl.DataFrame:
    """读本地原始价并按 hfq_factor 复权，保留 raw_open/raw_close。"""
    raw = pl.read_parquet(_path(code)).with_columns(
        pl.col('date').cast(pl.Date)).filter(
        (pl.col('date') >= start) & (pl.col('date') <= end))
    if raw.is_empty():
        raise RuntimeError(f'{code} 在 {start}~{end} 无本地数据')
    raw = raw.with_columns(pl.col('hfq_factor').fill_null(1.0))
    adjusted = raw.with_columns([
        (pl.col(col) * pl.col('hfq_factor')).alias(col) for col in PRICE_COLS
    ])
    return adjusted.hstack(raw.select([
        pl.col('open').alias('raw_open'),
        pl.col('close').alias('raw_close'),
    ]))


def legacy_engine() -> RiskEngine:
    """改前默认集：仅涨跌停 + 停牌。"""
    return RiskEngine([PriceLimitTradable(), NoSuspendedTrade()])


def _metrics(result: dict) -> dict:
    perf = result.get('performance', {}) or {}
    trades = result.get('trades', []) or []
    initial = float(result.get('initial_capital') or INITIAL)
    notional = sum(abs(float(t.get('price', 0)) * float(t.get('shares', 0)))
                   for t in trades)
    return {
        'total_return': float(result.get('total_return', 0.0) or 0.0),
        'annual_return': float(perf.get('annual_return', 0.0) or 0.0),
        'max_drawdown': float(perf.get('max_drawdown', 0.0) or 0.0),
        'sharpe_ratio': float(perf.get('sharpe_ratio', 0.0) or 0.0),
        'trades': len(trades) // 2,
        'turnover': notional / initial if initial else 0.0,
        'rejected': len(result.get('rejected_orders', []) or []),
    }


def _row(label: str, m: dict) -> str:
    return (f'| {label} | {m["total_return"]:.2f}% | {m["annual_return"]:.2f}% | '
            f'{m["max_drawdown"]:.2f}% | {m["sharpe_ratio"]:.2f} | '
            f'{m["trades"]} | {m["turnover"]:.2f} | {m["rejected"]} |')


def run_single() -> None:
    data = load_hfq(SINGLE_CODE, START, END)
    strategy = MAStrategy(short_window=5, long_window=20)
    before = run_backtest(
        data=data, strategy=strategy, ts_code=SINGLE_CODE,
        initial_capital=INITIAL, risk_engine=legacy_engine(), oos_ratio=0.0,
    )
    after = run_backtest(
        data=data, strategy=MAStrategy(short_window=5, long_window=20),
        ts_code=SINGLE_CODE, initial_capital=INITIAL, oos_ratio=0.3,
    )

    print(f'\n### 单票 {SINGLE_CODE}（MA 5/20，{START}~{END}）\n')
    print('| 口径 | 总收益 | 年化 | 最大回撤 | 夏普 | 交易次数 | 换手(单边/初始资金) | 拒单数 |')
    print('|---|---|---|---|---|---|---|---|')
    print(_row('改前（2 条旧规则，全样本）', _metrics(before)))
    print(_row('改后（全样本，仅供参考）', _metrics(after)))
    oos = after['oos_validation']
    if oos.get('enabled'):
        print(_row('　样本内（purge+embargo）', oos['in_sample']))
        print(_row('　样本外（purge+embargo）', oos['out_of_sample']))
        print(f'\n- 切分：{oos["in_sample_range"]}（{oos["in_sample_days"]} 日）/ '
              f'{oos["out_of_sample_range"]}（{oos["out_of_sample_days"]} 日），'
              f'horizon={oos["horizon"]}、embargo={oos["embargo"]}、'
              f'隔离 {oos["purge_gap_days"]} 日')
    profile = after['risk_profile']
    print(f'- 生效规则：{", ".join(profile["rules"])}')
    print(f'- 生效参数：{profile["effective_limits"]}')
    for note in profile['notes']:
        print(f'- 说明：{note}')


def run_portfolio() -> None:
    data = {code: load_hfq(code, START, END) for code in PORTFOLIO_MAP}
    strategies: dict[str, BaseStrategy] = {
        code: MAStrategy(short_window=5, long_window=20) for code in PORTFOLIO_MAP}
    before = run_backtest(
        data=data, strategies=strategies, industry_map=PORTFOLIO_MAP,
        initial_capital=INITIAL, risk_engine=legacy_engine(), oos_ratio=0.0,
    )
    after = run_backtest(
        data={code: load_hfq(code, START, END) for code in PORTFOLIO_MAP},
        strategies={code: MAStrategy(short_window=5, long_window=20)
                    for code in PORTFOLIO_MAP},
        industry_map=PORTFOLIO_MAP, initial_capital=INITIAL, oos_ratio=0.3,
    )

    print(f'\n### 组合 {len(PORTFOLIO_MAP)} 只（MA 5/20，{START}~{END}）\n')
    print('| 口径 | 总收益 | 年化 | 最大回撤 | 夏普 | 交易次数 | 换手(单边/初始资金) | 拒单数 |')
    print('|---|---|---|---|---|---|---|---|')
    print(_row('改前（2 条旧规则，全样本）', _metrics(before)))
    print(_row('改后（全样本，仅供参考）', _metrics(after)))
    oos = after['oos_validation']
    if oos.get('enabled'):
        print(_row('　样本内（purge+embargo）', oos['in_sample']))
        print(_row('　样本外（purge+embargo）', oos['out_of_sample']))
        print('')
    profile = after['risk_profile']
    print(f'- 生效参数：{profile["effective_limits"]}')
    for note in profile['notes']:
        print(f'- 说明：{note}')


def run_cost_delta() -> None:
    """过户费默认值修正：不传 trade_date 的直接调用 vs 引擎实际走过的路径。"""
    old = CostModel({'transfer_fee_rate': 0.00001})
    new = CostModel()
    old_sell = old.calculate_sell_cost(10_000, 10.0)
    new_sell = new.calculate_sell_cost(10_000, 10.0)
    print('\n### 过户费默认值修正（10,000 股 @ 10 元，卖出单边）\n')
    print('| 口径 | 过户费率 | 卖出总成本(元) | 差额(元) |')
    print('|---|---|---|---|')
    print(f'| 改前（默认 0.00001） | 0.001% | {old_sell:.2f} | - |')
    print(f'| 改后（默认 0.00002） | 0.002% | {new_sell:.2f} | - |')
    print(f'- 单笔差额：{new_sell - old_sell:+.2f} 元（约 1bp）；'
          '10 万元仓位一次往返约多扣 2 元，量级很小。')
    print('- 既有回测数字变化 **0.00pp**：回测引擎撮合始终传 trade_date，'
          '走历史时间表（2015-08-01 起过户费 0.002%），本次只修正了'
          '“不传 trade_date”时的默认值，使其与时间表一致。')


def run_adv_capacity() -> None:
    print('\n### ADV 容量：单票最多能承接多少资金（20 日均成交额 × 参与率）\n')
    print('| 代码 | 行业 | 20 日均成交额(万元) | 5% 参与率(万元) | 10% 参与率(万元) | 20% 参与率(万元) |')
    print('|---|---|---|---|---|---|')
    for code, industry in PORTFOLIO_MAP.items():
        frame = load_hfq(code, START, END)
        last20 = frame.tail(20)['amount']
        avg_amount = float(cast(float, last20.mean() or 0.0)) / 1_0000.0
        print(f'| {code} | {industry} | {avg_amount:,.0f} | '
              f'{avg_amount * 0.05:,.0f} | {avg_amount * 0.10:,.0f} | '
              f'{avg_amount * 0.20:,.0f} |')
    print('\n- 10% 参与率下，单票可承接资金 ≈ 20 日均成交额 × 10%；'
          '初始资金 100 万若超过该值，回测撮合会按 ADV 规则拒单/推荐会裁剪目标权重。')


def run_advice_cost() -> None:
    # 改前口径快照：仅用于「改前 vs 改后」对照展示，不参与任何计算，
    # 也不得被当作现行真源（现行真源是 COST_DEFAULTS）。
    before = {'commission': 0.0003, 'stamp_tax': 0.001, 'impact': 0.001}
    old = 2 * before['commission'] + before['stamp_tax'] + 2 * before['impact']
    new = (2 * COST_DEFAULTS['commission_rate'] + COST_DEFAULTS['stamp_tax_rate']
           + 2 * COST_DEFAULTS['slippage'])
    print('\n### 推荐侧成本口径统一（往返成本率）\n')
    print(f'- 改前：佣金 2×{before["commission"]:.2%} + 印花税 {before["stamp_tax"]:.2%}'
          f' + 冲击 2×{before["impact"]:.2%} = {old:.2%}')
    print(f'- 改后：佣金 2×{COST_DEFAULTS["commission_rate"]:.2%}'
          f' + 印花税 {COST_DEFAULTS["stamp_tax_rate"]:.2%}'
          f' + 冲击 2×{COST_DEFAULTS["slippage"]:.2%} = {new:.2%}')
    print(f'- 每完成一次买入+卖出，净收益口径相差 {(old - new) * 100:.2f} 个百分点；'
          f'10 万元仓位约差 {(old - new) * 100_000:.0f} 元。')


def main() -> None:
    print('# 回测可置信性基准（改前 / 改后）')
    print(f'\n- 区间：{START} ~ {END}')
    print(f'- 初始资金：{INITIAL:,.0f} 元；策略：MA(5, 20)；数据：本地 hfq')
    print('- 改前 = 仅涨跌停/停牌两条旧默认规则；改后 = 补全默认规则集 + 样本外')
    run_single()
    run_portfolio()
    run_cost_delta()
    run_adv_capacity()
    run_advice_cost()


if __name__ == '__main__':
    main()
