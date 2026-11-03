#!/usr/bin/env python3

"""
Fama-French 风格因子 —— 规模 / 价值 / 动量 / 反转 / 波动 / 流动性。

出处：
    - Fama & French (1993), "Common risk factors in the returns on stocks and bonds", JFE；
    - Fama & French (2015), "A five-factor asset pricing model", JFE；
    - Jegadeesh & Titman (1993), "Returns to Buying Winners and Selling Losers", JF；
    - Amihud (2002), "Illiquidity and stock returns", JFM。

口径与限制：
    - 规模（SMB）需要流通股本/总市值，价值（HML）需要净资产（B/P），
      本项目本地库没有股本与财务数据 → 登记为不可用，不做事后拟合；
    - 动量、短期反转、波动、Amihud 非流动性、市场 beta 可由量价数据计算，
      其中 beta 的市场代理为「股票池等权收益」（status=approx）；
    - Amihud 非流动性 ``mean(|ret| / amount)`` 的量纲随 amount 单位（元）走，
      统一乘 1e8 表示「每亿元成交额对应的日绝对收益」。
"""

from __future__ import annotations

import polars as pl

from src.quant.factor_lib import ops as o
from src.quant.factor_lib.registry import factor

SOURCE_FF93 = 'Fama & French (1993), JFE 49(3): 核心三因子定义'
SOURCE_MOM = 'Jegadeesh & Titman (1993), Journal of Finance 48(1): 动量 12-1'
SOURCE_AMIHUD = 'Amihud (2002), Journal of Financial Markets 5(1): 非流动性指标'
C = pl.col
RET = C('ret')


@factor('ff_mom_12_1', 'famafrench',
         'MOM(12-1) = close[t-20] / close[t-250] - 1',
         'close', '+', SOURCE_MOM, lookback=251)
def ff_mom_12_1(_panel: pl.DataFrame) -> pl.Expr:
    """12 个月动量、跳过最近 1 个月（规避短期反转污染）。"""
    return o.safe_div(o.delay(C('close'), 20), o.delay(C('close'), 250)) - 1.0


@factor('ff_reversal_1m', 'famafrench',
         'REV(1M) = -(close / delay(close, 20) - 1)',
         'close', '+', 'Jegadeesh (1990), Journal of Finance 45(3): 短期反转', lookback=21)
def ff_reversal_1m(_panel: pl.DataFrame) -> pl.Expr:
    """近 1 个月反转：跌得多的后续反弹（因子取负收益，数值越大越看多）。"""
    return -1.0 * (o.safe_div(C('close'), o.delay(C('close'), 20)) - 1.0)


@factor('ff_vol_60', 'famafrench',
         'VOL60 = std(ret, 60)', 'close', '-', SOURCE_FF93, lookback=60)
def ff_vol_60(_panel: pl.DataFrame) -> pl.Expr:
    """60 日已实现波动率；低波动异象要求方向为负。"""
    return o.ts_std(RET, 60)


@factor('ff_amihud_60', 'famafrench',
         'ILLIQ60 = mean(|ret| / amount, 60) * 1e8', 'close, amount', '+', SOURCE_AMIHUD,
         lookback=60)
def ff_amihud_60(_panel: pl.DataFrame) -> pl.Expr:
    """Amihud 非流动性：单位成交额引起的价格冲击，数值越大越不流动。"""
    impact = o.safe_div(RET.abs(), C('amount'))
    return o.ts_mean(impact, 60) * 1e8


@factor('ff_beta_60', 'famafrench',
         'BETA60 = cov(ret, mkt_ret, 60) / var(mkt_ret, 60)',
         'close, 股票池等权收益', '+', SOURCE_FF93, lookback=60, status='approx',
         note='市场组合用股票池等权日收益代理，非真实指数')
def ff_beta_60(panel: pl.DataFrame) -> pl.Series:
    """60 日市场 beta（市场代理 = 当日股票池等权收益）。"""
    # 市场收益是横截面均值，必须先落列再进滚动协方差：
    # rolling_cov 不接受带 over 的入参。
    staged = o.stage(panel, _mkt=RET.mean().over(o.CROSS))
    return o.out(
        staged,
        o.safe_div(
            o.covariance(RET, C('_mkt'), 60),
            o.ts_std(C('_mkt'), 60).pow(2),
        ),
    )


@factor('ff_turnover_20', 'famafrench',
         'TURN20 = amount / mean(amount, 20)', 'amount', '?',
         '换手率代理：成交额相对自身 20 日均值的放大倍数（无股本数据，非真实换手率）',
         lookback=20, status='approx', note='缺流通股本，以成交额放大倍数代替换手率')
def ff_turnover_20(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(C('amount'), o.ts_mean(C('amount'), 20))


_UNAVAILABLE = {
    'ff_size': (
        'SIZE = ln(总市值)',
        '总股本/流通股本（本项目本地库无股本数据）',
        '需要每天的总股本或流通股本，本地列存只有行情与复权因子，无法计算',
    ),
    'ff_value_bp': (
        'HML = 账面市值比 (B/P)',
        '净资产、总市值（需要财务报表 + 股本）',
        '需要季度财报的净资产与股本，本地库无财务数据；不得用价格倒推',
    ),
    'ff_value_ep': (
        'E/P = 净利润 / 总市值',
        '净利润（TTM）、总市值',
        '需要财报净利润，本地库无财务数据',
    ),
    'ff_investment': (
        'CMA = 总资产同比增速',
        '资产负债表总资产（连续两期）',
        '需要财报总资产，本地库无财务数据',
    ),
}


def register_unavailable() -> None:
    """登记规模/价值/投资类不可用因子（幂等）。"""
    from src.quant.factor_lib.registry import _REGISTRY
    from src.quant.factor_lib.registry import register_unavailable as _reg

    for name, (expression, deps, note) in _UNAVAILABLE.items():
        if name in _REGISTRY:
            continue
        _reg(
            name, 'famafrench', expression, deps, '+',
            'Fama & French (1993/2015), JFE', note,
        )


__all__ = ['register_unavailable']
