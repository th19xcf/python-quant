#!/usr/bin/env python3

"""
WorldQuant Alpha101 —— 公开公式的可核验子集。

出处：Kakushadze, Z. (2016). *101 Formulaic Alphas*. arXiv:1601.00991.
公式原文来自论文 Appendix A（Alpha#1 ~ Alpha#101），逐条抄录在每项的
``expression`` 字段里，可直接对照论文核验。

实现方式（polars 窗口语义决定）：
    rolling_cov 不能接收带 ``.over()`` 的表达式，横截面 ``.over('date')``
    也不能套在时序 ``.over('ts_code')`` 外层（会静默变 null）。因此公式按
    「时序结果先落列 → 再做横截面排名 → 再做相关」分阶段求值，
    见 ``ops.stage`` / ``ops.out`` 的说明。这不改变公式本身。

本项目落地时的降级项（status='approx'）：
    - ``vwap``：论文用日内成交量加权均价，本项目只有日频数据，
      取 ``amount / (volume * 100) * hfq_factor``（当日成交均价）；
    - ``advN``：N 日平均成交金额，取 ``mean(amount, N)``；
    - ``indneutralize``：无行业维表，退化为横截面去均值；
    - ``rank``：横截面分位（1-based rank / 非空数量），取值 (0, 1]；
    - 依赖 ``ts_argmax`` / ``ts_argmin`` 的 Alpha#1/#57/#60/#96 等不实现，
      在登记表标为不可用。
"""

from __future__ import annotations

import polars as pl

from src.quant.factor_lib import ops as o
from src.quant.factor_lib.registry import factor

SOURCE = 'Kakushadze (2016) 101 Formulaic Alphas, arXiv:1601.00991, {tag}'
C = pl.col
RET = C('ret')


def adv(n: int) -> pl.Expr:
    """N 日平均成交金额（元），对应论文 advN。"""
    return o.ts_mean(C('amount'), n)


def rank_of(frame: pl.DataFrame, name: str, expr: pl.Expr) -> pl.DataFrame:
    """先把时序结果落列，再做横截面排名（绕开 polars 窗口嵌套限制）。"""
    raw = f'{name}__raw'
    staged = o.stage(frame, **{raw: expr})
    return o.stage(staged, **{name: o.rank_cs(pl.col(raw))})


# ----------------------------------------------------------------------
# Alpha#2 ~ Alpha#10
# ----------------------------------------------------------------------

@factor('a101_002', 'alpha101',
         '-1 * correlation(rank(delta(log(volume), 2)), rank((close - open) / open), 6)',
         'open, close, volume', '-', SOURCE.format(tag='Alpha#2'), lookback=8)
def a101_002(panel: pl.DataFrame) -> pl.Series:
    frame = rank_of(panel, 'r1', o.delta(o.log(C('volume')), 2))
    frame = rank_of(frame, 'r2', o.safe_div(C('close') - C('open'), C('open')))
    return o.out(frame, -1.0 * o.corr(C('r1'), C('r2'), 6))


@factor('a101_003', 'alpha101',
         '-1 * correlation(rank(open), rank(volume), 10)',
         'open, volume', '-', SOURCE.format(tag='Alpha#3'), lookback=10)
def a101_003(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, r1=o.rank_cs(C('open')), r2=o.rank_cs(C('volume')))
    return o.out(frame, -1.0 * o.corr(C('r1'), C('r2'), 10))


@factor('a101_004', 'alpha101', '-1 * ts_rank(rank(low), 9)',
         'low', '-', SOURCE.format(tag='Alpha#4'), lookback=9)
def a101_004(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, r=o.rank_cs(C('low')))
    return o.out(frame, -1.0 * o.ts_rank(C('r'), 9))


@factor('a101_005', 'alpha101',
         'rank((open - (sum(vwap, 10) / 10))) * (-1 * abs(rank((close - vwap))))',
         'open, close, vwap', '?', SOURCE.format(tag='Alpha#5'), lookback=10,
         status='approx', note='vwap 为日频成交均价近似')
def a101_005(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, s=o.ts_sum(C('vwap'), 10) / 10.0)
    frame = o.stage(frame, r1=o.rank_cs(C('open') - C('s')))
    frame = o.stage(frame, r2=o.rank_cs(C('close') - C('vwap')))
    return o.out(frame, C('r1') * (-1.0 * C('r2').abs()))


@factor('a101_006', 'alpha101', '-1 * correlation(open, volume, 10)',
         'open, volume', '-', SOURCE.format(tag='Alpha#6'), lookback=10)
def a101_006(panel: pl.DataFrame) -> pl.Series:
    return o.out(panel, -1.0 * o.corr(C('open'), C('volume'), 10))


@factor('a101_007', 'alpha101',
         '(adv20 < volume) ? (-1 * ts_rank(abs(delta(close, 7)), 60) * '
         'sign(delta(close, 7))) : -1',
         'close, amount', '?', SOURCE.format(tag='Alpha#7'), lookback=60,
         status='approx', note='adv20 用成交额均值，与当日成交额比较')
def a101_007(panel: pl.DataFrame) -> pl.Series:
    delta_close = o.delta(C('close'), 7)
    value = -1.0 * o.ts_rank(delta_close.abs(), 60) * o.sign(delta_close)
    return o.out(panel, pl.when(adv(20) < C('amount')).then(value).otherwise(-1.0))


@factor('a101_008', 'alpha101',
         '-1 * rank((sum(open, 5) * sum(returns, 5)) - '
         'delay((sum(open, 5) * sum(returns, 5)), 10))',
         'open, close', '-', SOURCE.format(tag='Alpha#8'), lookback=15)
def a101_008(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, combined=o.ts_sum(C('open'), 5) * o.ts_sum(RET, 5))
    frame = o.stage(frame, v=C('combined') - o.delay(C('combined'), 10))
    return o.out(frame, -1.0 * o.rank_cs(C('v')))


@factor('a101_009', 'alpha101',
         '(0 < ts_min(delta(close, 1), 5)) ? delta(close, 1) : '
         '((ts_max(delta(close, 1), 5) < 0) ? delta(close, 1) : -1 * delta(close, 1))',
         'close', '?', SOURCE.format(tag='Alpha#9'), lookback=6)
def a101_009(panel: pl.DataFrame) -> pl.Series:
    delta_close = o.delta(C('close'), 1)
    up = o.ts_min(delta_close, 5) > 0
    down = o.ts_max(delta_close, 5) < 0
    return o.out(panel, pl.when(up | down).then(delta_close).otherwise(-1.0 * delta_close))


@factor('a101_010', 'alpha101',
         'rank(((0 < ts_min(delta(close, 1), 4)) ? delta(close, 1) : '
         '((ts_max(delta(close, 1), 4) < 0) ? delta(close, 1) : -1 * delta(close, 1))))',
         'close', '?', SOURCE.format(tag='Alpha#10'), lookback=5)
def a101_010(panel: pl.DataFrame) -> pl.Series:
    delta_close = o.delta(C('close'), 1)
    up = o.ts_min(delta_close, 4) > 0
    down = o.ts_max(delta_close, 4) < 0
    frame = o.stage(panel, d=pl.when(up | down).then(delta_close)
                    .otherwise(-1.0 * delta_close))
    return o.out(frame, o.rank_cs(C('d')))


# ----------------------------------------------------------------------
# Alpha#11 ~ Alpha#20
# ----------------------------------------------------------------------

@factor('a101_011', 'alpha101',
         '(rank(ts_max(vwap - close, 3)) + rank(ts_min(vwap - close, 3))) * '
         'rank(delta(volume, 3))',
         'close, volume, vwap', '?', SOURCE.format(tag='Alpha#11'), lookback=3,
         status='approx', note='vwap 为日频成交均价近似')
def a101_011(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, gap=C('vwap') - C('close'))
    frame = rank_of(frame, 'r1', o.ts_max(C('gap'), 3))
    frame = rank_of(frame, 'r2', o.ts_min(C('gap'), 3))
    frame = rank_of(frame, 'r3', o.delta(C('volume'), 3))
    return o.out(frame, (C('r1') + C('r2')) * C('r3'))


@factor('a101_012', 'alpha101',
         'sign(delta(volume, 1)) * (-1 * delta(close, 1))',
         'close, volume', '?', SOURCE.format(tag='Alpha#12'), lookback=2)
def a101_012(panel: pl.DataFrame) -> pl.Series:
    return o.out(panel, o.sign(o.delta(C('volume'), 1)) * (-1.0 * o.delta(C('close'), 1)))


@factor('a101_013', 'alpha101',
         '-1 * rank(covariance(rank(close), rank(volume), 5))',
         'close, volume', '-', SOURCE.format(tag='Alpha#13'), lookback=5)
def a101_013(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, r1=o.rank_cs(C('close')), r2=o.rank_cs(C('volume')))
    frame = o.stage(frame, cov=o.covariance(C('r1'), C('r2'), 5))
    return o.out(frame, -1.0 * o.rank_cs(C('cov')))


@factor('a101_014', 'alpha101',
         '(-1 * rank(delta(returns, 3))) * correlation(open, volume, 10)',
         'open, volume, close', '-', SOURCE.format(tag='Alpha#14'), lookback=10)
def a101_014(panel: pl.DataFrame) -> pl.Series:
    frame = rank_of(panel, 'r1', o.delta(RET, 3))
    frame = o.stage(frame, c=o.corr(C('open'), C('volume'), 10))
    return o.out(frame, -1.0 * C('r1') * C('c'))


@factor('a101_015', 'alpha101',
         '-1 * sum(rank(correlation(rank(high), rank(volume), 3)), 3)',
         'high, volume', '-', SOURCE.format(tag='Alpha#15'), lookback=5)
def a101_015(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, r1=o.rank_cs(C('high')), r2=o.rank_cs(C('volume')))
    frame = o.stage(frame, c=o.corr(C('r1'), C('r2'), 3))
    frame = o.stage(frame, rc=o.rank_cs(C('c')))
    return o.out(frame, -1.0 * o.ts_sum(C('rc'), 3))


@factor('a101_016', 'alpha101',
         '-1 * rank(covariance(rank(high), rank(volume), 5))',
         'high, volume', '-', SOURCE.format(tag='Alpha#16'), lookback=5)
def a101_016(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, r1=o.rank_cs(C('high')), r2=o.rank_cs(C('volume')))
    frame = o.stage(frame, cov=o.covariance(C('r1'), C('r2'), 5))
    return o.out(frame, -1.0 * o.rank_cs(C('cov')))


@factor('a101_017', 'alpha101',
         '(-1 * rank(ts_rank(close, 10))) * rank(delta(delta(close, 1), 1)) * '
         'rank(ts_rank((volume / adv20), 5))',
         'close, volume, amount', '-', SOURCE.format(tag='Alpha#17'), lookback=20,
         status='approx', note='adv20 用成交额均值近似')
def a101_017(panel: pl.DataFrame) -> pl.Series:
    frame = rank_of(panel, 'r1', o.ts_rank(C('close'), 10))
    frame = rank_of(frame, 'r2', o.delta(o.delta(C('close'), 1), 1))
    frame = rank_of(frame, 'r3', o.ts_rank(o.safe_div(C('volume'), adv(20)), 5))
    return o.out(frame, -1.0 * C('r1') * C('r2') * C('r3'))


@factor('a101_018', 'alpha101',
         '-1 * rank((stddev(abs(close - open), 5) + (close - open)) + '
         'correlation(close, open, 10))',
         'open, close', '-', SOURCE.format(tag='Alpha#18'), lookback=10)
def a101_018(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, gap=C('close') - C('open'))
    frame = o.stage(frame, sd=o.ts_std(C('gap').abs(), 5),
                    c=o.corr(C('close'), C('open'), 10))
    return o.out(frame, -1.0 * o.rank_cs(C('sd') + C('gap') + C('c')))


@factor('a101_019', 'alpha101',
         '(-1 * sign((close - delay(close, 7)) + delta(close, 7))) * '
         '(1 + rank(1 + sum(returns, 250)))',
         'close', '-', SOURCE.format(tag='Alpha#19'), lookback=250)
def a101_019(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, momentum=C('close') - o.delay(C('close'), 7)
                    + o.delta(C('close'), 7), s=o.ts_sum(RET, 250))
    frame = o.stage(frame, r=o.rank_cs(1.0 + C('s')))
    return o.out(frame, -1.0 * o.sign(C('momentum')) * (1.0 + C('r')))


@factor('a101_020', 'alpha101',
         '(-1 * rank(open - delay(high, 1))) * rank(open - delay(close, 1)) * '
         'rank(open - delay(low, 1))',
         'open, high, low, close', '-', SOURCE.format(tag='Alpha#20'), lookback=2)
def a101_020(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel,
                    g1=C('open') - o.delay(C('high'), 1),
                    g2=C('open') - o.delay(C('close'), 1),
                    g3=C('open') - o.delay(C('low'), 1))
    frame = o.stage(frame, r1=o.rank_cs(C('g1')), r2=o.rank_cs(C('g2')),
                    r3=o.rank_cs(C('g3')))
    return o.out(frame, -1.0 * C('r1') * C('r2') * C('r3'))


# ----------------------------------------------------------------------
# Alpha#21 ~ Alpha#30
# ----------------------------------------------------------------------

@factor('a101_021', 'alpha101',
         '(((sum(close, 8) / 8) + stddev(close, 8)) < (sum(close, 2) / 2)) ? -1 : '
         '((((sum(close, 2) / 2) < ((sum(close, 8) / 8) - stddev(close, 8))) ? 1 : '
         '((1 < (volume / adv20)) || ((volume / adv20) == 1) ? 1 : -1))))',
         'close, volume, amount', '?', SOURCE.format(tag='Alpha#21'), lookback=20,
         status='approx', note='adv20 用成交额均值近似')
def a101_021(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel,
                    mean8=o.ts_sum(C('close'), 8) / 8.0,
                    mean2=o.ts_sum(C('close'), 2) / 2.0,
                    std8=o.ts_std(C('close'), 8),
                    ratio=o.safe_div(C('volume'), adv(20)))
    return o.out(frame, pl.when(C('mean8') + C('std8') < C('mean2')).then(-1.0)
                 .when(C('mean2') < C('mean8') - C('std8')).then(1.0)
                 .when(C('ratio') >= 1.0).then(1.0).otherwise(-1.0))


@factor('a101_022', 'alpha101',
         '-1 * (delta(correlation(high, volume, 5), 5) * rank(stddev(close, 20)))',
         'high, close, volume', '-', SOURCE.format(tag='Alpha#22'), lookback=20)
def a101_022(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, c=o.corr(C('high'), C('volume'), 5))
    frame = o.stage(frame, dc=o.delta(C('c'), 5))
    frame = rank_of(frame, 'rs', o.ts_std(C('close'), 20))
    return o.out(frame, -1.0 * C('dc') * C('rs'))


@factor('a101_023', 'alpha101',
         '((sum(high, 20) / 20) < high) ? (-1 * delta(high, 2)) : 0',
         'high', '?', SOURCE.format(tag='Alpha#23'), lookback=20)
def a101_023(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, mean20=o.ts_sum(C('high'), 20) / 20.0,
                    d2=o.delta(C('high'), 2))
    return o.out(frame, pl.when(C('mean20') < C('high')).then(-1.0 * C('d2'))
                 .otherwise(0.0))


@factor('a101_024', 'alpha101',
         '((delta(sum(close, 100) / 100, 100) / delay(close, 100)) <= 0.05) ? '
         '(-1 * (close - ts_min(close, 100))) : (-1 * delta(close, 3))',
         'close', '?', SOURCE.format(tag='Alpha#24'), lookback=200)
def a101_024(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(
        panel,
        trend=o.safe_div(
            o.delta(o.ts_sum(C('close'), 100) / 100.0, 100), o.delay(C('close'), 100)
        ),
        dmin=C('close') - o.ts_min(C('close'), 100),
        d3=o.delta(C('close'), 3),
    )
    return o.out(frame, pl.when(C('trend') <= 0.05).then(-1.0 * C('dmin'))
                 .otherwise(-1.0 * C('d3')))


@factor('a101_026', 'alpha101',
         '-1 * ts_max(correlation(ts_rank(volume, 5), ts_rank(high, 5), 5), 3)',
         'high, volume', '-', SOURCE.format(tag='Alpha#26'), lookback=10)
def a101_026(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, tv=o.ts_rank(C('volume'), 5),
                    th=o.ts_rank(C('high'), 5))
    frame = o.stage(frame, c=o.corr(C('tv'), C('th'), 5))
    return o.out(frame, -1.0 * o.ts_max(C('c'), 3))


@factor('a101_028', 'alpha101',
         'scale((correlation(adv20, low, 5) + ((high + low) / 2)) - close)',
         'high, low, close, amount', '?', SOURCE.format(tag='Alpha#28'), lookback=20,
         status='approx', note='adv20 用成交额均值近似')
def a101_028(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, c=o.corr(adv(20), C('low'), 5))
    return o.out(frame, o.scale(C('c') + (C('high') + C('low')) / 2.0 - C('close')))


@factor('a101_030', 'alpha101',
         '((1 - rank(((sign(close - delay(close, 1)) + sign(delay(close, 1) - '
         'delay(close, 2))) + sign(delay(close, 2) - delay(close, 3))))) * sum(volume, 5)) / '
         'sum(volume, 20)',
         'close, volume', '?', SOURCE.format(tag='Alpha#30'), lookback=20)
def a101_030(panel: pl.DataFrame) -> pl.Series:
    streak = (o.sign(C('close') - o.delay(C('close'), 1))
              + o.sign(o.delay(C('close'), 1) - o.delay(C('close'), 2))
              + o.sign(o.delay(C('close'), 2) - o.delay(C('close'), 3)))
    frame = o.stage(panel, streak=streak, v5=o.ts_sum(C('volume'), 5),
                    v20=o.ts_sum(C('volume'), 20))
    frame = o.stage(frame, r=o.rank_cs(C('streak')))
    return o.out(frame, o.safe_div((1.0 - C('r')) * C('v5'), C('v20')))


# ----------------------------------------------------------------------
# Alpha#31 ~ Alpha#40
# ----------------------------------------------------------------------

@factor('a101_031', 'alpha101',
         '(rank(rank(rank(decay_linear(-1 * rank(rank(delta(close, 10))), 10)))) + '
         'rank(-1 * delta(close, 3))) + sign(scale(correlation(adv20, low, 12)))',
         'close, low, amount', '-', SOURCE.format(tag='Alpha#31'), lookback=40,
         status='approx', note='adv20 用成交额均值近似')
def a101_031(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, d10=o.delta(C('close'), 10), d3=o.delta(C('close'), 3),
                    c=o.corr(adv(20), C('low'), 12))
    frame = o.stage(frame, r1=o.rank_cs(C('d10')))
    frame = o.stage(frame, r2=o.rank_cs(C('r1')))
    frame = o.stage(frame, inner=-1.0 * C('r2'))
    frame = o.stage(frame, dec=o.decay_linear(C('inner'), 10))
    frame = o.stage(frame, r3=o.rank_cs(C('dec')))
    frame = o.stage(frame, r4=o.rank_cs(C('r3')), r5=o.rank_cs(-1.0 * C('d3')),
                    sc=o.scale(C('c')))
    frame = o.stage(frame, r6=o.rank_cs(C('r4')))
    return o.out(frame, C('r6') + C('r5') + o.sign(C('sc')))


@factor('a101_032', 'alpha101',
         'scale((sum(close, 7) / 7) - close) + '
         '(20 * scale(correlation(vwap, delay(close, 5), 230)))',
         'close, vwap', '?', SOURCE.format(tag='Alpha#32'), lookback=230,
         status='approx', note='vwap 为日频成交均价近似')
def a101_032(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, a=o.ts_sum(C('close'), 7) / 7.0 - C('close'),
                    c=o.corr(C('vwap'), o.delay(C('close'), 5), 230))
    return o.out(frame, o.scale(C('a')) + 20.0 * o.scale(C('c')))


@factor('a101_033', 'alpha101',
         'rank(-1 * ((1 - (open / close))^1))',
         'open, close', '-', SOURCE.format(tag='Alpha#33'), lookback=1)
def a101_033(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, v=-1.0 * (1.0 - o.safe_div(C('open'), C('close'))))
    return o.out(frame, o.rank_cs(C('v')))


@factor('a101_034', 'alpha101',
         'rank((1 - rank(stddev(returns, 2) / stddev(returns, 5))) + '
         '(1 - rank(delta(close, 1))))',
         'close', '?', SOURCE.format(tag='Alpha#34'), lookback=5)
def a101_034(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, ratio=o.safe_div(o.ts_std(RET, 2), o.ts_std(RET, 5)),
                    d1=o.delta(C('close'), 1))
    frame = o.stage(frame, r1=o.rank_cs(C('ratio')), r2=o.rank_cs(C('d1')))
    return o.out(frame, o.rank_cs((1.0 - C('r1')) + (1.0 - C('r2'))))


@factor('a101_035', 'alpha101',
         '(ts_rank(volume, 32) * (1 - ts_rank((close + high) - low, 16))) * '
         '(1 - ts_rank(returns, 32))',
         'close, high, low, volume', '?', SOURCE.format(tag='Alpha#35'), lookback=32)
def a101_035(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, a=o.ts_rank(C('volume'), 32),
                    b=o.ts_rank((C('close') + C('high')) - C('low'), 16),
                    c=o.ts_rank(RET, 32))
    return o.out(frame, C('a') * (1.0 - C('b')) * (1.0 - C('c')))


@factor('a101_036', 'alpha101',
         '2.21 * rank(correlation(close - open, delay(volume, 1), 15)) + '
         '0.7 * rank(open - close) + 0.73 * rank(ts_rank(delay(-1 * returns, 6), 5)) + '
         'rank(abs(correlation(vwap, adv20, 6))) + '
         '0.6 * rank((sum(close, 200) / 200 - open) * (close - open))',
         'open, close, volume, amount, vwap', '?', SOURCE.format(tag='Alpha#36'),
         lookback=200, status='approx', note='vwap / adv20 均为日频近似')
def a101_036(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel,
                    c1=o.corr(C('close') - C('open'), o.delay(C('volume'), 1), 15),
                    c2=o.corr(C('vwap'), adv(20), 6),
                    x3=o.ts_rank(o.delay(-1.0 * RET, 6), 5),
                    x5=(o.ts_sum(C('close'), 200) / 200.0 - C('open'))
                    * (C('close') - C('open')))
    frame = o.stage(frame, r1=o.rank_cs(C('c1')), r2=o.rank_cs(C('open') - C('close')),
                    r3=o.rank_cs(C('x3')), r4=o.rank_cs(C('c2').abs()),
                    r5=o.rank_cs(C('x5')))
    return o.out(frame, 2.21 * C('r1') + 0.7 * C('r2') + 0.73 * C('r3')
                 + C('r4') + 0.6 * C('r5'))


@factor('a101_037', 'alpha101',
         'rank(correlation(delay(open - close, 1), close, 200)) + rank(open - close)',
         'open, close', '-', SOURCE.format(tag='Alpha#37'), lookback=201)
def a101_037(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, c=o.corr(o.delay(C('open') - C('close'), 1), C('close'), 200))
    frame = o.stage(frame, r1=o.rank_cs(C('c')),
                    r2=o.rank_cs(C('open') - C('close')))
    return o.out(frame, C('r1') + C('r2'))


@factor('a101_038', 'alpha101',
         '(-1 * rank(ts_rank(close, 10))) * rank(close / open)',
         'open, close', '-', SOURCE.format(tag='Alpha#38'), lookback=10)
def a101_038(panel: pl.DataFrame) -> pl.Series:
    frame = rank_of(panel, 'r1', o.ts_rank(C('close'), 10))
    frame = o.stage(frame, r2=o.rank_cs(o.safe_div(C('close'), C('open'))))
    return o.out(frame, -1.0 * C('r1') * C('r2'))


@factor('a101_039', 'alpha101',
         '(-1 * rank(delta(close, 7) * (1 - rank(decay_linear(volume / adv20, 9))))) * '
         '(1 + rank(sum(returns, 250)))',
         'close, volume, amount', '-', SOURCE.format(tag='Alpha#39'), lookback=250,
         status='approx', note='adv20 用成交额均值近似')
def a101_039(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, dec=o.decay_linear(o.safe_div(C('volume'), adv(20)), 9),
                    d7=o.delta(C('close'), 7), s=o.ts_sum(RET, 250))
    frame = o.stage(frame, rd=o.rank_cs(C('dec')), rs=o.rank_cs(C('s')))
    frame = o.stage(frame, v=C('d7') * (1.0 - C('rd')))
    return o.out(frame, -1.0 * o.rank_cs(C('v')) * (1.0 + C('rs')))


@factor('a101_040', 'alpha101',
         '(-1 * rank(stddev(high, 10))) * correlation(high, volume, 10)',
         'high, volume', '-', SOURCE.format(tag='Alpha#40'), lookback=10)
def a101_040(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, sd=o.ts_std(C('high'), 10),
                    c=o.corr(C('high'), C('volume'), 10))
    frame = o.stage(frame, r=o.rank_cs(C('sd')))
    return o.out(frame, -1.0 * C('r') * C('c'))


# ----------------------------------------------------------------------
# Alpha#41 ~ Alpha#50
# ----------------------------------------------------------------------

@factor('a101_041', 'alpha101', '(((high * low)^0.5) - vwap)',
         'high, low, vwap', '?', SOURCE.format(tag='Alpha#41'), lookback=1,
         status='approx', note='vwap 为日频成交均价近似')
def a101_041(panel: pl.DataFrame) -> pl.Series:
    return o.out(panel, (C('high') * C('low')).sqrt() - C('vwap'))


@factor('a101_042', 'alpha101',
         'rank(vwap - close) / rank(vwap + close)', 'close, vwap', '?',
         SOURCE.format(tag='Alpha#42'), lookback=1, status='approx',
         note='vwap 为日频成交均价近似')
def a101_042(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, r1=o.rank_cs(C('vwap') - C('close')),
                    r2=o.rank_cs(C('vwap') + C('close')))
    return o.out(frame, o.safe_div(C('r1'), C('r2')))


@factor('a101_043', 'alpha101',
         'ts_rank(volume / adv20, 20) * ts_rank(-1 * delta(close, 7), 8)',
         'close, volume, amount', '?', SOURCE.format(tag='Alpha#43'), lookback=20,
         status='approx', note='adv20 用成交额均值近似')
def a101_043(panel: pl.DataFrame) -> pl.Series:
    return o.out(panel, o.ts_rank(o.safe_div(C('volume'), adv(20)), 20)
                 * o.ts_rank(-1.0 * o.delta(C('close'), 7), 8))


@factor('a101_044', 'alpha101', '-1 * correlation(high, rank(volume), 5)',
         'high, volume', '-', SOURCE.format(tag='Alpha#44'), lookback=5)
def a101_044(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, rv=o.rank_cs(C('volume')))
    return o.out(frame, -1.0 * o.corr(C('high'), C('rv'), 5))


@factor('a101_045', 'alpha101',
         '-1 * ((rank(sum(delay(close, 5), 20) / 20) * correlation(close, volume, 2)) * '
         'rank(correlation(sum(close, 5), sum(close, 20), 2)))',
         'close, volume', '-', SOURCE.format(tag='Alpha#45'), lookback=25)
def a101_045(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, s5=o.ts_sum(C('close'), 5), s20=o.ts_sum(C('close'), 20),
                    lag=o.ts_sum(o.delay(C('close'), 5), 20) / 20.0,
                    c1=o.corr(C('close'), C('volume'), 2))
    frame = o.stage(frame, c2=o.corr(C('s5'), C('s20'), 2))
    frame = o.stage(frame, r1=o.rank_cs(C('lag')), r2=o.rank_cs(C('c2')))
    return o.out(frame, -1.0 * C('r1') * C('c1') * C('r2'))


@factor('a101_046', 'alpha101',
         '((0.25 < (((delay(close, 20) - delay(close, 10)) / 10) - '
         '((delay(close, 10) - close) / 10))) ? -1 : '
         '((((((delay(close, 20) - delay(close, 10)) / 10) - '
         '((delay(close, 10) - close) / 10)) < 0) ? 1 : (-1 * (close - delay(close, 1)))))',
         'close', '?', SOURCE.format(tag='Alpha#46'), lookback=21)
def a101_046(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, trend=(o.delay(C('close'), 20) - o.delay(C('close'), 10)) / 10.0
                    - (o.delay(C('close'), 10) - C('close')) / 10.0,
                    d1=C('close') - o.delay(C('close'), 1))
    return o.out(frame, pl.when(C('trend') > 0.25).then(-1.0)
                 .when(C('trend') < 0).then(1.0).otherwise(-1.0 * C('d1')))


@factor('a101_047', 'alpha101',
         '((rank(1 / close) * volume) / adv20) * '
         '((high * rank(high - close)) / (sum(high, 5) / 5)) - '
         'rank(vwap - delay(vwap, 5))',
         'close, high, volume, amount, vwap', '?', SOURCE.format(tag='Alpha#47'),
         lookback=20, status='approx', note='vwap / adv20 均为日频近似')
def a101_047(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, s5h=o.ts_sum(C('high'), 5) / 5.0,
                    dvwap=C('vwap') - o.delay(C('vwap'), 5))
    frame = o.stage(frame, r1=o.rank_cs(o.safe_div(pl.lit(1.0), C('close'))),
                    r2=o.rank_cs(C('high') - C('close')),
                    r3=o.rank_cs(C('dvwap')))
    return o.out(frame, o.safe_div(C('r1') * C('volume'), adv(20))
                 * o.safe_div(C('high') * C('r2'), C('s5h')) - C('r3'))


@factor('a101_048', 'alpha101',
         'indneutralize(correlation(delta(close, 1), delta(delay(close, 1), 1), 250) * '
         'delta(close, 1) / close) / sum((delta(close, 1) / delay(close, 1))^2, 250)',
         'close', '?', SOURCE.format(tag='Alpha#48'), lookback=250, status='approx',
         note='indneutralize 无行业维表，退化为横截面去均值')
def a101_048(panel: pl.DataFrame) -> pl.Series:
    delta_close = o.delta(C('close'), 1)
    frame = o.stage(panel, d1=delta_close,
                    c=o.corr(delta_close, o.delta(o.delay(C('close'), 1), 1), 250),
                    den=o.ts_sum(o.safe_div(delta_close, o.delay(C('close'), 1)).pow(2), 250))
    frame = o.stage(frame, x=C('c') * o.safe_div(C('d1'), C('close')))
    frame = o.stage(frame, neutral=o.ind_neutral(C('x')))
    return o.out(frame, o.safe_div(C('neutral'), C('den')))


@factor('a101_049', 'alpha101',
         '(((((delay(close, 20) - delay(close, 10)) / 10) - '
         '((delay(close, 10) - close) / 10)) < (-1 * 0.05)) ? 1 : '
         '(-1 * (close - delay(close, 1))))',
         'close', '?', SOURCE.format(tag='Alpha#49'), lookback=21)
def a101_049(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, trend=(o.delay(C('close'), 20) - o.delay(C('close'), 10)) / 10.0
                    - (o.delay(C('close'), 10) - C('close')) / 10.0,
                    d1=C('close') - o.delay(C('close'), 1))
    return o.out(frame, pl.when(C('trend') < -0.05).then(1.0).otherwise(-1.0 * C('d1')))


@factor('a101_050', 'alpha101',
         '-1 * ts_max(rank(correlation(rank(volume), rank(vwap), 5)), 5)',
         'volume, vwap', '-', SOURCE.format(tag='Alpha#50'), lookback=9,
         status='approx', note='vwap 为日频成交均价近似')
def a101_050(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, rv=o.rank_cs(C('volume')), rw=o.rank_cs(C('vwap')))
    frame = o.stage(frame, c=o.corr(C('rv'), C('rw'), 5))
    frame = o.stage(frame, rc=o.rank_cs(C('c')))
    return o.out(frame, -1.0 * o.ts_max(C('rc'), 5))


# ----------------------------------------------------------------------
# Alpha#53 ~ Alpha#65
# ----------------------------------------------------------------------

@factor('a101_053', 'alpha101',
         '-1 * delta(((close - low) - (high - close)) / (close - low), 9)',
         'close, high, low', '-', SOURCE.format(tag='Alpha#53'), lookback=10)
def a101_053(panel: pl.DataFrame) -> pl.Series:
    return o.out(panel, -1.0 * o.delta(
        o.safe_div((C('close') - C('low')) - (C('high') - C('close')),
                   C('close') - C('low')), 9))


@factor('a101_054', 'alpha101',
         '-1 * ((low - close) * (open^5)) / ((low - high) * (close^5))',
         'open, high, low, close', '-', SOURCE.format(tag='Alpha#54'), lookback=1)
def a101_054(panel: pl.DataFrame) -> pl.Series:
    return o.out(panel, -1.0 * o.safe_div(
        (C('low') - C('close')) * C('open').pow(5),
        (C('low') - C('high')) * C('close').pow(5)))


@factor('a101_055', 'alpha101',
         '-1 * correlation(rank((close - ts_min(low, 12)) / '
         '(ts_max(high, 12) - ts_min(low, 12))), rank(volume), 6)',
         'close, high, low, volume', '-', SOURCE.format(tag='Alpha#55'), lookback=17)
def a101_055(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel,
                    pos=o.safe_div(C('close') - o.ts_min(C('low'), 12),
                                   o.ts_max(C('high'), 12) - o.ts_min(C('low'), 12)))
    frame = o.stage(frame, rp=o.rank_cs(C('pos')), rv=o.rank_cs(C('volume')))
    return o.out(frame, -1.0 * o.corr(C('rp'), C('rv'), 6))


@factor('a101_065', 'alpha101',
         '(-1 * rank(correlation(open * 0.008695 + vwap * 0.991305, adv60, 9))) * '
         'rank(open - vwap)',
         'open, vwap, amount', '-', SOURCE.format(tag='Alpha#65'), lookback=60,
         status='approx', note='vwap 为日频成交均价近似')
def a101_065(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, blended=C('open') * 0.008695 + C('vwap') * 0.991305)
    frame = o.stage(frame, c=o.corr(C('blended'), adv(60), 9))
    frame = o.stage(frame, r1=o.rank_cs(C('c')), r2=o.rank_cs(C('open') - C('vwap')))
    return o.out(frame, -1.0 * C('r1') * C('r2'))


# ----------------------------------------------------------------------
# Alpha#71 ~ Alpha#84
# ----------------------------------------------------------------------

@factor('a101_071', 'alpha101',
         'max(ts_rank(decay_linear(correlation(ts_rank(close, 3), ts_rank(adv180, 12), 18), 4), 15), '
         'ts_rank(decay_linear(rank((low + open) - (vwap + vwap)), 16), 4))',
         'close, low, open, vwap, amount', '?', SOURCE.format(tag='Alpha#71'),
         lookback=180, status='approx', note='vwap / adv180 均为日频近似')
def a101_071(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel,
                    c=o.corr(o.ts_rank(C('close'), 3), o.ts_rank(adv(180), 12), 18),
                    b=o.rank_cs((C('low') + C('open')) - 2.0 * C('vwap')))
    frame = o.stage(frame, da=o.decay_linear(C('c'), 4), db=o.decay_linear(C('b'), 16))
    frame = o.stage(frame, ta=o.ts_rank(C('da'), 15), tb=o.ts_rank(C('db'), 4))
    return o.out(frame, pl.max_horizontal(C('ta'), C('tb')))


@factor('a101_072', 'alpha101',
         'rank(decay_linear(correlation((high + low) / 2, adv40, 9), 10)) / '
         'rank(decay_linear(correlation(ts_rank(vwap, 4), ts_rank(volume, 19), 7), 3))',
         'high, low, vwap, volume, amount', '?', SOURCE.format(tag='Alpha#72'),
         lookback=40, status='approx', note='vwap 为日频成交均价近似')
def a101_072(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel,
                    c1=o.corr((C('high') + C('low')) / 2.0, adv(40), 9),
                    c2=o.corr(o.ts_rank(C('vwap'), 4), o.ts_rank(C('volume'), 19), 7))
    frame = o.stage(frame, d1=o.decay_linear(C('c1'), 10),
                    d2=o.decay_linear(C('c2'), 3))
    frame = o.stage(frame, r1=o.rank_cs(C('d1')), r2=o.rank_cs(C('d2')))
    return o.out(frame, o.safe_div(C('r1'), C('r2')))


@factor('a101_073', 'alpha101',
         'max(rank(decay_linear(delta(vwap, 5), 3)), ts_rank(decay_linear('
         'delta(open * 0.147155 + low * 0.852845, 2) / (open * 0.147155 + low * 0.852845), 3), 17)) '
         '* -1',
         'open, low, vwap', '?', SOURCE.format(tag='Alpha#73'), lookback=17,
         status='approx', note='vwap 为日频成交均价近似')
def a101_073(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, blended=C('open') * 0.147155 + C('low') * 0.852845,
                    dvwap=o.delta(C('vwap'), 5))
    frame = o.stage(frame, d1=o.decay_linear(C('dvwap'), 3),
                    rel=o.safe_div(o.delta(C('blended'), 2), C('blended')))
    frame = o.stage(frame, d2=o.decay_linear(C('rel'), 3))
    frame = o.stage(frame, r1=o.rank_cs(C('d1')), t2=o.ts_rank(C('d2'), 17))
    return o.out(frame, pl.max_horizontal(C('r1'), C('t2')) * -1.0)


@factor('a101_074', 'alpha101',
         '(-1 * rank(correlation(close, sum(adv30, 37), 15))) * '
         'rank(correlation(rank(high * 0.0261661 + vwap * 0.973833), rank(volume), 11))',
         'close, high, vwap, volume, amount', '?', SOURCE.format(tag='Alpha#74'),
         lookback=67, status='approx', note='vwap / adv30 均为日频近似')
def a101_074(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, s=o.ts_sum(adv(30), 37),
                    blended=C('high') * 0.0261661 + C('vwap') * 0.973833)
    frame = o.stage(frame, rb=o.rank_cs(C('blended')), rv=o.rank_cs(C('volume')))
    frame = o.stage(frame, c1=o.corr(C('close'), C('s'), 15),
                    c2=o.corr(C('rb'), C('rv'), 11))
    frame = o.stage(frame, r1=o.rank_cs(C('c1')), r2=o.rank_cs(C('c2')))
    return o.out(frame, -1.0 * C('r1') * C('r2'))


@factor('a101_075', 'alpha101',
         '(rank(correlation(vwap, volume, 4)) < rank(correlation(vwap, adv50, 12))) * -1',
         'vwap, volume, amount', '?', SOURCE.format(tag='Alpha#75'), lookback=50,
         status='approx', note='vwap / adv50 均为日频近似')
def a101_075(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, c1=o.corr(C('vwap'), C('volume'), 4),
                    c2=o.corr(C('vwap'), adv(50), 12))
    frame = o.stage(frame, r1=o.rank_cs(C('c1')), r2=o.rank_cs(C('c2')))
    return o.out(frame, pl.when(C('r1') < C('r2')).then(-1.0).otherwise(1.0))


@factor('a101_077', 'alpha101',
         'min(rank(decay_linear((high + low) / 2 + high - (vwap + high), 20)), '
         'rank(decay_linear(correlation((high + low) / 2, adv40, 3), 6)))',
         'high, low, vwap, amount', '?', SOURCE.format(tag='Alpha#77'), lookback=40,
         status='approx', note='vwap / adv40 均为日频近似')
def a101_077(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, a=(C('high') + C('low')) / 2.0 + C('high') - (C('vwap') + C('high')),
                    c=o.corr((C('high') + C('low')) / 2.0, adv(40), 3))
    frame = o.stage(frame, da=o.decay_linear(C('a'), 20), db=o.decay_linear(C('c'), 6))
    frame = o.stage(frame, ra=o.rank_cs(C('da')), rb=o.rank_cs(C('db')))
    return o.out(frame, pl.min_horizontal(C('ra'), C('rb')))


@factor('a101_083', 'alpha101',
         '(rank(delay((high - low) / (sum(close, 5) / 5), 2)) * rank(rank(volume))) / '
         '(((high - low) / (sum(close, 5) / 5)) / (vwap - close))',
         'high, low, close, volume, vwap', '?', SOURCE.format(tag='Alpha#83'),
         lookback=7, status='approx', note='vwap 为日频成交均价近似')
def a101_083(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel,
                    rr=(C('high') - C('low')) / (o.ts_sum(C('close'), 5) / 5.0))
    frame = o.stage(frame, lag=o.delay(C('rr'), 2))
    frame = o.stage(frame, r1=o.rank_cs(C('lag')), r2=o.rank_cs(o.rank_cs(C('volume'))))
    return o.out(frame, o.safe_div(C('r1') * C('r2'),
                                   o.safe_div(C('rr'), C('vwap') - C('close'))))


@factor('a101_084', 'alpha101',
         'signedpower(ts_rank(vwap - ts_max(vwap, 15), 21), delta(close, 5))',
         'close, vwap', '?', SOURCE.format(tag='Alpha#84'), lookback=21,
         status='approx', note='vwap 为日频成交均价近似')
def a101_084(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, base=o.ts_rank(C('vwap') - o.ts_max(C('vwap'), 15), 21),
                    exponent=o.delta(C('close'), 5))
    return o.out(frame, o.signed_power(C('base'), C('exponent')))


# ----------------------------------------------------------------------
# Alpha#94 ~ Alpha#101
# ----------------------------------------------------------------------

@factor('a101_094', 'alpha101',
         '-1 * (rank(vwap - ts_min(vwap, 12))^ts_rank('
         'correlation(ts_rank(vwap, 20), ts_rank(adv60, 4), 18), 3))',
         'vwap, amount', '?', SOURCE.format(tag='Alpha#94'), lookback=60,
         status='approx', note='vwap / adv60 均为日频近似')
def a101_094(panel: pl.DataFrame) -> pl.Series:
    frame = rank_of(panel, 'base', C('vwap') - o.ts_min(C('vwap'), 12))
    frame = o.stage(frame, tv=o.ts_rank(C('vwap'), 20), ta=o.ts_rank(adv(60), 4))
    frame = o.stage(frame, c=o.corr(C('tv'), C('ta'), 18))
    frame = o.stage(frame, exponent=o.ts_rank(C('c'), 3))
    return o.out(frame, -1.0 * C('base').pow(C('exponent')))


@factor('a101_095', 'alpha101',
         '(rank(open - ts_min(open, 12)) < ts_rank((rank('
         'correlation(sum((high + low) / 2, 19), sum(adv40, 19), 13))^5), 12)) * -1',
         'open, high, low, amount', '?', SOURCE.format(tag='Alpha#95'), lookback=32,
         status='approx', note='adv40 用成交额均值近似')
def a101_095(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel,
                    c=o.corr(o.ts_sum((C('high') + C('low')) / 2.0, 19),
                             o.ts_sum(adv(40), 19), 13))
    frame = o.stage(frame, rc=o.rank_cs(C('c')))
    frame = o.stage(frame, t=o.ts_rank(C('rc').pow(5), 12),
                    left=o.rank_cs(C('open') - o.ts_min(C('open'), 12)))
    return o.out(frame, pl.when(C('left') < C('t')).then(-1.0).otherwise(1.0))


@factor('a101_099', 'alpha101',
         '(-1 * rank(correlation(sum((high + low) / 2, 20), sum(adv60, 20), 9))) * '
         'rank(correlation(low, volume, 6))',
         'high, low, volume, amount', '-', SOURCE.format(tag='Alpha#99'), lookback=60,
         status='approx', note='adv60 用成交额均值近似')
def a101_099(panel: pl.DataFrame) -> pl.Series:
    frame = o.stage(panel, a=o.ts_sum((C('high') + C('low')) / 2.0, 20),
                    b=o.ts_sum(adv(60), 20))
    frame = o.stage(frame, c1=o.corr(C('a'), C('b'), 9),
                    c2=o.corr(C('low'), C('volume'), 6))
    frame = o.stage(frame, r1=o.rank_cs(C('c1')), r2=o.rank_cs(C('c2')))
    return o.out(frame, -1.0 * C('r1') * C('r2'))


@factor('a101_101', 'alpha101',
         '(close - open) / ((high - low) + 0.001)',
         'open, high, low, close', '?', SOURCE.format(tag='Alpha#101'), lookback=1)
def a101_101(panel: pl.DataFrame) -> pl.Series:
    return o.out(panel, o.safe_div(C('close') - C('open'), (C('high') - C('low')) + 0.001))


# ----------------------------------------------------------------------
# 依赖 ts_argmax 等不可用算子：只登记不计算
# ----------------------------------------------------------------------

_UNAVAILABLE = {
    'a101_001': ('rank(ts_argmax(signedpower((returns < 0 ? stddev(returns, 20) : close), 2), 5)) - 0.5',
                 'close, returns', 'Alpha#1'),
    'a101_025': ('rank((-1 * returns) * sum(returns, 250) * '
                 '(1 - rank(decay_linear(volume / adv20, 15))))', 'close, volume, amount',
                 'Alpha#25'),
    'a101_057': ('-1 * ((close - vwap) / decay_linear(rank(ts_argmax(close, 30)), 2))',
                 'close, vwap', 'Alpha#57'),
    'a101_060': ('-1 * ((2 * scale(rank(((close - low) - (high - close)) / (high - low)) '
                 '* volume)) - scale(rank(ts_argmax(close, 10))))',
                 'close, high, low, volume', 'Alpha#60'),
    'a101_096': ('max(ts_rank(decay_linear(correlation(rank(vwap), rank(volume), 4), 4), 8), '
                 'ts_rank(decay_linear(ts_argmax(correlation(ts_rank(close, 3), '
                 'ts_rank(adv60, 12), 4), 4), 6), 6))',
                 'close, vwap, volume, amount', 'Alpha#96'),
}


def register_unavailable() -> None:
    """把依赖缺失算子的 Alpha101 因子登记为不可用（幂等）。"""
    from src.quant.factor_lib.registry import _REGISTRY
    from src.quant.factor_lib.registry import register_unavailable as _reg

    for name, (expression, deps, tag) in _UNAVAILABLE.items():
        if name in _REGISTRY:
            continue
        _reg(name, 'alpha101', expression, deps, '?', SOURCE.format(tag=tag),
             note='依赖 ts_argmax；polars 无高效窗口 argmax 算子，不近似替代')


__all__ = ['adv', 'rank_of', 'register_unavailable']
