#!/usr/bin/env python3

"""
国泰君安 Alpha191 —— 公开研报公式的可核验子集。

出处：国泰君安证券《基于短周期价量特征的多因子选股体系 —— 数量化专题之
九十三》（2017-06）。公式原文与 DolphinDB 开源实现
``gtja191Alpha.dos``（https://docs.dolphindb.com/zh/modules/gtja191Alpha/）
逐条一致，每项 ``expression`` 字段即研报公式。

降级说明：
    - ``vwap`` 为日频成交均价 ``amount / (volume * 100) * hfq_factor``；
    - ``SMA(X, N, M)`` 为通达信口径 ``alpha = M/N`` 的指数平滑，
      本项目用 ``ops`` 里等价的 ``ewm_mean(alpha=1/N)`` 实现；
    - 研报中涉及市场指数（MKT/SMB/HML）或行业的因子（如 alpha30）
      数据依赖缺失，登记为不可用。
"""

from __future__ import annotations

import polars as pl

from src.quant.factor_lib import ops as o
from src.quant.factor_lib.registry import factor

SOURCE = '国泰君安《数量化专题之九十三》Alpha191 #{tag}'
C = pl.col

#: 标的内部行号，用于回归斜率类因子（替代研报的 SEQUENCE(N)）
SEQ = pl.int_range(0, pl.len()).over(o.GROUP).cast(pl.Float64)


def sma_tdx(expr: pl.Expr, period: int, m: int = 1) -> pl.Expr:
    """通达信 SMA(X, N, M)：alpha = M/N 的指数平滑，首值取 X。"""
    return expr.ewm_mean(alpha=float(m) / period, adjust=False, min_samples=1).over(o.GROUP)


# ----------------------------------------------------------------------
# alpha1 ~ alpha20
# ----------------------------------------------------------------------

@factor('gtja_001', 'gtja191',
         '-1 * CORR(RANK(DELTA(LOG(VOLUME), 1)), RANK((CLOSE - OPEN) / OPEN), 6)',
         'open, close, volume', '-', SOURCE.format(tag='1'), lookback=7)
def gtja_001(panel: pl.DataFrame) -> pl.Series:
    # 两个 corr 入参都含时序/横截面窗口，必须先落列再求滚动相关
    staged = o.stage(
        panel,
        _dv=o.delta(o.log(C('volume')), 1),
        _co=o.safe_div(C('close') - C('open'), C('open')),
    )
    staged = o.stage(staged, _r1=o.rank_cs(C('_dv')), _r2=o.rank_cs(C('_co')))
    return o.out(staged, -1.0 * o.corr(C('_r1'), C('_r2'), 6))


@factor('gtja_002', 'gtja191',
         '-1 * DELTA(((CLOSE - LOW) - (HIGH - CLOSE)) / (HIGH - LOW), 1)',
         'close, high, low', '-', SOURCE.format(tag='2'), lookback=2)
def gtja_002(_panel: pl.DataFrame) -> pl.Expr:
    return -1.0 * o.delta(
        o.safe_div((C('close') - C('low')) - (C('high') - C('close')), C('high') - C('low')), 1
    )


@factor('gtja_003', 'gtja191',
         'SUM((CLOSE = DELAY(CLOSE,1) ? 0 : CLOSE - (CLOSE > DELAY(CLOSE,1) ? '
         'MIN(LOW, DELAY(CLOSE,1)) : MAX(HIGH, DELAY(CLOSE,1)))), 6)',
         'close, high, low', '?', SOURCE.format(tag='3'), lookback=7)
def gtja_003(_panel: pl.DataFrame) -> pl.Expr:
    prev = o.delay(C('close'), 1)
    inner = pl.when(C('close') > prev).then(
        pl.min_horizontal(prev, C('low'))
    ).otherwise(pl.max_horizontal(prev, C('high')))
    return o.ts_sum(pl.when(C('close') == prev).then(0.0).otherwise(C('close') - inner), 6)


@factor('gtja_004', 'gtja191',
         '(((SUM(CLOSE,8)/8) + STD(CLOSE,8)) < (SUM(CLOSE,2)/2)) ? -1 : '
         '(((SUM(CLOSE,2)/2) < ((SUM(CLOSE,8)/8) - STD(CLOSE,8))) ? 1 : '
         '((1 < (VOLUME / MEAN(VOLUME,20))) || ((VOLUME / MEAN(VOLUME,20)) == 1) ? 1 : -1))',
         'close, volume', '?', SOURCE.format(tag='4'), lookback=20)
def gtja_004(_panel: pl.DataFrame) -> pl.Expr:
    mean8 = o.ts_sum(C('close'), 8) / 8.0
    mean2 = o.ts_sum(C('close'), 2) / 2.0
    std8 = o.ts_std(C('close'), 8)
    ratio = o.safe_div(C('volume'), o.ts_mean(C('volume'), 20))
    return (
        pl.when(mean8 + std8 < mean2).then(-1.0)
        .when(mean2 < mean8 - std8).then(1.0)
        .when(ratio >= 1.0).then(1.0)
        .otherwise(-1.0)
    )


@factor('gtja_009', 'gtja191',
         'SMA(((HIGH+LOW)/2 - (DELAY(HIGH,1)+DELAY(LOW,1))/2) * (HIGH-LOW) / VOLUME, 7, 2)',
         'high, low, volume', '?', SOURCE.format(tag='9'), lookback=8)
def gtja_009(_panel: pl.DataFrame) -> pl.Expr:
    mid = (C('high') + C('low')) / 2.0
    prev_mid = (o.delay(C('high'), 1) + o.delay(C('low'), 1)) / 2.0
    value = o.safe_div((mid - prev_mid) * (C('high') - C('low')), C('volume'))
    return sma_tdx(value, 7, 2)


@factor('gtja_010', 'gtja191',
         'RANK(MAX(((RET < 0) ? STD(RET, 20) : CLOSE)^2, 5))',
         'close', '?', SOURCE.format(tag='10'), lookback=20)
def gtja_010(panel: pl.DataFrame) -> pl.Series:
    base = pl.when(C('ret') < 0).then(o.ts_std(C('ret'), 20)).otherwise(C('close'))
    staged = o.stage(panel, _sq=o.ts_max(base.pow(2), 5))
    return o.out(staged, o.rank_cs(C('_sq')))


@factor('gtja_011', 'gtja191',
         'SUM(((CLOSE-LOW)-(HIGH-CLOSE)) / (HIGH-LOW) * VOLUME, 6)',
         'close, high, low, volume', '?', SOURCE.format(tag='11'), lookback=6)
def gtja_011(_panel: pl.DataFrame) -> pl.Expr:
    return o.ts_sum(
        o.safe_div((C('close') - C('low')) - (C('high') - C('close')), C('high') - C('low'))
        * C('volume'),
        6,
    )


@factor('gtja_012', 'gtja191',
         'RANK(OPEN - (SUM(VWAP, 10) / 10)) * (-1 * RANK(ABS(CLOSE - VWAP)))',
         'open, close, vwap', '?', SOURCE.format(tag='12'), lookback=10,
         status='approx', note='vwap 为日频成交均价近似')
def gtja_012(panel: pl.DataFrame) -> pl.Series:
    staged = o.stage(
        panel,
        _a=C('open') - o.ts_sum(C('vwap'), 10) / 10.0,
        _b=(C('close') - C('vwap')).abs(),
    )
    staged = o.stage(staged, _ra=o.rank_cs(C('_a')), _rb=o.rank_cs(C('_b')))
    return o.out(staged, C('_ra') * (-1.0 * C('_rb')))


@factor('gtja_013', 'gtja191',
         '((HIGH * LOW)^0.5) - VWAP', 'high, low, vwap', '?',
         SOURCE.format(tag='13'), lookback=1, status='approx', note='vwap 为日频成交均价近似')
def gtja_013(_panel: pl.DataFrame) -> pl.Expr:
    return (C('high') * C('low')).sqrt() - C('vwap')


@factor('gtja_014', 'gtja191',
         'CLOSE - DELAY(CLOSE, 5)', 'close', '?', SOURCE.format(tag='14'), lookback=6)
def gtja_014(_panel: pl.DataFrame) -> pl.Expr:
    return C('close') - o.delay(C('close'), 5)


@factor('gtja_015', 'gtja191',
         'OPEN / DELAY(CLOSE, 1) - 1', 'open, close', '?',
         SOURCE.format(tag='15'), lookback=2)
def gtja_015(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(C('open'), o.delay(C('close'), 1)) - 1.0


@factor('gtja_018', 'gtja191',
         'CLOSE / DELAY(CLOSE, 5)', 'close', '-', SOURCE.format(tag='18'), lookback=6)
def gtja_018(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(C('close'), o.delay(C('close'), 5))


@factor('gtja_019', 'gtja191',
         '(CLOSE < DELAY(CLOSE,5) ? (CLOSE - DELAY(CLOSE,5)) / DELAY(CLOSE,5) : '
         '(CLOSE = DELAY(CLOSE,5) ? 0 : (CLOSE - DELAY(CLOSE,5)) / CLOSE))',
         'close', '-', SOURCE.format(tag='19'), lookback=6)
def gtja_019(_panel: pl.DataFrame) -> pl.Expr:
    prev = o.delay(C('close'), 5)
    diff = C('close') - prev
    return (
        pl.when(C('close') < prev).then(o.safe_div(diff, prev))
        .when(C('close') == prev).then(0.0)
        .otherwise(o.safe_div(diff, C('close')))
    )


@factor('gtja_020', 'gtja191',
         '(CLOSE - DELAY(CLOSE, 6)) / DELAY(CLOSE, 6) * 100',
         'close', '?', SOURCE.format(tag='20'), lookback=7)
def gtja_020(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(o.delta(C('close'), 6), o.delay(C('close'), 6)) * 100.0


# ----------------------------------------------------------------------
# alpha24 ~ alpha55
# ----------------------------------------------------------------------

@factor('gtja_024', 'gtja191',
         'SMA(CLOSE - DELAY(CLOSE,5), 5, 1)', 'close', '?',
         SOURCE.format(tag='24'), lookback=6)
def gtja_024(_panel: pl.DataFrame) -> pl.Expr:
    return sma_tdx(o.delta(C('close'), 5), 5, 1)


@factor('gtja_029', 'gtja191',
         '(CLOSE - DELAY(CLOSE, 6)) / DELAY(CLOSE, 6) * VOLUME',
         'close, volume', '?', SOURCE.format(tag='29'), lookback=7)
def gtja_029(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(o.delta(C('close'), 6), o.delay(C('close'), 6)) * C('volume')


@factor('gtja_031', 'gtja191',
         '(CLOSE - MEAN(CLOSE, 12)) / MEAN(CLOSE, 12) * 100',
         'close', '-', SOURCE.format(tag='31'), lookback=12)
def gtja_031(_panel: pl.DataFrame) -> pl.Expr:
    mean12 = o.ts_mean(C('close'), 12)
    return o.safe_div(C('close') - mean12, mean12) * 100.0


@factor('gtja_034', 'gtja191',
         'MEAN(CLOSE, 12) / CLOSE', 'close', '+', SOURCE.format(tag='34'), lookback=12)
def gtja_034(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(o.ts_mean(C('close'), 12), C('close'))


@factor('gtja_037', 'gtja191',
         '-1 * RANK((SUM(OPEN,5) * SUM(RET,5)) - DELAY((SUM(OPEN,5) * SUM(RET,5)), 10))',
         'open, close', '-', SOURCE.format(tag='37'), lookback=15)
def gtja_037(panel: pl.DataFrame) -> pl.Series:
    combined = o.ts_sum(C('open'), 5) * o.ts_sum(C('ret'), 5)
    staged = o.stage(panel, _v=combined - o.delay(combined, 10))
    return o.out(staged, -1.0 * o.rank_cs(C('_v')))


@factor('gtja_038', 'gtja191',
         '((SUM(HIGH, 20) / 20) < HIGH) ? (-1 * DELTA(HIGH, 2)) : 0',
         'high', '?', SOURCE.format(tag='38'), lookback=20)
def gtja_038(_panel: pl.DataFrame) -> pl.Expr:
    return pl.when(o.ts_sum(C('high'), 20) / 20.0 < C('high')).then(
        -1.0 * o.delta(C('high'), 2)
    ).otherwise(0.0)


@factor('gtja_040', 'gtja191',
         'SUM(CLOSE > DELAY(CLOSE,1) ? VOLUME : 0, 26) / '
         'SUM(CLOSE <= DELAY(CLOSE,1) ? VOLUME : 0, 26) * 100',
         'close, volume', '?', SOURCE.format(tag='40'), lookback=27)
def gtja_040(_panel: pl.DataFrame) -> pl.Expr:
    prev = o.delay(C('close'), 1)
    up_volume = o.ts_sum(pl.when(C('close') > prev).then(C('volume')).otherwise(0.0), 26)
    down_volume = o.ts_sum(pl.when(C('close') <= prev).then(C('volume')).otherwise(0.0), 26)
    return o.safe_div(up_volume, down_volume) * 100.0


@factor('gtja_041', 'gtja191',
         'RANK(MAX(DELTA(VWAP, 3), 5)) * -1', 'vwap', '?',
         SOURCE.format(tag='41'), lookback=7, status='approx', note='vwap 为日频成交均价近似')
def gtja_041(panel: pl.DataFrame) -> pl.Series:
    staged = o.stage(panel, _v=o.ts_max(o.delta(C('vwap'), 3), 5))
    return o.out(staged, -1.0 * o.rank_cs(C('_v')))


@factor('gtja_042', 'gtja191',
         '(-1 * RANK(STD(HIGH, 10))) * CORR(HIGH, VOLUME, 10)',
         'high, volume', '-', SOURCE.format(tag='42'), lookback=10)
def gtja_042(panel: pl.DataFrame) -> pl.Series:
    staged = o.stage(panel, _s=o.ts_std(C('high'), 10))
    staged = o.stage(staged, _rs=o.rank_cs(C('_s')))
    return o.out(staged, -1.0 * C('_rs') * o.corr(C('high'), C('volume'), 10))


@factor('gtja_043', 'gtja191',
         'SUM(CLOSE > DELAY(CLOSE,1) ? VOLUME : (CLOSE < DELAY(CLOSE,1) ? -VOLUME : 0), 6)',
         'close, volume', '?', SOURCE.format(tag='43'), lookback=7)
def gtja_043(_panel: pl.DataFrame) -> pl.Expr:
    prev = o.delay(C('close'), 1)
    signed = (
        pl.when(C('close') > prev).then(C('volume'))
        .when(C('close') < prev).then(-1.0 * C('volume'))
        .otherwise(0.0)
    )
    return o.ts_sum(signed, 6)


@factor('gtja_046', 'gtja191',
         '(MEAN(CLOSE,3) + MEAN(CLOSE,6) + MEAN(CLOSE,12) + MEAN(CLOSE,24)) / (4 * CLOSE)',
         'close', '+', SOURCE.format(tag='46'), lookback=24)
def gtja_046(_panel: pl.DataFrame) -> pl.Expr:
    total = (
        o.ts_mean(C('close'), 3) + o.ts_mean(C('close'), 6)
        + o.ts_mean(C('close'), 12) + o.ts_mean(C('close'), 24)
    )
    return o.safe_div(total, 4.0 * C('close'))


@factor('gtja_047', 'gtja191',
         'SMA((TSMAX(HIGH,6) - CLOSE) / (TSMAX(HIGH,6) - TSMIN(LOW,6)) * 100, 9, 1)',
         'close, high, low', '?', SOURCE.format(tag='47'), lookback=15)
def gtja_047(_panel: pl.DataFrame) -> pl.Expr:
    high6 = o.ts_max(C('high'), 6)
    low6 = o.ts_min(C('low'), 6)
    value = o.safe_div(high6 - C('close'), high6 - low6) * 100.0
    return sma_tdx(value, 9, 1)


@factor('gtja_048', 'gtja191',
         '-1 * RANK(SIGN(CLOSE-DELAY(CLOSE,1)) + SIGN(DELAY(CLOSE,1)-DELAY(CLOSE,2)) + '
         'SIGN(DELAY(CLOSE,2)-DELAY(CLOSE,3))) * SUM(VOLUME,5) / SUM(VOLUME,20)',
         'close, volume', '?', SOURCE.format(tag='48'), lookback=20)
def gtja_048(panel: pl.DataFrame) -> pl.Series:
    streak = (
        o.sign(C('close') - o.delay(C('close'), 1))
        + o.sign(o.delay(C('close'), 1) - o.delay(C('close'), 2))
        + o.sign(o.delay(C('close'), 2) - o.delay(C('close'), 3))
    )
    staged = o.stage(panel, _st=streak)
    return o.out(
        staged,
        -1.0 * o.rank_cs(C('_st'))
        * o.safe_div(o.ts_sum(C('volume'), 5), o.ts_sum(C('volume'), 20)),
    )


@factor('gtja_049', 'gtja191',
         'SUM((HIGH+LOW >= DELAY(HIGH,1)+DELAY(LOW,1) ? 0 : '
         'MAX(ABS(HIGH-DELAY(HIGH,1)), ABS(LOW-DELAY(LOW,1)))), 12) / '
         '(SUM(上述,12) + SUM((HIGH+LOW <= DELAY(HIGH,1)+DELAY(LOW,1) ? 0 : '
         'MAX(ABS(HIGH-DELAY(HIGH,1)), ABS(LOW-DELAY(LOW,1)))), 12))',
         'high, low', '?', SOURCE.format(tag='49'), lookback=13)
def gtja_049(_panel: pl.DataFrame) -> pl.Expr:
    prev_sum = o.delay(C('high'), 1) + o.delay(C('low'), 1)
    move = pl.max_horizontal(
        (C('high') - o.delay(C('high'), 1)).abs(),
        (C('low') - o.delay(C('low'), 1)).abs(),
    )
    up = o.ts_sum(pl.when(C('high') + C('low') >= prev_sum).then(0.0).otherwise(move), 12)
    down = o.ts_sum(pl.when(C('high') + C('low') <= prev_sum).then(0.0).otherwise(move), 12)
    return o.safe_div(up, up + down)


@factor('gtja_052', 'gtja191',
         'SUM(MAX(0, HIGH - DELAY((HIGH+LOW+CLOSE)/3,1)), 26) / '
         'SUM(MAX(0, DELAY((HIGH+LOW+CLOSE)/3,1) - LOW), 26) * 100',
         'high, low, close', '?', SOURCE.format(tag='52'), lookback=27)
def gtja_052(_panel: pl.DataFrame) -> pl.Expr:
    typical = o.delay((C('high') + C('low') + C('close')) / 3.0, 1)
    up = o.ts_sum(pl.max_horizontal(C('high') - typical, pl.lit(0.0)), 26)
    down = o.ts_sum(pl.max_horizontal(typical - C('low'), pl.lit(0.0)), 26)
    return o.safe_div(up, down) * 100.0


@factor('gtja_053', 'gtja191',
         'COUNT(CLOSE > DELAY(CLOSE,1), 12) / 12 * 100',
         'close', '+', SOURCE.format(tag='53'), lookback=13)
def gtja_053(_panel: pl.DataFrame) -> pl.Expr:
    return o.ts_count(C('close') > o.delay(C('close'), 1), 12) / 12.0 * 100.0


@factor('gtja_054', 'gtja191',
         '-1 * RANK(STD(ABS(CLOSE - OPEN), 10) + (CLOSE - OPEN) + CORR(CLOSE, OPEN, 10))',
         'open, close', '-', SOURCE.format(tag='54'), lookback=10)
def gtja_054(panel: pl.DataFrame) -> pl.Series:
    gap = C('close') - C('open')
    inner = o.ts_std(gap.abs(), 10) + gap + o.corr(C('close'), C('open'), 10)
    staged = o.stage(panel, _v=inner)
    return o.out(staged, -1.0 * o.rank_cs(C('_v')))


@factor('gtja_055', 'gtja191',
         'SUM(16 * (CLOSE-DELAY(CLOSE,1)+(CLOSE-OPEN)/2+DELAY(CLOSE,1)-DELAY(OPEN,1)) / '
         '(条件分支的真实波幅分母) * MAX(ABS(HIGH-DELAY(CLOSE,1)), ABS(LOW-DELAY(CLOSE,1))), 20)',
         'open, close, high, low', '?', SOURCE.format(tag='55'), lookback=21,
         status='approx',
         note='研报分母为三段条件分支的波幅组合，此处按 DolphinDB 实现逐段还原')
def gtja_055(_panel: pl.DataFrame) -> pl.Expr:
    prev_close = o.delay(C('close'), 1)
    prev_open = o.delay(C('open'), 1)
    high_gap = (C('high') - prev_close).abs()
    low_gap = (C('low') - prev_close).abs()
    true_range = (C('high') - o.delay(C('low'), 1)).abs()

    denominator = (
        pl.when((high_gap > low_gap) & (high_gap > true_range))
        .then(high_gap + low_gap / 2.0 + (prev_close - prev_open).abs() / 4.0)
        .when((low_gap > true_range) & (low_gap > high_gap))
        .then(low_gap + high_gap / 2.0 + (prev_close - prev_open).abs() / 4.0)
        .otherwise(true_range + (prev_close - prev_open).abs() / 4.0)
    )
    numerator = 16.0 * (C('close') - prev_close + (C('close') - C('open')) / 2.0
                        + prev_close - prev_open)
    value = o.safe_div(numerator, denominator) * pl.max_horizontal(high_gap, low_gap)
    return o.ts_sum(value, 20)


# ----------------------------------------------------------------------
# 不可用因子（缺指数 / 行业 / 财务数据）
# ----------------------------------------------------------------------

_UNAVAILABLE = {
    'gtja_005': ('-1 * TSMAX(CORR(TSRANK(VOLUME,5), TSRANK(HIGH,5),5), 3)', 'volume, high',
                 '与 Alpha101#26 公式重复，且公开实现版本互相矛盾，未纳入'),
    'gtja_021': ('REGBETA(MEAN(CLOSE,6), SEQUENCE(6))', 'close',
                 '回归斜率需序列算子，等价实现留待后续（非数据依赖缺失）'),
    'gtja_030': ('WMA((REGRESI(CLOSE/DELAY(CLOSE)-1, MKT, SMB, HML, 60))^2, 20)',
                 'close, 市场指数, FF 三因子',
                 '需要 MKT/SMB/HML 三因子收益序列，本地库无 FF 因子数据'),
    'gtja_056': ('(RANK(OPEN-TSMIN(OPEN,12)) < TSRANK((RANK(CORR(SUM((HIGH+LOW)/2,19),'
                 'SUM(MEAN(VOLUME,40),19),13))^5),12)) * -1', 'open, high, low, volume',
                 '依赖 40 日成交额均值（可算）但公开实现版本文献不一致，暂不纳入'),
}


def register_unavailable() -> None:
    """登记 GTJA191 中数据依赖缺失的因子（幂等）。"""
    from src.quant.factor_lib.registry import _REGISTRY
    from src.quant.factor_lib.registry import register_unavailable as _reg

    for name, (expression, deps, note) in _UNAVAILABLE.items():
        if name in _REGISTRY:
            continue
        _reg(name, 'gtja191', expression, deps, '?', SOURCE.format(tag=name.split('_')[1]), note)


__all__ = ['SEQ', 'register_unavailable', 'sma_tdx']
