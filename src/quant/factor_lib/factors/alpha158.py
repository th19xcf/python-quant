#!/usr/bin/env python3

"""
Qlib Alpha158 —— 公开特征集的可核验子集。

出处：Microsoft Qlib（MIT License），
``qlib/contrib/data/loader.py`` 中 ``Alpha158DL`` 的 ``fields`` 列表与
``qlib/contrib/data/utils/sepdf.py`` 中的算子实现。本项目只取
「K 线形态 + 价格时序 + 成交量」三类中不依赖额外数据的特征，
共 50 个（完整 158 个含 IMAX/IMIN 等依赖窗口内 argmax 的特征，见文末）。

口径对齐 Qlib：所有特征都是「无量纲比值」（价格比 / 均值比 / 波动比），
因此可以直接在复权价空间计算，不受复权因子整体缩放影响。
"""

from __future__ import annotations

import polars as pl

from src.quant.factor_lib import ops as o
from src.quant.factor_lib.registry import factor

SOURCE = 'Qlib Alpha158 (MIT): qlib/contrib/data/loader.py, feature {tag}'
C = pl.col
RET = C('ret')

#: 标的内部行号（BETA/RSQR/RESI 的时间自变量）。
#: 必须是真实列：polars 的 ``rolling_cov`` 不接受带 ``.over()`` 的入参，
#: 因此行号由 ``data.load_panel`` 预置为面板列 ``seq``，这里只做列引用。
SEQ = C('seq')


def _seq_var(n: int) -> float:
    """满窗连续整数 0..n-1 的样本方差，恒等于 (n² - 1) / 12。

    polars 2.1 没有 ``rolling_var``，且窗口满员时该方差与数据无关，
    直接给常数比回调计算更稳定（前 n-1 行本来就被 min_samples 置 null）。
    """
    return (n * n - 1) / 12.0


def _slope(n: int, price: str = 'close') -> pl.Expr:
    """OLS 斜率 cov(y, x) / var(x)，x 为窗口内行号。"""
    return o.safe_div(o.covariance(C(price), SEQ, n), pl.lit(_seq_var(n)))


def _rsqr(n: int, price: str = 'close') -> pl.Expr:
    """回归 R²，等于 corr(y, x)²。"""
    return o.corr(C(price), SEQ, n).pow(2)


# ----------------------------------------------------------------------
# K 线形态（6）
# ----------------------------------------------------------------------

@factor('qlib_kmid', 'alpha158', 'KMID = (close - open) / open', 'open, close', '+',
         SOURCE.format(tag='KMID'), lookback=1)
def qlib_kmid(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(C('close') - C('open'), C('open'))


@factor('qlib_klen', 'alpha158', 'KLEN = (high - low) / open', 'high, low, open', '?',
         SOURCE.format(tag='KLEN'), lookback=1)
def qlib_klen(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(C('high') - C('low'), C('open'))


@factor('qlib_kmid2', 'alpha158', 'KMID2 = (close - open) / (high - low)', 'open, close, high, low',
         '+', SOURCE.format(tag='KMID2'), lookback=1)
def qlib_kmid2(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(C('close') - C('open'), C('high') - C('low'))


@factor('qlib_kup', 'alpha158', 'KUP = (high - max(open, close)) / open', 'high, open, close', '-',
         SOURCE.format(tag='KUP'), lookback=1)
def qlib_kup(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(C('high') - pl.max_horizontal(C('open'), C('close')), C('open'))


@factor('qlib_klow', 'alpha158', 'KLOW = (min(open, close) - low) / open', 'low, open, close', '-',
         SOURCE.format(tag='KLOW'), lookback=1)
def qlib_klow(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(pl.min_horizontal(C('open'), C('close')) - C('low'), C('open'))


@factor('qlib_ksft', 'alpha158', 'KSFT = 2 * (close - open) / (high + low)', 'open, close, high, low',
         '?', SOURCE.format(tag='KSFT'), lookback=1)
def qlib_ksft(_panel: pl.DataFrame) -> pl.Expr:
    return 2.0 * o.safe_div(C('close') - C('open'), C('high') + C('low'))


# ----------------------------------------------------------------------
# 价格时序（ROC / MA / STD）
# ----------------------------------------------------------------------

def _roc(n: int) -> pl.Expr:
    return o.safe_div(C('close'), o.delay(C('close'), n)) - 1.0


for _n in (5, 10, 20, 60):
    def _make_roc(n: int):
        @factor(f'qlib_roc{n}', 'alpha158', f'ROC{n} = close / delay(close, {n}) - 1',
                 'close', '+', SOURCE.format(tag=f'ROC{n}'), lookback=n + 1)
        def _builder(_panel: pl.DataFrame) -> pl.Expr:
            return _roc(n)
        return _builder

    _make_roc(_n)


def _ma(n: int) -> pl.Expr:
    return o.safe_div(C('close'), o.ts_mean(C('close'), n))


def _std(n: int) -> pl.Expr:
    return o.safe_div(o.ts_std(C('close'), n), C('close'))


for _n in (5, 10, 20, 60):
    def _make_ma(n: int):
        @factor(f'qlib_ma{n}', 'alpha158', f'MA{n} = close / mean(close, {n})', 'close', '+',
                 SOURCE.format(tag=f'MA{n}'), lookback=n)
        def _builder(_panel: pl.DataFrame) -> pl.Expr:
            return _ma(n)
        return _builder

    _make_ma(_n)


for _n in (5, 10, 20, 60):
    def _make_std(n: int):
        @factor(f'qlib_std{n}', 'alpha158', f'STD{n} = std(close, {n}) / close', 'close', '-',
                 SOURCE.format(tag=f'STD{n}'), lookback=n)
        def _builder(_panel: pl.DataFrame) -> pl.Expr:
            return _std(n)
        return _builder

    _make_std(_n)


# ----------------------------------------------------------------------
# 回归类（BETA / RSQR / RESI）
# ----------------------------------------------------------------------

for _n in (5, 10, 20, 60):
    def _make_beta(n: int):
        @factor(f'qlib_beta{n}', 'alpha158', f'BETA{n} = slope(close, {n}) / close', 'close', '?',
                 SOURCE.format(tag=f'BETA{n}'), lookback=n)
        def _builder(_panel: pl.DataFrame) -> pl.Expr:
            return o.safe_div(_slope(n), C('close'))
        return _builder

    _make_beta(_n)


for _n in (5, 10, 20):
    def _make_rsqr(n: int):
        @factor(f'qlib_rsqr{n}', 'alpha158', f'RSQR{n} = corr(close, seq, {n})^2', 'close', '?',
                 SOURCE.format(tag=f'RSQR{n}'), lookback=n)
        def _builder(_panel: pl.DataFrame) -> pl.Expr:
            return _rsqr(n)
        return _builder

    _make_rsqr(_n)


for _n in (5, 10, 20):
    def _make_resi(n: int):
        @factor(f'qlib_resi{n}', 'alpha158',
                 f'RESI{n} = std(close, {n}) * sqrt(1 - RSQR{n}) / close', 'close', '-',
                 SOURCE.format(tag=f'RESI{n}'), lookback=n)
        def _builder(_panel: pl.DataFrame) -> pl.Expr:
            resid = o.ts_std(C('close'), n) * (1.0 - _rsqr(n)).clip(0.0, 1.0).sqrt()
            return o.safe_div(resid, C('close'))
        return _builder

    _make_resi(_n)


# ----------------------------------------------------------------------
# 极值与分位（MAX / MIN / QTLU / QTLD / RSV）
# ----------------------------------------------------------------------

for _n in (5, 10, 20):
    def _make_max(n: int):
        @factor(f'qlib_max{n}', 'alpha158', f'MAX{n} = close / max(close, {n})', 'close', '-',
                 SOURCE.format(tag=f'MAX{n}'), lookback=n)
        def _builder(_panel: pl.DataFrame) -> pl.Expr:
            return o.safe_div(C('close'), o.ts_max(C('close'), n))
        return _builder

    _make_max(_n)


for _n in (5, 10, 20):
    def _make_min(n: int):
        @factor(f'qlib_min{n}', 'alpha158', f'MIN{n} = close / min(close, {n})', 'close', '+',
                 SOURCE.format(tag=f'MIN{n}'), lookback=n)
        def _builder(_panel: pl.DataFrame) -> pl.Expr:
            return o.safe_div(C('close'), o.ts_min(C('close'), n))
        return _builder

    _make_min(_n)


@factor('qlib_qtlu20', 'alpha158', 'QTLU20 = close / quantile(close, 0.8, 20)',
         'close', '-', SOURCE.format(tag='QTLU20'), lookback=20)
def qlib_qtlu20(_panel: pl.DataFrame) -> pl.Expr:
    upper = C('close').rolling_quantile(0.8, 'linear', window_size=20, min_samples=20).over(o.GROUP)
    return o.safe_div(C('close'), upper)


@factor('qlib_qtld20', 'alpha158', 'QTLD20 = close / quantile(close, 0.2, 20)',
         'close', '+', SOURCE.format(tag='QTLD20'), lookback=20)
def qlib_qtld20(_panel: pl.DataFrame) -> pl.Expr:
    lower = C('close').rolling_quantile(0.2, 'linear', window_size=20, min_samples=20).over(o.GROUP)
    return o.safe_div(C('close'), lower)


@factor('qlib_rsv20', 'alpha158',
         'RSV20 = (close - min(low, 20)) / (max(high, 20) - min(low, 20))',
         'close, high, low', '+', SOURCE.format(tag='RSV20'), lookback=20)
def qlib_rsv20(_panel: pl.DataFrame) -> pl.Expr:
    lowest = o.ts_min(C('low'), 20)
    highest = o.ts_max(C('high'), 20)
    return o.safe_div(C('close') - lowest, highest - lowest)


# ----------------------------------------------------------------------
# 量与价的相关性（CORR / CORD）
# ----------------------------------------------------------------------

@factor('qlib_corr20', 'alpha158', 'CORR20 = corr(close, log(volume), 20)',
         'close, volume', '?', SOURCE.format(tag='CORR20'), lookback=20)
def qlib_corr20(_panel: pl.DataFrame) -> pl.Expr:
    return o.corr(C('close'), o.log(C('volume')), 20)


@factor('qlib_cord20', 'alpha158',
         'CORD20 = corr(close / delay(close,1), log(volume / delay(volume,1)), 20)',
         'close, volume', '?', SOURCE.format(tag='CORD20'), lookback=21)
def qlib_cord20(panel: pl.DataFrame) -> pl.Series:
    # 两个入参各含 shift(...).over()，必须先落列再算滚动相关：
    # polars 的 rolling_cov 不接受带 over 的入参（否则整列静默变 null）。
    staged = o.stage(
        panel,
        _pr=o.safe_div(C('close'), o.delay(C('close'), 1)),
        _vr=o.log(o.safe_div(C('volume'), o.delay(C('volume'), 1))),
    )
    return o.out(staged, o.corr(C('_pr'), C('_vr'), 20))


# ----------------------------------------------------------------------
# 涨跌天数与收益分解（CNTP / CNTN / CNTD / SUMP / SUMN / SUMD）
# ----------------------------------------------------------------------

@factor('qlib_cntp20', 'alpha158', 'CNTP20 = count(close > delay(close,1), 20) / 20',
         'close', '+', SOURCE.format(tag='CNTP20'), lookback=21)
def qlib_cntp20(_panel: pl.DataFrame) -> pl.Expr:
    return o.ts_count(RET > 0, 20) / 20.0


@factor('qlib_cntn20', 'alpha158', 'CNTN20 = count(close < delay(close,1), 20) / 20',
         'close', '-', SOURCE.format(tag='CNTN20'), lookback=21)
def qlib_cntn20(_panel: pl.DataFrame) -> pl.Expr:
    return o.ts_count(RET < 0, 20) / 20.0


@factor('qlib_cntd20', 'alpha158', 'CNTD20 = CNTP20 - CNTN20', 'close', '+',
         SOURCE.format(tag='CNTD20'), lookback=21)
def qlib_cntd20(_panel: pl.DataFrame) -> pl.Expr:
    return (o.ts_count(RET > 0, 20) - o.ts_count(RET < 0, 20)) / 20.0


@factor('qlib_sump20', 'alpha158',
         'SUMP20 = sum(max(ret,0), 20) / sum(|ret|, 20)', 'close', '+',
         SOURCE.format(tag='SUMP20'), lookback=21)
def qlib_sump20(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(o.ts_sum(pl.max_horizontal(RET, pl.lit(0.0)), 20), o.ts_sum(RET.abs(), 20))


@factor('qlib_sumn20', 'alpha158',
         'SUMN20 = sum(|min(ret,0)|, 20) / sum(|ret|, 20)', 'close', '-',
         SOURCE.format(tag='SUMN20'), lookback=21)
def qlib_sumn20(_panel: pl.DataFrame) -> pl.Expr:
    return o.safe_div(o.ts_sum((-RET).clip(0.0, None), 20), o.ts_sum(RET.abs(), 20))


@factor('qlib_sumd20', 'alpha158', 'SUMD20 = SUMP20 - SUMN20', 'close', '+',
         SOURCE.format(tag='SUMD20'), lookback=21)
def qlib_sumd20(_panel: pl.DataFrame) -> pl.Expr:
    gross = o.ts_sum(RET.abs(), 20)
    up = o.ts_sum(pl.max_horizontal(RET, pl.lit(0.0)), 20)
    down = o.ts_sum((-RET).clip(0.0, None), 20)
    return o.safe_div(up - down, gross)


# ----------------------------------------------------------------------
# 成交量（VMA / VSTD / WVMA / VSUMP）
# ----------------------------------------------------------------------

for _n in (20, 60):
    def _make_vma(n: int):
        @factor(f'qlib_vma{n}', 'alpha158', f'VMA{n} = volume / mean(volume, {n})',
                 'volume', '?', SOURCE.format(tag=f'VMA{n}'), lookback=n)
        def _builder(_panel: pl.DataFrame) -> pl.Expr:
            return o.safe_div(C('volume'), o.ts_mean(C('volume'), n))
        return _builder

    _make_vma(_n)


for _n in (20, 60):
    def _make_vstd(n: int):
        @factor(f'qlib_vstd{n}', 'alpha158', f'VSTD{n} = std(volume, {n}) / mean(volume, {n})',
                 'volume', '?', SOURCE.format(tag=f'VSTD{n}'), lookback=n)
        def _builder(_panel: pl.DataFrame) -> pl.Expr:
            return o.safe_div(o.ts_std(C('volume'), n), o.ts_mean(C('volume'), n))
        return _builder

    _make_vstd(_n)


for _n in (20, 60):
    def _make_wvma(n: int):
        @factor(f'qlib_wvma{n}', 'alpha158',
                 f'WVMA{n} = std(|ret| * volume, {n}) / mean(|ret| * volume, {n})',
                 'close, volume', '?', SOURCE.format(tag=f'WVMA{n}'), lookback=n)
        def _builder(_panel: pl.DataFrame) -> pl.Expr:
            weighted = RET.abs() * C('volume')
            return o.safe_div(o.ts_std(weighted, n), o.ts_mean(weighted, n))
        return _builder

    _make_wvma(_n)


@factor('qlib_vsump20', 'alpha158',
         'SUMP20(VOLUME) = sum(max(delta(volume,1),0),20) / sum(|delta(volume,1)|,20)',
         'volume', '?', SOURCE.format(tag='VSUMP20'), lookback=21)
def qlib_vsump20(panel: pl.DataFrame) -> pl.Series:
    # delta 含 shift(...).over()，滚动求和前先落列，避免嵌套窗口静默 null。
    staged = o.stage(panel, _vd=o.delta(C('volume'), 1))
    up = o.ts_sum(pl.max_horizontal(C('_vd'), pl.lit(0.0)), 20)
    total = o.ts_sum(C('_vd').abs(), 20)
    return o.out(staged, o.safe_div(up, total))


__all__ = ['SEQ']
