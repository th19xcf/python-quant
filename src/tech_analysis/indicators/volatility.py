#!/usr/bin/env python3

"""
波动率类指标计算模块
包含：BOLL, WR, BRAR, ASI, EMV, MCST等
"""

import polars as pl

from ..common_calculations import add_default_columns
from ..utils import to_float32


def calculate_boll(lazy_df: pl.LazyFrame, windows: list, std_dev: float) -> pl.LazyFrame:
    """
    计算Boll指标（布林带）

    Args:
        lazy_df: Polars LazyFrame
        windows: Boll计算窗口列表
        std_dev: 标准差倍数

    Returns:
        pl.LazyFrame: 包含Boll指标的LazyFrame
    """
    # 批量计算所有窗口的Boll指标
    for window in windows:
        # 计算移动平均线（中轨线，通达信 MA 不足 N 根不输出）
        mb_expr = to_float32(pl.col('close').rolling_mean(window_size=window, min_samples=window)).alias(f'mb{window}')
        # 计算上轨线和下轨线（STD 同 MA 口径，不足 N 根为空）
        up_expr = to_float32(mb_expr + to_float32(pl.col('close').rolling_std(window_size=window, min_samples=window)) * std_dev).alias(f'up{window}')
        dn_expr = to_float32(mb_expr - to_float32(pl.col('close').rolling_std(window_size=window, min_samples=window)) * std_dev).alias(f'dn{window}')

        # 添加到DataFrame
        lazy_df = lazy_df.with_columns([mb_expr, up_expr, dn_expr])

    # 添加默认列名
    if len(windows) >= 1:
        window = windows[0]
        lazy_df = lazy_df.with_columns(
            pl.col(f'mb{window}').alias('mb'),
            pl.col(f'up{window}').alias('up'),
            pl.col(f'dn{window}').alias('dn')
        )

    return lazy_df


def calculate_wr(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算WR指标（威廉指标）

    Args:
        lazy_df: Polars LazyFrame
        windows: WR计算窗口列表

    Returns:
        pl.LazyFrame: 包含WR指标的LazyFrame
    """
    for window in windows:
        highest = pl.col(f'high_n_{window}')
        lowest = pl.col(f'low_n_{window}')
        # 分母为 0 表示窗口内价格完全不动（连续一字板 / 停牌），按通达信口径取 0，
        # 避免 0/0 产生 NaN/inf 静默污染图表与回测
        wr = (
            pl.when(highest == lowest)
            .then(0.0)
            .otherwise((highest - pl.col('close')) / (highest - lowest) * 100)
        )
        lazy_df = lazy_df.with_columns(to_float32(wr).alias(f'wr{window}'))

    # 添加默认列名
    lazy_df = add_default_columns(lazy_df, 'wr', windows)
    if len(windows) >= 2:
        lazy_df = lazy_df.with_columns(
            pl.col(f'wr{windows[1]}').alias('wr2')
        )

    return lazy_df


def calculate_brar(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算BRAR指标（情绪指标）

    Args:
        lazy_df: Polars LazyFrame
        windows: BRAR计算窗口列表

    Returns:
        pl.LazyFrame: 包含BRAR指标的LazyFrame
    """
    # 1. 计算必要的中间变量
    # 前一日收盘价
    prev_close = pl.col('close').shift(1)
    # AR的分子：(最高价 - 开盘价)的正值部分
    ar_up = pl.when((pl.col('high') - pl.col('open')) > 0).then(pl.col('high') - pl.col('open')).otherwise(0.0).alias('ar_up')
    # AR的分母：(开盘价 - 最低价)的正值部分
    ar_down = pl.when((pl.col('open') - pl.col('low')) > 0).then(pl.col('open') - pl.col('low')).otherwise(0.0).alias('ar_down')
    # BR的分子：(最高价 - 前一日收盘价)的正值部分
    br_up = pl.when((pl.col('high') - prev_close) > 0).then(pl.col('high') - prev_close).otherwise(0.0).alias('br_up')
    # BR的分母：(前一日收盘价 - 最低价)的正值部分
    br_down = pl.when((prev_close - pl.col('low')) > 0).then(prev_close - pl.col('low')).otherwise(0.0).alias('br_down')

    # 2. 添加中间变量到DataFrame（这些列会被 cleanup_temp_columns 清理）
    lazy_df = lazy_df.with_columns([ar_up, ar_down, br_up, br_down])

    # 3. 计算各窗口的AR和BR值
    for window in windows:
        # 计算AR = (ar_up的N日和 / ar_down的N日和) * 100
        ar_expr = to_float32((pl.col('ar_up').rolling_sum(window_size=window, min_samples=1) / (pl.col('ar_down').rolling_sum(window_size=window, min_samples=1) + 0.0001)) * 100).alias(f'ar{window}')
        # 计算BR = (br_up的N日和 / br_down的N日和) * 100
        br_expr = to_float32((pl.col('br_up').rolling_sum(window_size=window, min_samples=1) / (pl.col('br_down').rolling_sum(window_size=window, min_samples=1) + 0.0001)) * 100).alias(f'br{window}')

        # 添加到DataFrame
        lazy_df = lazy_df.with_columns([ar_expr, br_expr])

    # 4. 添加默认列名
    if len(windows) >= 1:
        window = windows[0]
        lazy_df = lazy_df.with_columns(
            pl.col(f'ar{window}').alias('ar'),
            pl.col(f'br{window}').alias('br')
        )

    return lazy_df


def calculate_asi(lazy_df: pl.LazyFrame, signal_period: int) -> pl.LazyFrame:
    """
    计算ASI指标（振动升降指标）

    Args:
        lazy_df: Polars LazyFrame
        signal_period: ASI信号线周期

    Returns:
        pl.LazyFrame: 包含ASI指标的LazyFrame
    """
    # 1. 计算前一日的数据
    pl.col('high').shift(1).alias('prev_high')
    pl.col('low').shift(1).alias('prev_low')
    prev_close = pl.col('close').shift(1).alias('prev_close')

    # 2. 计算各种价格差
    # 今日开盘价与前一日收盘价的差
    pl.col('open') - prev_close
    # 今日最高价与前一日收盘价的差
    pl.col('high') - prev_close
    # 今日最低价与前一日收盘价的差
    pl.col('low') - prev_close
    # 今日最高价与今日开盘价的差
    pl.col('high') - pl.col('open')
    # 今日最低价与今日开盘价的差
    pl.col('low') - pl.col('open')
    # 前一日收盘价与前一日开盘价的差
    prev_close - pl.col('open').shift(1)

    # 3. 计算真实波幅（简化计算，避免中间变量）
    tr = pl.max_horizontal(
        (pl.col('high').shift(1) - pl.col('close')).abs(),
        (pl.col('low').shift(1) - pl.col('close')).abs(),
        (pl.col('high').shift(1) - pl.col('low').shift(1)).abs()
    )

    # 4. 计算ASI的分子部分
    asi_numerator = (pl.col('open') - pl.col('close').shift(1)) + 0.5 * (pl.col('open') - pl.col('close')) + 0.25 * (pl.col('close').shift(1) - pl.col('open').shift(1))

    # 5. 计算单日 SI 与累计 ASI（通达信 ASI:SUM(SI,0)，必须逐日累加才有趋势形态）
    si = to_float32(pl.when(tr != 0).then(asi_numerator / tr * 16).otherwise(0.0))
    asi_cum = si.cum_sum()
    asi = asi_cum.alias('asi')

    # 6. 计算ASI的信号线（ASI_SIG = ASI的signal_period天MA，不足 N 根不输出）
    asi_sig = to_float32(asi_cum.rolling_mean(window_size=signal_period, min_samples=signal_period)).alias('asi_sig')

    # 7. 添加到DataFrame
    return lazy_df.with_columns([asi, asi_sig])


def calculate_emv(lazy_df: pl.LazyFrame, windows: list, constant: float = 1.0) -> pl.LazyFrame:
    """
    计算EMV指标（简易波动指标，通达信原式）

    通达信公式：
        VOLUME:=MA(VOL,N)/VOL;
        MID:=100*(HIGH+LOW-REF(HIGH+LOW,1))/(HIGH+LOW);
        EMV:MA(MID*VOLUME*(HIGH-LOW)/MA(HIGH-LOW,N),N);

    旧实现多乘了 1e8（量纲错位），已去掉；``constant`` 默认 1.0 即通达信原式。

    Args:
        lazy_df: Polars LazyFrame
        windows: EMV计算窗口列表
        constant: 缩放常数，默认 1.0（通达信原式，无额外缩放）

    Returns:
        pl.LazyFrame: 包含EMV指标的LazyFrame
    """
    hl = pl.col('high') + pl.col('low')
    hl_range = pl.col('high') - pl.col('low')
    # MID：中点价相对昨日的百分比变化（×100）
    mid = 100.0 * (hl - hl.shift(1)) / hl

    for window in windows:
        # VOLUME:=MA(VOL,N)/VOL（量能倍数，不足 N 根为空）
        vol_ratio = pl.col('volume').rolling_mean(window_size=window, min_samples=window) / pl.col('volume')
        # MA(HIGH-LOW,N)
        range_ma = hl_range.rolling_mean(window_size=window, min_samples=window)
        emv_base = pl.when(
            range_ma.is_null() | (range_ma == 0) | (pl.col('volume') <= 0)
        ).then(None).otherwise(mid * vol_ratio * hl_range / range_ma)
        # EMV:MA(基础值,N)，不足 N 根不输出
        emv_expr = to_float32((emv_base * constant).rolling_mean(window_size=window, min_samples=window)).alias(f'emv{window}')
        lazy_df = lazy_df.with_columns(emv_expr)

    # 4. 添加默认列名
    if len(windows) >= 1:
        window = windows[0]
        lazy_df = lazy_df.with_columns(
            pl.col(f'emv{window}').alias('emv')
        )

    return lazy_df


def calculate_mcst(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算MCST指标（市场成本）

    Args:
        lazy_df: Polars LazyFrame
        windows: MCST计算窗口列表

    Returns:
        pl.LazyFrame: 包含MCST指标的LazyFrame
    """
    # 1. 成交额近似：close × volume（与 VOL 同量纲，比值仍为每股价格）
    price_volume = pl.col('close') * pl.col('volume')

    # 2. 通达信 MCST:=SUM(AMOUNT,N)/SUM(VOL,N) —— N 日窗口滚动，非全历史累加
    for window in windows:
        cost_sum = price_volume.rolling_sum(window_size=window, min_samples=window)
        volume_sum = pl.col('volume').rolling_sum(window_size=window, min_samples=window)
        mcst_n = to_float32(
            pl.when(volume_sum > 0).then(cost_sum / volume_sum).otherwise(None)
        ).alias(f'mcst{window}')
        lazy_df = lazy_df.with_columns(mcst_n)

    if len(windows) >= 1:
        first = windows[0]
        lazy_df = lazy_df.with_columns(pl.col(f'mcst{first}').alias('mcst'))

    # 3. 各窗口的 MCST 移动平均线（MA 不足 N 根不输出）
    for window in windows:
        mcst_ma_expr = to_float32(
            pl.col('mcst').rolling_mean(window_size=window, min_samples=window)
        ).alias(f'mcst_ma{window}')
        lazy_df = lazy_df.with_columns(mcst_ma_expr)

    return lazy_df


def calculate_volatility_indicators(lazy_df: pl.LazyFrame, indicator_types: list, **params) -> pl.LazyFrame:
    """
    计算所有波动率类指标

    Args:
        lazy_df: Polars LazyFrame
        indicator_types: 需要计算的波动率类指标列表
        **params: 指标计算参数

    Returns:
        pl.LazyFrame: 包含所有计算波动率类指标的LazyFrame
    """
    # 计算BOLL指标
    if 'boll' in indicator_types:
        windows = params.get('boll_windows', params.get('windows', [20]))
        std_dev = params.get('boll_std_dev', params.get('std_dev', 2.0))
        lazy_df = calculate_boll(lazy_df, windows, std_dev)

    # 计算WR指标
    if 'wr' in indicator_types:
        windows = params.get('wr_windows', [10, 6])
        lazy_df = calculate_wr(lazy_df, windows)

    # 计算BRAR指标
    if 'brar' in indicator_types:
        windows = params.get('brar_windows', [26])
        lazy_df = calculate_brar(lazy_df, windows)

    # 计算ASI指标
    if 'asi' in indicator_types:
        signal_period = params.get('asi_signal_period', 20)
        lazy_df = calculate_asi(lazy_df, signal_period)

    # 计算EMV指标
    if 'emv' in indicator_types:
        windows = params.get('emv_windows', [14])
        constant = params.get('emv_constant', 1.0)
        lazy_df = calculate_emv(lazy_df, windows, constant)

    # 计算MCST指标
    if 'mcst' in indicator_types:
        windows = params.get('mcst_windows', [12])
        lazy_df = calculate_mcst(lazy_df, windows)

    return lazy_df
