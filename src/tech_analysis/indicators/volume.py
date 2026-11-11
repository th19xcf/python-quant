#!/usr/bin/env python3

"""
成交量类指标计算模块
包含：OBV, VR, PSY, VOL_MA等
"""

import polars as pl
from loguru import logger

from src.utils.dataframe_columns import resolve_date_column

from ..common_calculations import add_default_columns, calculate_moving_average
from ..utils import to_float32


def _attach_turn(lazy_df: pl.LazyFrame, ts_code: str) -> pl.LazyFrame:
    """给行情帧追加 ``turn``（百分数）与 ``turn_source``，来源分级由数据层决定。

    优先级：逐日流通股本反算 > 当前股本静态近似 > 已落库 baostock turn。
    读取失败（股本表/换手率表缺失、文件损坏）按“缺失”处理，回退估算口径；
    该模块不反向依赖数据层的初始化状态。
    """
    try:
        from src.data.local_store.turnover import attach_turn
        return attach_turn(lazy_df, ts_code)
    except (ImportError, OSError, ValueError, TypeError, pl.exceptions.PolarsError) as exc:
        logger.debug(f'附加换手率失败({ts_code}): {exc}')
        return lazy_df


def calculate_vol_ma(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算成交量移动平均线指标

    Args:
        lazy_df: Polars LazyFrame
        windows: 成交量移动平均窗口列表

    Returns:
        pl.LazyFrame: 包含成交量移动平均线的LazyFrame
    """
    # 使用通用移动平均计算函数
    lazy_df = calculate_moving_average(lazy_df, 'volume', windows, 'vol_')
    # 添加默认列名
    lazy_df = add_default_columns(lazy_df, 'vol_ma', windows)
    return lazy_df


def calculate_obv(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    """
    计算OBV指标

    Args:
        lazy_df: Polars LazyFrame

    Returns:
        pl.LazyFrame: 包含OBV指标的LazyFrame
    """
    # 1. 计算价格变化方向
    obv_change = to_float32(pl.when(pl.col('close') > pl.col('close').shift(1)).then(pl.col('volume')).when(pl.col('close') < pl.col('close').shift(1)).then(-pl.col('volume')).otherwise(0.0))

    # 2. 累积计算OBV
    return lazy_df.with_columns(
        to_float32(obv_change.cum_sum()).alias('obv')
    )


def calculate_vr(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算VR指标

    Args:
        lazy_df: Polars LazyFrame
        windows: VR计算窗口列表

    Returns:
        pl.LazyFrame: 包含VR指标的LazyFrame
    """
    # 1. 计算价格变化方向和分类成交量
    prev_close = pl.col('close').shift(1)
    up_vol = pl.when(pl.col('close') > prev_close).then(pl.col('volume')).otherwise(0.0)
    down_vol = pl.when(pl.col('close') < prev_close).then(pl.col('volume')).otherwise(0.0)
    flat_vol = pl.when(pl.col('close') == prev_close).then(pl.col('volume')).otherwise(0.0)

    # 2. 计算各窗口的VR值
    for window in windows:
        # 计算N日上涨、下跌、平盘成交量总和
        up_sum = to_float32(up_vol.rolling_sum(window_size=window, min_samples=1))
        down_sum = to_float32(down_vol.rolling_sum(window_size=window, min_samples=1))
        flat_sum = to_float32(flat_vol.rolling_sum(window_size=window, min_samples=1))

        # 计算VR值 = (上涨总和 + 1/2平盘总和) / (下跌总和 + 1/2平盘总和) * 100
        vr = to_float32((up_sum + flat_sum / 2) /
             (down_sum + flat_sum / 2 + 0.0001) * 100).alias(f'vr{window}')

        lazy_df = lazy_df.with_columns(vr)

    # 3. 添加默认列名
    lazy_df = add_default_columns(lazy_df, 'vr', windows)
    return lazy_df


def calculate_psy(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算PSY指标

    Args:
        lazy_df: Polars LazyFrame
        windows: PSY计算窗口列表

    Returns:
        pl.LazyFrame: 包含PSY指标的LazyFrame
    """
    # 1. 计算上涨天数标记（上涨为1，否则为0）
    up_day = to_float32(pl.when(pl.col('close') > pl.col('close').shift(1)).then(1.0).otherwise(0.0))

    # 2. 计算各窗口的PSY值（N天内上涨天数百分比）
    for window in windows:
        psy_expr = to_float32(up_day.rolling_sum(window_size=window, min_samples=1) / window * 100).alias(f'psy{window}')
        lazy_df = lazy_df.with_columns(psy_expr)

    # 3. 添加默认列名
    lazy_df = add_default_columns(lazy_df, 'psy', windows)
    return lazy_df


def calculate_cr(lazy_df: pl.LazyFrame, windows: list) -> pl.LazyFrame:
    """
    计算CR指标（能量指标）
    CR = (N日内上涨日成交量总和 - N日内下跌日成交量总和) / (N日内上涨日成交量总和 + N日内下跌日成交量总和) * 100

    Args:
        lazy_df: Polars LazyFrame
        windows: CR计算窗口列表

    Returns:
        pl.LazyFrame: 包含CR指标的LazyFrame
    """
    # 1. 计算价格变化方向和分类成交量
    prev_close = pl.col('close').shift(1)
    up_vol = pl.when(pl.col('close') > prev_close).then(pl.col('volume')).otherwise(0.0)
    down_vol = pl.when(pl.col('close') < prev_close).then(pl.col('volume')).otherwise(0.0)

    # 2. 计算各窗口的CR值
    for window in windows:
        # 计算N日上涨、下跌成交量总和
        up_sum = to_float32(up_vol.rolling_sum(window_size=window, min_samples=1))
        down_sum = to_float32(down_vol.rolling_sum(window_size=window, min_samples=1))

        # 计算CR值 = (上涨总和 - 下跌总和) / (上涨总和 + 下跌总和) * 100
        cr = to_float32((up_sum - down_sum) /
             (up_sum + down_sum + 0.0001) * 100).alias(f'cr{window}')

        lazy_df = lazy_df.with_columns(cr)

    # 3. 添加默认列名
    lazy_df = add_default_columns(lazy_df, 'cr', windows)
    return lazy_df


def calculate_hsl(lazy_df: pl.LazyFrame, float_share: float | None = None,
                  turn_frame: pl.DataFrame | None = None) -> pl.LazyFrame:
    """
    计算HSL指标（换手率）

    数值来源优先级（列名 ``hsl`` 与 0~100 百分比语义保持不变，只改数值来源）：
    1. 输入帧自带 ``turn`` 列（真实换手率，**百分数**，0.1979 即 0.1979%；
       日线由 ``turnover.attach_turn`` 按「逐日股本 > 静态股本 > baostock」
       分级填充，周/月线由聚合层按区间成交量加权计算）；
    2. ``turn_frame``（列 ``trade_date/turn``）按日期左连接；
    3. ``float_share`` 有效时按 ``volume(手) × 100 / float_share × 100``；
    4. 以上都没有 → 旧口径估算 ``volume / MA60(volume) × 5``，并在
       ``hsl_source`` 列标注 ``estimated``。

    Args:
        lazy_df: Polars LazyFrame（需含 ``volume``，可选 ``date``/``turn``）
        float_share: 流通股本（股）；仅在第 3 优先级使用
        turn_frame: 真实换手率帧，列 ``trade_date/turn``（百分数）

    Returns:
        pl.LazyFrame: 含 ``hsl`` / ``hsl_ma5`` / ``hsl_ma10`` / ``hsl_source``
    """
    names = lazy_df.collect_schema().names()
    date_key = resolve_date_column(names)

    turn_expr: pl.Expr | None = None
    if 'turn' in names:
        turn_expr = pl.col('turn').cast(pl.Float64)
    elif turn_frame is not None and not turn_frame.is_empty() and date_key is not None:
        joined = turn_frame.rename({'trade_date': date_key}).select([date_key, 'turn'])
        lazy_df = lazy_df.join(joined.lazy(), on=date_key, how='left')
        turn_expr = pl.col('turn').cast(pl.Float64)

    # 旧口径估算：60 日平均成交量作为流通股本的参考量
    avg_volume = pl.col('volume').rolling_mean(window_size=60, min_samples=1)
    estimated = pl.when(avg_volume > 0).then(
        pl.col('volume') / avg_volume * 5).otherwise(0.0)

    if turn_expr is not None:
        real = turn_expr.is_not_null() & (turn_expr > 0)
        hsl_expr = pl.when(real).then(turn_expr).otherwise(estimated)
        source_expr = pl.when(real).then(pl.lit('turn')).otherwise(pl.lit('estimated'))
    elif float_share is not None and float_share > 0:
        # volume 单位「手」：×100 换股，再 ×100 转百分数
        hsl_expr = pl.col('volume') * 100.0 * 100.0 / float_share
        source_expr = pl.lit('float_share')
    else:
        hsl_expr = estimated
        source_expr = pl.lit('estimated')

    lazy_df = lazy_df.with_columns([
        to_float32(hsl_expr).alias('hsl'),
        source_expr.alias('hsl_source'),
    ])

    # 计算HSL的5日和10日移动平均
    hsl_ma5 = to_float32(pl.col('hsl').rolling_mean(window_size=5, min_samples=1)).alias('hsl_ma5')
    hsl_ma10 = to_float32(pl.col('hsl').rolling_mean(window_size=10, min_samples=1)).alias('hsl_ma10')

    return lazy_df.with_columns([hsl_ma5, hsl_ma10])


def calculate_lb(lazy_df: pl.LazyFrame, period: int = 5) -> pl.LazyFrame:
    """
    计算LB指标（量比）
    LB = 当日成交量 / 过去N日平均成交量

    Args:
        lazy_df: Polars LazyFrame
        period: 计算平均成交量的周期，默认5日

    Returns:
        pl.LazyFrame: 包含LB指标的LazyFrame
    """
    # 计算过去N日平均成交量（不包含当日）
    avg_volume = pl.col('volume').shift(1).rolling_mean(window_size=period, min_samples=1)

    # 计算量比
    lb = to_float32(pl.when(avg_volume > 0).then(pl.col('volume') / avg_volume).otherwise(1)).alias('lb')

    return lazy_df.with_columns(lb)


def calculate_volume_indicators(lazy_df: pl.LazyFrame, indicator_types: list, **params) -> pl.LazyFrame:
    """
    计算所有成交量类指标

    Args:
        lazy_df: Polars LazyFrame
        indicator_types: 需要计算的成交量类指标列表
        **params: 指标计算参数

    Returns:
        pl.LazyFrame: 包含所有计算成交量类指标的LazyFrame
    """
    # 计算VOL_MA指标
    if 'vol_ma' in indicator_types:
        windows = params.get('vol_ma_windows', [5, 10])
        lazy_df = calculate_vol_ma(lazy_df, windows)

    # 计算OBV指标
    if 'obv' in indicator_types:
        lazy_df = calculate_obv(lazy_df)

    # 计算VR指标
    if 'vr' in indicator_types:
        windows = params.get('vr_windows', [26])
        lazy_df = calculate_vr(lazy_df, windows)

    # 计算PSY指标
    if 'psy' in indicator_types:
        windows = params.get('psy_windows', [12])
        lazy_df = calculate_psy(lazy_df, windows)

    # 计算CR指标
    if 'cr' in indicator_types:
        windows = params.get('cr_windows', [26])
        lazy_df = calculate_cr(lazy_df, windows)

    # 计算HSL指标（换手率）
    if 'hsl' in indicator_types:
        float_share = params.get('float_share')
        turn_frame = params.get('turn_frame')
        ts_code = params.get('ts_code')
        if turn_frame is None and ts_code:
            # 分级来源：逐日股本 > 静态股本 > baostock turn；周/月线帧自带聚合 turn
            lazy_df = _attach_turn(lazy_df, ts_code)
        has_turn = 'turn' in lazy_df.collect_schema().names()
        if not has_turn and (turn_frame is None or turn_frame.is_empty()):
            msg = (f'HSL 未取到 {ts_code or "<unknown>"} 的真实换手率，'
                   f'回退估算口径 (hsl_source=estimated)')
            if ts_code:
                logger.warning(msg)
            else:
                logger.debug(msg)
        lazy_df = calculate_hsl(lazy_df, float_share, turn_frame)

    # 计算LB指标（量比）
    if 'lb' in indicator_types:
        period = params.get('lb_period', 5)
        lazy_df = calculate_lb(lazy_df, period)

    return lazy_df
