#!/usr/bin/env python3

"""扩展因子候选：6 类风格 + 统一、无量纲、只用当日及以前数据。

设计约束：
- 优先复用 ``factor_lib`` 已登记公式（公开口径，带 lookback/status）；
- ``src/tech_analysis/indicators/formulas.py`` 是 EMA/RSI/ATR/KDJ 的唯一真源；
- 新增技术指标只做「取指标 → 构造无量纲因子」，不重写平滑公式；
- 输出列名统一 ``ext_*``，与现有 12 因子列不冲突；
- 所有窗口算子都是 ``rolling_*``/``shift``，只用当日及以前数据。

风格分组（用于分层接入与组内共线控制）：
    trend / momentum / volatility / volume / reversal / cost
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from src.quant.factor_lib import ops as o
from src.quant.factor_lib.registry import FactorSpec, get_spec
from src.tech_analysis.indicators import formulas as f

GROUP = 'ts_code'
C = pl.col

#: 6 类风格（与配置分组名一致）
STYLE_LABELS: dict[str, str] = {
    'trend': '趋势',
    'momentum': '动量',
    'volatility': '波动',
    'volume': '量能',
    'reversal': '超买超卖',
    'cost': '成本分布',
}


@dataclass(frozen=True)
class Candidate:
    """候选因子：列名 + 风格 + 来源 + 方向。"""

    column: str
    style: str
    source: str
    direction: float = 1.0
    status: str = 'verified'
    note: str = ''


#: 直接复用 factor_lib 登记的候选（列名 = 登记名）
FACTOR_LIB_CANDIDATES: tuple[str, ...] = (
    # 趋势
    'tech_macd_hist', 'tech_ema_bias_20', 'tech_boll_pos_20', 'tech_ma_gap_5_20',
    'qlib_ma20', 'qlib_ma60', 'qlib_rsqr20', 'qlib_rsqr10', 'gtja_024', 'gtja_003',
    # 动量
    'tech_mom_20', 'qlib_roc20', 'qlib_roc60', 'ff_mom_12_1',
    # 波动
    'tech_atr_14_pct', 'qlib_std20', 'ff_vol_60', 'ff_beta_60', 'gtja_010',
    # 量能
    'tech_volume_ratio_5', 'qlib_vma20', 'qlib_vma60', 'ff_amihud_60', 'a101_042',
    'gtja_041', 'gtja_012', 'a101_012', 'a101_077', 'a101_020',
    # 超买超卖
    'tech_rsi_14', 'tech_kdj_k', 'tech_kdj_j', 'tech_up_streak', 'a101_024',
    # 成本分布（factor_lib 暂无，下面自研）
)

#: 新增技术指标候选（transform 后统一无量纲）
EXTRA_CANDIDATES: tuple[Candidate, ...] = (
    # 趋势
    Candidate('ext_adx_14', 'trend', 'DMI/ADX(14) / 100', 1.0, 'approx',
              'Wilder 平滑，ADX 归一到 0~1'),
    Candidate('ext_bbi_gap', 'trend', 'close / BBI - 1', 1.0, 'approx',
              'BBI = (MA3+MA6+MA12+MA24)/4'),
    Candidate('ext_sar_gap', 'trend', 'close / SAR - 1', 1.0, 'approx',
              '通达信 SAR 简化口径'),
    # 动量
    Candidate('ext_roc_60', 'momentum', 'ROC60 = close/delay(close,60)-1', 1.0, 'verified',
              '与 qlib_roc60 等价的独立实现（用于交叉校验）'),
    Candidate('ext_mtm_12', 'momentum', 'MTM12 / close', 1.0, 'verified',
              '12 日动量归一'),
    # 波动
    Candidate('ext_boll_width_20', 'volatility', '2*STD20/MA20', -1.0, 'verified',
              '布林带宽，越大越不稳定'),
    Candidate('ext_wr_10', 'volatility', 'WR10 = (HHV-close)/(HHV-LLV)*100', -1.0,
              'verified', '威廉指标，数值越大越超卖'),
    # 量能
    Candidate('ext_vr_26', 'volume', 'VR26 = 上涨量/下跌量 / 100', 1.0, 'verified',
              '量比指标，归一到约 0~10'),
    Candidate('ext_psy_12', 'volume', 'PSY12 = 上涨日数/12', 1.0, 'verified',
              '心理线，0~1'),
    Candidate('ext_obv_slope_20', 'volume', 'OBV 20 日斜率 / |OBV|', 1.0, 'approx',
              'OBV 变化率，避免累计量纲'),
    # 超买超卖
    Candidate('ext_cci_14', 'reversal', 'CCI14 = (TP-MA(TP,14))/(0.015*MAD)', -1.0,
              'verified', '顺势指标，极值反转'),
    Candidate('ext_rsi_6', 'reversal', 'RSI6', -1.0, 'verified', '短周期 RSI'),
    # 成本分布
    Candidate('ext_mcst_gap_13', 'cost', 'close / MCST13 - 1', 1.0, 'approx',
              '市场成本均线，成交量加权累计成本'),
    Candidate('ext_cyc_gap_13', 'cost', 'close / CYC13 - 1', 1.0, 'approx',
              '成本均线 CYC13'),
    Candidate('ext_cys_13', 'cost', 'CYS13 = (close-CYC13)/CYC13*100', 1.0, 'approx',
              '市场盈亏，百分比'),
)

#: 所有新增候选列
EXTRA_COLUMNS: tuple[str, ...] = tuple(item.column for item in EXTRA_CANDIDATES)


def _eval_lib_spec(panel: pl.DataFrame, spec: FactorSpec) -> pl.Series:
    """执行 factor_lib builder，返回与面板等长的 Series。"""
    assert spec.builder is not None
    value = spec.builder(panel)
    if isinstance(value, pl.Series):
        return value
    return panel.select(value.alias(spec.name))[spec.name]


def _wilder(expr: pl.Expr, n: int) -> pl.Expr:
    return f.wilder_expr(expr, n).over(GROUP)


def _compute_extra(panel: pl.DataFrame) -> pl.DataFrame:
    """新增 15 个技术候选（全部按 ts_code 分组、只用历史窗口）。"""
    high, low, close, volume = C('high'), C('low'), C('close'), C('volume')
    prev_close = C('pre_close') if 'pre_close' in panel.columns else close.shift(1).over(GROUP)
    tr = pl.max_horizontal(
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    )
    # ADX
    up_move = high - high.shift(1).over(GROUP)
    down_move = low.shift(1).over(GROUP) - low
    plus_dm = pl.when((up_move > down_move) & (up_move > 0)).then(up_move).otherwise(0.0)
    minus_dm = pl.when((down_move > up_move) & (down_move > 0)).then(down_move).otherwise(0.0)
    atr14 = _wilder(tr, 14)
    pdi = 100.0 * o.safe_div(_wilder(plus_dm, 14), atr14)
    ndi = 100.0 * o.safe_div(_wilder(minus_dm, 14), atr14)
    dx = 100.0 * o.safe_div((pdi - ndi).abs(), pdi + ndi)
    adx = _wilder(dx, 14) / 100.0
    # BBI / SAR
    bbi = (o.ts_mean(close, 3) + o.ts_mean(close, 6)
           + o.ts_mean(close, 12) + o.ts_mean(close, 24)) / 4.0
    bbi_gap = o.safe_div(close, bbi) - 1.0
    # SAR（简化：用前一日方向与 2 日极值近似，仅作候选，标注 approx）
    sar: pl.Expr = pl.when(close >= close.shift(1).over(GROUP)).then(
        low.rolling_min(window_size=2, min_samples=2).over(GROUP)
    ).otherwise(
        high.rolling_max(window_size=2, min_samples=2).over(GROUP)
    )
    sar_gap = o.safe_div(close, sar) - 1.0
    # MTM
    mtm12 = o.safe_div(close - o.delay(close, 12), close)
    # BOLL width
    ma20 = o.ts_mean(close, 20)
    std20 = o.ts_std(close, 20)
    boll_width = o.safe_div(2.0 * std20, ma20)
    # WR
    hhv10 = o.ts_max(high, 10)
    llv10 = o.ts_min(low, 10)
    wr10 = 100.0 * o.safe_div(hhv10 - close, hhv10 - llv10)
    # VR
    up_vol = pl.when(close > prev_close).then(volume).otherwise(0.0)
    down_vol = pl.when(close < prev_close).then(volume).otherwise(0.0)
    vr26 = o.safe_div(o.ts_sum(up_vol, 26), o.ts_sum(down_vol, 26)) / 10.0
    # PSY
    psy12 = o.ts_count(close > prev_close, 12) / 12.0
    # OBV slope
    obv_change = pl.when(close > prev_close).then(volume).when(
        close < prev_close).then(-volume).otherwise(0.0)
    obv = obv_change.cum_sum().over(GROUP)
    obv_slope = o.safe_div(obv - o.delay(obv, 20), o.ts_mean(obv.abs(), 20) + 1.0)
    # CCI
    tp = (high + low + close) / 3.0
    ma_tp = o.ts_mean(tp, 14)
    mad = (tp - ma_tp).abs().rolling_mean(window_size=14, min_samples=14).over(GROUP)
    cci14 = o.safe_div(tp - ma_tp, 0.015 * mad)
    # RSI6
    rsi6 = f.rsi_expr('close', 6).over(GROUP)
    # 成本分布
    price_volume = close * volume
    cum_cost = price_volume.cum_sum().over(GROUP)
    cum_vol = volume.cum_sum().over(GROUP)
    mcst = o.safe_div(cum_cost, cum_vol)
    mcst_gap = o.safe_div(close, mcst) - 1.0
    cyc13 = (o.ts_sum(price_volume, 13) / 13.0) / (o.ts_sum(volume, 13) / 13.0)
    cyc_gap = o.safe_div(close, cyc13) - 1.0
    cys13 = o.safe_div(close - cyc13, cyc13) * 100.0
    _ = bbi  # bbi 仅用于构造 bbi_gap

    return panel.with_columns([
        adx.alias('ext_adx_14'),
        bbi_gap.alias('ext_bbi_gap'),
        sar_gap.alias('ext_sar_gap'),
        (o.safe_div(close, o.delay(close, 60)) - 1.0).alias('ext_roc_60'),
        mtm12.alias('ext_mtm_12'),
        boll_width.alias('ext_boll_width_20'),
        wr10.alias('ext_wr_10'),
        vr26.alias('ext_vr_26'),
        psy12.alias('ext_psy_12'),
        obv_slope.alias('ext_obv_slope_20'),
        cci14.alias('ext_cci_14'),
        rsi6.alias('ext_rsi_6'),
        mcst_gap.alias('ext_mcst_gap_13'),
        cyc_gap.alias('ext_cyc_gap_13'),
        cys13.alias('ext_cys_13'),
    ])


def compute_extended_candidates(panel: pl.DataFrame, columns: tuple[str, ...] | None = None,
                                mask_invalid: bool = True) -> pl.DataFrame:
    """在面板上计算扩展候选列（只追加，不改入参）。

    ``mask_invalid=True`` 时把停牌（volume<=0）与缺失价格行的候选置 null；
    新股/历史不足由调用方按 ``history_rows`` 再掩码，缺数据绝不填 0。
    """
    if panel.is_empty():
        return panel
    from src.quant.factor_lib.factors import load_all

    load_all()
    wanted = set(columns or (*FACTOR_LIB_CANDIDATES, *EXTRA_COLUMNS))
    frame = panel.sort(['ts_code', 'date'])
    if 'seq' not in frame.columns:
        frame = frame.with_columns(
            pl.int_range(0, pl.len()).over('ts_code').cast(pl.Float64).alias('seq'))
    if 'pre_close' not in frame.columns:
        frame = frame.with_columns(
            pl.col('close').shift(1).over('ts_code').alias('pre_close'))
    if 'ret' not in frame.columns:
        frame = frame.with_columns(
            (pl.col('close') / pl.col('pre_close') - 1.0).alias('ret'))
    if 'vwap' not in frame.columns:
        factor = (pl.col('close') / pl.col('raw_close')
                  if 'raw_close' in frame.columns else pl.lit(1.0))
        frame = frame.with_columns(
            (pl.col('amount') / (pl.col('volume') * 100.0) * factor).alias('vwap'))
    lib_names = [name for name in FACTOR_LIB_CANDIDATES if name in wanted]
    extra_names = [name for name in EXTRA_COLUMNS if name in wanted]

    exprs: dict[str, pl.Series] = {}
    for name in lib_names:
        exprs[name] = _eval_lib_spec(frame, get_spec(name)).rename(name)
    if extra_names:
        with_extra = _compute_extra(frame)
        for name in extra_names:
            exprs[name] = with_extra[name]
    if not exprs:
        return frame
    out = frame.with_columns([series.alias(name) for name, series in exprs.items()])

    if mask_invalid:
        valid = (pl.col('volume') > 0) & (pl.col('close') > 0)
        out = out.with_columns([
            pl.when(valid).then(pl.col(name)).otherwise(None).alias(name)
            for name in exprs
        ])
    return out


__all__ = [
    'EXTRA_CANDIDATES',
    'EXTRA_COLUMNS',
    'FACTOR_LIB_CANDIDATES',
    'STYLE_LABELS',
    'Candidate',
    'compute_extended_candidates',
]
