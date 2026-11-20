#!/usr/bin/env python3

"""
多指标共振择时策略 v3（锚点 + 确认窗口版，2026-11-20）

v3 不再试图让「互斥条件同日同时成立」，而是把每条规则拆成
「锚点事件（触发角色）」+「确认事件（确认角色）」两类，各自独立可触发：

    买入共振 = 锚点：MACD 金叉且当日 DIF<0
               确认：锚点后第 1~W 个交易日内，RSI12 上穿 50 且 近20日最低 > 近60日最低
               确认当日 T 出信号 → 引擎 T+1 开盘建仓 100%；W 日内未确认则丢弃锚点
    卖出共振 = 锚点：MACD 死叉且当日 DIF>0
               确认：锚点后第 1~W 个交易日内，RSI12 下穿 50 且 近20日最高 < 近60日最高
               确认当日清仓；W 日内未确认则丢弃锚点
    震荡低吸 = 近 10 个交易日 RSI12 最小值 <= rsi_low_threshold
               且当日 RSI12 上穿 35 且 收盘 >= MA20 → 加仓至 50%（已满仓则引擎生成零单）
    震荡高抛 = 近 10 个交易日 RSI12 最大值 >= rsi_high_threshold
               且当日 RSI12 下穿 65 → 减仓至 50%
    趋势持股 = DIF>0 且 DIF>DEA 且 收盘 >= MA20（沿用 v2）

其余沿用 v1/v2：MACD(12,26,9)、RSI(12)、20/60 日新高新低、MA20、
冲突优先级 卖出 > 高抛 > 买入 > 低吸 > 持有、T 日信号 T+1 开盘成交。

无未来函数：所有判定只用当日及更早的 bar（确认日收盘才知 RSI 上穿/下穿），
信号由引擎在下一交易日开盘执行（execution_core.py 的挂单-执行两阶段）。
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import polars as pl

from src.backtest.strategies.base_strategy import BaseStrategy
from src.tech_analysis.indicators.formulas import macd_exprs, rsi_expr

TRIGGER_NONE = 0
TRIGGER_BUY = 1
TRIGGER_SELL = 2
TRIGGER_LOW_BUY = 3
TRIGGER_HIGH_SELL = 4
TRIGGER_HOLD = 5

TRIGGER_NAMES = {
    TRIGGER_NONE: '无',
    TRIGGER_BUY: '买入共振',
    TRIGGER_SELL: '卖出共振',
    TRIGGER_LOW_BUY: '震荡低吸',
    TRIGGER_HIGH_SELL: '震荡高抛',
    TRIGGER_HOLD: '趋势持股',
}

_INTENTS = {
    TRIGGER_BUY: {'action': 'buy', 'target_weight': 1.0},
    TRIGGER_LOW_BUY: {'action': 'buy', 'target_weight': 0.5},
    TRIGGER_SELL: {'action': 'sell', 'fraction': 1.0},
    TRIGGER_HIGH_SELL: {'action': 'sell', 'target_weight': 0.5},
    TRIGGER_HOLD: None,
    TRIGGER_NONE: None,
}


def _gt(a: Any, b: Any) -> bool:
    return a is not None and b is not None and a > b


def _window_min(vals: List[Any], i: int, w: int) -> Any:
    """近 w 根 bar（含当日）的最小值；窗口内出现 null 则返回 None。"""
    if i + 1 < w:
        return None
    seg = vals[i - w + 1:i + 1]
    if any(v is None for v in seg):
        return None
    return min(seg)


def _window_max(vals: List[Any], i: int, w: int) -> Any:
    if i + 1 < w:
        return None
    seg = vals[i - w + 1:i + 1]
    if any(v is None for v in seg):
        return None
    return max(seg)


def scan_triggers(
    dif: List[Any], dea: List[Any], rsi: List[Any], close: List[Any],
    ma20: List[Any], low20: List[Any], low60: List[Any],
    high20: List[Any], high60: List[Any], osc_window: int,
    rsi_low_threshold: float, rsi_low_cross: float,
    rsi_high_threshold: float, rsi_high_cross: float,
    confirm_window: int,
) -> Tuple[List[int], Dict[str, int], List[int], List[int], List[int], List[int]]:
    """逐 bar 状态机：锚点登记 → 确认窗口判定 → 优先级落成触发码。"""
    n = len(dif)
    golden = [False] * n
    death = [False] * n
    up_mid = [False] * n
    down_mid = [False] * n
    up_osc = [False] * n
    down_osc = [False] * n
    for i in range(1, n):
        d, s, dp, sp = dif[i], dea[i], dif[i - 1], dea[i - 1]
        if None not in (d, s, dp, sp):
            golden[i] = d > s and dp <= sp
            death[i] = d < s and dp >= sp
        r, rp = rsi[i], rsi[i - 1]
        if None not in (r, rp):
            up_mid[i] = r > 50 and rp <= 50
            down_mid[i] = r < 50 and rp >= 50
            up_osc[i] = r > rsi_low_cross and rp <= rsi_low_cross
            down_osc[i] = r < rsi_high_cross and rp >= rsi_high_cross

    buy_sig = [False] * n
    sell_sig = [False] * n
    low_buy = [False] * n
    high_sell = [False] * n
    hold = [False] * n
    anchor_buy_mark = [0] * n
    confirm_buy_mark = [0] * n
    anchor_sell_mark = [0] * n
    confirm_sell_mark = [0] * n

    buy_pending: List[int] = []
    sell_pending: List[int] = []
    stats = {'buy_anchors': 0, 'buy_confirms': 0,
             'sell_anchors': 0, 'sell_confirms': 0}

    for i in range(n):
        # 1) 先做确认判定：锚点必须落在 1~confirm_window 个交易日之前
        if buy_pending and up_mid[i] and _gt(low20[i], low60[i]):
            hit = [a for a in buy_pending if 1 <= i - a <= confirm_window]
            if hit:
                buy_sig[i] = True
                confirm_buy_mark[i] = len(hit)
                stats['buy_confirms'] += len(hit)
                buy_pending = [a for a in buy_pending if a not in set(hit)]
        if sell_pending and down_mid[i] and _gt(high60[i], high20[i]):
            hit = [a for a in sell_pending if 1 <= i - a <= confirm_window]
            if hit:
                sell_sig[i] = True
                confirm_sell_mark[i] = len(hit)
                stats['sell_confirms'] += len(hit)
                sell_pending = [a for a in sell_pending if a not in set(hit)]

        # 2) 登记当日新锚点
        if golden[i] and dif[i] is not None and dif[i] < 0:
            buy_pending.append(i)
            anchor_buy_mark[i] = 1
            stats['buy_anchors'] += 1
        if death[i] and dif[i] is not None and dif[i] > 0:
            sell_pending.append(i)
            anchor_sell_mark[i] = 1
            stats['sell_anchors'] += 1

        # 3) 丢弃过期锚点（超过 confirm_window 未确认）
        buy_pending = [a for a in buy_pending if i - a <= confirm_window]
        sell_pending = [a for a in sell_pending if i - a <= confirm_window]

        # 4) 震荡低吸 / 震荡高抛 / 趋势持股
        if (up_osc[i] and _window_min(rsi, i, osc_window) is not None
                and _window_min(rsi, i, osc_window) <= rsi_low_threshold
                and close[i] is not None and ma20[i] is not None
                and close[i] >= ma20[i]):
            low_buy[i] = True
        if (down_osc[i] and _window_max(rsi, i, osc_window) is not None
                and _window_max(rsi, i, osc_window) >= rsi_high_threshold):
            high_sell[i] = True
        if None not in (dif[i], dea[i], ma20[i], close[i]):
            hold[i] = dif[i] > 0 and dif[i] > dea[i] and close[i] >= ma20[i]

    # 5) 冲突优先级：卖出 > 高抛 > 买入 > 低吸 > 持有
    codes = [TRIGGER_NONE] * n
    for i in range(n):
        if sell_sig[i]:
            codes[i] = TRIGGER_SELL
        elif high_sell[i]:
            codes[i] = TRIGGER_HIGH_SELL
        elif buy_sig[i]:
            codes[i] = TRIGGER_BUY
        elif low_buy[i]:
            codes[i] = TRIGGER_LOW_BUY
        elif hold[i]:
            codes[i] = TRIGGER_HOLD
    return codes, stats, anchor_buy_mark, confirm_buy_mark, anchor_sell_mark, confirm_sell_mark


class MultiResonanceTimingV3Strategy(BaseStrategy):
    """多指标共振择时 v3：锚点事件 + 后续确认窗口。"""

    TRIGGER_COL = 'trigger_type'

    def __init__(
        self,
        rsi_low_threshold: float = 25,
        rsi_low_cross: float = 35,
        rsi_high_threshold: float = 75,
        rsi_high_cross: float = 65,
        confirm_window: int = 5,
        osc_window: int = 10,
        high_low_window: int = 60,
        signal_lag: int = 0,
    ) -> None:
        super().__init__(name='多指标共振择时v3')
        self.params.update({
            'rsi_low_threshold': rsi_low_threshold,
            'rsi_low_cross': rsi_low_cross,
            'rsi_high_threshold': rsi_high_threshold,
            'rsi_high_cross': rsi_high_cross,
            'confirm_window': confirm_window,
            'osc_window': osc_window,
            'high_low_window': high_low_window,
            'signal_lag': signal_lag,
        })

    def _indicator_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        macd = macd_exprs('close', 12, 26, 9)
        win = int(self.params['high_low_window'])
        return df.with_columns([
            macd['macd'].alias('_dif'),
            macd['signal'].alias('_dea'),
            rsi_expr('close', 12).alias('_rsi'),
            pl.col('close').rolling_mean(window_size=20, min_samples=20).alias('_ma20'),
            pl.col('low').rolling_min(window_size=20, min_samples=20).alias('_low20'),
            pl.col('low').rolling_min(window_size=win, min_samples=win).alias('_low60'),
            pl.col('high').rolling_max(window_size=20, min_samples=20).alias('_high20'),
            pl.col('high').rolling_max(window_size=win, min_samples=win).alias('_high60'),
        ])

    def _scan(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, Dict[str, int]]:
        p = self.params
        frame = self._indicator_frame(df)
        cols = {name: frame[name].to_list() for name in
                ('_dif', '_dea', '_rsi', 'close', '_ma20',
                 '_low20', '_low60', '_high20', '_high60')}
        codes, stats, ab, cb, asell, csell = scan_triggers(
            dif=cols['_dif'], dea=cols['_dea'], rsi=cols['_rsi'], close=cols['close'],
            ma20=cols['_ma20'], low20=cols['_low20'], low60=cols['_low60'],
            high20=cols['_high20'], high60=cols['_high60'],
            osc_window=int(p['osc_window']),
            rsi_low_threshold=float(p['rsi_low_threshold']),
            rsi_low_cross=float(p['rsi_low_cross']),
            rsi_high_threshold=float(p['rsi_high_threshold']),
            rsi_high_cross=float(p['rsi_high_cross']),
            confirm_window=int(p['confirm_window']),
        )
        lag = int(p['signal_lag'])
        if lag > 0:
            codes = [TRIGGER_NONE] * lag + codes[:-lag]
        names = [TRIGGER_NAMES[c] for c in codes]
        frame = frame.with_columns([
            pl.Series(self.TRIGGER_COL, names, dtype=pl.Utf8),
            pl.Series('signal', [_INTENTS[c] for c in codes], dtype=pl.Object),
            pl.Series('anchor_buy_mark', ab, dtype=pl.Int16),
            pl.Series('confirm_buy_mark', cb, dtype=pl.Int16),
            pl.Series('anchor_sell_mark', asell, dtype=pl.Int16),
            pl.Series('confirm_sell_mark', csell, dtype=pl.Int16),
        ])
        return frame, stats

    def build_signal_frame(self, df: pl.DataFrame) -> pl.DataFrame:
        return self._scan(df)[0]

    def diagnose(self, df: pl.DataFrame) -> Dict[str, int]:
        return self._scan(df)[1]

    def generate_signals_vectorized(self, df: pl.DataFrame) -> pl.Series:
        return self.build_signal_frame(df)['signal']

    def generate_signal(self, row: Dict[str, Any], index: int) -> Any:
        """逐 bar 兜底接口：只回放 build_signal_frame 已写好的触发列。"""
        name = row.get(self.TRIGGER_COL)
        code = next((c for c, n in TRIGGER_NAMES.items() if n == name), TRIGGER_NONE)
        return _INTENTS[code]
