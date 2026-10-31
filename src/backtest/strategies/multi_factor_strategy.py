#!/usr/bin/env python3

"""
多因子策略：结合向量化预计算的技术指标生成信号

依赖 BacktestEngine._precompute_indicators 预计算好的列：
    ma_{short_window}, ma_{long_window}   （MA 因子）
    macd, macd_signal                     （MACD 因子）
    rsi                                   （RSI 因子）

generate_signal 按权重聚合三个因子的二元贡献（1 / 0 / -1），
加权得分超过阈值（默认 0.5）买入、低于负阈值卖出。
默认权重 {ma:0.3, macd:0.4, rsi:0.3} 下，单因子最大贡献 0.4、
两因子最小共振和 0.6，阈值取 0.5 落在二者之间，
保证至少两个因子同向共振才触发交易（MACD 一票不能独决）。
"""

from typing import Any, Dict, List

import polars as pl

from src.backtest.strategies.base_strategy import BaseStrategy


class MultiFactorStrategy(BaseStrategy):
    """结合 MA / MACD / RSI 的综合因子策略（默认需至少两因子共振）"""

    def __init__(self, factors: List[str] | None = None, weights: Dict[str, float] | None = None,
                 threshold: float = 0.5):
        super().__init__(name="多因子策略")
        self.params['factors'] = factors or ['ma', 'macd', 'rsi']
        self.params['weights'] = weights or {'ma': 0.3, 'macd': 0.4, 'rsi': 0.3}
        self.params['threshold'] = threshold

    def _ma_factor(self, data: Dict[str, Any]) -> float:
        short_w = self.params.get('short_window', 10)
        long_w = self.params.get('long_window', 20)
        short_ma = data.get(f'ma_{short_w}')
        long_ma = data.get(f'ma_{long_w}')
        if short_ma is None or long_ma is None:
            return 0
        if short_ma > long_ma:
            return 1
        if short_ma < long_ma:
            return -1
        return 0

    def _macd_factor(self, data: Dict[str, Any]) -> float:
        macd = data.get('macd')
        signal = data.get('macd_signal')
        if macd is None or signal is None:
            return 0
        # 当前 MACD 穿越信号线的方向（回测里没有 prev_macd/prev_signal 历史，
        # 这里退化为同方向打分；若需精确金叉/死叉可在引擎里再加 prev_* 列）
        if macd > signal:
            return 1
        if macd < signal:
            return -1
        return 0

    def _rsi_factor(self, data: Dict[str, Any]) -> float:
        rsi = data.get('rsi')
        if rsi is None:
            return 0
        if rsi < 30:
            return 1
        if rsi > 70:
            return -1
        return 0

    def _warmup_bars(self) -> int:
        """暖机根数：窗口未满的指标值是噪声，暖机期内不交易。

        MA 列由引擎以 min_samples=window 预计算（窗口内为 null，因子已判 None 自保），
        RSI 为 Wilder 种子口径（前 period-1 根为 null），
        MACD 按通达信口径从第 0 根有值，需按 慢线+信号线 根数暖机。
        """
        return max(
            int(self.params.get('long_window', 20)),
            int(self.params.get('rsi_period', 14)),
            26 + 9,
        )

    def generate_signal(self, data: Dict[str, Any], index: int) -> str:
        if index < self._warmup_bars():
            return 'hold'
        factor_values = {}
        factors = self.params['factors']
        if 'ma' in factors:
            factor_values['ma'] = self._ma_factor(data)
        if 'macd' in factors:
            factor_values['macd'] = self._macd_factor(data)
        if 'rsi' in factors:
            factor_values['rsi'] = self._rsi_factor(data)

        weights = self.params['weights']
        score = sum(
            factor_values[f] * weights.get(f, 0)
            for f in factor_values
        )

        threshold = self.params['threshold']
        if score > threshold:
            return 'buy'
        if score < -threshold:
            return 'sell'
        return 'hold'

    def _factor_expr(self, name: str, df: pl.DataFrame) -> pl.Expr | None:
        """单因子贡献表达式（1 / 0 / -1），与对应 _*_factor 逐行版语义一致。

        依赖列缺失时返回 None，等价逐行版 data.get() 取 None → 贡献 0。
        """
        if name == 'ma':
            short_w = self.params.get('short_window', 10)
            long_w = self.params.get('long_window', 20)
            short_col = f'ma_{short_w}'
            long_col = f'ma_{long_w}'
            if short_col not in df.columns or long_col not in df.columns:
                return None
            short_ma = pl.col(short_col)
            long_ma = pl.col(long_col)
            return (
                pl.when(short_ma.is_null() | long_ma.is_null()).then(pl.lit(0.0))
                  .when(short_ma > long_ma).then(pl.lit(1.0))
                  .when(short_ma < long_ma).then(pl.lit(-1.0))
                  .otherwise(pl.lit(0.0))
            )
        if name == 'macd':
            if 'macd' not in df.columns or 'macd_signal' not in df.columns:
                return None
            macd = pl.col('macd')
            sig = pl.col('macd_signal')
            # 当前 MACD 穿越信号线的方向（同逐行版退化为同方向打分）
            return (
                pl.when(macd.is_null() | sig.is_null()).then(pl.lit(0.0))
                  .when(macd > sig).then(pl.lit(1.0))
                  .when(macd < sig).then(pl.lit(-1.0))
                  .otherwise(pl.lit(0.0))
            )
        if name == 'rsi':
            if 'rsi' not in df.columns:
                return None
            rsi = pl.col('rsi')
            return (
                pl.when(rsi.is_null()).then(pl.lit(0.0))
                  .when(rsi < 30).then(pl.lit(1.0))
                  .when(rsi > 70).then(pl.lit(-1.0))
                  .otherwise(pl.lit(0.0))
            )
        return None

    def generate_signals_vectorized(self, df: pl.DataFrame) -> pl.Series:
        weights = self.params['weights']
        threshold = self.params['threshold']

        # 按 factors 顺序加权累加；sum() 从 0 起左折叠，与逐行版 sum()
        # 的浮点累加顺序完全一致（表达式体避免语句级循环，架构守卫要求）
        contributions = [
            expr * weights.get(name, 0)
            for name in self.params['factors']
            if (expr := self._factor_expr(name, df)) is not None
        ]

        if contributions:
            score = sum(contributions)
            decision = (
                pl.when(score > threshold).then(pl.lit('buy'))
                  .when(score < -threshold).then(pl.lit('sell'))
                  .otherwise(pl.lit('hold'))
            )
        else:
            decision = pl.lit('hold')

        # 暖机期内一律 hold（逐行版 index < warmup 直接返回，先于因子计算）
        warmup = pl.int_range(0, df.height) < self._warmup_bars()
        return df.select(
            pl.when(warmup).then(pl.lit('hold')).otherwise(decision).alias('signal')
        ).to_series()
