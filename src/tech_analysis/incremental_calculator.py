#!/usr/bin/env python3

"""
增量计算模块，提供技术指标的增量计算功能。

统一复用 indicator_calculator.calculate_multiple_indicators_polars 的
单一公式实现，incremental 层只做三件事：
  1. _combine(existing, new, OHLCV)  — 合并历史与新增数据
  2. calculate_multiple_indicators_polars  — 在完整 combined 上算指标
  3. tail(len(new)) + 列裁剪  — 只提取新增数据对应的指标列返回

这样消除了 incremental 与 indicators/ 之间可能的口径分叉
（如 WR 正负号、OBV cumsum 方法名、to_float32 精度丢失等）。
"""

from typing import Any, Dict, List

import polars as pl
from loguru import logger

from src.tech_analysis.indicator_calculator import calculate_multiple_indicators_polars

# 基础 OHLCV 列集合（所有指标计算都能访问）
_BASE_COLS = ['date', 'open', 'high', 'low', 'close', 'volume']


class IncrementalCalculator:
    """增量计算类，提供技术指标的增量计算功能"""

    def __init__(self):
        self.supported_indicators = {
            'ma': self.incremental_calculate_ma,
            'vol_ma': self.incremental_calculate_vol_ma,
            'macd': self.incremental_calculate_macd,
            'rsi': self.incremental_calculate_rsi,
            'kdj': self.incremental_calculate_kdj,
            'boll': self.incremental_calculate_boll,
            'wr': self.incremental_calculate_wr,
            'obv': self.incremental_calculate_obv,
            'expma': self.incremental_calculate_expma,
            'dmi': self.incremental_calculate_dmi,
            'trix': self.incremental_calculate_trix,
            'sar': self.incremental_calculate_sar,
            'dma': self.incremental_calculate_dma,
            'fsl': self.incremental_calculate_fsl,
            'bbi': self.incremental_calculate_bbi,
        }

    # ------------------------------------------------------------------
    # 通用辅助
    # ------------------------------------------------------------------

    @staticmethod
    def _combine(existing_data: pl.DataFrame, new_data: pl.DataFrame,
                 columns: List[str]) -> pl.DataFrame:
        """
        合并历史数据与新增数据。两侧 dtype 可能不一致（Float32 vs Float64），
        先按需要的列对齐，再用 vertical_relaxed 合并（自动提升为公共超类型）。
        """
        cols = [c for c in columns
                if c in existing_data.columns and c in new_data.columns]
        return pl.concat(
            [existing_data.select(cols), new_data.select(cols)],
            how="vertical_relaxed"
        )

    def is_supported(self, indicator_type: str) -> bool:
        return indicator_type in self.supported_indicators

    def incremental_calculate(self,
                              indicator_type: str,
                              existing_data: pl.DataFrame,
                              new_data: pl.DataFrame,
                              **kwargs) -> pl.DataFrame:
        if not self.is_supported(indicator_type):
            raise ValueError(f"指标{indicator_type}不支持增量计算")
        try:
            return self.supported_indicators[indicator_type](
                existing_data, new_data, **kwargs)
        except Exception as e:
            logger.error(f"增量计算{indicator_type}失败: {e}")
            raise

    def _reuse(self, indicator_type: str,
               existing: pl.DataFrame, new: pl.DataFrame,
               calc_kwargs: Dict[str, Any]) -> pl.DataFrame:
        """
        复用 calculate_multiple_indicators_polars 完成增量指标计算的通用逻辑。

        Args:
            indicator_type: 指标类型名（如 'ma', 'sar'）
            existing: 历史数据（含指标列）
            new: 新增数据（不含指标列）
            calc_kwargs: 传给 calculate_multiple_indicators_polars 的 kwargs
                         （参数名已按 indicator_calculator 约定加前缀，如 ma_windows）
        Returns:
            pl.DataFrame: 仅含 new 行数 + 新增指标列的 DataFrame
        """
        combined = self._combine(existing, new, _BASE_COLS)
        full_result = calculate_multiple_indicators_polars(
            combined, [indicator_type], **calc_kwargs)
        tail_result = full_result.tail(len(new))
        new_cols = [c for c in tail_result.columns if c not in combined.columns]
        result = new.clone()
        for c in new_cols:
            result = result.with_columns(tail_result[c].alias(c))
        return result

    # ------------------------------------------------------------------
    # 15 个指标的轻量封装
    # ------------------------------------------------------------------

    def incremental_calculate_ma(self, existing, new,
                                 windows: List[int] = None, **_):
        if windows is None:
            windows = [5, 10, 20, 60]
        return self._reuse('ma', existing, new, {'ma_windows': windows})

    def incremental_calculate_vol_ma(self, existing, new,
                                      windows: List[int] = None, **_):
        if windows is None:
            windows = [5, 10]
        return self._reuse('vol_ma', existing, new, {'vol_ma_windows': windows})

    def incremental_calculate_macd(self, existing, new,
                                    fast_period=12, slow_period=26,
                                    signal_period=9, **_):
        return self._reuse('macd', existing, new, {
            'fast_period': fast_period,
            'slow_period': slow_period,
            'signal_period': signal_period,
        })

    def incremental_calculate_rsi(self, existing, new,
                                   windows: List[int] = None, **_):
        if windows is None:
            windows = [14]
        return self._reuse('rsi', existing, new, {'rsi_windows': windows})

    def incremental_calculate_kdj(self, existing, new,
                                   windows: List[int] = None, **_):
        if windows is None:
            windows = [14]
        return self._reuse('kdj', existing, new, {'kdj_windows': windows})

    def incremental_calculate_boll(self, existing, new,
                                   windows: List[int] = None,
                                   std_dev: float = 2.0, **_):
        if windows is None:
            windows = [20]
        return self._reuse('boll', existing, new, {
            'boll_windows': windows,
            'boll_std_dev': std_dev,
        })

    def incremental_calculate_wr(self, existing, new,
                                 windows: List[int] = None, **_):
        if windows is None:
            windows = [10, 6]
        return self._reuse('wr', existing, new, {'wr_windows': windows})

    def incremental_calculate_obv(self, existing, new, **_):
        return self._reuse('obv', existing, new, {})

    def incremental_calculate_expma(self, existing, new,
                                     windows: List[int] = None, **_):
        if windows is None:
            windows = [12, 50]
        return self._reuse('expma', existing, new,
                            {'expma_windows': windows})

    def incremental_calculate_dmi(self, existing, new,
                                  windows: List[int] = None, **_):
        if windows is None:
            windows = [14]
        return self._reuse('dmi', existing, new, {'dmi_windows': windows})

    def incremental_calculate_trix(self, existing, new,
                                    windows: List[int] = None,
                                    signal_period: int = 9, **_):
        if windows is None:
            windows = [12]
        return self._reuse('trix', existing, new, {
            'trix_windows': windows,
            'trix_signal_period': signal_period,
        })

    def incremental_calculate_sar(self, existing, new,
                                    af_step: float = 0.02,
                                    max_af: float = 0.2, **_):
        return self._reuse('sar', existing, new, {
            'sar_af_step': af_step,
            'sar_max_af': max_af,
        })

    def incremental_calculate_dma(self, existing, new,
                                    short_period=10, long_period=50,
                                    signal_period=10, **_):
        return self._reuse('dma', existing, new, {
            'dma_short_period': short_period,
            'dma_long_period': long_period,
            'dma_signal_period': signal_period,
        })

    def incremental_calculate_fsl(self, existing, new, **_):
        return self._reuse('fsl', existing, new, {})

    def incremental_calculate_bbi(self, existing, new, **_):
        return self._reuse('bbi', existing, new, {})


# 全局增量计算器实例
global_incremental_calculator = IncrementalCalculator()
