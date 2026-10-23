#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
增量计算 vs 批量计算 逐位相等 parity 测试

核心假设：
    incremental_calculator 是薄壳 → _combine(existing, new) 拼历史+新增
                                    → calculate_multiple_indicators_polars(combined, [type])
                                    → tail(len(new)) 抽新增段
    同一份 combined 数据直接走 calculate_multiple_indicators_polars
    → tail(len(new)) 的新增段 应该和 incremental 输出完全一致

如果这个测试挂了，说明 incremental_calculator 的某条路径偷偷用了
不同的种子值/warmup/公式，回测和实盘就是两个策略。
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import polars as pl
import pytest
from datetime import date, timedelta

from src.tech_analysis.incremental_calculator import IncrementalCalculator
from src.tech_analysis.indicator_calculator import calculate_multiple_indicators_polars


# ======================================================================
# Fixtures
# ======================================================================

def _make_kline(n=500, seed=42) -> pl.DataFrame:
    """生成随机但 realistic 的日线 OHLCV"""
    rng = np.random.default_rng(seed)
    dates = pl.date_range(date(2022, 1, 1), date(2022, 1, 1) + timedelta(days=n - 1), eager=True)
    close = 100 + np.cumsum(rng.normal(0, 1, n))
    high = close + np.abs(rng.normal(0, 2, n))
    low = close - np.abs(rng.normal(0, 2, n))
    open_ = close + rng.normal(0, 1, n)
    open_ = np.clip(open_, low, high)
    volume = rng.integers(100_000, 5_000_000, n).astype(float)
    return pl.DataFrame({
        'date': dates, 'open': open_, 'high': high,
        'low': low, 'close': close, 'volume': volume,
    })


@pytest.fixture
def full_df():
    """完整 500 bar 数据"""
    return _make_kline(500)


@pytest.fixture
def split_dfs(full_df):
    """切成 existing=前 400 bar + new=后 100 bar"""
    return full_df.head(400), full_df.tail(100)


@pytest.fixture
def calc():
    return IncrementalCalculator()


# ======================================================================
# Helper: 对比两个 DataFrame 的所有共有数值列
# ======================================================================

def _compare(a: pl.DataFrame, b: pl.DataFrame, rtol=1e-5, atol=1e-8) -> None:
    """逐列断言 a 和 b 逐元素相等（NaN 视为相等）。不相等则输出 diff 详情。"""
    common_cols = [c for c in a.columns if c in b.columns]
    assert len(common_cols) > 0, f"无共有列: a={a.columns} b={b.columns}"

    for col in common_cols:
        sa, sb = a[col].to_numpy(), b[col].to_numpy()
        assert len(sa) == len(sb), f"{col}: length mismatch {len(sa)} vs {len(sb)}"

        # NaN mask
        nan_a = np.isnan(sa.astype(float)) if sa.dtype.kind == 'f' else np.zeros_like(sa, dtype=bool)
        nan_b = np.isnan(sb.astype(float)) if sb.dtype.kind == 'f' else np.zeros_like(sb, dtype=bool)
        both_nan = nan_a & nan_b
        one_nan = nan_a ^ nan_b
        assert not one_nan.any(), f"{col}: 单边 NaN 出现 {one_nan.sum()} 次"

        mask = ~(nan_a | nan_b)
        if mask.any():
            diff = np.abs(sa[mask] - sb[mask])
            big = diff > atol + rtol * np.abs(sa[mask])
            if big.any():
                idx = np.where(big)[0][:5]
                details = [f"  [{i}] incremental={sa[mask][i]:.12f} batch={sb[mask][i]:.12f} diff={diff[i]:.2e}"
                           for i in idx]
                raise AssertionError(
                    f"{col}: 有 {big.sum()}/{mask.sum()} 个值差异超容限 (rtol={rtol}, atol={atol}):\n"
                    + "\n".join(details)
                )


# ======================================================================
# 逐指标 parity 测试
# ======================================================================

class TestIncrementalBatchParity:

    # --- 工具 ---
    def _run(self, indicator_type, existing, new, calc, full_df, **kwargs):
        """走两条路径：incremental._reuse  vs  直接 batch 全量 + tail"""
        inc = calc.incremental_calculate(indicator_type, existing, new, **kwargs)

        combined = pl.concat(
            [existing.select(['date','open','high','low','close','volume']),
             new.select(['date','open','high','low','close','volume'])],
            how='vertical_relaxed',
        )
        kwargs_key = {'ma': 'ma_windows', 'expma': 'expma_windows',
                      'rsi': 'rsi_windows', 'kdj': 'kdj_windows',
                      'boll': 'boll_windows', 'wr': 'wr_windows',
                      'vol_ma': 'vol_ma_windows', 'dmi': 'dmi_windows',
                      'trix': 'trix_windows', 'sar': 'sar_af_step',
                      'dma': 'dma_short_period',
                      }.get(indicator_type, None)

        batch = calculate_multiple_indicators_polars(combined, [indicator_type], **kwargs)
        tail_batch = batch.tail(len(new))

        # 提取指标列（去掉 OHLCV + date）
        base_cols = {'date','open','high','low','close','volume'}
        indicator_cols = [c for c in tail_batch.columns if c not in base_cols]
        assert indicator_cols, f"{indicator_type}: batch 输出无指标列"

        inc_view = inc.select(indicator_cols)
        batch_view = tail_batch.select(indicator_cols)
        _compare(inc_view, batch_view)

    # --- MA ---
    def test_ma_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('ma', existing, new, calc, full_df, windows=[5, 10, 20, 60])

    # --- EXPMA (EMA) ---
    def test_expma_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('expma', existing, new, calc, full_df, windows=[12, 50])

    # --- MACD (三重 EMA) ---
    def test_macd_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('macd', existing, new, calc, full_df,
                  fast_period=12, slow_period=26, signal_period=9)

    # --- RSI ---
    def test_rsi_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('rsi', existing, new, calc, full_df, windows=[14])

    # --- KDJ ---
    def test_kdj_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('kdj', existing, new, calc, full_df, windows=[14])

    # --- BOLL ---
    def test_boll_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('boll', existing, new, calc, full_df, windows=[20], std_dev=2.0)

    # --- WR ---
    def test_wr_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('wr', existing, new, calc, full_df, windows=[10, 6])

    # --- OBV ---
    def test_obv_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('obv', existing, new, calc, full_df)

    # --- DMI ---
    def test_dmi_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('dmi', existing, new, calc, full_df, windows=[14])

    # --- TRIX ---
    def test_trix_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('trix', existing, new, calc, full_df, windows=[12], signal_period=9)

    # --- SAR ---
    def test_sar_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('sar', existing, new, calc, full_df, af_step=0.02, max_af=0.2)

    # --- DMA ---
    def test_dma_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('dma', existing, new, calc, full_df,
                  short_period=10, long_period=50, signal_period=10)

    # --- FSL ---
    def test_fsl_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('fsl', existing, new, calc, full_df)

    # --- BBI ---
    def test_bbi_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('bbi', existing, new, calc, full_df)

    # --- VOL_MA ---
    def test_vol_ma_parity(self, calc, split_dfs, full_df):
        existing, new = split_dfs
        self._run('vol_ma', existing, new, calc, full_df, windows=[5, 10])

    # --- 种子级边界：不同 split 位置 ---
    def test_expma_parity_various_split(self, calc, full_df):
        """不同 split 位置都要逐位相等（覆盖 warmup 边界）"""
        for split_pos in [50, 100, 200, 350, 450]:
            existing = full_df.head(split_pos)
            new = full_df.tail(500 - split_pos)
            self._run('expma', existing, new, calc, full_df, windows=[12, 50])

    # --- 种子级边界：增量追加多轮（实盘逐 bar 更新场景）---
    def test_incremental_append_rounds(self, calc, full_df):
        """模拟实盘逐 bar 追加：existing=前 200 bar, 然后每轮 append 1 bar
        多轮后的最后 20 bar 应与 batch(full_df).tail(20) 逐位相等"""
        existing = full_df.head(200)
        tail = full_df.tail(300)
        rounds = 300
        # 逐 bar 追加最后 300 bar
        for i in range(rounds):
            bar = tail[i:i+1]
            existing = pl.concat([existing, bar], how='vertical_relaxed')
        # 最终 incremental 对最后 20 bar 的输出
        existing = full_df.head(480)
        new = full_df.tail(20)
        inc = calc.incremental_calculate('expma', existing, new, windows=[12, 50])

        batch_all = calculate_multiple_indicators_polars(
            full_df, ['expma'], expma_windows=[12, 50])
        tail_batch = batch_all.tail(20)

        base_cols = {'date','open','high','low','close','volume'}
        indicator_cols = [c for c in tail_batch.columns if c not in base_cols]
        _compare(inc.select(indicator_cols), tail_batch.select(indicator_cols))


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
