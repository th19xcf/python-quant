#!/usr/bin/env python3

"""
指标公式一致性测试

验证「共享公式层」（src/tech_analysis/indicators/formulas.py）收口后，
回测引擎、GPU 加速路径、Alpha 因子库与技术指标权威实现数值一致。
全部使用构造数据，不依赖数据库与网络。
"""

import numpy as np
import polars as pl

from src.alpha.factors.price_momentum import PriceMomentumFactors
from src.backtest.engine.backtest_engine import BacktestEngine
from src.tech_analysis.gpu_acceleration import GPUAccelerator
from src.tech_analysis.indicator_calculator import calculate_multiple_indicators_polars
from src.tech_analysis.indicators.formulas import (
    atr_expr,
    kdj_exprs,
    macd_exprs,
    rsi_expr,
)


def _make_ohlcv(n: int = 300) -> pl.DataFrame:
    """构造 300 根稳定的 OHLCV 数据（np.random.seed(0)）"""
    np.random.seed(0)
    close = 100.0 + np.cumsum(np.random.randn(n))
    high = close + np.abs(np.random.randn(n))
    low = close - np.abs(np.random.randn(n))
    open_ = np.clip(close + np.random.randn(n) * 0.5, low, high)
    volume = np.random.randint(100_000, 1_000_000, n).astype(float)
    return pl.DataFrame({
        'open': open_,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
    })


class _FakeStrategy:
    """只提供 params 字典的最小策略替身，供 _precompute_indicators 使用"""

    def __init__(self, params: dict):
        self.params = params


def _assert_close(a, b, rtol, atol, name: str) -> None:
    np.testing.assert_allclose(
        np.asarray(a, dtype=float),
        np.asarray(b, dtype=float),
        rtol=rtol,
        atol=atol,
        equal_nan=True,
        err_msg=f'{name} 数值不一致',
    )


def test_backtest_engine_matches_authoritative_impl():
    """回测引擎预计算的 MACD / RSI 必须与权威实现一致（权威为 float32）"""
    df = _make_ohlcv()
    engine = BacktestEngine.__new__(BacktestEngine)
    strategy = _FakeStrategy({
        'short_window': 10,
        'long_window': 20,
        'rsi_period': 14,
    })
    precomputed = engine._precompute_indicators(df, strategy)

    authoritative = calculate_multiple_indicators_polars(df, ['macd', 'rsi'])

    for col in ('macd', 'macd_signal', 'macd_hist', 'rsi'):
        _assert_close(
            precomputed[col].to_numpy(),
            authoritative[col].to_numpy(),
            rtol=1e-4,
            atol=1e-6,
            name=col,
        )


def test_gpu_path_matches_shared_formulas():
    """GPU 加速路径的 MACD / RSI 必须与共享公式完全一致"""
    close = _make_ohlcv()['close'].to_numpy()
    accelerator = GPUAccelerator()

    macd_result = accelerator.calculate_macd_gpu(close, 12, 26, 9)
    exprs = macd_exprs('close', 12, 26, 9)
    expected = pl.DataFrame({'close': close}).with_columns([
        exprs['macd'].alias('macd'),
        exprs['signal'].alias('macd_signal'),
        exprs['hist'].alias('macd_hist'),
    ])
    for key in ('macd', 'macd_signal', 'macd_hist'):
        _assert_close(
            macd_result[key],
            expected[key].to_numpy(),
            rtol=1e-6,
            atol=1e-9,
            name=f'gpu_{key}',
        )

    rsi_result = accelerator.calculate_rsi_gpu(close, 14)
    expected_rsi = (
        pl.DataFrame({'close': close})
        .with_columns(rsi_expr('close', 14).alias('rsi'))['rsi']
        .to_numpy()
    )
    _assert_close(rsi_result, expected_rsi, rtol=1e-6, atol=1e-9, name='gpu_rsi')


def test_ma_precompute_warmup_rows_are_null():
    """MA 预计算列在窗口未满时必须为 null（暖机保护），窗口满后非 null。

    回归：曾用 min_samples=1，ma_20 在第 0 根就输出 close[0]，
    策略把窗口未满的污染值当合法信号，暖机期内产生虚假成交。
    """
    df = _make_ohlcv(60)
    engine = BacktestEngine.__new__(BacktestEngine)
    strategy = _FakeStrategy({'short_window': 10, 'long_window': 20, 'rsi_period': 14})
    precomputed = engine._precompute_indicators(df, strategy)

    ma10, ma20 = precomputed['ma_10'], precomputed['ma_20']
    assert ma10[:9].is_null().all() and ma10[9:].is_not_null().all()
    assert ma20[:19].is_null().all() and ma20[19:].is_not_null().all()


def test_macd_strategy_warmup_guard():
    """MACD 策略在 慢线+信号线 暖机根数内必须一律 hold（MACD 从第 0 根即有值）"""
    from src.backtest.strategies.macd_strategy import MACDStrategy

    strategy = MACDStrategy()
    data = {'macd': 1.0, 'macd_signal': -1.0}  # 明显多头排列
    for i in range(35):
        assert strategy.generate_signal(data, i) == 'hold', f'暖机期第 {i} 根不应发信号'
    assert strategy.generate_signal(data, 35) == 'buy'


def test_multi_factor_warmup_guard():
    """多因子策略在暖机期内必须一律 hold，即便因子方向明确"""
    from src.backtest.strategies.multi_factor_strategy import MultiFactorStrategy

    strategy = MultiFactorStrategy()
    # MA + MACD 双因子共振（得分 0.7 > 阈值 0.5）
    data = {'ma_10': 11.0, 'ma_20': 10.0, 'macd': 1.0, 'macd_signal': -1.0}
    assert strategy.generate_signal(data, 34) == 'hold'
    assert strategy.generate_signal(data, 35) == 'buy'


def test_multi_factor_requires_two_factor_resonance():
    """默认阈值 0.5 落在单因子最大值 0.4 与两因子最小和 0.6 之间：
    MACD 单因子（0.4）不得独决买入，MA+RSI 共振（0.6）可以。"""
    from src.backtest.strategies.multi_factor_strategy import MultiFactorStrategy

    strategy = MultiFactorStrategy()
    macd_only = {'macd': 1.0, 'macd_signal': -1.0}
    assert strategy.generate_signal(macd_only, 100) == 'hold'

    ma_rsi = {'ma_10': 11.0, 'ma_20': 10.0, 'rsi': 20.0}  # MA 多头 + RSI 超卖
    assert strategy.generate_signal(ma_rsi, 100) == 'buy'

    # 阈值可配：降低到 0.3 时 MACD 单因子即可买入（兼容旧行为）
    legacy = MultiFactorStrategy(threshold=0.3)
    assert legacy.generate_signal(macd_only, 100) == 'buy'


def test_rsi_seed_and_value_range():
    """RSI 前 13 行为 null，种子在索引 13；值域 0~100"""
    df = _make_ohlcv()
    rsi = df.select(rsi_expr('close', 14).alias('rsi'))['rsi']

    assert rsi[:13].is_null().all(), 'RSI 前 13 行应为 null'
    assert rsi[13:].is_not_null().all(), 'RSI 从索引 13 起应非 null'

    values = rsi.drop_nulls().to_numpy()
    assert values.min() >= 0.0 and values.max() <= 100.0, 'RSI 值域应为 0~100'


def test_alpha_macd_uses_shared_formula():
    """Alpha 因子层的 MACD 列直接复用共享公式（保持原有不做 shift 的语义）"""
    df = _make_ohlcv()
    factors = PriceMomentumFactors(df)
    factors._calculate_macd_momentum()
    result = factors.data

    exprs = macd_exprs('close', 12, 26, 9)
    expected = df.with_columns([
        exprs['macd'].fill_null(0.0).alias('macd'),
        exprs['signal'].fill_null(0.0).alias('macd_signal'),
        exprs['hist'].fill_null(0.0).alias('macd_hist'),
    ])

    for col in ('macd', 'macd_signal', 'macd_hist'):
        _assert_close(
            result[col].to_numpy(),
            expected[col].to_numpy(),
            rtol=1e-4,
            atol=1e-6,
            name=f'alpha_{col}',
        )


def test_kdj_matches_tdx_recursive_sma():
    """图表 KDJ 的 K/D 应为通达信 SMA(X,3,1) 递推（α=1/3），而非 3 周期简单均值"""
    df = _make_ohlcv()
    result = calculate_multiple_indicators_polars(df, ['kdj'], kdj_windows=[14])

    close = df['close'].to_numpy()
    high = df['high'].to_numpy()
    low = df['low'].to_numpy()

    # 参考实现：RSV 含当日（与图表 high_n_14 / low_n_14 一致）
    rsv = []
    for i in range(len(close)):
        hh = high[max(0, i - 13):i + 1].max()
        ll = low[max(0, i - 13):i + 1].min()
        rsv.append(0.0 if hh == ll else (close[i] - ll) / (hh - ll) * 100.0)

    # K(t) = (RSV(t) + 2*K(t-1)) / 3，首值取 RSV 本身；D 为 K 的同参数递推
    k_ref, d_ref = [], []
    for i, value in enumerate(rsv):
        k_value = value if i == 0 else (value + 2 * k_ref[-1]) / 3.0
        d_value = k_value if i == 0 else (k_value + 2 * d_ref[-1]) / 3.0
        k_ref.append(k_value)
        d_ref.append(d_value)
    j_ref = [3 * k - 2 * d for k, d in zip(k_ref, d_ref, strict=False)]

    for col, ref in (('k', k_ref), ('d', d_ref), ('j', j_ref)):
        _assert_close(
            result[col].to_numpy(),
            np.array(ref),
            rtol=1e-4,
            atol=1e-3,
            name=f'kdj_{col}',
        )


def test_alpha_kdj_matches_chart():
    """Alpha 因子层的 KDJ 与图表同源（RSV 含当日 + SMA(3,1) 递推）"""
    from src.alpha.factors.price_momentum import PriceMomentumFactors
    from src.alpha.factors.technical import TechnicalFactors

    df = _make_ohlcv()
    chart = calculate_multiple_indicators_polars(df, ['kdj'], kdj_windows=[14])

    price_momentum = PriceMomentumFactors(df)
    price_momentum._calculate_stochastic_momentum()

    technical = TechnicalFactors(df)
    technical._calculate_stochastic_oscillator()

    for factor_col, chart_col in (('kdj_k', 'k'), ('kdj_d', 'd'), ('kdj_j', 'j')):
        _assert_close(
            price_momentum.data[factor_col].to_numpy(),
            chart[chart_col].to_numpy(),
            rtol=1e-4,
            atol=1e-3,
            name=f'price_momentum_{factor_col}',
        )
        _assert_close(
            technical.data[factor_col].to_numpy(),
            chart[chart_col].to_numpy(),
            rtol=1e-4,
            atol=1e-3,
            name=f'technical_{factor_col}',
        )


def test_factor_analyzer_uses_shared_formulas():
    """因子分析器的 MACD/RSI/KDJ/ATR 应直接复用共享公式层"""
    from src.quant.analysis.factor_analyzer import FactorAnalyzer

    df = _make_ohlcv()
    result = FactorAnalyzer()._calculate_factors(
        df, ['MACD', 'RSI', 'KDJ', 'ATR']
    )

    macd = macd_exprs('close', 12, 26, 9)
    kdj = kdj_exprs('high', 'low', 'close', window=14)
    expected = df.with_columns([
        macd['macd'].alias('macd'),
        macd['signal'].alias('macd_signal'),
        macd['hist'].alias('macd_hist'),
        rsi_expr('close', 14).alias('rsi'),
        kdj['k'].alias('kdj_k'),
        kdj['d'].alias('kdj_d'),
        kdj['j'].alias('kdj_j'),
        atr_expr('high', 'low', 'close', 14).alias('atr'),
    ])

    # 早期行存在 null（Wilder 种子未就位），用哨兵值参与比较
    for col in ('macd', 'macd_signal', 'macd_hist', 'rsi', 'kdj_k', 'kdj_d', 'kdj_j', 'atr'):
        _assert_close(
            result[col].fill_null(-999.0).to_numpy(),
            expected[col].fill_null(-999.0).to_numpy(),
            rtol=1e-4,
            atol=1e-6,
            name=f'factor_analyzer_{col}',
        )


def test_factor_analyzer_kdj_window_is_configurable():
    """因子分析器的 KDJ 周期应可通过 kdj_window 调整"""
    from src.quant.analysis.factor_analyzer import FactorAnalyzer

    df = _make_ohlcv()
    analyzer = FactorAnalyzer()

    default = analyzer._calculate_factors(df, ['KDJ'])
    custom = analyzer._calculate_factors(df, ['KDJ'], kdj_window=9)

    expected = df.with_columns(
        kdj_exprs('high', 'low', 'close', window=9)['k'].alias('kdj_k')
    )

    _assert_close(
        custom['kdj_k'].to_numpy(),
        expected['kdj_k'].to_numpy(),
        rtol=1e-4,
        atol=1e-6,
        name='factor_analyzer_kdj_window_9',
    )
    assert not np.allclose(
        default['kdj_k'].to_numpy(), custom['kdj_k'].to_numpy()
    ), '周期不同时 KDJ 取值应不同'


def _make_flat_ohlcv(n: int = 60, price: float = 10.0) -> pl.DataFrame:
    """构造价格完全不动的 OHLCV（模拟连续一字板 / 停牌），用于触发除零边界"""
    flat = np.full(n, price, dtype=float)
    return pl.DataFrame({
        'open': flat,
        'high': flat,
        'low': flat,
        'close': flat,
        'volume': np.zeros(n, dtype=float),
    })


def test_gpu_ma_matches_authoritative_impl():
    """GPU 路径 MA 必须与权威实现一致（首窗口边界与 Float32 口径同源）"""
    df = _make_ohlcv()
    close = df['close'].to_numpy()
    accelerator = GPUAccelerator()

    gpu_result = accelerator.calculate_ma_gpu(close, [5, 10, 20, 60])
    authoritative = calculate_multiple_indicators_polars(
        df, ['ma'], windows=[5, 10, 20, 60]
    )

    for col in ('ma5', 'ma10', 'ma20', 'ma60', 'ma'):
        _assert_close(
            gpu_result[col],
            authoritative[col].to_numpy(),
            rtol=1e-6,
            atol=1e-6,
            name=f'gpu_{col}',
        )


def test_gpu_rsi_columns_match_authoritative_naming(monkeypatch):
    """GPU RSI 必须输出 rsi{N} + 默认别名 rsi，避免窗口状态被误标记后丢列"""
    from src.tech_analysis import gpu_acceleration as gpu_module

    df = _make_ohlcv()
    close = df['close'].to_numpy()
    monkeypatch.setattr(gpu_module.global_gpu_accelerator, 'gpu_available', True)
    # 无 CuPy 时 cp 回退为 numpy，to_cpu 内的 cp.asnumpy 不存在，这里旁路为恒等转换
    monkeypatch.setattr(
        gpu_module.global_gpu_accelerator, 'to_cpu', lambda data: np.asarray(data)
    )

    result = gpu_module.calculate_with_gpu('rsi', {'close': close}, windows=[6])
    assert set(result) == {'rsi6', 'rsi'}, f'GPU RSI 列名不一致: {set(result)}'

    authoritative = calculate_multiple_indicators_polars(df, ['rsi'], rsi_windows=[6])
    for col in ('rsi6', 'rsi'):
        _assert_close(
            result[col],
            authoritative[col].to_numpy(),
            rtol=1e-6,
            atol=1e-6,
            name=f'gpu_{col}',
        )


def test_wr_flat_window_is_zero_not_inf():
    """窗口内 high == low 时 WR 应取 0，不得出现 NaN / inf"""
    df = _make_flat_ohlcv()
    result = calculate_multiple_indicators_polars(df, ['wr'], wr_windows=[10, 6])

    for col in ('wr10', 'wr6', 'wr2', 'wr'):
        values = result[col].fill_null(0.0).to_numpy()
        assert np.isfinite(values).all(), f'{col} 出现 NaN/inf'
        assert np.allclose(values, 0.0), f'{col} 在无波动窗口应为 0'


def test_dmi_flat_window_is_zero_not_inf():
    """真实波幅为 0 时 DMI 各线应取 0，不得出现 NaN / inf"""
    df = _make_flat_ohlcv()
    result = calculate_multiple_indicators_polars(df, ['dmi'], dmi_windows=[14])

    for col in ('pdi_14', 'ndi_14', 'adx_14', 'adxr_14', 'pdi', 'ndi', 'adx', 'adxr'):
        values = result[col].fill_null(0.0).to_numpy()
        assert np.isfinite(values).all(), f'{col} 出现 NaN/inf'
        assert np.allclose(values, 0.0), f'{col} 无波动时应为 0'
