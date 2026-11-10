"""GPU 可用性探测与 CPU 回退等价性测试。

核心断言：探测失败（未装 cupy / 无设备 / 驱动失配）时，GPUAccelerator 的四个
calculate_*_gpu 必须与 global_indicator_manager 主路径**逐位相同**——不允许存在
绕过 indicator_manager 的旁路，也不允许 dtype 归一不一致。

用 monkeypatch 注入伪 cupy 模块来覆盖三种探测结果，无需真实 CUDA 设备。
"""

from __future__ import annotations

import sys
import types

import numpy as np
import polars as pl
import pytest

from src.tech_analysis import gpu_acceleration as ga
from src.tech_analysis.indicator_manager import global_indicator_manager

N = 300
WINDOWS = [5, 10]
FAST, SLOW, SIGNAL = 12, 26, 9
RSI_WINDOW = 6


def _series(seed: int = 7):
    """构造无 NaN 的合成 OHLC 序列。"""
    rng = np.random.default_rng(seed)
    close = np.cumsum(rng.normal(0, 1, N)) + 100.0
    return close, close + 1.0, close - 1.0


def _frame(**columns) -> pl.DataFrame:
    """按主路径的输入契约组装帧（与 GPUAccelerator._build_frame 独立实现）。"""
    length = len(next(iter(columns.values())))
    data = {'date': pl.int_range(0, length, eager=True)}
    for name, values in columns.items():
        data[name] = np.asarray(values, dtype=np.float64)
    return pl.DataFrame(data)


def _main_path(indicator_type: str, columns, **params) -> pl.DataFrame:
    """主路径：直接经 global_indicator_manager 计算并统一 Float64。"""
    result = global_indicator_manager.calculate_indicator(
        _frame(**columns), indicator_type, return_polars=True, **params
    )
    return result.with_columns([
        pl.col(name).cast(pl.Float64)
        for name, dtype in result.schema.items()
        if dtype.is_float()
    ])


def _install_fake_cupy(monkeypatch, device_count: int, error: Exception | None = None):
    """注入伪 cupy 模块，模拟「已安装」状态。"""

    def _get_device_count():
        if error is not None:
            raise error
        return device_count

    fake = types.ModuleType('cupy')
    fake.cuda = types.SimpleNamespace(
        runtime=types.SimpleNamespace(getDeviceCount=_get_device_count),
        ndarray=np.ndarray,
    )
    fake.asarray = np.asarray
    fake.asnumpy = lambda value: np.asarray(value)
    monkeypatch.setitem(sys.modules, 'cupy', fake)
    return fake


# ----------------------------------------------------------------------
# 探测：三种失败原因必须可区分
# ----------------------------------------------------------------------


def test_detect_gpu_cupy_missing(monkeypatch):
    """cupy 未安装 → cupy_missing。"""
    monkeypatch.setitem(sys.modules, 'cupy', None)
    assert ga.detect_gpu() == (False, ga.GPU_REASON_CUPY_MISSING)


def test_detect_gpu_no_device(monkeypatch):
    """cupy 已装但设备数为 0 → no_device（这是 import 成功却不可用的主因）。"""
    _install_fake_cupy(monkeypatch, device_count=0)
    assert ga.detect_gpu() == (False, ga.GPU_REASON_NO_DEVICE)


def test_detect_gpu_driver_mismatch(monkeypatch):
    """cupy 已装但 getDeviceCount 抛错 → driver_mismatch。"""
    _install_fake_cupy(monkeypatch, device_count=0, error=RuntimeError('cudaErrorInsufficientDriver'))
    assert ga.detect_gpu() == (False, ga.GPU_REASON_DRIVER_MISMATCH)


def test_detect_gpu_available(monkeypatch):
    """cupy 已装且有设备 → available。"""
    _install_fake_cupy(monkeypatch, device_count=2)
    assert ga.detect_gpu() == (True, ga.GPU_AVAILABLE_CODE)


def test_detect_gpu_only_importable_is_not_available(monkeypatch):
    """反例守卫：仅「可 import」不得判定为可用。"""
    _install_fake_cupy(monkeypatch, device_count=0)
    available, reason = ga.detect_gpu()
    assert available is False
    assert reason != ga.GPU_AVAILABLE_CODE


# ----------------------------------------------------------------------
# 回退等价性：不可用状态下，四个指标必须与主路径逐位相同
# ----------------------------------------------------------------------


@pytest.mark.parametrize('scenario', ['cupy_missing', 'no_device'])
def test_fallback_ma_bitwise_equal(monkeypatch, scenario):
    if scenario == 'cupy_missing':
        monkeypatch.setitem(sys.modules, 'cupy', None)
    else:
        _install_fake_cupy(monkeypatch, device_count=0)

    available, _ = ga.detect_gpu()
    assert available is False

    accelerator = ga.GPUAccelerator()
    accelerator.gpu_available = available

    close, _, _ = _series()
    got = accelerator.calculate_ma_gpu(close, WINDOWS)
    expected = _main_path('ma', {'close': close}, windows=WINDOWS)

    for name in [f'ma{w}' for w in WINDOWS] + ['ma']:
        assert name in got
        np.testing.assert_array_equal(got[name], expected[name].to_numpy(), err_msg=f'{scenario}:{name}')


@pytest.mark.parametrize('scenario', ['cupy_missing', 'no_device'])
def test_fallback_macd_bitwise_equal(monkeypatch, scenario):
    if scenario == 'cupy_missing':
        monkeypatch.setitem(sys.modules, 'cupy', None)
    else:
        _install_fake_cupy(monkeypatch, device_count=0)

    accelerator = ga.GPUAccelerator()
    accelerator.gpu_available = ga.detect_gpu()[0]

    close, _, _ = _series()
    got = accelerator.calculate_macd_gpu(close, FAST, SLOW, SIGNAL)
    expected = _main_path(
        'macd', {'close': close},
        fast_period=FAST, slow_period=SLOW, signal_period=SIGNAL,
    )

    for name in ('macd', 'macd_signal', 'macd_hist'):
        np.testing.assert_array_equal(got[name], expected[name].to_numpy(), err_msg=f'{scenario}:{name}')


@pytest.mark.parametrize('scenario', ['cupy_missing', 'no_device'])
def test_fallback_rsi_bitwise_equal(monkeypatch, scenario):
    if scenario == 'cupy_missing':
        monkeypatch.setitem(sys.modules, 'cupy', None)
    else:
        _install_fake_cupy(monkeypatch, device_count=0)

    accelerator = ga.GPUAccelerator()
    accelerator.gpu_available = ga.detect_gpu()[0]

    close, _, _ = _series()
    got = accelerator.calculate_rsi_gpu(close, RSI_WINDOW)
    expected = _main_path('rsi', {'close': close}, windows=[RSI_WINDOW])

    np.testing.assert_array_equal(
        got, expected[f'rsi{RSI_WINDOW}'].to_numpy(), err_msg=f'{scenario}:rsi'
    )


@pytest.mark.parametrize('scenario', ['cupy_missing', 'no_device'])
def test_fallback_sar_bitwise_equal(monkeypatch, scenario):
    if scenario == 'cupy_missing':
        monkeypatch.setitem(sys.modules, 'cupy', None)
    else:
        _install_fake_cupy(monkeypatch, device_count=0)

    accelerator = ga.GPUAccelerator()
    accelerator.gpu_available = ga.detect_gpu()[0]

    close, high, low = _series()
    got = accelerator.calculate_sar_gpu(high, low, close)
    expected = _main_path('sar', {'high': high, 'low': low, 'close': close})

    np.testing.assert_array_equal(got, expected['sar'].to_numpy(), err_msg=f'{scenario}:sar')


# ----------------------------------------------------------------------
# 分支可达性
# ----------------------------------------------------------------------


def test_calculate_with_gpu_returns_none_when_unavailable(monkeypatch):
    """不可用时必须返回 None，由调用方回退主路径。"""
    monkeypatch.setattr(ga.global_gpu_accelerator, 'gpu_available', False)
    close, _, _ = _series()
    assert ga.calculate_with_gpu('ma', {'close': close}, windows=WINDOWS) is None
    assert ga.is_gpu_available() is False


def test_calculate_with_gpu_routes_to_gpu_branch_when_available(monkeypatch):
    """可用时走上 GPU 分支（不要求真算 kernel，只断言路由与形状）。"""
    monkeypatch.setattr(ga.global_gpu_accelerator, 'gpu_available', True)

    close, high, low = _series()
    expected_names = {
        'ma': {f'ma{w}' for w in WINDOWS} | {'ma'},
        'macd': {'macd', 'macd_signal', 'macd_hist'},
        'rsi': {f'rsi{RSI_WINDOW}', 'rsi'},
        'sar': {'sar'},
    }
    outputs = {
        'ma': ga.calculate_with_gpu('ma', {'close': close}, windows=WINDOWS),
        'macd': ga.calculate_with_gpu('macd', {'close': close}),
        'rsi': ga.calculate_with_gpu('rsi', {'close': close}, windows=[RSI_WINDOW]),
        'sar': ga.calculate_with_gpu('sar', {'high': high, 'low': low, 'close': close}),
    }

    for indicator, result in outputs.items():
        assert result is not None, indicator
        assert set(result) == expected_names[indicator], indicator
        for values in result.values():
            assert len(values) == N
