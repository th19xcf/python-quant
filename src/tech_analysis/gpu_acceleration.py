#!/usr/bin/env python3

"""
GPU加速计算模块
使用CuPy实现CUDA加速，支持OpenCL作为备选
"""

from typing import Any, Dict, List

import numpy as np
import polars as pl

from src.utils.logger import logger

from .indicators.formulas import macd_exprs, rsi_expr
from .indicators.trend import _sar_core

# 尝试导入CuPy，如果不可用则使用NumPy作为备选
try:
    import cupy as cp
    GPU_AVAILABLE = True
    logger.info("CuPy可用，将使用GPU加速")
except ImportError:
    logger.info("CuPy不可用，将使用CPU计算")
    cp = np
    GPU_AVAILABLE = False

# 尝试导入PyOpenCL作为备选
try:
    import pyopencl as cl
    OPENCL_AVAILABLE = True
    logger.info("OpenCL可用，将作为GPU加速备选")
except ImportError:
    logger.info("OpenCL不可用")
    OPENCL_AVAILABLE = False


class GPUAccelerator:
    """
    GPU加速器类，提供GPU加速计算功能
    """

    def __init__(self):
        """
        初始化GPU加速器
        """
        self.gpu_available = GPU_AVAILABLE
        self.opencl_available = OPENCL_AVAILABLE
        self._opencl_context = None
        self._opencl_queue = None

        if self.opencl_available:
            self._init_opencl()

    def _init_opencl(self):
        """
        初始化OpenCL环境
        """
        try:
            platforms = cl.get_platforms()
            if platforms:
                platform = platforms[0]
                devices = platform.get_devices(cl.device_type.GPU)
                if devices:
                    self._opencl_context = cl.Context(devices)
                    self._opencl_queue = cl.CommandQueue(self._opencl_context)
                    logger.info(f"OpenCL初始化成功，设备: {devices[0].name}")
                else:
                    logger.info("未找到GPU设备，使用CPU设备")
                    devices = platform.get_devices(cl.device_type.CPU)
                    if devices:
                        self._opencl_context = cl.Context(devices)
                        self._opencl_queue = cl.CommandQueue(self._opencl_context)
        # 兜底理由：OpenCL 初始化失败降级到 CPU/NumPy
        except Exception as e:  # noqa: BLE001
            logger.warning(f"OpenCL初始化失败: {e}")
            self.opencl_available = False

    def is_gpu_available(self) -> bool:
        """
        检查GPU是否可用

        Returns:
            bool: GPU是否可用
        """
        return self.gpu_available

    def to_gpu(self, data: np.ndarray) -> Any:
        """
        将数据转移到GPU

        Args:
            data: NumPy数组

        Returns:
            Any: GPU上的数据
        """
        if self.gpu_available:
            return cp.asarray(data)
        return data

    def to_cpu(self, data: Any) -> np.ndarray:
        """
        将数据从GPU转移到CPU

        Args:
            data: GPU上的数据

        Returns:
            np.ndarray: NumPy数组
        """
        if self.gpu_available and isinstance(data, cp.ndarray):
            return cp.asnumpy(data)
        return data

    def calculate_ma_gpu(self, data: np.ndarray, windows: List[int]) -> Dict[str, np.ndarray]:
        """
        GPU加速计算移动平均线

        Args:
            data: 收盘价数据
            windows: 移动平均窗口列表

        Returns:
            Dict[str, np.ndarray]: 各窗口的移动平均线结果
        """
        result = {}
        gpu_data = self.to_gpu(data)

        for window in windows:
            # 使用卷积计算移动平均
            kernel = self.to_gpu(np.ones(window) / window)
            ma = cp.convolve(gpu_data, kernel, mode='valid')
            # 补全前面的NaN值
            ma = cp.concatenate([cp.full(window-1, np.nan), ma])
            result[f'ma{window}'] = self.to_cpu(ma)

        return result

    def calculate_macd_gpu(self, data: np.ndarray, fast_period: int, slow_period: int, signal_period: int) -> Dict[str, np.ndarray]:
        """
        GPU加速计算MACD指标

        Args:
            data: 收盘价数据
            fast_period: 快速EMA周期
            slow_period: 慢速EMA周期
            signal_period: 信号线EMA周期

        Returns:
            Dict[str, np.ndarray]: MACD指标结果
        """
        # EMA / MACD 统一走共享公式层：先把 numpy 转为 Polars，
        # 用标准 EMA 口径一次性算出三线，再转回 numpy
        exprs = macd_exprs('close', fast_period, slow_period, signal_period)
        result = pl.DataFrame({'close': np.asarray(data, dtype=np.float64)}).with_columns([
            exprs['macd'].alias('macd'),
            exprs['signal'].alias('macd_signal'),
            exprs['hist'].alias('macd_hist'),
        ])

        return {
            'macd': self.to_cpu(result['macd'].to_numpy()),
            'macd_signal': self.to_cpu(result['macd_signal'].to_numpy()),
            'macd_hist': self.to_cpu(result['macd_hist'].to_numpy()),
        }

    def calculate_rsi_gpu(self, data: np.ndarray, window: int) -> np.ndarray:
        """
        GPU加速计算RSI指标

        Args:
            data: 收盘价数据
            window: RSI计算窗口

        Returns:
            np.ndarray: RSI指标结果
        """
        # RSI 统一走共享公式层：Polars 计算时种子行之前自然是 null，
        # to_numpy 后转为 nan，与原「前 window-1 个无效」语义一致
        result = (
            pl.DataFrame({'close': np.asarray(data, dtype=np.float64)})
            .with_columns(rsi_expr('close', window).alias('rsi'))['rsi']
            .to_numpy()
        )
        return self.to_cpu(result)

    def calculate_sar_gpu(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, af_step: float = 0.02, max_af: float = 0.2) -> np.ndarray:
        """
        GPU加速计算SAR指标

        Args:
            high: 最高价数据
            low: 最低价数据
            close: 收盘价数据
            af_step: 加速因子步长
            max_af: 最大加速因子

        Returns:
            np.ndarray: SAR指标结果
        """
        # SAR 是状态机式顺序递推（依赖前一根 SAR/EP/AF/趋势方向），GPU 无收益；
        # 直接复用 indicators/trend.py 中 Numba JIT 的 _sar_core
        # （种子、限幅、反转、EP/AF 更新规则与本方法原逐行实现完全一致）
        highs = np.ascontiguousarray(high, dtype=np.float64)
        lows = np.ascontiguousarray(low, dtype=np.float64)
        closes = np.ascontiguousarray(close, dtype=np.float64)
        return _sar_core(highs, lows, closes, af_step, max_af)


# 创建全局GPU加速器实例
global_gpu_accelerator = GPUAccelerator()


def is_gpu_available() -> bool:
    """
    检查GPU是否可用

    Returns:
        bool: GPU是否可用
    """
    return global_gpu_accelerator.is_gpu_available()


def calculate_with_gpu(indicator_type: str, data: Dict[str, np.ndarray], **kwargs) -> Dict[str, np.ndarray]:
    """
    使用GPU加速计算指标

    Args:
        indicator_type: 指标类型
        data: 输入数据
        **kwargs: 计算参数

    Returns:
        Dict[str, np.ndarray]: 计算结果
    """
    if not global_gpu_accelerator.is_gpu_available():
        return None

    if indicator_type == 'ma':
        close = data.get('close')
        if close is not None:
            windows = kwargs.get('windows', [5, 10, 20, 60])
            return global_gpu_accelerator.calculate_ma_gpu(close, windows)

    elif indicator_type == 'macd':
        close = data.get('close')
        if close is not None:
            fast_period = kwargs.get('fast_period', 12)
            slow_period = kwargs.get('slow_period', 26)
            signal_period = kwargs.get('signal_period', 9)
            return global_gpu_accelerator.calculate_macd_gpu(close, fast_period, slow_period, signal_period)

    elif indicator_type == 'rsi':
        close = data.get('close')
        if close is not None:
            window = kwargs.get('window', 14)
            result = global_gpu_accelerator.calculate_rsi_gpu(close, window)
            return {'rsi': result}

    elif indicator_type == 'sar':
        high = data.get('high')
        low = data.get('low')
        close = data.get('close')
        if high is not None and low is not None and close is not None:
            af_step = kwargs.get('af_step', 0.02)
            max_af = kwargs.get('max_af', 0.2)
            result = global_gpu_accelerator.calculate_sar_gpu(high, low, close, af_step, max_af)
            return {'sar': result}

    return None
