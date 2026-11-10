#!/usr/bin/env python3

"""
GPU 加速计算模块

CUDA 设备探测 + CPU 回退。判定原则：**import 成功不等于可用**。
`import cupy` 只能证明库装上了；驱动缺失、驱动与 cupy 编译时的 CUDA 版本不匹配、
或机器上没有任何设备时，import 依然成功，但任何 kernel 调用都会在运行时抛
CUDARuntimeError。因此可用性一律由 detect_gpu() 判定，探测失败必须静默回退 CPU。
"""

import importlib
from typing import Any, Dict, List, Tuple, cast

import numpy as np
import polars as pl

from src.utils.logger import logger

# GPU 可用性原因码
GPU_AVAILABLE_CODE = 'available'
GPU_REASON_CUPY_MISSING = 'cupy_missing'
GPU_REASON_NO_DEVICE = 'no_device'
GPU_REASON_DRIVER_MISMATCH = 'driver_mismatch'

_GPU_REASON_TEXT = {
    GPU_AVAILABLE_CODE: 'CuPy 可用且检测到 CUDA 设备，启用 GPU 加速',
    GPU_REASON_CUPY_MISSING: 'CuPy 未安装，使用 CPU 计算',
    GPU_REASON_NO_DEVICE: 'CuPy 已安装但未检测到 CUDA 设备，使用 CPU 计算',
    GPU_REASON_DRIVER_MISMATCH: 'CuPy 已安装但 CUDA 驱动不可用或不匹配，使用 CPU 计算',
}

# OpenCL 可用性原因码
OPENCL_REASON_PYOPENCL_MISSING = 'pyopencl_missing'
OPENCL_REASON_NO_DEVICE = 'no_device'
OPENCL_REASON_INIT_FAILED = 'init_failed'


def _import_cupy():
    """尝试导入 cupy，失败返回 None（不抛异常）。"""
    try:
        return importlib.import_module('cupy')
    except ImportError:
        return None


def detect_gpu() -> Tuple[bool, str]:
    """
    探测 CUDA 设备是否真的可用。

    Returns:
        Tuple[bool, str]: (是否可用, 原因码)
            原因码取值：available / cupy_missing / no_device / driver_mismatch
    """
    cupy = _import_cupy()
    if cupy is None:
        return False, GPU_REASON_CUPY_MISSING
    try:
        device_count = cupy.cuda.runtime.getDeviceCount()
    # 兜底理由：驱动缺失或与 cupy 编译时的 CUDA 版本失配时此处抛 CUDARuntimeError，
    # 一律按「驱动不可用」处理，不向上抛以免阻断启动
    except Exception:  # noqa: BLE001
        return False, GPU_REASON_DRIVER_MISMATCH
    if device_count <= 0:
        return False, GPU_REASON_NO_DEVICE
    return True, GPU_AVAILABLE_CODE


def detect_opencl() -> Tuple[bool, str]:
    """
    探测 OpenCL 是否存在可用的 GPU 设备。

    与 detect_gpu 同理：`import pyopencl` 成功不代表有设备，必须实际枚举。

    Returns:
        Tuple[bool, str]: (是否可用, 原因码)
    """
    try:
        pyopencl = importlib.import_module('pyopencl')
    except ImportError:
        return False, OPENCL_REASON_PYOPENCL_MISSING
    try:
        for platform in pyopencl.get_platforms():
            if platform.get_devices(pyopencl.device_type.GPU):
                return True, GPU_AVAILABLE_CODE
    # 兜底理由：平台/驱动异常时按不可用处理，OpenCL 只是备选路径
    except Exception:  # noqa: BLE001
        return False, OPENCL_REASON_INIT_FAILED
    return False, OPENCL_REASON_NO_DEVICE


# 模块级 cp 仅用于 to_gpu / to_cpu 的类型判定与传输，不可作为可用性判据
cp = _import_cupy()
if cp is None:
    cp = np

try:
    import pyopencl as cl
except ImportError:
    cl = None

GPU_AVAILABLE, GPU_AVAILABLE_REASON = detect_gpu()
OPENCL_AVAILABLE, OPENCL_AVAILABLE_REASON = detect_opencl()

# 降级是多数部署机的常态，用 debug 避免每个进程启动都刷屏
logger.debug(_GPU_REASON_TEXT[GPU_AVAILABLE_REASON])
if not OPENCL_AVAILABLE:
    logger.debug(f'OpenCL 不可用（{OPENCL_AVAILABLE_REASON}）')


class GPUAccelerator:
    """
    GPU加速器类，提供GPU加速计算功能
    """

    def __init__(self):
        """
        初始化GPU加速器
        """
        self.gpu_available = GPU_AVAILABLE
        self.gpu_unavailable_reason = None if GPU_AVAILABLE else GPU_AVAILABLE_REASON
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
                    logger.debug(f"OpenCL初始化成功，设备: {devices[0].name}")
                else:
                    logger.debug("未找到GPU设备，使用CPU设备")
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

    # ------------------------------------------------------------------
    # 计算入口
    # ------------------------------------------------------------------

    @staticmethod
    def _build_frame(**columns: np.ndarray) -> pl.DataFrame:
        """
        把 numpy 序列组装成 indicator_manager 需要的 OHLCV 帧。

        合成索引列仅用于满足「首列为时间列」的约定（缓存键与结果合并都依赖它），
        不参与指标计算。
        """
        length = len(next(iter(columns.values())))
        # 显式标成 Dict[str, Any]：同一字典同时装 Series（索引列）与 ndarray（价格列）
        data: Dict[str, Any] = {'date': pl.int_range(0, length, eager=True)}
        for name, values in columns.items():
            data[name] = np.asarray(values, dtype=np.float64)
        return pl.DataFrame(data)

    def _calculate_via_manager(
        self, indicator_type: str, frame: pl.DataFrame, columns: List[str], **params
    ) -> Dict[str, np.ndarray]:
        """
        经 global_indicator_manager 计算（与主路径同一入口、同一 dtype 归一）。

        GPU 与 CPU 分支共用本实现：四个指标目前都没有真正的 GPU kernel，
        两条分支的差别仅在于「是否被调用」，数值必须完全一致。将来接入 kernel 时，
        只替换本方法内部的计算部分，入口与 dtype 归一保持不变。
        """
        # 延迟导入避免模块级循环依赖（indicator_manager 与 streaming_processor 互相引用）
        from .indicator_manager import global_indicator_manager

        result = global_indicator_manager.calculate_indicator(
            frame, indicator_type, return_polars=True, **params
        )
        # 与 streaming_processor 主路径一致：统一 Float64，避免后续 concat dtype 抖动
        result = result.with_columns([
            pl.col(name).cast(pl.Float64)
            for name, dtype in result.schema.items()
            if dtype.is_float()
        ])
        return {name: result[name].to_numpy() for name in columns if name in result.columns}

    def calculate_ma_gpu(self, data: np.ndarray, windows: List[int]) -> Dict[str, np.ndarray]:
        """
        计算移动平均线（与 CPU 权威实现逐位一致）

        MA 是 O(n) 滚动均值，GPU 无收益；历史上用 cp.convolve(mode='valid') 补 NaN
        前缀，与权威实现 rolling_mean(min_samples=1) 的窗口边界口径不同（首窗口 CPU
        有值、GPU 为 NaN），且精度为 Float64。此处统一委托 indicator_manager。

        Args:
            data: 收盘价数据
            windows: 移动平均窗口列表

        Returns:
            Dict[str, np.ndarray]: 各窗口的移动平均线结果（含首个窗口的默认别名 ma）
        """
        if not windows:
            return {}

        window_list = list(windows)
        frame = self._build_frame(close=np.asarray(data, dtype=np.float64))
        columns = [f'ma{window}' for window in window_list] + ['ma']
        return self._calculate_via_manager('ma', frame, columns, windows=window_list)

    def calculate_macd_gpu(self, data: np.ndarray, fast_period: int, slow_period: int, signal_period: int) -> Dict[str, np.ndarray]:
        """
        计算MACD指标（与 CPU 权威实现逐位一致）

        Args:
            data: 收盘价数据
            fast_period: 快速EMA周期
            slow_period: 慢速EMA周期
            signal_period: 信号线EMA周期

        Returns:
            Dict[str, np.ndarray]: MACD指标结果
        """
        frame = self._build_frame(close=np.asarray(data, dtype=np.float64))
        return self._calculate_via_manager(
            'macd', frame, ['macd', 'macd_signal', 'macd_hist'],
            fast_period=fast_period,
            slow_period=slow_period,
            signal_period=signal_period,
        )

    def calculate_rsi_gpu(self, data: np.ndarray, window: int) -> np.ndarray:
        """
        计算RSI指标（与 CPU 权威实现逐位一致）

        Args:
            data: 收盘价数据
            window: RSI计算窗口

        Returns:
            np.ndarray: RSI指标结果
        """
        frame = self._build_frame(close=np.asarray(data, dtype=np.float64))
        result = self._calculate_via_manager(
            'rsi', frame, [f'rsi{window}', 'rsi'], windows=[window]
        )
        # cast 仅为类型层收敛：manager 恒定输出 rsi{N} 与默认别名 rsi，两者必然命中其一
        return cast(np.ndarray, result.get(f'rsi{window}', result.get('rsi')))

    def calculate_sar_gpu(self, high: np.ndarray, low: np.ndarray, close: np.ndarray, af_step: float = 0.02, max_af: float = 0.2) -> np.ndarray:
        """
        计算SAR指标（与 CPU 权威实现逐位一致）

        SAR 是状态机式顺序递推（依赖前一根 SAR/EP/AF/趋势方向），GPU 无收益；
        此处统一委托 indicator_manager，避免另维护一份递推实现。

        Args:
            high: 最高价数据
            low: 最低价数据
            close: 收盘价数据
            af_step: 加速因子步长
            max_af: 最大加速因子

        Returns:
            np.ndarray: SAR指标结果
        """
        frame = self._build_frame(
            high=np.asarray(high, dtype=np.float64),
            low=np.asarray(low, dtype=np.float64),
            close=np.asarray(close, dtype=np.float64),
        )
        result = self._calculate_via_manager('sar', frame, ['sar'], af_step=af_step, max_af=max_af)
        return result['sar']


# 创建全局GPU加速器实例
global_gpu_accelerator = GPUAccelerator()


def is_gpu_available() -> bool:
    """
    检查GPU是否可用

    Returns:
        bool: GPU是否可用
    """
    return global_gpu_accelerator.is_gpu_available()


def calculate_with_gpu(indicator_type: str, data: Dict[str, np.ndarray | None], **kwargs) -> Dict[str, np.ndarray] | None:
    """
    使用GPU加速计算指标

    Args:
        indicator_type: 指标类型
        data: 输入数据
        **kwargs: 计算参数

    Returns:
        Dict[str, np.ndarray]: 计算结果；GPU 不可用时返回 None，由调用方回退主路径
    """
    if not global_gpu_accelerator.is_gpu_available():
        return None

    if indicator_type == 'ma':
        close = data.get('close')
        if close is not None:
            windows = kwargs.get('windows', [5, 10, 20, 60])
            if not isinstance(windows, list):
                windows = [windows]
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
            # 参数名兼容 window / windows，避免 GPU 侧默认 14 与调用方窗口不一致
            window = kwargs.get('window')
            if window is None:
                window = kwargs.get('windows', 14)
            if isinstance(window, (list, tuple)):
                window = window[0] if window else 14
            result = global_gpu_accelerator.calculate_rsi_gpu(close, window)
            # 列名与权威实现对齐：rsi{N} + 默认别名 rsi，避免窗口计算状态被误标记
            return {f'rsi{window}': result, 'rsi': result}

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
