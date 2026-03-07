#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GPU加速计算模块
使用CuPy实现CUDA加速，支持OpenCL作为备选
"""

import os
import sys
import numpy as np
from typing import List, Dict, Any, Optional

# 尝试导入CuPy，如果不可用则使用NumPy作为备选
try:
    import cupy as cp
    GPU_AVAILABLE = True
    print("CuPy可用，将使用GPU加速")
except ImportError:
    print("CuPy不可用，将使用CPU计算")
    cp = np
    GPU_AVAILABLE = False

# 尝试导入PyOpenCL作为备选
try:
    import pyopencl as cl
    OPENCL_AVAILABLE = True
    print("OpenCL可用，将作为GPU加速备选")
except ImportError:
    print("OpenCL不可用")
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
                    print(f"OpenCL初始化成功，设备: {devices[0].name}")
                else:
                    print("未找到GPU设备，使用CPU设备")
                    devices = platform.get_devices(cl.device_type.CPU)
                    if devices:
                        self._opencl_context = cl.Context(devices)
                        self._opencl_queue = cl.CommandQueue(self._opencl_context)
        except Exception as e:
            print(f"OpenCL初始化失败: {e}")
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
        gpu_data = self.to_gpu(data)
        
        # 计算EMA
        def ema(data, period):
            alpha = 2.0 / (period + 1)
            result = cp.zeros_like(data)
            result[0] = data[0]
            for i in range(1, len(data)):
                result[i] = alpha * data[i] + (1 - alpha) * result[i-1]
            return result
        
        ema12 = ema(gpu_data, fast_period)
        ema26 = ema(gpu_data, slow_period)
        
        # 计算MACD线
        macd_line = ema12 - ema26
        
        # 计算信号线
        macd_signal = ema(macd_line, signal_period)
        
        # 计算柱状图
        macd_hist = macd_line - macd_signal
        
        return {
            'macd': self.to_cpu(macd_line),
            'macd_signal': self.to_cpu(macd_signal),
            'macd_hist': self.to_cpu(macd_hist)
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
        gpu_data = self.to_gpu(data)
        
        # 计算价格变化
        delta = cp.diff(gpu_data)
        delta = cp.concatenate([cp.array([0]), delta])
        
        # 计算上涨和下跌
        gain = cp.where(delta > 0, delta, 0)
        loss = cp.where(delta < 0, -delta, 0)
        
        # 计算平均上涨和下跌
        def sma(data, window):
            result = cp.zeros_like(data)
            result[window-1] = cp.mean(data[:window])
            for i in range(window, len(data)):
                result[i] = (result[i-1] * (window-1) + data[i]) / window
            return result
        
        avg_gain = sma(gain, window)
        avg_loss = sma(loss, window)
        
        # 计算RSI
        rs = avg_gain / (avg_loss + 1e-10)  # 避免除零
        rsi = 100 - (100 / (1 + rs))
        
        # 补全前面的NaN值
        rsi[:window-1] = np.nan
        
        return self.to_cpu(rsi)
    
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
        n = len(close)
        sar = np.zeros(n)
        sar.fill(np.nan)
        
        if n < 2:
            return sar
        
        # 初始化
        ep = high[0]  # 极点价格
        af = af_step   # 加速因子
        long = True    # 当前趋势（True=多头，False=空头）
        
        # 确定初始趋势
        if close[1] > close[0]:
            long = True
            sar[0] = low[0]
            ep = high[0]
        else:
            long = False
            sar[0] = high[0]
            ep = low[0]
        
        # 迭代计算
        for i in range(1, n):
            if long:
                # 多头趋势
                sar[i] = sar[i-1] + af * (ep - sar[i-1])
                # 限制SAR不超过前n周期的最低价
                if i >= 2:
                    sar[i] = max(sar[i], low[i-1], low[i-2])
                else:
                    sar[i] = max(sar[i], low[i-1])
                
                # 检查趋势反转
                if low[i] < sar[i]:
                    # 转为空头
                    long = False
                    sar[i] = ep
                    ep = low[i]
                    af = af_step
                elif high[i] > ep:
                    # 更新极点
                    ep = high[i]
                    af = min(af + af_step, max_af)
            else:
                # 空头趋势
                sar[i] = sar[i-1] + af * (ep - sar[i-1])
                # 限制SAR不低于前n周期的最高价
                if i >= 2:
                    sar[i] = min(sar[i], high[i-1], high[i-2])
                else:
                    sar[i] = min(sar[i], high[i-1])
                
                # 检查趋势反转
                if high[i] > sar[i]:
                    # 转为多头
                    long = True
                    sar[i] = ep
                    ep = high[i]
                    af = af_step
                elif low[i] < ep:
                    # 更新极点
                    ep = low[i]
                    af = min(af + af_step, max_af)
        
        return sar


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
