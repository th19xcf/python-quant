#!/usr/bin/env python3

"""
流式处理框架，支持实时数据的技术指标计算。

设计要点：
  - 历史数据 MAX_BARS 上限（默认 5000），长时间运行不会无限增长
  - 全链路强制 Float64，避免 diagonal_relaxed 触发 dtype 抖动
  - concat 用 strict 'vertical'，schema 由 history_data 权威定义
  - rechunk 控制 chunk 数，防止 to_numpy 零拷贝退化为复制
"""

from typing import List, cast

import numpy as np
import polars as pl
from loguru import logger

from .gpu_acceleration import calculate_with_gpu, is_gpu_available
from .incremental_calculator import global_incremental_calculator

# 历史数据保留上限：覆盖最长指标窗口的若干倍，同时控制内存
MAX_BARS = 5000


class StreamingProcessor:
    """
    流式处理器类，支持实时数据的技术指标计算
    """

    def __init__(self, indicators: List[str], **indicator_params):
        self.indicators = indicators
        self.indicator_params = indicator_params
        self.history_data: pl.DataFrame | None = None
        self.calculated_indicators: dict[str, bool] = dict.fromkeys(indicators, False)

    # ------------------------------------------------------------------
    # 对外入口
    # ------------------------------------------------------------------
    def process_data(self, new_data: pl.DataFrame) -> pl.DataFrame:
        if new_data is None or new_data.is_empty():
            return new_data

        if 'date' in new_data.columns:
            new_data = new_data.sort('date')

        if self.history_data is None:
            result = self._calculate_initial(new_data)
            self.history_data = result
        else:
            result = self._calculate_incremental(new_data)
            # 用 vertical concat + schema 对齐，替代 diagonal_relaxed（会触发 dtype 抖动）
            self.history_data = self._append_history(self.history_data, result)

        return result

    # ------------------------------------------------------------------
    # 内部：历史数据合并（schema 对齐 + vertical + MAX_BARS + rechunk）
    # ------------------------------------------------------------------
    @staticmethod
    def _append_history(history: pl.DataFrame, new: pl.DataFrame) -> pl.DataFrame:
        """将新帧追加到历史，schema 强制对齐到 history，截断到 MAX_BARS。

        修复：
          - 原 diagonal_relaxed 遇到 dtype 不一致（Float32/Float64）会整列转超类型，O(n) 代价累积为 O(m·n)
          - history_data 无上限导致内存持续增长
          - concat 累积 chunk 后下游 to_numpy 从零拷贝退化为复制
        """
        schema = history.schema
        # 对齐到 history 的 schema：缺列补 typed null，dtype 强制转换
        aligned_exprs: list[pl.Expr] = []
        for col, dtype in schema.items():
            if col in new.columns:
                aligned_exprs.append(pl.col(col).cast(dtype))
            else:
                aligned_exprs.append(pl.lit(None, dtype=dtype).alias(col))
        aligned = new.select(aligned_exprs)

        merged = pl.concat([history, aligned], how='vertical', rechunk=False)

        # 截断 + rechunk
        if merged.height > MAX_BARS:
            merged = merged.tail(MAX_BARS)
        if merged.n_chunks() > 32:
            merged = merged.rechunk()

        return merged

    # ------------------------------------------------------------------
    # 首次全量计算
    # ------------------------------------------------------------------
    def _calculate_initial(self, data: pl.DataFrame) -> pl.DataFrame:
        result = data.clone()

        # 循环不变量：OHLCV 的 numpy 视图在循环外生成一次（原代码在 for indicator 循环内，每指标转 4 次）
        gpu_data = {
            'close': data['close'].to_numpy() if 'close' in data.columns else None,
            'high': data['high'].to_numpy() if 'high' in data.columns else None,
            'low': data['low'].to_numpy() if 'low' in data.columns else None,
            'volume': data['volume'].to_numpy() if 'volume' in data.columns else None,
        }
        use_gpu = is_gpu_available()

        for indicator in self.indicators:
            try:
                if use_gpu:
                    gpu_result = calculate_with_gpu(indicator, gpu_data, **self.indicator_params)
                    if gpu_result:
                        logger.info(f"使用GPU计算{indicator}")
                        for col_name, values in gpu_result.items():
                            result = result.with_columns(pl.Series(col_name, values).alias(col_name))
                        self.calculated_indicators[indicator] = True
                        continue

                from .indicator_manager import global_indicator_manager
                logger.info(f"使用常规计算{indicator}")
                result = global_indicator_manager.calculate_indicator(
                    result, indicator, return_polars=True, **self.indicator_params,
                )
                self.calculated_indicators[indicator] = True
            # 兜底理由：单个指标计算失败不中断其余指标
            except Exception as e:  # noqa: BLE001
                logger.error(f"计算{indicator}失败: {e}")

        # 统一 Float64，避免后续 concat dtype 抖动
        result = result.with_columns([
            pl.col(c).cast(pl.Float64)
            for c, dt in result.schema.items()
            if dt.is_float()
        ])
        return result

    # ------------------------------------------------------------------
    # 增量计算
    # ------------------------------------------------------------------
    def _calculate_incremental(self, new_data: pl.DataFrame) -> pl.DataFrame:
        # process_data 仅在 history_data 非 None 时进入增量分支
        history_data = cast(pl.DataFrame, self.history_data)
        # 先把 new_data 按 history 的列集合补齐（填 null + 对齐 dtype + 重排顺序）
        result = self._align_to_history_schema(new_data)

        for indicator in self.indicators:
            try:
                if global_incremental_calculator.is_supported(indicator):
                    logger.info(f"使用增量计算{indicator}")
                    raw_new = new_data.select(['date', 'open', 'high', 'low', 'close', 'volume'])
                    raw_hist = history_data.select(['date', 'open', 'high', 'low', 'close', 'volume'])

                    inc_result = global_incremental_calculator.incremental_calculate(
                        indicator, raw_hist, raw_new, **self.indicator_params,
                    )
                    for col in inc_result.columns:
                        if col in result.columns:
                            result = result.with_columns(inc_result[col].alias(col))
                        else:
                            result = result.with_columns(inc_result[col])
                else:
                    logger.info(f"使用完整计算{indicator}")
                    from .indicator_manager import global_indicator_manager
                    # 用 history schema 对齐后 strict vertical concat，替代 diagonal_relaxed
                    aligned = self._align_to_history_schema(result)
                    combined = pl.concat([history_data, aligned], how='vertical', rechunk=False)
                    full_result = global_indicator_manager.calculate_indicator(
                        combined, indicator, return_polars=True, **self.indicator_params,
                    )
                    for col in full_result.columns:
                        if col not in ('date', 'open', 'high', 'low', 'close', 'volume'):
                            new_values = full_result[col].tail(len(new_data))
                            result = result.with_columns(new_values.alias(col))
            # 兜底理由：单个指标增量计算失败降级为全量结果
            except Exception as e:  # noqa: BLE001
                logger.error(f"增量计算{indicator}失败: {e}")

        return result

    def _align_to_history_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        """按 self.history_data 的 schema 对齐 df：缺列补 typed null + 强制 dtype + 重排顺序"""
        if self.history_data is None:
            return df
        schema = self.history_data.schema
        exprs: list[pl.Expr] = []
        for col, dtype in schema.items():
            if col in df.columns:
                exprs.append(pl.col(col).cast(dtype))
            else:
                exprs.append(pl.lit(None, dtype=dtype).alias(col))
        return df.select(exprs)

    # ------------------------------------------------------------------
    # 管理接口
    # ------------------------------------------------------------------
    def get_history_data(self) -> pl.DataFrame | None:
        return self.history_data

    def reset(self):
        self.history_data = None
        for ind in self.indicators:
            self.calculated_indicators[ind] = False

    def add_indicator(self, indicator: str):
        if indicator not in self.indicators:
            self.indicators.append(indicator)
            self.calculated_indicators[indicator] = False

    def remove_indicator(self, indicator: str):
        if indicator in self.indicators:
            self.indicators.remove(indicator)
            del self.calculated_indicators[indicator]


# ------------------------------------------------------------------
# 实时批处理（无改动）
# ------------------------------------------------------------------
class RealTimeDataProcessor:
    def __init__(self, batch_size: int = 1):
        self.batch_size = batch_size
        self.batch_data: list[pl.DataFrame] = []
        self.streaming_processors: dict[str, StreamingProcessor] = {}

    def register_processor(self, name: str, processor: StreamingProcessor):
        self.streaming_processors[name] = processor

    def process(self, data: pl.DataFrame, processor_name: str) -> pl.DataFrame | None:
        if processor_name not in self.streaming_processors:
            logger.error(f"处理器{processor_name}不存在")
            return None
        self.batch_data.append(data)
        if len(self.batch_data) >= self.batch_size:
            batch = pl.concat(self.batch_data)
            self.batch_data = []
            return self.streaming_processors[processor_name].process_data(batch)
        return None

    def flush(self, processor_name: str) -> pl.DataFrame | None:
        if not self.batch_data:
            return None
        if processor_name not in self.streaming_processors:
            logger.error(f"处理器{processor_name}不存在")
            return None
        batch = pl.concat(self.batch_data)
        self.batch_data = []
        return self.streaming_processors[processor_name].process_data(batch)

    def get_processor(self, name: str) -> StreamingProcessor | None:
        return self.streaming_processors.get(name)

    def remove_processor(self, name: str):
        if name in self.streaming_processors:
            del self.streaming_processors[name]


global_realtime_processor = RealTimeDataProcessor()


def create_streaming_processor(indicators: List[str], **kwargs) -> StreamingProcessor:
    return StreamingProcessor(indicators, **kwargs)


def process_realtime_data(data: pl.DataFrame, processor_name: str, batch_size: int = 1) -> pl.DataFrame | None:
    if processor_name not in global_realtime_processor.streaming_processors:
        processor = StreamingProcessor(['ma', 'macd', 'rsi'])
        global_realtime_processor.register_processor(processor_name, processor)
        global_realtime_processor.batch_size = batch_size
    return global_realtime_processor.process(data, processor_name)
