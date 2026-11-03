#!/usr/bin/env python3

"""因子计算调度：把登记簿里的公式算到面板上，并隔离单因子失败。"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl
from loguru import logger

from src.quant.factor_lib import factors as _factors
from src.quant.factor_lib.registry import FactorSpec, computable_specs


@dataclass
class ComputeResult:
    """计算结果：宽表 + 失败清单 + 已计算因子名。"""

    frame: pl.DataFrame
    names: list[str]
    failures: dict[str, str] = field(default_factory=dict)


def compute_all(
    panel: pl.DataFrame,
    categories: list[str] | None = None,
) -> ComputeResult:
    """在面板上计算全部可计算因子。

    单因子异常只记录到 ``failures``，不中断整批计算——这是「引入一批因子」
    场景的必需行为：一个公式写错不应该让其余 100 个因子的评估结果一起丢失。

    Args:
        panel: ``data.load_panel`` 产出的面板
        categories: 只计算指定大类（None 表示全部）

    Returns:
        ComputeResult: 宽表（date, ts_code, 各因子列）
    """
    _factors.load_all()
    specs = computable_specs(categories)
    base = panel.select(['date', 'ts_code', 'close', 'volume', 'next_tradable'])
    failures: dict[str, str] = {}
    names: list[str] = []

    for spec in specs:
        try:
            value = _evaluate_spec(panel, spec)
        except (pl.exceptions.PolarsError, ValueError, TypeError, ArithmeticError) as exc:
            failures[spec.name] = f'{type(exc).__name__}: {exc}'
            logger.warning(f'因子计算失败，已跳过 {spec.name}: {exc}')
            continue
        base = base.with_columns(value.alias(spec.name))
        names.append(spec.name)

    logger.info(f'因子计算完成：成功 {len(names)} / 登记 {len(specs)}，失败 {len(failures)}')
    return ComputeResult(frame=base, names=names, failures=failures)


def _evaluate_spec(panel: pl.DataFrame, spec: FactorSpec) -> pl.Series:
    """计算单个因子并做基本可用性检查（全 null / 长度不符视为失败）。

    builder 允许返回两种形态：
        - ``pl.Series``：分阶段求值（``ops.stage`` / ``ops.out``）的结果，
          可绕开 polars「滚动相关入参不得带 over、横截面外层不得套时序」
          的窗口限制；
        - ``pl.Expr``：单层窗口的简单公式，直接在面板上求值。
    """
    assert spec.builder is not None
    result = spec.builder(panel)
    if isinstance(result, pl.Series):
        series = result
    else:
        series = panel.select(result.alias('factor_value'))['factor_value']
    if series.len() != panel.height:
        raise ValueError(f'因子结果长度 {series.len()} 与面板 {panel.height} 不符')
    if series.null_count() >= series.len():
        raise ValueError('因子结果全为 null（数据窗口不足或分母恒为 0）')
    return series.rename(spec.name)


def warmup_trim(frame: pl.DataFrame, specs: list[FactorSpec]) -> pl.DataFrame:
    """按最大 lookback 剔除预热行，保证所有因子在比较区间内都有值。"""
    if not specs:
        return frame
    max_lookback = max(spec.lookback for spec in specs)
    dates = frame['date'].unique().sort()
    if dates.len() <= max_lookback:
        return frame
    cutoff = dates[max_lookback]
    return frame.filter(pl.col('date') >= cutoff)


__all__ = ['ComputeResult', 'compute_all', 'warmup_trim']
