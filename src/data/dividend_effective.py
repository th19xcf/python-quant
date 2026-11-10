#!/usr/bin/env python3

"""分红事件表「有效层」的唯一取值收口点。

三层列语义
----------
``stock_dividend``（本地快照 ``factors/stock_dividend.parquet`` 与 MySQL 同表）
同时存原始值与人工修正值：

===========================  ====================================================
列                            含义
===========================  ====================================================
``cash_div`` / ``share_div``  上游采集原样，保留可追溯
``cash_div_fix``             人工修正的每股派现，非空时覆盖 ``cash_div``
``share_div_fix``            人工修正的每股送转，非空时覆盖 ``share_div``
``no_ex_rights``             该事件不向原股东分配、原始价不除权（重整转增抵债等）
===========================  ====================================================

唯一口径::

    有效值 = {base}_fix.fill_null(原始列)
    no_ex_rights=True 的事件对「权益记账」与「复权因子」都必须剔除

任何消费者都必须走本模块，不得各自写一遍 ``fill_null``。

为什么必须收口（2026-11-10 修复的实质缺陷）
-------------------------------------------
601258.SH 2019-12-30：原始 ``share_div=0.56416`` / ``cash_div=0.0``，修正
``share_div_fix=0.0`` / ``cash_div_fix=0.04``。复权因子层读有效层（正确），
撮合层只读原始列 → 给这笔记了 56.4% 送转股、漏记 0.04 元派现，持仓权益
永久虚增约 56%，且净值曲线平滑、不报任何错。

缺列契约
--------
``*_fix`` / ``no_ex_rights`` 缺失**不等于「没有修正」**，而是数据契约违约：
帧不是从权威快照读出来的（读错文件、读错层、或字段在层间传递时被裁掉，
``tdx_handler`` 就曾把本地帧压成只含 4 个原始列的 ``SimpleNamespace``）。

因此默认 ``expect=CONTRACT_COLUMNS``，缺列直接抛 :class:`DividendSchemaError`。
确实按原始层消费的调用方（单测自建帧、研究脚本、基金分红表）必须显式传
``expect=()``，让「我就是要原始层」在代码里可检索，而不是靠沉默。

判断依据：撮合账本是权益真相来源，静默降级会直接虚增权益而不报错；这个
缺陷此前存在了数月没有被任何断言拦住，正是因为缺列时两条口径各自沉默。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import polars as pl

#: 需要修正覆盖的基列
BASE_COLUMNS: tuple[str, ...] = ('cash_div', 'share_div')
#: 修正列（非空时覆盖同名基列）
FIX_COLUMNS: tuple[str, ...] = tuple(f'{col}_fix' for col in BASE_COLUMNS)
#: 完整契约：两组修正列 + 不除权标记
CONTRACT_COLUMNS: tuple[str, ...] = (*FIX_COLUMNS, 'no_ex_rights')


class DividendSchemaError(ValueError):
    """分红帧缺少有效层必需列（数据契约违约，不是「没有修正」）。"""


def missing_columns(frame: pl.DataFrame,
                    expect: Sequence[str] = CONTRACT_COLUMNS) -> list[str]:
    """返回 ``frame`` 相对 ``expect`` 缺失的列名（保持 ``expect`` 顺序）。"""
    return [col for col in expect if col not in frame.columns]


def _require(frame: pl.DataFrame, expect: Sequence[str], context: str) -> None:
    missing = missing_columns(frame, expect)
    if not missing:
        return
    where = f'{context}' if context else '分红帧'
    raise DividendSchemaError(
        f'{where} 缺少列 {missing}：缺列不能当作「无修正」退回原始列 '
        f'(601258.SH 2019-12-30 就是这样把 share_div_fix=0.0 丢成 56.4% 送转的)。'
        f'确属原始层消费请显式传 expect=()。')


def apply_effective_columns(frame: pl.DataFrame, *,
                            expect: Sequence[str] = CONTRACT_COLUMNS,
                            context: str = '') -> pl.DataFrame:
    """把 ``*_fix`` 覆盖到同名基列，返回新帧（不修改入参）。

    Args:
        frame: 分红事件帧。
        expect: 必须存在的列；缺任一列抛 :class:`DividendSchemaError`。
            原始层消费者（无修正列的表）传 ``()``。
        context: 报错时用于定位调用点的前缀。

    Returns:
        基列已按「修正非空优先、否则原值」重写的帧。若基列或修正列单独缺失
        （``expect`` 未要求时），该基列原样保留。
    """
    _require(frame, expect, context)
    out = frame
    for base in BASE_COLUMNS:
        fix = f'{base}_fix'
        if base in out.columns and fix in out.columns:
            out = out.with_columns(
                pl.col(fix).cast(pl.Float64, strict=False)
                .fill_null(pl.col(base).cast(pl.Float64, strict=False))
                .alias(base))
    return out


def drop_non_ex_rights(frame: pl.DataFrame, *,
                       expect: Sequence[str] = CONTRACT_COLUMNS,
                       context: str = '') -> pl.DataFrame:
    """剔除 ``no_ex_rights=True`` 的事件（不除权、原股东不获分配）。

    该标记缺省视为 ``False`` 只在 ``expect`` 未要求该列时成立；撮合与因子
    计算入口都在契约列里，缺列会先抛错，不会静默当作「全部除权」。
    """
    _require(frame, expect, context)
    if 'no_ex_rights' not in frame.columns:
        return frame
    return frame.filter(~pl.col('no_ex_rights').fill_null(False))


def effective_value(row: Mapping[str, Any], base: str) -> Any:
    """逐行取有效值：``{base}_fix`` 非空优先，否则原始列（供 dict 行消费者）。"""
    fix = row.get(f'{base}_fix')
    return fix if fix is not None else row.get(base)
