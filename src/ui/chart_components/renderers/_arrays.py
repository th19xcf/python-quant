"""渲染器辅助工具：统一 Series → numpy 的安全转换。

用法：
    from ._arrays import as_f64
    arr = as_f64(df['k'])   # 零拷贝（已是 Float64 单 chunk）或单次复制

核心优势：
  - 已是 Float64 + 单 chunk → .to_numpy() 直接零拷贝返回视图，不走 cast → to_numpy 多一步
  - 是 Float64 但多 chunk → 先 rechunk 再 to_numpy（一次复制 vs 两次）
  - 是 Float32/其他 → cast(Float64) → to_numpy（只复制一次，替代原代码 to_numpy + astype 的两次）

原代码 58+ 处 `df[col].to_numpy().astype(np.float64)`：
  to_numpy() 如果源是多 chunk 先复制一次 → astype(copy=True) 总是再复制一次 → **两次复制**
现在用 as_f64 至少降为一次，最佳情况零拷贝。
"""

from __future__ import annotations

import numpy as np
import polars as pl


def as_f64(s: pl.Series, start: int = 0, length: int | None = None) -> np.ndarray:
    """将 Polars Series 转为 float64 numpy array。

    - 已是 Float64 且单 chunk：零拷贝视图（唯一复制源是 to_numpy 本身）
    - 已是 Float64 但多 chunk：先 rechunk 再 to_numpy（避免 to_numpy 内部复制）
    - 非 Float64：cast(Float64) → to_numpy（只一次复制）
    """
    if length is not None:
        s = s.slice(start, length)

    if s.dtype != pl.Float64:
        # 非 Float64：cast 到 Float64 再 to_numpy（Polars cast 本身是惰性的，真正的复制在 to_numpy 时发生一次）
        return s.cast(pl.Float64).to_numpy()

    # 已是 Float64
    if s.n_chunks() > 1:
        s = s.rechunk()

    arr = s.to_numpy()
    # 已保证 Float64 + 单 chunk → to_numpy 返回零拷贝视图
    return arr


def as_any(s: pl.Series, start: int = 0, length: int | None = None) -> np.ndarray:
    """泛化 helper：按源 dtype to_numpy，不做 Float64 强转。

    用于那些不要求 float64 的路径（如 volume/amount 原始显示）。
    """
    if length is not None:
        s = s.slice(start, length)
    if s.n_chunks() > 1:
        s = s.rechunk()
    return s.to_numpy()
