#!/usr/bin/env python3

"""
因子冗余处理 —— |Spearman corr| > 0.7 聚类，每簇保留 ICIR 最高者。

方法：
    1. 只取非重叠调仓日样本，避免重叠窗口把相关系数抬高；
    2. 每个交易日内先做横截面百分位排名（消除量纲与非正态影响），
       再在池化样本上算 Pearson 相关 —— 等价于「逐日 Spearman 的池化近似」；
    3. 距离 ``1 - |corr|``，average linkage 层次聚类，
       在距离 0.3 处切树 → 同簇内 |corr| ≥ 0.7；
    4. 每簇保留 ``|ICIR|`` 最大的因子（ICIR 缺失时按 |IC| 兜底），
       其余标为冗余，输出簇报告与剪枝后的因子清单。
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import polars as pl
from loguru import logger
from scipy.cluster.hierarchy import fcluster, linkage
from scipy.spatial.distance import squareform

#: |corr| 超过该阈值视为同一簇
CORR_THRESHOLD = 0.7
#: 池化样本的最小有效观测数（避免稀疏因子拉低相关）
MIN_OBS = 100


@dataclass(frozen=True, slots=True)
class ClusterResult:
    """聚类结果。"""

    correlation: pd.DataFrame
    clusters: pl.DataFrame
    kept: list[str]
    dropped: list[str]


def correlation_matrix(
    values: pl.DataFrame,
    names: list[str],
    dates: list,
) -> pd.DataFrame:
    """逐日内百分位排名后的池化相关矩阵。"""
    usable = [name for name in names if name in values.columns]
    frame = values.filter(pl.col('date').is_in(dates)).select(['date', *usable])
    pdf = frame.to_pandas()
    ranked = pdf.groupby('date', sort=True)[usable].rank(pct=True)
    corr = ranked.corr(min_periods=MIN_OBS)
    return corr.fillna(0.0)


def build_clusters(
    corr: pd.DataFrame,
    metrics: pl.DataFrame,
    threshold: float = CORR_THRESHOLD,
) -> ClusterResult:
    """按 |corr| 阈值聚类并保留每簇最优因子。"""
    names = list(corr.columns)
    if not names:
        empty = pl.DataFrame(schema={'cluster': pl.Int32, 'size': pl.Int32})
        return ClusterResult(corr, empty, [], [])

    distance = 1.0 - corr.abs().to_numpy()
    np.fill_diagonal(distance, 0.0)
    condensed = squareform(distance, checks=False)
    tree = linkage(condensed, method='average')
    labels = fcluster(tree, t=1.0 - threshold, criterion='distance')

    scores = {
        row['factor']: _score(row)
        for row in metrics.iter_rows(named=True)
        if row.get('factor') in names
    }
    cluster_rows: list[dict[str, object]] = []
    kept: list[str] = []
    dropped: list[str] = []
    for cluster_id in sorted({int(label) for label in labels}):
        members = [name for name, label in zip(names, labels, strict=True)
                   if int(label) == cluster_id]
        best = max(members, key=lambda name: scores.get(name, 0.0))
        kept.append(best)
        dropped.extend(name for name in members if name != best)
        sub = corr.loc[members, members].abs().to_numpy()
        off_diagonal = sub[~np.eye(len(members), dtype=bool)] if len(members) > 1 else np.array([])
        cluster_rows.append({
            'cluster': cluster_id,
            'size': len(members),
            'kept': best,
            'members': ', '.join(sorted(members)),
            'max_abs_corr': float(off_diagonal.max()) if off_diagonal.size else 0.0,
            'mean_abs_corr': float(off_diagonal.mean()) if off_diagonal.size else 0.0,
            'kept_icir': float(scores.get(best, 0.0)),
        })

    clusters = pl.DataFrame(cluster_rows).sort(['size', 'cluster'], descending=[True, False])
    logger.info(f'冗余聚类：{len(names)} 个因子 → {len(kept)} 簇，剪掉 {len(dropped)} 个冗余')
    return ClusterResult(corr, clusters, sorted(kept), sorted(dropped))


def _score(row: dict) -> float:
    """簇内排序分：|ICIR| 优先，缺失时退化为 |IC|。"""
    icir = row.get('icir')
    if icir is not None and np.isfinite(icir):
        return abs(float(icir))
    mean_ic = row.get('mean_ic')
    if mean_ic is not None and np.isfinite(mean_ic):
        return abs(float(mean_ic))
    return 0.0


__all__ = [
    'CORR_THRESHOLD',
    'ClusterResult',
    'build_clusters',
    'correlation_matrix',
]
