"""分红覆盖检查：ETF 缺分红时回测收益会被静默低估，这里显式标注。

判定口径（只对 ETF，即 :func:`src.data.local_store.fund_factors.is_etf_code`
为真的代码）：

- ``factors/fund_dividend.parquet`` 里有该代码的除息记录 → 覆盖；
- 或 ``factors/fund_adj_factor.parquet`` 里该代码 ``hfq_factor`` 去重值超过
  1 档（发生过复权阶跃） → 覆盖；
- 两者都没有 → 未覆盖，回报里给出该标的与原因。

只读本地文件，不补数、不联网；股票分红不在基金表口径内，不参与判定
（``checked`` 为 0 时视为通过，避免把「口径外」误报成「缺数据」）。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

import polars as pl
from loguru import logger

from src.data.local_store.fund_factors import (
    FUND_DIVIDEND_FILENAME,
    FUND_FACTOR_FILENAME,
    is_etf_code,
)

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_FACTOR_DIR = ROOT / 'data/local_store/factors'
HINT_TEMPLATE = '{count} 只标的分红覆盖不足，收益可能低估'


def _dividend_codes(path: Path, codes: List[str]) -> Set[str]:
    if not path.exists():
        return set()
    try:
        frame = (pl.scan_parquet(str(path)).select('ts_code')
                 .filter(pl.col('ts_code').is_in(codes)).collect())
    except Exception as exc:  # noqa: BLE001
        logger.warning('分红记录表读取失败，按无记录处理：{}（{}）', path, exc)
        return set()
    return set(frame['ts_code'].unique().to_list())


def _step_codes(path: Path, codes: List[str]) -> Set[str]:
    """复权因子发生阶跃的代码：``hfq_factor`` 去重值 > 1 档。"""
    if not path.exists():
        return set()
    try:
        frame = (pl.scan_parquet(str(path))
                 .select(['ts_code', 'hfq_factor'])
                 .filter(pl.col('ts_code').is_in(codes))
                 .group_by('ts_code')
                 .agg(pl.col('hfq_factor').drop_nulls().n_unique().alias('steps'))
                 .filter(pl.col('steps') > 1)
                 .collect())
    except Exception as exc:  # noqa: BLE001
        logger.warning('复权因子表读取失败，按无阶跃处理：{}（{}）', path, exc)
        return set()
    return set(frame['ts_code'].to_list())


def dividend_coverage(codes: Iterable[str],
                      root: Path | None = None) -> Dict[str, Any]:
    """给定标的代码，返回分红覆盖检查结果。

    返回 ``{'ok', 'checked', 'issues', 'message'}``：``issues`` 逐只列出未覆盖
    的代码与原因；``ok`` 为真时 ``message`` 为空字符串。
    """
    factor_dir = Path(root) if root is not None else DEFAULT_FACTOR_DIR
    etfs = sorted({str(code).strip() for code in codes
                   if code and is_etf_code(str(code).strip())})
    if not etfs:
        return {'ok': True, 'checked': 0, 'issues': [], 'message': ''}

    div_path = factor_dir / FUND_DIVIDEND_FILENAME
    factor_path = factor_dir / FUND_FACTOR_FILENAME
    covered = (_dividend_codes(div_path, etfs)
               | _step_codes(factor_path, etfs))
    issues: List[Dict[str, str]] = []
    for code in etfs:
        if code in covered:
            continue
        if div_path.exists():
            reason = '无分红记录，且复权因子无阶跃'
        else:
            reason = f'分红表 {FUND_DIVIDEND_FILENAME} 缺失，且复权因子无阶跃'
        issues.append({'code': code, 'reason': reason})

    message = HINT_TEMPLATE.format(count=len(issues)) if issues else ''
    if issues:
        logger.warning('分红覆盖不足：{}', [item['code'] for item in issues])
    return {'ok': not issues, 'checked': len(etfs), 'issues': issues,
            'message': message}
