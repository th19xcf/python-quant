#!/usr/bin/env python3

r"""P2 元数据对齐与瑕疵台账（不改动任何行情文件）。

1. 修正 ``factors/fund_meta.json`` 里与实际文件不符的行数/代码数；
2. 登记基金 ``close=1.0`` 占位桩，供回测显式排除；
3. 登记各数据集的代码覆盖缺口，避免「缺数据」被误读成「因子为零」。

默认 dry-run 只出报告；``--commit`` 才写 meta 与 fund_meta.json。

用法::

    python tools/reconcile_p2_metadata.py
    python tools/reconcile_p2_metadata.py --commit
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_ROOT = ROOT / 'data' / 'local_store'
DEFAULT_REPORT = ROOT / 'artifacts' / 'p2_reconcile' / 'report.json'


def _codes(path: Path, column: str = 'ts_code') -> set[str]:
    if not path.is_file():
        return set()
    return set(pl.read_parquet(path, columns=[column])[column].to_list())


def _stock_universe(root: Path) -> set[str]:
    files = (root / 'quotes' / 'stock').rglob('*.parquet')
    return {path.stem for path in files}


def _placeholder_funds(root: Path) -> pl.DataFrame:
    rows = []
    for path in sorted((root / 'quotes' / 'fund').rglob('*.parquet')):
        try:
            frame = pl.read_parquet(path, columns=['close'])
        except (OSError, ValueError, RuntimeError):
            continue
        if frame.is_empty():
            continue
        ones = frame.filter(pl.col('close') == 1.0).height
        rows.append({
            'ts_code': path.stem, 'rows': frame.height,
            'close_one_rows': ones, 'ratio': ones / frame.height,
        })
    frame = pl.DataFrame(rows)
    if frame.is_empty():
        return frame
    return frame.filter(pl.col('ratio') >= 0.9).with_columns(
        pl.lit('close 恒为 1.0 的占位桩，非真实净值').alias('reason'),
        pl.lit(datetime.now().date()).alias('detected_at'),
    ).sort('ts_code')


def _coverage_gaps(root: Path, universe: set[str]) -> pl.DataFrame:
    datasets = {
        'financials_income': root / 'meta/financials/income.parquet',
        'valuation_daily': root / 'meta/financials/valuation_daily.parquet',
        'stock_shares': root / 'meta/factors/stock_shares.parquet',
        'stock_dividend': root / 'factors/stock_dividend.parquet',
        'stock_adj_factor': root / 'factors/stock_adj_factor.parquet',
        'stock_turnover': root / 'factors/stock_turnover.parquet',
    }
    rows = []
    for name, path in datasets.items():
        covered = _codes(path)
        missing = sorted(universe - covered)
        rows.append({
            'dataset': name, 'covered': len(universe & covered),
            'missing': len(missing), 'sample_codes': ','.join(missing[:15]),
        })
    return pl.DataFrame(rows).sort('dataset')


def _fund_meta_fix(root: Path) -> dict[str, Any]:
    path = root / 'factors' / 'fund_meta.json'
    payload = json.loads(path.read_text(encoding='utf-8'))
    actual = {
        'share_change_rows': pl.read_parquet(
            root / 'factors/fund_share_change.parquet').height,
        'share_change_codes': pl.read_parquet(
            root / 'factors/fund_share_change.parquet',
            columns=['ts_code'])['ts_code'].n_unique(),
        'factor_rows': pl.read_parquet(
            root / 'factors/fund_adj_factor.parquet').height,
        'dividend_rows': pl.read_parquet(
            root / 'factors/fund_dividend.parquet').height,
    }
    changes = {}
    for key, value in actual.items():
        if payload.get(key) != value:
            changes[key] = {'before': payload.get(key), 'after': value}
            payload[key] = value
    payload['reconciled_at'] = datetime.now().isoformat(timespec='seconds')
    return {'path': path, 'payload': payload, 'changes': changes}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='P2 元数据对齐与瑕疵台账')
    parser.add_argument('--store-root', type=Path, default=DEFAULT_ROOT)
    parser.add_argument('--report', type=Path, default=DEFAULT_REPORT)
    parser.add_argument('--commit', action='store_true',
                        help='写入台账与修正后的 fund_meta.json')
    args = parser.parse_args(argv)
    root = args.store_root

    universe = _stock_universe(root)
    placeholders = _placeholder_funds(root)
    gaps = _coverage_gaps(root, universe)
    meta = _fund_meta_fix(root)

    report = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'committed': args.commit,
        'stock_universe': len(universe),
        'placeholder_funds': placeholders.height,
        'placeholder_sample': placeholders.head(10).to_dicts()
            if not placeholders.is_empty() else [],
        'coverage_gaps': gaps.to_dicts(),
        'fund_meta_changes': meta['changes'],
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2,
                                      default=str), encoding='utf-8')

    if args.commit:
        anomaly_dir = root / 'meta' / 'anomalies'
        anomaly_dir.mkdir(parents=True, exist_ok=True)
        placeholders.write_parquet(anomaly_dir / 'placeholder_codes.parquet')
        gaps.write_parquet(anomaly_dir / 'coverage_gaps.parquet')
        meta['path'].write_text(
            json.dumps(meta['payload'], ensure_ascii=False, indent=2) + '\n',
            encoding='utf-8')

    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == '__main__':
    sys.exit(main())
