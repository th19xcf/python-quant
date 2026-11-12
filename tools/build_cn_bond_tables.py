#!/usr/bin/env python3

r"""把 staging 的中债指数与收益率曲线整理成正式表。

产物（``--apply`` 后）
=====================
- ``meta/macro/cn_bond_index.parquet``：``date, index_name, value, source``
- ``meta/macro/cn_treasury_yield_curve.parquet``：``date, tenor, ytm_pct, curve, source``
- 两张表各自的 ``*.manifest.json``

口径
====
- 指数只收**财富指数**（含票息再投资）的「总值」段；净价/全价指数不含票息，
  当资产收益会系统性低估，故不入表。
- 收益率曲线只收「中债国债收益率曲线」；接口同时返回的中债中短票(AAA)
  属信用债口径，本次不落盘（登记 G-042）。
- 对拍：新曲线的 10Y 列必须与既有 ``cn_treasury_yield_10y.parquet`` 在重叠日逐值一致，
  差异 > 1e-4 即报错不落盘——同源（``chinabond:bond_china_yield``）不允许出现分歧。

用法::

    .venv\\Scripts\\python.exe tools/build_cn_bond_tables.py            # 只校验
    .venv\\Scripts\\python.exe tools/build_cn_bond_tables.py --apply    # 落正式路径
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

import polars as pl

from src.data.atomic_write import write_parquet_atomic

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / 'data' / 'local_store'
STAGING = ROOT / '_staging' / 'multi_asset_tier2_20261110' / 'cbond'
MACRO = STORE / 'meta' / 'macro'

TENOR_MAP = {'3月': 0.25, '6月': 0.5, '1年': 1.0, '3年': 3.0, '5年': 5.0,
             '7年': 7.0, '10年': 10.0, '30年': 30.0}
SOURCE_YIELD = 'chinabond:bond_china_yield'
SOURCE_INDEX = 'chinabond:single_index_query'


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def build_index_table() -> pl.DataFrame:
    frames = []
    for path in sorted(STAGING.glob('index_*.parquet')):
        name = path.stem.removeprefix('index_')
        df = pl.read_parquet(path).with_columns(
            pl.lit(name).alias('index_name'),
            pl.lit(SOURCE_INDEX).alias('source'),
            pl.col('date').cast(pl.Utf8).str.strptime(pl.Date, '%Y-%m-%d', strict=False),
            pl.col('value').cast(pl.Float64),
        )
        frames.append(df)
    if not frames:
        raise SystemExit('staging/cbond 无指数文件，先跑 tools/fetch_cn_bond_data.py')
    out = pl.concat(frames).select(['date', 'index_name', 'value', 'source'])
    return out.filter(pl.col('date').is_not_null() & pl.col('value').is_not_null()).sort(
        ['index_name', 'date'])


def build_yield_table() -> pl.DataFrame:
    frames = []
    for path in sorted(STAGING.glob('yield_curve_*.parquet')):
        df = pl.read_parquet(path)
        if df.height == 0:
            continue
        df = df.rename({'日期': 'date'}).with_columns(
            pl.col('date').cast(pl.Utf8).str.strptime(pl.Date, '%Y-%m-%d', strict=False))
        for raw, years in TENOR_MAP.items():
            if raw not in df.columns:
                continue
            frames.append(df.select([
                'date', pl.lit(years).alias('tenor_years'), pl.lit(raw).alias('tenor'),
                pl.col(raw).cast(pl.Float64).alias('ytm_pct'),
            ]))
    if not frames:
        raise SystemExit('staging/cbond 无收益率曲线文件')
    out = pl.concat(frames).with_columns(
        pl.lit('中债国债收益率曲线').alias('curve'),
        pl.lit(SOURCE_YIELD).alias('source'),
    ).filter(pl.col('date').is_not_null())
    return out.sort(['date', 'tenor_years'])


def check_10y(yields: pl.DataFrame) -> tuple[int, float]:
    """新曲线 10Y 与既有 10Y 表逐值对拍。"""
    old_path = MACRO / 'cn_treasury_yield_10y.parquet'
    if not old_path.exists():
        return 0, 0.0
    old = pl.read_parquet(old_path).select([pl.col('date'), pl.col('y10').alias('old')])
    new = yields.filter(pl.col('tenor_years') == 10.0).select(
        [pl.col('date'), pl.col('ytm_pct').alias('new')])
    joined = old.join(new, on='date', how='inner').drop_nulls()
    if joined.height == 0:
        return 0, 0.0
    diff = (joined['new'] - joined['old']).abs()
    return joined.height, float(diff.max())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='构建中债指数与国债收益率曲线正式表')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args(argv)

    idx = build_index_table()
    yld = build_yield_table()
    print('指数行数:', idx.height, '指数:', idx['index_name'].unique().to_list())
    print('曲线行数:', yld.height, '期限:', sorted(yld['tenor_years'].unique().to_list()))
    print('曲线覆盖:', yld['date'].min(), '~', yld['date'].max())

    overlap, max_diff = check_10y(yld)
    print(f'10Y 对拍: 重叠 {overlap} 日，最大绝对差 {max_diff:.6f}pp')
    problems = []
    if overlap == 0:
        problems.append('10Y 与既有表无重叠日，无法对拍')
    if max_diff > 1e-4:
        problems.append(f'10Y 与既有表最大绝对差 {max_diff:.6f}pp 超过 1e-4')
    if yld.group_by(['date', 'tenor_years']).len().filter(pl.col('len') > 1).height:
        problems.append('曲线主键 (date, tenor) 重复')
    if idx.group_by(['date', 'index_name']).len().filter(pl.col('len') > 1).height:
        problems.append('指数主键 (date, index_name) 重复')
    print('校验问题:', problems if problems else '无')
    if problems:
        print('[STOP] 校验未通过，不落盘。')
        return 2
    if not args.apply:
        print('dry-run 完成（加 --apply 落正式路径）')
        return 0

    MACRO.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(idx, MACRO / 'cn_bond_index.parquet')
    write_parquet_atomic(yld, MACRO / 'cn_treasury_yield_curve.parquet')

    now = datetime.now().isoformat(timespec='seconds')
    idx_manifest = {
        'dataset': 'cn_bond_index',
        'semantics': '中债债券总指数（财富指数，含票息再投资），债券原生资产的总收益序列',
        'produced_by': 'tools/build_cn_bond_tables.py',
        'fetched_by': 'tools/fetch_cn_bond_data.py',
        'source': 'chinabond:yield.chinabond.com.cn/cbweb-mn/indices/singleIndexQueryResult',
        'built_at': now,
        'rows': idx.height,
        'indexes': {name: int(idx.filter(pl.col('index_name') == name).height)
                    for name in idx['index_name'].unique().to_list()},
        'coverage': {name: [str(idx.filter(pl.col('index_name') == name)['date'].min()),
                            str(idx.filter(pl.col('index_name') == name)['date'].max())]
                     for name in idx['index_name'].unique().to_list()},
        'rule': 'indicator=财富, period=总值；净价/全价指数不含票息，不入表',
        'path': 'meta/macro/cn_bond_index.parquet',
        'sha256': _sha256(MACRO / 'cn_bond_index.parquet'),
    }
    yld_manifest = {
        'dataset': 'cn_treasury_yield_curve',
        'semantics': '中债国债到期收益率曲线（多期限），与既有 10Y 表同源',
        'produced_by': 'tools/build_cn_bond_tables.py',
        'fetched_by': 'tools/fetch_cn_bond_data.py',
        'source': 'chinabond:yield.chinabond.com.cn/cbweb-pbc-web/pbc/historyQuery',
        'built_at': now,
        'rows': yld.height,
        'tenors': sorted(yld['tenor_years'].unique().to_list()),
        'coverage': [str(yld['date'].min()), str(yld['date'].max())],
        'cross_check_10y': {'overlap_days': overlap, 'max_abs_diff_pp': round(max_diff, 8),
                            'against': 'meta/macro/cn_treasury_yield_10y.parquet'},
        'known_gaps': '接口仅提供 3M/6M/1Y/3Y/5Y/7Y/10Y/30Y；2Y/4Y/15Y/20Y/50Y 不提供（G-040）',
        'path': 'meta/macro/cn_treasury_yield_curve.parquet',
        'sha256': _sha256(MACRO / 'cn_treasury_yield_curve.parquet'),
    }
    from src.data.manifest_semantics import apply_semantics  # noqa: PLC0415
    for name, manifest in (('cn_bond_index', idx_manifest),
                           ('cn_treasury_yield_curve', yld_manifest)):
        apply_semantics(manifest, f'meta/macro/{name}.manifest.json')
        (MACRO / f'{name}.manifest.json').write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    print('落盘完成:', MACRO / 'cn_bond_index.parquet', MACRO / 'cn_treasury_yield_curve.parquet')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
