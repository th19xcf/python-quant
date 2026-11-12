#!/usr/bin/env python3

r"""采集中债债券指数与国债收益率曲线到 staging（akshare，单连接串行 + 节流）。

产物（staging）
===============
- ``<staging>/cbond/index_<类别>.parquet``：``date, value``
- ``<staging>/cbond/yield_curve_<年>.parquet``：当年逐日的多期限曲线长表
- ``<staging>/cbond/progress.json``：逐项完成记录与耗时

口径
====
- 指数一律取**财富指数**（含票息再投资），期限段取「总值」；净价/全价指数不含票息，
  当作资产收益会系统性低估，故不落盘。
- 收益率曲线接口 ``bond_china_yield`` 单次跨度须 < 1 年，按自然年分页；
  接口固定返回两条曲线（中债国债、中债中短期票据 AAA），只保留国债曲线，
  另一条登记为「接口可取但本次不落盘」（见 docs/数据缺口登记.md G-042）。
- 断点：按年文件存在且非空即跳过，重跑只补缺失年份。
- 增量：``--since YYYY-MM-DD`` 对已存在的年文件/指数文件重取后**只追加末日之后的新行**，
  重叠日必须逐值一致（源端修订不得静默覆盖已入库值），一致后才落盘。

用法::

    .venv\\Scripts\\python.exe tools/fetch_cn_bond_data.py
    .venv\\Scripts\\python.exe tools/fetch_cn_bond_data.py --skip-yield
    .venv\\Scripts\\python.exe tools/fetch_cn_bond_data.py --since 2026-11-11
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path

import akshare as ak
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
STAGING = ROOT / '_staging' / 'multi_asset_tier2_20261110' / 'cbond'

#: 三个债券指数：国债总指数 = 全期限国债，固定利率国债 = 剔除浮息，
#: 国开行债券总指数 = 政金债代表（用户点名的「政金债」）
INDEX_CATEGORIES = ('国债总指数', '固定利率国债指数', '国开行债券总指数')

#: 收益率曲线年份范围：本地 10Y 起点 2005-01-04，但中债渠道 2006-03-01 才有数据
YIELD_YEARS = range(2006, 2027)

#: 接口返回的八个期限列（2Y/4Y/15Y/20Y/50Y 该接口不提供）
TENOR_COLS = ('3月', '6月', '1年', '3年', '5年', '7年', '10年', '30年')

THROTTLE = 1.3
RATE_LIMIT_HINTS = ('频率', '超过', 'limit', 'too many', 'forbidden')


def _sleep(t: float = THROTTLE) -> None:
    time.sleep(t)


def _append_new(existing: pd.DataFrame, fetched: pd.DataFrame, date_col: str,
                value_cols: tuple[str, ...], label: str) -> tuple[pd.DataFrame, int]:
    """只追加 ``fetched`` 中晚于本地末日的行；重叠日值不一致即停手。

    返回 (合并后表, 追加行数)。追加 0 行时原样返回 ``existing``。
    """
    last = existing[date_col].max()
    overlap = fetched[fetched[date_col] <= last]
    if len(overlap):
        left = existing.set_index(date_col)
        right = overlap.set_index(date_col)
        common = left.index.intersection(right.index)
        changed = 0
        for col in value_cols:
            a, b = left.loc[common, col], right.loc[common, col]
            changed += int((~((a == b) | (a.isna() & b.isna()))).sum())
        if changed:
            raise SystemExit(
                f'[STOP] {label}: 重叠 {len(common)} 日中有 {changed} 个值发生变化，'
                '源端修订需人工判定后另行处理，不静默覆盖已入库值。')
        print(f'{label} 重叠对拍: {len(common)} 日，值改动 0')
    new = fetched[fetched[date_col] > last]
    if new.empty:
        print(f'[NO-NEW] {label}: 源端最新 {fetched[date_col].max()} 未超过本地 {last}')
        return existing, 0
    merged = pd.concat([existing, new]).sort_values(date_col).reset_index(drop=True)
    return merged, len(new)


def fetch_indices(progress: dict[str, object], since: date | None = None) -> None:
    for cat in INDEX_CATEGORIES:
        target = STAGING / f'index_{cat}.parquet'
        existing = None
        if target.exists() and target.stat().st_size > 0:
            if since is None:
                print(f'[SKIP] {cat}')
                continue
            existing = pd.read_parquet(target)
        _sleep()
        df = ak.bond_index_general_cbond(cat, '财富', '总值')
        if df is None or df.empty:
            print(f'[EMPTY] {cat} —— 先排查接口参数，不按「源端无数据」处理')
            sys.exit(3)
        df = df.rename(columns={'date': 'date', 'value': 'value'})[['date', 'value']]
        df['date'] = pd.to_datetime(df['date']).dt.date.astype(str)
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df = df.dropna().sort_values('date').reset_index(drop=True)
        added = len(df)
        if existing is not None:
            df, added = _append_new(existing, df, 'date', ('value',), f'index:{cat}')
            if added == 0:
                progress[f'index:{cat}'] = {'rows': len(df), 'last': df['date'].iloc[-1],
                                            'added': 0}
                continue
        df.to_parquet(target, index=False)
        progress[f'index:{cat}'] = {'rows': len(df), 'first': df['date'].iloc[0],
                                    'last': df['date'].iloc[-1], 'added': added}
        print(f'[OK] {cat}: {len(df)} 行 {df["date"].iloc[0]}~{df["date"].iloc[-1]}'
              f'（本次追加 {added}）')


def fetch_yield_curve(progress: dict[str, object], since: date | None = None) -> None:
    for year in YIELD_YEARS:
        target = STAGING / f'yield_curve_{year}.parquet'
        existing = None
        if target.exists() and target.stat().st_size > 0:
            if since is None or year < since.year:
                continue
            existing = pd.read_parquet(target)
        _sleep()
        start, end = f'{year}0101', f'{year}1231'
        try:
            df = ak.bond_china_yield(start, end)
        except Exception as exc:  # noqa: BLE001
            msg = f'{type(exc).__name__}: {exc}'
            print(f'[ERR] {year} {msg[:120]}')
            if any(h in msg.lower() for h in RATE_LIMIT_HINTS):
                print('[STOP] 命中限流关键字，停压退出。')
                sys.exit(3)
            progress[f'yield:{year}'] = {'error': msg}
            continue
        if df is None or df.empty:
            # 2006 年以前接口确实没有；此处记录「空」并继续，由构建环节按年份核对
            progress[f'yield:{year}'] = {'rows': 0}
            print(f'[EMPTY] {year}')
            continue
        df = df[df['曲线名称'].astype(str).str.contains('国债')].copy()
        if df.empty:
            progress[f'yield:{year}'] = {'rows': 0, 'note': 'no treasury curve'}
            print(f'[EMPTY-TREASURY] {year}')
            continue
        df['日期'] = pd.to_datetime(df['日期']).dt.date.astype(str)
        for col in TENOR_COLS:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.drop(columns=['曲线名称']).sort_values('日期').reset_index(drop=True)
        added = len(df)
        if existing is not None:
            df, added = _append_new(existing, df, '日期', TENOR_COLS, f'yield:{year}')
            if added == 0:
                progress[f'yield:{year}'] = {'rows': len(df), 'last': df['日期'].iloc[-1],
                                             'added': 0}
                continue
        df.to_parquet(target, index=False)
        progress[f'yield:{year}'] = {'rows': len(df), 'first': df['日期'].iloc[0],
                                     'last': df['日期'].iloc[-1], 'added': added}
        print(f'[OK] {year}: {len(df)} 行（本次追加 {added}）')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='采集中债指数与收益率曲线到 staging')
    parser.add_argument('--skip-index', action='store_true')
    parser.add_argument('--skip-yield', action='store_true')
    parser.add_argument('--throttle', type=float, default=THROTTLE)
    parser.add_argument('--since', type=date.fromisoformat,
                        help='增量模式：只对已存在的年文件/指数文件追加该日期之后的新行')
    args = parser.parse_args(argv)

    STAGING.mkdir(parents=True, exist_ok=True)
    progress_path = STAGING / 'progress.json'
    progress: dict[str, object] = {}
    if progress_path.exists():
        progress = json.loads(progress_path.read_text(encoding='utf-8'))
    progress['updated_at'] = datetime.now().isoformat(timespec='seconds')

    if not args.skip_index:
        fetch_indices(progress, args.since)
    if not args.skip_yield:
        fetch_yield_curve(progress, args.since)

    progress_path.write_text(json.dumps(progress, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'written {progress_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
