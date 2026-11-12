#!/usr/bin/env python3

r"""构建「中债中短期票据收益率曲线(AAA)」正式表与 manifest（G-056 常规入口）。

原一次性脚本 ``_staging/gap_close_20261111/build_credit_curve.py`` 的计算口径
原样收拢到本文件，不再单独维护：staging 年报分片 → 长表 → 同接口国债对拍 →
原子落 ``meta/macro/cn_credit_yield_curve.parquet``。

产物
====
- ``data/local_store/meta/macro/cn_credit_yield_curve.parquet``
  （``date, tenor_years, tenor, ytm_pct, curve, source``）
- 同目录 ``cn_credit_yield_curve.manifest.json``

口径
====
- 只收同批响应里 ``曲线名称 = 中债中短期票据收益率曲线(AAA)`` 一段；同批返回的
  中债国债曲线不落本表，只用于对拍——与既有 ``cn_treasury_yield_curve.parquet``
  逐值一致（差异 > 1e-4 即不落盘），证明同一批响应、同一口径。
- 接口固定返回 3M/6M/1Y/3Y/5Y/7Y/10Y/30Y 八列，源端为空的期限在长表里直接缺行，
  不做插值、不用相邻期限代填。
- 断点：staging 年报分片存在且非空即跳过；增量补数写独立分片
  ``curve_supp_<起>_<止>.parquet``，重跑同区间直接复用，不清空历史分片。
- 增量重叠日必须逐值一致，不一致即停手报错（源端修订不得静默覆盖已入库值）。

用法::

    .venv\\Scripts\\python.exe tools/build_credit_yield_curve.py --fetch
    .venv\\Scripts\\python.exe tools/build_credit_yield_curve.py --check   # 只校验
    .venv\\Scripts\\python.exe tools/build_credit_yield_curve.py           # 校验并落盘

"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

from src.data.atomic_write import write_parquet_atomic, write_text_atomic
from src.data.manifest_semantics import apply_semantics

ROOT = Path(__file__).resolve().parents[1]
#: G-056 信用曲线任务的 staging（年报分片 + 增量分片 + progress.json）
STAGE = ROOT / '_staging' / 'gap_close_20261111' / 'cbond'
MACRO = ROOT / 'data' / 'local_store' / 'meta' / 'macro'

TENORS = {'3月': 0.25, '6月': 0.5, '1年': 1.0, '3年': 3.0, '5年': 5.0,
          '7年': 7.0, '10年': 10.0, '30年': 30.0}
CREDIT = '中债中短期票据收益率曲线(AAA)'
TREASURY = '中债国债收益率曲线'
SOURCE = 'chinabond:bond_china_yield'

THROTTLE = 1.3
RATE_LIMIT_HINTS = ('频率', '超过', 'limit', 'too many', 'forbidden')


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def _melt(df: pl.DataFrame, curve: str) -> pl.DataFrame:
    frames = []
    for raw, years in TENORS.items():
        if raw not in df.columns:
            continue
        frames.append(df.select([
            pl.col('日期').alias('date'),
            pl.lit(years).alias('tenor_years'),
            pl.lit(raw).alias('tenor'),
            pl.col(raw).cast(pl.Float64).alias('ytm_pct'),
        ]))
    return pl.concat(frames).with_columns(
        pl.lit(curve).alias('curve'), pl.lit(SOURCE).alias('source')
    ).filter(pl.col('date').is_not_null() & pl.col('ytm_pct').is_not_null())


def _staging_max_date() -> date | None:
    dates = []
    for path in sorted(STAGE.glob('curve_*.parquet')):
        if path.stat().st_size == 0:
            continue
        col = pl.read_parquet(path, columns=['日期'])['日期'].cast(pl.Utf8).str.strptime(
            pl.Date, '%Y-%m-%d', strict=False)
        last = col.max()
        if isinstance(last, date):
            dates.append(last)
    if not dates:
        return None
    return max(dates)


def fetch_supplement(since: date, until: date, throttle: float) -> Path:
    """增量采集 ``[since, until]`` 到独立分片；重叠日不一致即停手。"""
    import akshare as ak  # 延迟导入：只做构建时不必拉起网络栈
    import pandas as pd

    target = STAGE / f'curve_supp_{since:%Y%m%d}_{until:%Y%m%d}.parquet'
    if target.exists() and target.stat().st_size > 0:
        print(f'[SKIP] 增量分片已存在 {target.name}')
        return target

    STAGE.mkdir(parents=True, exist_ok=True)
    time.sleep(throttle)
    try:
        raw = ak.bond_china_yield(f'{since:%Y%m%d}', f'{until:%Y%m%d}')
    except Exception as exc:  # noqa: BLE001
        msg = f'{type(exc).__name__}: {exc}'
        print(f'[ERR] {since}~{until} {msg[:160]}')
        if any(h in msg.lower() for h in RATE_LIMIT_HINTS):
            print('[STOP] 命中限流关键字，停压退出。')
            raise SystemExit(3) from exc
        raise SystemExit(4) from exc
    if raw is None or raw.empty:
        print('[EMPTY] 返回空 —— 先排查时间范围与接口开关，不按「源端无数据」处理')
        raise SystemExit(3)

    raw = raw.copy()
    raw['日期'] = pd.to_datetime(raw['日期']).dt.date.astype(str)
    for col in TENORS:
        if col in raw.columns:
            raw[col] = pd.to_numeric(raw[col], errors='coerce')

    # 重叠日逐值对拍：源端修订不得静默覆盖已入库值
    existing = []
    for path in sorted(STAGE.glob('curve_*.parquet')):
        if path == target or path.stat().st_size == 0:
            continue
        existing.append(pl.read_parquet(path).select(['日期', '曲线名称', *TENORS.keys()]))
    if existing:
        old = pl.concat(existing).with_columns(
            pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, '%Y-%m-%d', strict=False))
        new = pl.from_pandas(raw).with_columns(
            pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, '%Y-%m-%d', strict=False))
        overlap = old.join(new, on=['日期', '曲线名称'], how='inner', suffix='_new')
        changed = 0
        for col in TENORS:
            a, b = f'{col}', f'{col}_new'
            if a in overlap.columns and b in overlap.columns:
                changed += overlap.filter(
                    pl.col(a).is_not_null() & pl.col(b).is_not_null()
                    & ((pl.col(a) - pl.col(b)).abs() > 1e-9)).height
        if overlap.height and changed:
            print(f'[STOP] 重叠 {overlap.height} 行中有 {changed} 个值与 staging 不一致，'
                  '源端修订需人工判定，不覆盖已入库值。')
            raise SystemExit(2)
        print(f'重叠对拍: {overlap.height} 行，值改动 {changed}')

    raw = raw[[c for c in ('日期', '曲线名称', *TENORS) if c in raw.columns]]
    raw = raw.sort_values(['曲线名称', '日期']).reset_index(drop=True)
    raw.to_parquet(target, index=False)
    progress_path = STAGE / 'progress.json'
    progress = (json.loads(progress_path.read_text(encoding='utf-8'))
                if progress_path.exists() else {})
    progress[f'supp:{since}~{until}'] = {
        'rows': int(len(raw)),
        'curves': {str(k): int(v) for k, v in raw['曲线名称'].value_counts().items()},
        'first': str(raw['日期'].min()), 'last': str(raw['日期'].max()),
    }
    progress['updated_at'] = datetime.now().isoformat(timespec='seconds')
    write_text_atomic(json.dumps(progress, ensure_ascii=False, indent=2), progress_path)
    print(f'[OK] 增量分片 {target.name}: {len(raw)} 行 '
          f'{raw["日期"].min()}~{raw["日期"].max()}')
    return target


def build(apply: bool) -> int:
    paths = [p for p in sorted(STAGE.glob('curve_*.parquet')) if p.stat().st_size > 0]
    if not paths:
        raise SystemExit(f'staging 无曲线分片: {STAGE}')
    # 列顺序按 canonical 对齐：历年年报分片与增量分片（akshare 原生列序）不一致
    canon = ('日期', '曲线名称', *TENORS)
    frames = []
    for p in paths:
        df = pl.read_parquet(p)
        lack = [c for c in canon if c not in df.columns]
        if lack:
            df = df.with_columns([pl.lit(None, dtype=pl.Float64).alias(c) for c in lack])
        frames.append(df.select([c for c in canon if c in df.columns]))
    raw = pl.concat(frames).with_columns(
        pl.col('日期').cast(pl.Utf8).str.strptime(pl.Date, '%Y-%m-%d', strict=False))
    dup_raw = raw.group_by(['日期', '曲线名称']).len().filter(pl.col('len') > 1).height

    credit = _melt(raw.filter(pl.col('曲线名称') == CREDIT), CREDIT).sort(
        ['date', 'tenor_years'])
    treasury_new = _melt(raw.filter(pl.col('曲线名称') == TREASURY), TREASURY)

    print('分片:', [p.name for p in paths])
    print('信用债曲线行数:', credit.height, '覆盖:', credit['date'].min(), '~',
          credit['date'].max(), '期限:',
          sorted(credit['tenor_years'].unique().to_list()))
    dup = credit.group_by(['date', 'tenor_years']).len().filter(pl.col('len') > 1).height
    print('主键重复:', dup)

    old = pl.read_parquet(MACRO / 'cn_treasury_yield_curve.parquet').select(
        ['date', 'tenor_years', 'ytm_pct'])
    joined = old.join(treasury_new.select(['date', 'tenor_years', 'ytm_pct']),
                      on=['date', 'tenor_years'], how='inner', suffix='_new')
    diff = (joined['ytm_pct'] - joined['ytm_pct_new']).abs()
    raw_max = diff.max()
    max_diff = float(raw_max) if isinstance(raw_max, (int, float)) else float('nan')
    print('国债曲线同接口对拍: 重叠', joined.height, '最大差', round(max_diff, 10))

    problems = []
    if dup_raw:
        problems.append(f'staging 分片间 (日期, 曲线名称) 重复 {dup_raw} 行')
    if credit.height == 0:
        problems.append('信用债曲线为空')
    if dup:
        problems.append(f'主键重复 {dup}')
    if joined.height < 4000:
        problems.append(f'国债对拍重叠过少 {joined.height}')
    if max_diff > 1e-4:
        problems.append(f'国债曲线与既有表最大差 {max_diff:.6f}')
    by_year = credit.with_columns(pl.col('date').dt.year().alias('y')).group_by('y').agg(
        pl.col('date').n_unique().alias('days'), pl.len().alias('rows')).sort('y')
    print(by_year.tail(5))
    print('校验问题:', problems if problems else '无')
    if problems:
        print('[STOP] 校验未通过，不落盘。')
        return 2
    if not apply:
        print('dry-run 完成（不加 --check 即落正式路径）')
        return 0

    out = MACRO / 'cn_credit_yield_curve.parquet'
    write_parquet_atomic(credit, out)
    manifest = {
        'dataset': 'cn_credit_yield_curve',
        'semantics': '中债中短期票据到期收益率曲线(AAA)，多期限；信用债无风险基准之上的 AAA 级信用利差来源',
        'produced_by': 'tools/build_credit_yield_curve.py',
        'fetched_by': 'tools/build_credit_yield_curve.py --fetch（原 collect_credit_curve.py 收拢）',
        'source': 'chinabond:yield.chinabond.com.cn/cbweb-pbc-web/pbc/historyQuery（akshare bond_china_yield）',
        'built_at': datetime.now().isoformat(timespec='seconds'),
        'path': 'meta/macro/cn_credit_yield_curve.parquet',
        'rows': credit.height,
        'bytes': out.stat().st_size,
        'tenors': sorted(credit['tenor_years'].unique().to_list()),
        'coverage': [str(credit['date'].min()), str(credit['date'].max())],
        'days_by_year': {str(r['y']): r['days'] for r in by_year.iter_rows(named=True)},
        'cross_check_treasury': {
            'overlap_rows': joined.height, 'max_abs_diff_pp': round(max_diff, 10),
            'against': 'meta/macro/cn_treasury_yield_curve.parquet',
            'note': '同一次接口调用返回两条曲线，国债曲线逐值一致即证明同一批响应、同一口径',
        },
        'known_gaps': '接口仅返回中短票AAA；AA/AA+/企业债需 chinamoney 收盘曲线接口（bond_china_close_return，月度分页、约 720 次请求），本次未采',
        'sha256': _sha256(out),
    }
    apply_semantics(manifest, 'meta/macro/cn_credit_yield_curve.manifest.json')
    write_text_atomic(json.dumps(manifest, ensure_ascii=False, indent=2),
                      MACRO / 'cn_credit_yield_curve.manifest.json')
    print('落盘:', out, 'sha256:', manifest['sha256'])
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='构建中债中短期票据收益率曲线(AAA)正式表')
    parser.add_argument('--fetch', action='store_true', help='先增量采集 staging 缺口')
    parser.add_argument('--since', type=date.fromisoformat,
                        help='增量采集起点（默认 staging 现有末日 + 1 天）')
    parser.add_argument('--until', type=date.fromisoformat,
                        help='增量采集终点（默认今天）')
    parser.add_argument('--throttle', type=float, default=THROTTLE)
    parser.add_argument('--check', action='store_true', help='只校验，不落盘')
    args = parser.parse_args(argv)

    if args.fetch:
        last = _staging_max_date()
        since = args.since or ((last + timedelta(days=1)) if last else date(2006, 1, 1))
        until = args.until or date.today()
        if since > until:
            print(f'[SKIP] staging 已覆盖到 {last}，无需补采')
        else:
            fetch_supplement(since, until, args.throttle)
    return build(apply=not args.check)


if __name__ == '__main__':
    raise SystemExit(main())
