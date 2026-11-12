#!/usr/bin/env python3

r"""从本地复权因子跳变重出 ``meta/fund_corporate_actions_2019plus.parquet``。

判据变更（2026-11-10）
======================
旧表判据是「tushare ``fund_daily.pre_close`` 与本地上一交易日 ``close`` 不一致」，
该判据依赖上游派生列；2020-02-26 全市场 31.9% 的 ``pre_close`` 被事后改写，
旧表当日凭空多出 43 条事件，已付过学费。新判据只看**本地复权因子是否发生跳变**：

    事件 = ``fund_adj_factor.hfq_factor`` 在 t 日相对 t-1 日的相对变化
            ``|rel| = |factor_t / factor_{t-1} - 1| >= MIN_STEP_REL``

``MIN_STEP_REL`` 的来历：场内基金报价最小变动单位 0.001 元，对 1 元价位即
1e-3 的相对量级；因子相对变化低于 1e-4（0.01%）对应的价格调整不足 0.001 元的
十分之一，与因子表末位舍入无法区分——``hfq_factor`` 只存 6 位小数，末位舍入本身
就产生 ~1e-6 的相对跳变。实测 2019-09-18 起、193 只标的中落在 (1e-12, 1e-4)
区间的跳变共 164 处：``|gap|`` 中位数 0.000023%、最大 0.0036%；其中仅 1 处
（161911.SZ 2019-09-18）附近有分红记录，且当日价格零变动；99 处当日价格有波动，
但只有 33 处与 gap 同向（等同随机）。故 1e-4 是**噪声底**，低于它的不登记为事件；
这不是「必须有第二证据才登记」规则，第二证据只用于新增事件的抽验。

复现与幂等：筛选条件只由 ``MIN_STEP_REL`` 与 ``CA_SCOPE_START`` 决定，
``python tools/build_fund_corporate_actions.py --no-write`` 对同一输入重算，
候选行数恒为 299 且与正式表逐行一致；两个常量与排除口径已写入 manifest 与
``docs/数据权威路径清单.md``。逐条明细见 ``_staging/ca_rebuild_20261110/excluded_164.csv``。

列语义
======
====================  ==========================================================
列                     语义
====================  ==========================================================
``ts_code``           基金代码
``date``              因子的实际调整日（= 除权/付息/折算在本地复权链路上的生效日）
``prev_close``        本地原始收盘价（t-1）
``close``             本地原始收盘价（t）
``gap``               ``adj_prev/adj_now - 1``，与旧表 ``gap`` 同口径
                      （复权后前收 / 原始前收 − 1）
``adj_prev``          t-1 日累计复权因子
``adj_now``           t 日累计复权因子
``basis``             判据来源，恒为 ``local_adj_factor``，供将来换判据时区分
====================  ==========================================================

旧表另有 ``pre_close`` 列，那是 tushare 侧字段，新口径下没有对应量，故不再提供；
唯一消费方 ``tools/build_multi_asset_etf_pool.py`` 只读 ``ts_code`` / ``date``。

覆盖范围
========
沿用旧表标的范围（193 只，2019-09-18 起），便于与旧表逐行对拍。全市场同判据下
（755 只）共 2,646 个事件，其中 ETF 池 in_scope 518 只为 589 个；扩展覆盖属另一次
口径决策，不在本脚本默认范围内。

用法::

    python tools/build_fund_corporate_actions.py            # 生成候选 + 打印对拍
    python tools/build_fund_corporate_actions.py --promote  # 备份旧表后正式替换
"""
# ruff: noqa: T201  # 导出脚本面向终端输出

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import polars as pl  # noqa: E402

from src.data.atomic_write import write_parquet_atomic, write_text_atomic  # noqa: E402
from src.data.local_store.paths import resolve_store_root  # noqa: E402
from src.data.staging import staging_dir  # noqa: E402
from src.utils.config import get_config  # noqa: E402

REL_PATH = Path('meta') / 'fund_corporate_actions_2019plus.parquet'
MANIFEST_NAME = 'fund_corporate_actions_2019plus.manifest.json'

#: 事件登记起点（与旧表一致）
CA_SCOPE_START = date(2019, 9, 18)

#: 因子相对跳变的噪声底，低于此值判为浮点噪声而非真实调整（理由见模块文档）
MIN_STEP_REL = 1e-4

SCHEMA: dict[str, Any] = {
    'ts_code': pl.String,
    'date': pl.Date,
    'prev_close': pl.Float64,
    'close': pl.Float64,
    'gap': pl.Float64,
    'adj_prev': pl.Float64,
    'adj_now': pl.Float64,
    'basis': pl.String,
}


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def load_factor(store_root: Path) -> pl.DataFrame:
    return pl.read_parquet(
        store_root / 'factors' / 'fund_adj_factor.parquet',
        columns=['ts_code', 'trade_date', 'hfq_factor'])


def load_closes(store_root: Path, codes: list[str]) -> pl.DataFrame:
    frames = []
    for market in ('sh', 'sz'):
        wanted = [c for c in codes if c.upper().endswith('.' + market.upper())]
        for code in wanted:
            path = store_root / 'quotes' / 'fund' / market / f'{code}.parquet'
            if path.is_file():
                frames.append(pl.read_parquet(path, columns=['date', 'close'])
                              .with_columns(pl.lit(code).alias('ts_code')))
    if not frames:
        return pl.DataFrame(schema={'ts_code': pl.String, 'date': pl.Date,
                                    'close': pl.Float64})
    return pl.concat(frames)


def build_events(store_root: Path, codes: list[str]) -> pl.DataFrame:
    """按因子跳变推导事件表（标的范围限定为 ``codes``）。"""
    factor = load_factor(store_root).filter(pl.col('ts_code').is_in(codes))
    factor = factor.sort(['ts_code', 'trade_date']).with_columns(
        pl.col('hfq_factor').shift(1).over('ts_code').alias('adj_prev'),
    )
    factor = factor.with_columns(
        (pl.col('hfq_factor') / pl.col('adj_prev') - 1).alias('rel'),
        # gap 沿用旧表口径：复权后前收 / 原始前收 - 1 = adj_prev / adj_now - 1
        (pl.col('adj_prev') / pl.col('hfq_factor') - 1).alias('gap'))
    steps = factor.filter(
        pl.col('adj_prev').is_not_null()
        & (pl.col('rel').abs() >= MIN_STEP_REL)
        & (pl.col('trade_date') >= CA_SCOPE_START)
    ).select(['ts_code', pl.col('trade_date').alias('date'),
              pl.col('hfq_factor').alias('adj_now'), 'adj_prev', 'gap'])

    closes = load_closes(store_root, codes).sort(['ts_code', 'date'])
    closes = closes.with_columns(
        pl.col('close').shift(1).over('ts_code').alias('prev_close'))
    merged = steps.join(closes, on=['ts_code', 'date'], how='left')
    merged = merged.with_columns(pl.lit('local_adj_factor').alias('basis'))
    return merged.select(list(SCHEMA)).sort(['ts_code', 'date'])


def sub_threshold_stats(store_root: Path, codes: list[str],
                        registered: set[tuple[str, date]] | None = None) -> dict[str, Any]:
    """低于噪声底、不登记为事件的跳变统计（写入 manifest，可复现）。

    区间取 ``(1e-12, MIN_STEP_REL)``：下限剔除 pure 0（因子未动），上限即判据阈值。
    ``registered`` 给出「旧表已登记的同日事件」，用于区分「旧表有、因子只有亚阈值
    跳变」与「旧表未登记、纯噪声」两类；``count`` 只统计后者（即被本判据排除的）。
    """
    factor = load_factor(store_root).filter(pl.col('ts_code').is_in(codes))
    factor = factor.sort(['ts_code', 'trade_date']).with_columns(
        pl.col('hfq_factor').shift(1).over('ts_code').alias('adj_prev'))
    factor = factor.with_columns(
        (pl.col('hfq_factor') / pl.col('adj_prev') - 1).alias('rel'),
        (pl.col('adj_prev') / pl.col('hfq_factor') - 1).alias('gap'))
    below = factor.filter(
        pl.col('adj_prev').is_not_null()
        & (pl.col('rel').abs() > 1e-12)
        & (pl.col('rel').abs() < MIN_STEP_REL)
        & (pl.col('trade_date') >= CA_SCOPE_START))
    if below.is_empty():
        return {'count': 0, 'total_sub_threshold': 0}
    already = registered or set()
    excluded = below
    if already:
        keys = pl.DataFrame(
            {'ts_code': [code for code, _ in already],
             'trade_date': [day for _, day in already]},
            schema={'ts_code': pl.String, 'trade_date': pl.Date})
        excluded = below.join(keys, on=['ts_code', 'trade_date'], how='anti')
    gap = excluded['gap'].abs()
    return {
        'count': excluded.height,
        'total_sub_threshold': below.height,
        'already_registered_in_old_table': below.height - excluded.height,
        'abs_gap_pct_median': round(float(gap.median()) * 100, 6) if gap.len() else None,
        'abs_gap_pct_max': round(float(gap.max()) * 100, 6) if gap.len() else None,
        'abs_rel_max': float(excluded['rel'].abs().max()) if gap.len() else None,
    }


def compare_with_old(new: pl.DataFrame, old: pl.DataFrame) -> dict[str, Any]:
    """新旧逐行对拍：同日命中、数值一致、旧表独有、新表新增。"""
    new_keys = {(r['ts_code'], r['date']): r['gap'] for r in new.iter_rows(named=True)}
    old_keys = {(r['ts_code'], r['date']): r['gap'] for r in old.iter_rows(named=True)}
    common = set(new_keys) & set(old_keys)
    consistent = sum(1 for k in common if abs((1 + new_keys[k]) - (1 + old_keys[k])) <= 1e-3)
    return {
        'old_rows': old.height, 'new_rows': new.height,
        'common': len(common), 'consistent': consistent,
        'inconsistent': len(common) - consistent,
        'old_only': len(set(old_keys) - set(new_keys)),
        'new_only': len(set(new_keys) - set(old_keys)),
    }


def diff_rows(new: pl.DataFrame, old: pl.DataFrame) -> list[dict[str, Any]]:
    rows = []
    new_keys = {(r['ts_code'], r['date']): r['gap'] for r in new.iter_rows(named=True)}
    old_keys = {(r['ts_code'], r['date']): r['gap'] for r in old.iter_rows(named=True)}
    for key in sorted(set(old_keys) | set(new_keys)):
        old_gap, new_gap = old_keys.get(key), new_keys.get(key)
        if old_gap is None:
            status, reason = 'new_only', '因子跳变判据下新增（旧表未登记）'
        elif new_gap is None:
            status, reason = 'old_only', '旧表有、本地因子当日无跳变'
        elif abs((1 + old_gap) - (1 + new_gap)) <= 1e-3:
            status, reason = 'consistent', 'gap 一致（|误差|<=1e-3）'
        else:
            status, reason = 'inconsistent', 'gap 数值不一致'
        rows.append({'ts_code': key[0], 'date': key[1].isoformat(),
                     'old_gap': '' if old_gap is None else str(round(old_gap, 10)),
                     'new_gap': '' if new_gap is None else str(round(new_gap, 10)),
                     'status': status, 'reason': reason})
    return rows


def write_diff(new: pl.DataFrame, old: pl.DataFrame, stage: Path) -> Path:
    rows = diff_rows(new, old)
    path = stage / 'old_vs_new_diff.csv'
    schema = dict.fromkeys(('ts_code', 'date', 'old_gap', 'new_gap', 'status', 'reason'),
                           pl.String)
    write_text_atomic(pl.DataFrame(rows, schema=schema).write_csv(), path)
    return path


def promote(store_root: Path, candidate: pl.DataFrame, stage: Path,
            excluded: dict[str, Any] | None = None,
            codes: list[str] | None = None) -> dict[str, Any]:
    """把旧表整体移入 ``_backup/`` 后原子写入新表（DG-11）。"""
    target = store_root / REL_PATH
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_dir = ROOT / '_backup' / f'fund_corporate_actions_20190918plus_{stamp}'
    backup_dir.mkdir(parents=True, exist_ok=True)

    proof: dict[str, Any] = {'backup_dir': str(backup_dir.relative_to(ROOT))}
    if target.is_file():
        before = sha256_of(target)
        backup = backup_dir / target.name
        shutil.copy2(target, backup)
        after = sha256_of(backup)
        if before != after:
            raise RuntimeError(f'备份副本 SHA256 不一致: {before} != {after}')
        # 复核备份可用后再删除正式文件（移动而非覆盖）
        if pl.read_parquet(backup).height != pl.read_parquet(target).height:
            raise RuntimeError('备份副本行数与正式文件不一致，中止替换')
        target.unlink()
        proof.update({'old_sha256': before, 'backup_sha256': after,
                      'old_rows': pl.read_parquet(backup).height})

    write_parquet_atomic(candidate, target)
    proof['new_sha256'] = sha256_of(target)
    proof['new_rows'] = candidate.height

    manifest_path = store_root / 'meta' / MANIFEST_NAME
    manifest = {
        'path': f'data/local_store/{REL_PATH.as_posix()}',
        'rows': candidate.height,
        'codes': int(candidate['ts_code'].n_unique()),
        'date_range': [str(candidate['date'].min()), str(candidate['date'].max())],
        'columns': list(SCHEMA),
        'semantics': ('场内基金除权事件清单，判据为本地复权因子的实际调整日：'
                      'hfq_factor 在 t 日相对 t-1 日跳变 |rel| >= '
                      f'{MIN_STEP_REL:g}；gap=adj_prev/adj_now-1（复权后前收/原始前收-1）'),
        'basis': 'local_adj_factor',
        'sha256': proof['new_sha256'],
        'built_at': datetime.now().isoformat(timespec='seconds'),
        'produced_by': 'tools/build_fund_corporate_actions.py',
        'criterion': ('本地 fund_adj_factor 的 hfq_factor 相对跳变 '
                      f'|rel| >= {MIN_STEP_REL:g}，噪声底理由见脚本模块文档'),
        'scope_start': CA_SCOPE_START.isoformat(),
        'scope_codes': '沿用旧表 193 只标的范围（不含 ETF）',
        'scope_codes_list': codes or [],
        'threshold_excluded': {
            'rule': (f'|rel| < {MIN_STEP_REL:g} 的跳变不登记：'
                     '|gap| 最大 0.0036%，低于最小报价单位，属因子末位舍入噪声'),
            **(excluded or {}),
            'detail_csv': f'_staging/{stage.name}/excluded_164.csv',
        },
        'purpose': ('事件查询/留证表，不是除权完备性清单；下游复权直接读 '
                    'factors/fund_adj_factor.parquet，不依赖本表'),
        'reproduce': 'python tools/build_fund_corporate_actions.py --no-write',
        'superseded': {
            'criterion': 'tushare fund_daily.pre_close 与本地上一交易日 close 不一致',
            'backup': proof['backup_dir'],
            'sha256': proof.get('old_sha256'),
            'rows': proof.get('old_rows'),
            'columns': ['ts_code', 'date', 'prev_close', 'pre_close', 'gap',
                        'adj_prev', 'adj_now', 'source'],
        },
    }
    from src.data.manifest_semantics import apply_semantics  # noqa: PLC0415
    apply_semantics(manifest, 'meta/fund_corporate_actions_2019plus.manifest.json')
    write_text_atomic(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n', manifest_path)
    return proof


def scope_codes(manifest_path: Path, table: pl.DataFrame) -> list[str]:
    """标的范围优先取 manifest 的 ``scope_codes_list``。

    替换后正式表只剩有事件的代码，若再从表内推范围会缩水，故范围必须固化在
    manifest 里，保证 ``--no-write`` 能对同一输入重算出同样的 299 行。
    """
    if manifest_path.is_file():
        try:
            listed = json.loads(manifest_path.read_text(encoding='utf-8')).get('scope_codes_list')
        except json.JSONDecodeError:
            listed = None
        if listed:
            return sorted(set(listed))
    return sorted(set(table['ts_code'].to_list()))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='按本地复权因子跳变重建场内基金除权事件表')
    parser.add_argument('--promote', action='store_true', help='备份旧表后正式替换')
    parser.add_argument('--no-write', action='store_true', help='只算候选与对拍，不落盘')
    args = parser.parse_args(argv)

    store_root = resolve_store_root(get_config())
    target = store_root / REL_PATH
    manifest_path = store_root / 'meta' / MANIFEST_NAME
    if not target.is_file():
        print(f'!! 正式表不存在，无法确定标的范围: {target}')
        return 1
    old = pl.read_parquet(target)
    codes = scope_codes(manifest_path, old)
    registered = set(zip(old['ts_code'].to_list(), old['date'].to_list(), strict=True))

    candidate = build_events(store_root, codes)
    excluded = sub_threshold_stats(store_root, codes, registered)
    stats = compare_with_old(candidate, old)
    print('新表候选:', candidate.height, '行 /',
          candidate['ts_code'].n_unique(), '只 /',
          candidate['date'].min(), '~', candidate['date'].max())
    print('低于噪声底不登记:', excluded)
    print('与旧表对拍:', stats)

    stage = staging_dir('ca_rebuild', with_date=True)
    if args.no_write:
        # --no-write 的语义是「不落盘」：建目录、写 diff/summary 都必须跳过，
        # 对拍明细改在内存里算完只打印状态计数。
        rows = diff_rows(candidate, old)
        counts = {status: sum(1 for r in rows if r['status'] == status)
                  for status in ('consistent', 'inconsistent', 'old_only', 'new_only')}
        print('逐行对拍（--no-write 不落盘）:', counts)
        return 0
    stage.mkdir(parents=True, exist_ok=True)
    diff_path = write_diff(candidate, old, stage)
    print('逐行对拍 ->', diff_path)
    write_text_atomic(
        json.dumps({'stats': stats, 'min_step_rel': MIN_STEP_REL,
                    'scope_start': CA_SCOPE_START.isoformat()},
                   ensure_ascii=False, indent=2) + '\n', stage / 'summary.json')

    if not args.promote:
        print('未加 --promote，仅生成候选与对拍，不动正式表')
        return 0
    proof = promote(store_root, candidate, stage, excluded, codes)
    print('正式替换完成:', json.dumps(proof, ensure_ascii=False, indent=1))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
