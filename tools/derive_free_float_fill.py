#!/usr/bin/env python3

r"""自由流通口径派生列：``free_float_share_filled`` / ``free_float_source``。

背景：``meta/factors/stock_shares.parquet`` 的 ``free_float_share`` 有 17,697 行
为空（行级 10.6%），但其中 16,404 行是「源端当年就没有」，只有 1,293 行是
「有过值、后来某条记录漏披露」。直接 ffill 会把早已失效的股本沿用至今，
直接删除又会丢掉近一年内正常换手的样本。

口径（用户 2026-11-10 裁定，②+①；2026-11-13 补截断）：

* ``raw``        —— 该记录 ``free_float_share`` 非空，直接取原值；
* ``ffill_250d`` —— 该记录为空，且上一非空记录距该记录 ``effective_date``
  不超过 250 个交易日，沿用上一非空值；
* ``excluded``   —— 无上一非空记录，或超过 250 个交易日，下游按剔除处理。

继承值必须再按语义上界收敛：自由流通股本是流通股本的子集，因此
``free_float_share_filled = min(继承值, 当期 float_share)``。不加这一步时
549 行 ffill 继承会越过上界（最大 +857%，详见 ``b1_slice7_fix.md`` §4）；
截断只动派生列，原 ``free_float_share`` 一个字节不动。

原 8 列一个字节不动，只在末尾追加两列。窗口取 250 的理由见
``stock_shares.meta.json`` 的 ``free_float_fill.window_rationale``。

本脚本是这两列的唯一产出方；**股本表重建（``assemble_shares``）后必须重跑**，
否则新表会退回 8 列。写入走 ``write_parquet_atomic``，改前 ``copy2`` 备份并
记录新旧 SHA256。

用法::

    python tools/derive_free_float_fill.py            # 派生并落盘
    python tools/derive_free_float_fill.py --dry-run  # 只打统计，不写

``check()`` 校验 ``free_float_share_filled <= float_share``，额度固定为 0：
截断已在 :func:`derive` 内完成，任何越界都说明派生逻辑被改坏，直接失败。
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import shutil
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.atomic_write import write_parquet_atomic, write_text_atomic  # noqa: E402
from src.data.manifest_refresh import refresh_manifest_files  # noqa: E402

WINDOW = 250
BASE_COLUMNS: tuple[str, ...] = (
    'ts_code', 'effective_date', 'total_share', 'float_share',
    'free_float_share', 'change_reason', 'source', 'fetched_at',
)
DERIVED_COLUMNS: tuple[str, ...] = ('free_float_share_filled', 'free_float_source')


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _store_root(root: Path | None) -> Path:
    if root is not None:
        return root
    from src.data.local_store.paths import default_store_root
    return default_store_root()


def derive(frame: pl.DataFrame, calendar: list[date], window: int = WINDOW, *,
           clip: bool = True) -> pl.DataFrame:
    """在 8 列事实表末尾追加两列派生列（不改动原列）。

    ``clip=True``（默认）把继承值收敛到 ``min(继承值, float_share)``；
    ``clip=False`` 只做原始 ffill，供 ``fill_free_float_shares`` 统计截断行数。
    """
    missing = [c for c in BASE_COLUMNS if c not in frame.columns]
    if missing:
        raise ValueError(f'股本表缺少基础列: {missing}')

    pos: dict[date, int | None] = {}
    for value in frame['effective_date'].unique().to_list():
        idx = bisect.bisect_right(calendar, value) - 1
        pos[value] = idx if idx >= 0 else None

    ordered = (
        frame.with_row_index('_rid')
        .sort(['ts_code', 'effective_date'])
        .with_columns(
            pl.col('effective_date')
            .replace_strict(pos, default=None, return_dtype=pl.Int64)
            .alias('_epos')
        )
        .with_columns([
            pl.when(pl.col('free_float_share').is_not_null())
            .then(pl.col('_epos')).otherwise(None).alias('_ok_epos'),
            pl.when(pl.col('free_float_share').is_not_null())
            .then(pl.col('free_float_share')).otherwise(None).alias('_ok_val'),
        ])
        .with_columns([
            pl.col('_ok_epos').forward_fill().over('ts_code').alias('_prev_epos'),
            pl.col('_ok_val').forward_fill().over('ts_code').alias('_prev_val'),
        ])
        .with_columns((pl.col('_epos') - pl.col('_prev_epos')).alias('_gap'))
    )

    usable = (
        pl.col('_prev_val').is_not_null()
        & pl.col('_gap').is_not_null()
        & (pl.col('_gap') <= window)
    )
    ordered = ordered.with_columns([
        pl.when(pl.col('free_float_share').is_not_null())
        .then(pl.lit('raw'))
        .when(usable).then(pl.lit(f'ffill_{window}d'))
        .otherwise(pl.lit('excluded'))
        .alias('free_float_source'),
        pl.when(pl.col('free_float_share').is_not_null())
        .then(pl.col('free_float_share'))
        .when(usable).then(pl.col('_prev_val'))
        .otherwise(None)
        .alias('free_float_share_filled'),
    ])

    out = ordered.sort('_rid').select(
        list(BASE_COLUMNS) + list(DERIVED_COLUMNS))
    if clip:
        out, _ = clip_filled_to_float(out)
    return out


def clip_filled_to_float(derived: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """把派生列 ``free_float_share_filled`` 越界的值截断到当期 ``float_share``。

    未命中行显式写回原值（``otherwise(原值)`` 兜底，DG-31）；原列
    ``free_float_share`` 不参与，``free_float_source`` 三态也不变。
    """
    over = (
        pl.col('free_float_share_filled').is_not_null()
        & pl.col('float_share').is_not_null()
        & (pl.col('free_float_share_filled') > pl.col('float_share'))
    )
    hits = derived.filter(over).height
    if hits == 0:
        return derived, 0
    clipped = derived.with_columns(
        pl.when(over).then(pl.col('float_share'))
        .otherwise(pl.col('free_float_share_filled'))
        .alias('free_float_share_filled'))
    return clipped, hits


def filled_gt_float_rows(out: pl.DataFrame) -> pl.DataFrame:
    """``free_float_share_filled`` 非空且严格大于同行 ``float_share`` 的行。

    语义上界：自由流通股本不可能超过流通股本。原值层（``free_float_share``）
    在采集时已被 ``<= float_share`` 规则约束，但 ffill 继承会让「上一报告期
    的自由流通股本」落到「本期流通股本已下降」的行上，因此必须单独校验。
    """
    return out.filter(
        pl.col('free_float_share_filled').is_not_null()
        & pl.col('float_share').is_not_null()
        & (pl.col('free_float_share_filled') > pl.col('float_share')))


def check(out: pl.DataFrame, source: pl.DataFrame) -> None:
    """行数守恒 / 原列逐值不变 / 三态与取值互斥 / filled 不越 float 上界。

    上界额度固定为 0：截断已在 :func:`derive` 内完成，任何越界都说明派生
    逻辑被改坏，直接失败（DG-26 不放宽断言）。
    """
    if out.height != source.height:
        raise AssertionError(f'行数变化 {out.height} != {source.height}')
    if not out.select(list(BASE_COLUMNS)).equals(source.select(list(BASE_COLUMNS))):
        raise AssertionError('原 8 列被改动')
    if out['free_float_share'].null_count() != source['free_float_share'].null_count():
        raise AssertionError('free_float_share 空值数变化')
    if out['free_float_source'].null_count() != 0:
        raise AssertionError('free_float_source 出现空值')
    excluded = out['free_float_source'] == 'excluded'
    filled_null = out['free_float_share_filled'].is_null()
    if bool((excluded != filled_null).any()):
        raise AssertionError('三态与填充值不互斥：excluded 必须恰好对应 filled 为空')
    if out['free_float_source'].value_counts()['count'].sum() != source.height:
        raise AssertionError('三态计数之和 != 行数')
    violations = filled_gt_float_rows(out)
    if violations.height > 0:
        worst = violations.sort(
            (pl.col('free_float_share_filled') / pl.col('float_share')),
            descending=True).head(3).select(
            ['ts_code', 'effective_date', 'free_float_share_filled', 'float_share'])
        raise AssertionError(
            f'free_float_share_filled > float_share 违例 {violations.height} 行'
            f'（允许额度 0）；样例 {worst.to_dicts()}')


def _meta_payload(meta: dict, stats: dict, backup: Path, sha_before: str,
                  sha_after: str) -> dict:
    meta = dict(meta)
    meta['built_at'] = datetime.now().isoformat(timespec='seconds')
    meta['rows'] = stats['rows']
    cols = dict(meta.get('columns', {}))
    cols['free_float_share_filled'] = (
        f'自由流通股本填充值（股）；仅在 free_float_source=raw/ffill_{WINDOW}d 时非空，'
        'excluded 时为 NULL。继承值已按语义上界截断到当期 float_share'
        '（自由流通 ⊆ 流通）；原 free_float_share 不受影响')
    cols['free_float_source'] = (
        f'自由流通口径归属：raw=源端直接披露；ffill_{WINDOW}d=该记录为空且上一非空记录在 '
        f'{WINDOW} 个交易日内，沿用上一非空值；excluded=无上一非空值或超过 {WINDOW} 交易日，'
        '下游按剔除处理')
    meta['columns'] = cols
    meta['free_float_fill'] = {
        'window_trading_days': WINDOW,
        'rule': (f'仅空记录且上一非空记录距该记录 effective_date <= {WINDOW} 个交易日时填充；'
                 '超窗/无前值落 excluded；继承值超过当期 float_share 时截断到 float_share'),
        'window_rationale': (
            '空值行 gap 分位 p50=74 / p75=133 / p90=291 / p99=1332 个交易日（max 3015）：'
            '250 落在 p75~p90 之间，可覆盖约 88% 的可填缺口'
            '（1,293 行空值有上一非空值，其中 1,145 行 <=250 天被填充）；'
            '120（约半年）只覆盖到 p75 以下，会漏掉近三成可填缺口；'
            '500（约两年）已远超 p90，会把早已失效的股本沿用过久、口径漂移不可控'),
        'counts': stats,
        'producer': 'tools/derive_free_float_fill.py',
        'regenerate_after': 'assemble_shares / write_shares 重建股本表后必须重跑本脚本',
        'backup_file': str(backup),
        'backup_sha256': sha_before,
        'output_sha256': sha_after,
    }
    return meta


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='派生自由流通口径两列')
    parser.add_argument('--root', default=None, help='local_store 根目录（默认按项目定位）')
    parser.add_argument('--dry-run', action='store_true', help='只打统计，不写盘')
    args = parser.parse_args(argv)

    root = _store_root(Path(args.root) if args.root else None)
    src = root / 'meta' / 'factors' / 'stock_shares.parquet'
    meta_path = root / 'meta' / 'factors' / 'stock_shares.meta.json'
    cal_path = root / 'quotes' / 'index' / 'sh' / '000001.SH.parquet'
    if not src.is_file():
        print(f'股本表不存在: {src}')
        return 2
    if not cal_path.is_file():
        print(f'交易日历不存在（需要 000001.SH 日线）: {cal_path}')
        return 2

    df = pl.read_parquet(src)
    base = df.select(list(BASE_COLUMNS)) if all(c in df.columns for c in BASE_COLUMNS) else df
    calendar = (
        pl.read_parquet(cal_path).select('date').unique().sort('date')['date'].to_list()
    )
    out = derive(base, calendar)
    check(out, base)

    counts = {r['free_float_source']: r['count']
              for r in out['free_float_source'].value_counts().iter_rows(named=True)}
    stats = {'rows': out.height, 'raw_rows': counts.get('raw', 0),
             f'ffill_{WINDOW}d_rows': counts.get(f'ffill_{WINDOW}d', 0),
             'excluded_rows': counts.get('excluded', 0),
             'null_rows_total': out['free_float_share'].null_count(),
             'filled_gt_float_rows': filled_gt_float_rows(out).height}
    print(f'行数 {out.height}（守恒）  三态 {stats}')

    if args.dry_run:
        print('dry-run：未写盘')
        return 0

    sha_before = _sha256(src)
    backup_dir = PROJECT_ROOT / '_backup' / f'stock_shares_{date.today():%Y%m%d}'
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / src.name
    if not backup.exists():
        shutil.copy2(src, backup)
    if _sha256(backup) != sha_before:
        raise RuntimeError('备份校验失败')

    write_parquet_atomic(out, src)
    sha_after = _sha256(src)
    reread = pl.read_parquet(src)
    check(reread, base)
    print(f'sha256 旧={sha_before} 新={sha_after} 备份={backup}')

    # 与本脚本的派生列同步回写 manifest：只改统计字段，人工登记项原样保留。
    refreshed = refresh_manifest_files(
        root / 'meta' / 'factors' / 'manifest.json', root, [src], required=True)
    print(f'manifest 已按磁盘回写: {refreshed}')

    if meta_path.is_file():
        meta = json.loads(meta_path.read_text(encoding='utf-8'))
        payload = _meta_payload(meta, stats, backup, sha_before, sha_after)
        write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2), meta_path)
        print(f'meta 已更新: {meta_path}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
