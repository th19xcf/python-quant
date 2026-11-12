#!/usr/bin/env python3

r"""停牌推断表（占位 bar 型）重建入口：``meta/suspension/implied_suspension.parquet``。

为什么有这个脚本（G-113）
------------------------
本表 2026-11-09 落盘时的生成算法只存在于一次性脚本
``reports/audit_quotes_20261109/fix2_scan_implied_suspension.py`` 与
``fix2_write_table.py``；``src/`` / ``tools/`` 只有 5 个读者、零写入口，
等于「表在、重建路径不在」（DG-3 的「丢了能说清重建路径」不成立）。
本脚本把当时的口径原样固化，避免重算只能回翻历史 ``_staging`` / ``reports``。

判定口径（与 2026-11-09 原脚本逐条一致）
--------------------------------------
- 命中条件：``volume IS NULL`` 或 ``amount IS NULL``；
- ``reason``：``volume_null`` / ``amount_null`` / ``ohlc_flat`` 按序以 ``+`` 连接；
- ``ohlc_flat``：四价（open/high/low/close）全部相等（任一为空按 ``False``，
  与 pandas ``nan == nan → False`` 同语义）；
- ``in_existing_suspension``：该 ``(ts_code, trade_date)`` 是否已登记在
  ``meta/suspension/suspension_daily.parquet``。只认 ``is_suspended != false``
  的行，与 ``src/data/quality/suspension_union.py`` 的采纳规则一致；2026-11-12
  新增的 ``false``（``source=bar_present_prev_quote_gap``）是留档行，不参与；
- ``conflicts_with_existing_suspension = not in_existing_suspension``；
- ``source``：固定串 ``volume_isnull|amount_isnull|ohlc_flat``，记录判据本身，
  不是行级数据来源（DG-19 的字段级标记另见 ``reason``）；
- ``detected_at``：本次扫描日期，可用 ``--detected-at`` 指定。要复现 2026-11-09
  落盘的那一版请显式给 ``--detected-at 2026-11-09``——该列是运行时字段，
  不指定时每次重跑都会变（DG-34）。

边界（DG-5 / DG-11 / DG-22）
---------------------------
- 默认只把候选写到 ``_staging/build_implied_suspension_<YYYYMMDD>/``，并与
  正式表逐值对拍后打印差异与进度文件，**不碰** ``data/local_store/`` 任何文件；
- 覆盖正式文件必须显式 ``--apply --backup-dir <目录>``：先把现有正式文件
  ``copy2`` 到备份目录并记录改前 SHA256，再走 ``write_parquet_atomic`` 原子替换，
  写后回读断言与候选逐值一致。

典型用法::

    python tools/build_implied_suspension.py                    # 候选落 staging + 对拍
    python tools/build_implied_suspension.py --detected-at 2026-11-09
    python tools/build_implied_suspension.py --apply --backup-dir _backup/meta/suspension_20261113
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STORE_ROOT = PROJECT_ROOT / 'data' / 'local_store'
ASSETS: tuple[str, ...] = ('stock', 'index', 'fund')
FORMAL_REL = Path('meta') / 'suspension' / 'implied_suspension.parquet'
SUSPENSION_DAILY_REL = Path('meta') / 'suspension' / 'suspension_daily.parquet'
SOURCE_TAG = 'volume_isnull|amount_isnull|ohlc_flat'
OUT_COLUMNS = (
    'ts_code', 'trade_date', 'asset_class', 'reason', 'ohlc_flat',
    'in_existing_suspension', 'conflicts_with_existing_suspension',
    'source', 'detected_at',
)


def scan_asset(store_root: Path, asset: str) -> pl.DataFrame:
    """扫单个资产类的全部分片，返回命中行（``reason`` 尚未拼装）。"""
    files = sorted((store_root / 'quotes' / asset).rglob('*.parquet'))
    if not files:
        return pl.DataFrame()
    hits = (
        pl.scan_parquet(files)
        .select(['ts_code', 'date', 'open', 'high', 'low', 'close',
                 'volume', 'amount'])
        .with_columns(
            # 与 pandas ``nan == nan → False`` 对齐：任一为空即判「非四价相等」
            (
                (pl.col('open') == pl.col('high'))
                & (pl.col('high') == pl.col('low'))
                & (pl.col('low') == pl.col('close'))
            ).fill_null(False).alias('ohlc_flat'),
            pl.col('volume').is_null().alias('volume_null'),
            pl.col('amount').is_null().alias('amount_null'),
        )
        .filter(pl.col('volume_null') | pl.col('amount_null'))
        .select(['ts_code', 'date', 'volume_null', 'amount_null', 'ohlc_flat'])
        .collect()
    )
    if hits.is_empty():
        return hits
    reason = pl.concat_str(
        [
            pl.when(pl.col('volume_null')).then(pl.lit('volume_null'))
            .otherwise(pl.lit('')),
            pl.when(pl.col('amount_null')).then(pl.lit('amount_null'))
            .otherwise(pl.lit('')),
            pl.when(pl.col('ohlc_flat')).then(pl.lit('ohlc_flat'))
            .otherwise(pl.lit('')),
        ],
        separator='+',
    ).str.strip_chars('+').str.replace_all(r'\+\+', '+')
    return hits.with_columns(
        reason.alias('reason'),
        pl.lit(asset).alias('asset_class'),
        pl.col('date').cast(pl.Date).alias('trade_date'),
    ).select(['ts_code', 'trade_date', 'asset_class', 'reason', 'ohlc_flat'])


def suspension_keys(store_root: Path) -> set[tuple[str, date]]:
    """既有停牌表的键集合；只取 ``is_suspended != false`` 的行。"""
    path = store_root / SUSPENSION_DAILY_REL
    if not path.is_file():
        return set()
    rows = pl.read_parquet(path, columns=['ts_code', 'trade_date', 'is_suspended'])
    rows = rows.filter(pl.col('is_suspended').is_null() | pl.col('is_suspended'))
    return {
        (str(code), day if isinstance(day, date) else date.fromisoformat(str(day)))
        for code, day in rows.select('ts_code', 'trade_date').iter_rows()
    }


def build_frame(store_root: Path, assets: tuple[str, ...], detected_at: date) -> pl.DataFrame:
    """扫全库并装配成正式表的 9 列结构（未落盘）。"""
    parts = [part for part in (scan_asset(store_root, asset) for asset in assets)
             if not part.is_empty()]
    if not parts:
        raise RuntimeError(f'quotes 下未扫到任何占位 bar 命中：{store_root}')
    hits = pl.concat(parts)
    keys = suspension_keys(store_root)
    flags = [((str(code), day) in keys)
             for code, day in hits.select('ts_code', 'trade_date').iter_rows()]
    frame = hits.with_columns(
        pl.Series('in_existing_suspension', flags, dtype=pl.Boolean),
    ).with_columns(
        (~pl.col('in_existing_suspension')).alias('conflicts_with_existing_suspension'),
        pl.lit(SOURCE_TAG).alias('source'),
        pl.lit(detected_at).cast(pl.Date).alias('detected_at'),
    ).select(list(OUT_COLUMNS)).sort(['ts_code', 'trade_date'])
    if frame.height != frame.unique(subset=['ts_code', 'trade_date']).height:
        raise RuntimeError('(ts_code, trade_date) 存在重复键，停止落盘')
    return frame


def compare_formal(candidate: pl.DataFrame, formal: pl.DataFrame) -> dict[str, Any]:
    """候选与正式表逐值对拍（``detected_at`` 是运行时字段，单列计差异）。"""
    report: dict[str, Any] = {
        'candidate_rows': candidate.height,
        'formal_rows': formal.height,
        'rows_equal': False,
        'column_diff_rows': {},
    }
    missing_cols = [c for c in OUT_COLUMNS if c not in formal.columns]
    extra_cols = [c for c in formal.columns if c not in OUT_COLUMNS]
    report['missing_columns_in_formal'] = missing_cols
    report['extra_columns_in_formal'] = extra_cols
    if missing_cols:
        return report
    left = candidate.sort(['ts_code', 'trade_date'])
    right = formal.select(list(OUT_COLUMNS)).sort(['ts_code', 'trade_date'])
    if left.height != right.height:
        report['keys_only_in_candidate'] = left.join(
            right, on=['ts_code', 'trade_date'], how='anti').height
        report['keys_only_in_formal'] = right.join(
            left, on=['ts_code', 'trade_date'], how='anti').height
        return report
    joined = left.join(right, on=['ts_code', 'trade_date'], how='inner',
                       suffix='_formal')
    if joined.height != left.height:
        report['join_rows'] = joined.height
        return report
    diffs: dict[str, int] = {}
    for column in OUT_COLUMNS:
        if column in ('ts_code', 'trade_date'):
            continue
        left_col = pl.col(column)
        right_col = pl.col(f'{column}_formal')
        if formal.schema[column] != candidate.schema[column]:
            left_col = left_col.cast(formal.schema[column])
        diff = joined.filter(
            (left_col != right_col).fill_null(True)
            & ~(left_col.is_null() & right_col.is_null())
        ).height
        diffs[column] = diff
    report['column_diff_rows'] = diffs
    report['rows_equal'] = all(count == 0 for count in diffs.values())
    return report


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_progress(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n',
                    encoding='utf-8')


def _write_failed_header(path: Path) -> None:
    with path.open('w', encoding='utf-8', newline='') as handle:
        csv.writer(handle).writerow(['stage', 'error', 'failed_at'])


def apply_formal(candidate: pl.DataFrame, formal_path: Path,
                 backup_dir: Path) -> dict[str, Any]:
    """DG-11 / DG-22：先 copy2 备份并记 SHA256，再原子替换并回读断言。"""
    from src.data.atomic_write import write_parquet_atomic  # noqa: PLC0415

    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / formal_path.name
    before_sha = None
    if formal_path.is_file():
        shutil.copy2(formal_path, backup_path)
        before_sha = _sha256(formal_path)
        if _sha256(backup_path) != before_sha:
            raise RuntimeError(f'备份 SHA256 与源文件不一致：{backup_path}')
    write_parquet_atomic(candidate, formal_path)
    back = pl.read_parquet(formal_path)
    if back.height != candidate.height:
        raise RuntimeError(
            f'写后行数不守恒：候选 {candidate.height} 行，正式 {back.height} 行')
    report = compare_formal(candidate, back)
    if not report['rows_equal']:
        raise RuntimeError(f'写后回读与候选不一致：{report}')
    return {
        'formal_path': str(formal_path),
        'backup_path': str(backup_path) if before_sha else None,
        'sha256_before': before_sha,
        'sha256_after': _sha256(formal_path),
    }


def main(argv: list[str] | None = None) -> int:
    today = date.today()
    parser = argparse.ArgumentParser(
        description='重建 meta/suspension/implied_suspension.parquet（占位 bar 型停牌推断）')
    parser.add_argument('--store-root', default=str(DEFAULT_STORE_ROOT))
    parser.add_argument('--assets', default=','.join(ASSETS),
                        help='逗号分隔的资产类子目录，默认 stock,index,fund')
    parser.add_argument('--out', default=None,
                        help='候选落点；缺省 _staging/build_implied_suspension_<日期>/')
    parser.add_argument('--detected-at', default=None, help='detected_at，默认今天')
    parser.add_argument('--apply', action='store_true',
                        help='覆盖正式文件（必须同时给 --backup-dir）')
    parser.add_argument('--backup-dir', default=None,
                        help='--apply 时改前快照落点（仓库根 _backup/ 下）')
    args = parser.parse_args(argv)

    store_root = Path(args.store_root)
    detected_at = (date.fromisoformat(args.detected_at) if args.detected_at
                   else today)
    assets = tuple(item.strip() for item in args.assets.split(',') if item.strip())
    out_path = Path(args.out) if args.out else (
        PROJECT_ROOT / '_staging' / f'build_implied_suspension_{today:%Y%m%d}'
        / 'implied_suspension.parquet')
    staging = out_path.parent
    staging.mkdir(parents=True, exist_ok=True)
    progress_path = staging / 'progress.json'
    failed_path = staging / 'failed.csv'
    _write_failed_header(failed_path)

    frame: pl.DataFrame
    try:
        frame = build_frame(store_root, assets, detected_at)
    except Exception as exc:  # noqa: BLE001 — 失败也要留断点记录，再原样抛出
        with failed_path.open('a', encoding='utf-8', newline='') as handle:
            csv.writer(handle).writerow(['build_frame', str(exc), f'{today}'])
        _write_progress(progress_path, {'status': 'failed', 'error': str(exc)})
        raise

    from src.data.atomic_write import write_parquet_atomic  # noqa: PLC0415

    write_parquet_atomic(frame, out_path)
    print(f'候选已写入 {out_path}：{frame.height:,} 行 / '
          f'{frame["ts_code"].n_unique()} 只 / '
          f'{frame["trade_date"].min()}~{frame["trade_date"].max()}')
    print(frame.group_by(['asset_class', 'reason']).len().sort('len', descending=True)
          .to_pandas().to_string(index=False))

    formal_path = store_root / FORMAL_REL
    compare: dict[str, Any] | None = None
    if formal_path.is_file():
        compare = compare_formal(frame, pl.read_parquet(formal_path))
        print(f'\n与正式表对拍：{compare}')
    else:
        print(f'\n正式表不存在：{formal_path}（首次落盘请走 --apply）')

    applied = None
    if args.apply:
        if not args.backup_dir:
            raise SystemExit('--apply 必须同时给 --backup-dir（DG-11：先备份再替换）')
        applied = apply_formal(frame, formal_path, Path(args.backup_dir))
        print(f'\n已替换正式表：{applied}')

    _write_progress(progress_path, {
        'status': 'ok',
        'store_root': str(store_root),
        'candidate': str(out_path),
        'rows': frame.height,
        'codes': frame['ts_code'].n_unique(),
        'date_range': [str(frame['trade_date'].min()), str(frame['trade_date'].max())],
        'detected_at': str(detected_at),
        'compare_formal': compare,
        'applied': applied,
        'finished_at': f'{date.today()}',
    })
    print(f'\n进度已写 {progress_path}；失败清单 {failed_path}（无失败时只有表头）')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
