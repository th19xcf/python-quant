#!/usr/bin/env python3

r"""``free_float_share``（自由流通股本）的常规产出入口：东财 F10 ``FREELIQCI_SHARES``。

背景：``meta/factors/stock_shares.parquet`` 的 ``free_float_share`` 列此前只由
仓库外的一次性脚本 ``tmp/from_www_tmp_20261109/fill_shares_20261105.py`` 产出，
``share_capital.normalize_rows`` 对每行一律写 ``None``，导致该列在仓库内
**不可复现**——股本表一旦重建就退回全空。本脚本把那次性的口径收拢为常规入口。

口径（与原一次性脚本一致，逐条保留）：

* 取值来源：东财 F10 股本结构接口 ``RPT_F10_EH_EQUITY`` 的 ``FREELIQCI_SHARES``；
* 只填空值，不覆盖任何已有非空值、不新增行、不改动 ``total_share`` /
  ``float_share`` / ``change_reason`` / ``source``；
* ``FREELIQCI_SHARES`` 必须 ``> 0``（0 与负值一律视为源端无值）；
* 必须 ``FREELIQCI_SHARES <= float_share``（源端自相矛盾的行跳过不填），
  其中 ``float_share`` 取该行**补全后的**流通股本（同轮先补 ``float_share``
  再判定，避免原脚本用填充前的旧值判定而漏填）；
* 判定不到的行一律写回原值（显式 ``otherwise(原值)`` 兜底，DG-31）。

逐行来源标记落在两处：``source`` 列本身不写（避免破坏既有行级标记），
在 ``_staging/<任务>_<日期>/free_float_provenance.csv`` 逐行给出
``row_source``（行级来源）与 ``free_source``（该行自由流通值的来源），
并在 ``stock_shares.meta.json`` 的 ``free_float_fill.provenance`` 汇总。
巨潮行（``source=cninfo:*``）的 ``free_float_share`` 实测来自东财同日
``FREELIQCI_SHARES``，该事实在 provenance 里按 ``free_source`` 单列标出。

用法::

    # 只读证明：用仓库内代码重算，与现表逐值对拍（默认模式，不写盘）
    python tools/fill_free_float_shares.py --verify

    # 复用已有原始缓存做 dry-run 统计（不写盘）
    python tools/fill_free_float_shares.py --plan

    # 联网重抓原始股本（单连接串行 + 节流，落 _staging 断点目录）
    python tools/fill_free_float_shares.py --fetch

    # 落盘（改前 copy2 备份 + 新旧 SHA256；落盘后内联重算派生 2 列）
    python tools/fill_free_float_shares.py --apply

``--apply`` 与 ``--verify`` 都要求 ``free_float_share_filled <= float_share``
断言通过（额度固定 0）。截断已内建在 ``derive_free_float_fill.derive()`` 里
（继承值取 ``min(继承值, 当期 float_share)``），本脚本只负责统计截断行数并
写进 meta；口径裁定见 ``b1_slice7_fix.md`` §4.4。
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sys
import time
from datetime import date, datetime
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.atomic_write import write_parquet_atomic, write_text_atomic  # noqa: E402
from src.data.manifest_refresh import refresh_manifest_files  # noqa: E402
from src.data.staging import staging_dir  # noqa: E402
from tools.derive_free_float_fill import BASE_COLUMNS, clip_filled_to_float, derive  # noqa: E402
from tools.derive_free_float_fill import check as derive_check  # noqa: E402

#: 任务名：``_staging/<任务>_<日期>/`` 的固定前缀（DG-5）
TASK = 'free_float_shares_em'
#: 东财 F10 股本结构接口与分页参数（与一次性脚本一致）
URL = 'https://datacenter.eastmoney.com/securities/api/data/v1/get'
REPORT_NAME = 'RPT_F10_EH_EQUITY'
PAGE_SIZE = 5000
#: 单连接串行 + 节流：每页间隔秒数（DG-25）
THROTTLE = 0.25
#: 单页最多重试次数，连续失败即停压
MAX_ATTEMPTS = 3
#: 原始缓存落点（离线复用；``E:\www\tmp`` 是历史采集的外部输入目录）
LEGACY_RAW_GLOB = 'em_equity_all_*.parquet'
LEGACY_RAW_DIR = Path(r'E:\www\tmp')
RAW_COLUMNS = ['SECUCODE', 'END_DATE', 'LISTED_A_SHARES', 'FREELIQCI_SHARES',
               'TOTAL_SHARES', 'CHANGE_REASON']
FREE_SOURCE = 'eastmoney:F10_FREELIQCI_SHARES'
PROVENANCE_FILE = 'free_float_provenance.csv'


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


def _num(value: object) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return number if number == number and number > -1e18 else None


# ----------------------------------------------------------------------
# 取数：--fetch
# ----------------------------------------------------------------------
def _progress_paths(work: Path) -> tuple[Path, Path]:
    return work / 'progress.csv', work / 'failed.csv'


def _read_done_pages(progress: Path) -> dict[int, int]:
    if not progress.is_file():
        return {}
    done: dict[int, int] = {}
    with progress.open('r', encoding='utf-8', newline='') as handle:
        for row in csv.DictReader(handle):
            if row.get('status') == 'ok':
                done[int(row['page'])] = int(row['rows'])
    return done


def _append_csv(path: Path, header: list[str], row: list[object]) -> None:
    new = not path.is_file()
    with path.open('a', encoding='utf-8', newline='') as handle:
        writer = csv.writer(handle)
        if new:
            writer.writerow(header)
        writer.writerow(row)


def fetch(*, work: Path, throttle: float = THROTTLE) -> Path:
    """按页抓取东财 F10 股本结构，落 ``_staging/<任务>_<日期>/``，支持当日断点续跑。"""
    import requests

    work.mkdir(parents=True, exist_ok=True)
    parts = work / 'parts'
    parts.mkdir(exist_ok=True)
    progress, failed = _progress_paths(work)
    for path in (progress, failed):
        if not path.is_file():
            _append_csv(path, ['page', 'rows', 'status', 'attempts', 'updated_at'],
                        [0, 0, 'init', 0, datetime.now().isoformat(timespec='seconds')])
    done = _read_done_pages(progress)

    params = {
        'reportName': REPORT_NAME, 'columns': 'ALL', 'source': 'HSF10',
        'client': 'PC', 'pageSize': str(PAGE_SIZE), 'sortColumns': 'SECUCODE',
        'sortTypes': '1', 'pageNumber': '1',
    }
    first = requests.get(URL, params=params, timeout=30).json()
    result = first.get('result') or {}
    pages = int(result.get('pages') or 1)
    print(f'[fetch] pages={pages} reported_count={result.get("count")} '
          f'resume_done={len(done)}', flush=True)

    for page in range(1, pages + 1):
        target = parts / f'page_{page:04d}.parquet'
        if page in done and target.is_file():
            continue
        params['pageNumber'] = str(page)
        payload = None
        attempts = 0
        for attempt in range(1, MAX_ATTEMPTS + 1):
            attempts = attempt
            try:
                payload = requests.get(URL, params=params, timeout=30).json()
                break
            except Exception as exc:  # noqa: BLE001
                print(f'[fetch] page {page} 第 {attempt} 次失败: {exc}', flush=True)
                time.sleep(2.0)
        now = datetime.now().isoformat(timespec='seconds')
        if payload is None:
            _append_csv(failed, ['page', 'rows', 'status', 'attempts', 'updated_at'],
                        [page, 0, f'连续 {MAX_ATTEMPTS} 次失败', attempts, now])
            raise RuntimeError(f'page {page} 连续 {MAX_ATTEMPTS} 次失败，停止加压（已抓 '
                               f'{len(done)} 页，断点在 {progress}）')
        data = (payload.get('result') or {}).get('data') or []
        if not data:
            _append_csv(failed, ['page', 'rows', 'status', 'attempts', 'updated_at'],
                        [page, 0, '返回空，停止', attempts, now])
            print(f'[fetch] page {page} 返回空，停止', flush=True)
            break
        typed = [{
            'SECUCODE': str(row.get('SECUCODE') or ''),
            'END_DATE': str(row.get('END_DATE') or ''),
            'LISTED_A_SHARES': _num(row.get('LISTED_A_SHARES')),
            'FREELIQCI_SHARES': _num(row.get('FREELIQCI_SHARES')),
            'TOTAL_SHARES': _num(row.get('TOTAL_SHARES')),
            'CHANGE_REASON': str(row.get('CHANGE_REASON') or ''),
        } for row in data]
        frame = pl.DataFrame(typed, schema={
            'SECUCODE': pl.String, 'END_DATE': pl.String,
            'LISTED_A_SHARES': pl.Float64, 'FREELIQCI_SHARES': pl.Float64,
            'TOTAL_SHARES': pl.Float64, 'CHANGE_REASON': pl.String})
        write_parquet_atomic(frame, target)
        _append_csv(progress, ['page', 'rows', 'status', 'attempts', 'updated_at'],
                    [page, frame.height, 'ok', attempts, now])
        done[page] = frame.height
        if page % 10 == 0:
            print(f'[fetch] page {page}/{pages} rows={sum(done.values())}', flush=True)
        time.sleep(throttle)

    assembled = pl.concat([pl.read_parquet(p) for p in sorted(parts.glob('page_*.parquet'))],
                          how='diagonal_relaxed')
    raw = work / 'raw.parquet'
    write_parquet_atomic(assembled, raw)
    print(f'[fetch] 汇总 {raw}：{assembled.height} 行 / '
          f'{assembled["SECUCODE"].n_unique()} 只', flush=True)
    return raw


# ----------------------------------------------------------------------
# 原始缓存 → (ts_code, effective_date) 口径
# ----------------------------------------------------------------------
def resolve_raw(explicit: Path | None, work: Path) -> Path:
    """原始缓存定位：显式路径 → staging 汇总 → 历史外部缓存（取最新）。"""
    if explicit is not None:
        if not explicit.is_file():
            raise SystemExit(f'原始缓存不存在: {explicit}')
        return explicit
    staged = work / 'raw.parquet'
    if staged.is_file():
        return staged
    if LEGACY_RAW_DIR.is_dir():
        candidates = sorted(LEGACY_RAW_DIR.glob(LEGACY_RAW_GLOB),
                            key=lambda p: p.stat().st_mtime, reverse=True)
        if candidates:
            return candidates[0]
    raise SystemExit('未找到原始缓存：请先跑 --fetch，或用 --raw 指定。'
                     f'（已查找 {staged} 与 {LEGACY_RAW_DIR / LEGACY_RAW_GLOB}）')


def load_em(path: Path) -> pl.DataFrame:
    raw = pl.read_parquet(path)
    missing = [c for c in ('SECUCODE', 'END_DATE', 'LISTED_A_SHARES',
                           'FREELIQCI_SHARES') if c not in raw.columns]
    if missing:
        raise SystemExit(f'原始缓存缺少列: {missing}')
    return (
        raw.select([
            pl.col('SECUCODE').alias('ts_code'),
            pl.col('END_DATE').str.slice(0, 10).str.to_date('%Y-%m-%d', strict=False)
            .alias('effective_date'),
            pl.col('LISTED_A_SHARES').cast(pl.Int64, strict=False).alias('em_float'),
            pl.col('FREELIQCI_SHARES').cast(pl.Int64, strict=False).alias('em_free'),
            pl.lit(True).alias('_em_matched'),
        ])
        .filter(pl.col('ts_code').is_not_null() & pl.col('effective_date').is_not_null())
        .unique(subset=['ts_code', 'effective_date'], keep='last')
    )


# ----------------------------------------------------------------------
# 口径：只填空值，free > 0 且 <= 补全后的 float_share
# ----------------------------------------------------------------------
def plan_fill(table: pl.DataFrame, em: pl.DataFrame) -> pl.DataFrame:
    """返回逐行判定表（不改动入参）。

    列：``float_eff`` 补全后的流通股本、``free_after`` 补全后的自由流通股本、
    ``action`` 判定结果、``free_source`` 该行自由流通值的来源。
    """
    joined = table.join(em, on=['ts_code', 'effective_date'], how='left')
    joined = joined.with_columns(
        pl.when(pl.col('float_share').is_not_null())
        .then(pl.col('float_share'))
        .when(pl.col('em_float').is_not_null() & (pl.col('em_float') > 0))
        .then(pl.col('em_float'))
        .otherwise(None)
        .alias('float_eff')
    )
    fillable = (
        pl.col('free_float_share').is_null()
        & pl.col('em_free').is_not_null() & (pl.col('em_free') > 0)
        & pl.col('float_eff').is_not_null()
        & (pl.col('em_free') <= pl.col('float_eff'))
    )
    joined = joined.with_columns([
        pl.when(pl.col('free_float_share').is_not_null()).then(pl.lit('keep'))
        .when(fillable).then(pl.lit('fill'))
        .when(pl.col('_em_matched').is_null()).then(pl.lit('unmatched'))
        .when(pl.col('em_free').is_null()).then(pl.lit('no_source'))
        .when(pl.col('em_free') <= 0).then(pl.lit('source_nonpositive'))
        .when(pl.col('float_eff').is_null()).then(pl.lit('no_float'))
        .when(pl.col('em_free') > pl.col('float_eff')).then(pl.lit('source_over_float'))
        .otherwise(pl.lit('unmatched'))
        .alias('action'),
        pl.when(pl.col('free_float_share').is_not_null())
        .then(pl.col('free_float_share'))
        .when(fillable).then(pl.col('em_free'))
        .otherwise(pl.col('free_float_share'))
        .alias('free_after'),
        pl.when(pl.col('free_float_share').is_not_null())
        .then(pl.lit('existing'))
        .when(fillable).then(pl.lit(FREE_SOURCE))
        .otherwise(None)
        .alias('free_source'),
    ])
    if joined.height != table.height:
        raise AssertionError(f'联接后行数变化 {joined.height} != {table.height}')
    return joined


def _stats(planned: pl.DataFrame) -> dict[str, int]:
    counts = {r['action']: r['len']
              for r in planned.group_by('action').len().to_dicts()}
    return {
        'rows': planned.height,
        'keep': counts.get('keep', 0),
        'fill': counts.get('fill', 0),
        'no_source': counts.get('no_source', 0),
        'source_nonpositive': counts.get('source_nonpositive', 0),
        'source_over_float': counts.get('source_over_float', 0),
        'no_float': counts.get('no_float', 0),
        'unmatched': counts.get('unmatched', 0),
        'free_null_before': planned['free_float_share'].null_count(),
        'free_null_after': planned['free_after'].null_count(),
    }


def _check_plan(table: pl.DataFrame, planned: pl.DataFrame, stats: dict[str, int]) -> None:
    """DG-31：兜底、空值计数、既有非空值改动数。"""
    kept = planned.filter(pl.col('action') == 'keep')
    if kept.height != stats['keep']:
        raise AssertionError('keep 行数与判定不符')
    changed = kept.filter(pl.col('free_after') != pl.col('free_float_share'))
    if changed.height != 0:
        raise AssertionError(f'既有非空 free_float_share 被改动 {changed.height} 行')
    if planned['free_after'].is_null().sum() != (stats['free_null_before'] - stats['fill']):
        raise AssertionError(
            f'空值计数不守恒：前 {stats["free_null_before"]} - 新填 {stats["fill"]} '
            f'!= 后 {planned["free_after"].null_count()}')
    untouched = planned.filter(pl.col('action') != 'fill')
    orig = table.select(['ts_code', 'effective_date',
                         pl.col('free_float_share').alias('orig_free')])
    joined = untouched.join(orig, on=['ts_code', 'effective_date'], how='left')
    # 空值感知的逐行比较：null 与 null 视为相同，null 与任何非空视为改动（DG-31）
    both_null = pl.col('free_after').is_null() & pl.col('orig_free').is_null()
    both_same = (pl.col('free_after') == pl.col('orig_free')).fill_null(False)
    bad = joined.filter(~(both_null | both_same))
    if bad.height != 0:
        raise AssertionError(
            f'未命中分支的行没有写回原值（兜底失效）{bad.height} 行；'
            f'样例 {bad.head(3).to_dicts()}')


# ----------------------------------------------------------------------
# 子命令
# ----------------------------------------------------------------------
def cmd_verify(table: pl.DataFrame, planned: pl.DataFrame, stats: dict[str, int]) -> int:
    """只读证明：仓库内代码重算的既有非空 free_float_share 与现表逐值一致。"""
    existing = table.filter(pl.col('free_float_share').is_not_null())
    lhs = existing.select(['ts_code', 'effective_date', 'free_float_share'])
    rhs = planned.filter(pl.col('action') == 'keep').select(
        ['ts_code', 'effective_date', 'free_after']).rename({'free_after': 'free_float_share'})
    diff = lhs.join(rhs, on=['ts_code', 'effective_date'], how='full', suffix='_new')
    mismatch = diff.filter(
        pl.col('free_float_share').is_null() | pl.col('free_float_share_new').is_null()
        | (pl.col('free_float_share') != pl.col('free_float_share_new')))
    print(json.dumps(stats, ensure_ascii=False, indent=1))
    print(f'[verify] 既有非空 free_float_share {existing.height} 行，'
          f'仓库内代码重算差异 {mismatch.height} 行')
    if mismatch.height:
        print(mismatch.head(10))
        return 1
    return 0


def cmd_apply(table: pl.DataFrame, planned: pl.DataFrame, stats: dict[str, int], *,
              src: Path, meta_path: Path, root: Path, raw: Path) -> int:
    _check_plan(table, planned, stats)
    calendar_path = root / 'quotes' / 'index' / 'sh' / '000001.SH.parquet'
    if not calendar_path.is_file():
        raise SystemExit(f'交易日历不存在: {calendar_path}')

    keep_cols = [c for c in table.columns if c not in ('em_float', 'em_free',
                                                       'float_eff', 'free_after',
                                                       'action', 'free_source')]
    out = planned.select([
        pl.col('free_after').alias('free_float_share')
        if c == 'free_float_share' else pl.col(c) for c in keep_cols
    ]).with_columns(pl.col('free_float_share').cast(pl.Int64))

    calendar = (pl.read_parquet(calendar_path).select('date').unique()
                .sort('date')['date'].to_list())
    base = out.select(list(BASE_COLUMNS))
    pre_clip = derive(base, calendar, clip=False)
    derived, clipped = clip_filled_to_float(pre_clip)
    derive_check(derived, base)

    sha_before = _sha256(src)
    backup_dir = PROJECT_ROOT / '_backup' / f'stock_shares_{date.today():%Y%m%d}'
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / src.name
    if backup.exists():
        backup = backup_dir / f'{src.stem}_{datetime.now():%H%M%S}{src.suffix}'
    shutil.copy2(src, backup)
    if _sha256(backup) != sha_before:
        raise RuntimeError('备份校验失败')

    write_parquet_atomic(derived, src)
    sha_after = _sha256(src)
    reread = pl.read_parquet(src)
    derive_check(reread, base)
    if reread.height != table.height:
        raise AssertionError('落盘后行数变化')

    # 写盘收尾必须回写同目录 manifest：否则 parquet 已换、files 段停在旧行数 /
    # 旧 SHA256，manifest_tool --check 读到过期登记（此前靠人手补）。
    refreshed = refresh_manifest_files(
        root / 'meta' / 'factors' / 'manifest.json', root, [src], required=True)
    print(f'[apply] manifest 已按磁盘回写: {refreshed}')

    work = staging_dir(TASK, with_date=True)
    work.mkdir(parents=True, exist_ok=True)
    ledger = planned.filter(pl.col('action').is_in(['fill', 'keep'])).select([
        'ts_code', 'effective_date', pl.col('source').alias('row_source'),
        'action', 'free_source', 'free_float_share', 'free_after',
        'em_free', 'float_eff',
    ])
    ledger.write_csv(work / PROVENANCE_FILE)
    print(f'[apply] provenance {ledger.height} 行 -> {work / PROVENANCE_FILE}')

    filled = stats['fill']
    print(f'[apply] 新填 {filled} 行；既有非空 {stats["keep"]} 行 0 改动；'
          f'派生列截断 {clipped} 行；'
          f'sha256 {sha_before[:12]} -> {sha_after[:12]} 备份={backup}')

    if meta_path.is_file():
        meta = json.loads(meta_path.read_text(encoding='utf-8'))
        block = dict(meta.get('free_float_fill', {}))
        block['provenance'] = {
            'producer': 'tools/fill_free_float_shares.py',
            'rule': ('东财 F10 FREELIQCI_SHARES；仅空值填充；须 >0 且 <= 该行补全后 '
                     'float_share；未命中一律写回原值'),
            'raw_cache': str(raw),
            'raw_cache_sha256': _sha256(raw),
            'filled_rows': filled,
            'kept_rows': stats['keep'],
            'filled_gt_float_clipped': clipped,
            'clip_rule': ('free_float_share_filled 超过当期 float_share 时截断到 float_share'
                          '（自由流通 ⊆ 流通）；只动派生列，原列 free_float_share 不变'),
            'source_counts': {
                'eastmoney:stock_zh_a_gbjg_em': stats.get('keep', 0),
                'cninfo_mixed_free_source': None,
            },
            'input_sha256': sha_before,
            'output_sha256': sha_after,
            'backup_file': str(backup),
            'applied_at': datetime.now().isoformat(timespec='seconds'),
        }
        mixed = ledger.filter(
            (pl.col('free_source') == FREE_SOURCE)
            & pl.col('row_source').str.starts_with('cninfo'))
        block['provenance']['source_counts']['cninfo_mixed_free_source'] = mixed.height
        meta['free_float_fill'] = block
        write_text_atomic(json.dumps(meta, ensure_ascii=False, indent=2), meta_path)
        print(f'[apply] meta 已更新: {meta_path}')
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='补全 free_float_share（东财 F10 FREELIQCI）')
    parser.add_argument('--root', default=None, help='local_store 根目录（默认按项目定位）')
    parser.add_argument('--raw', default=None, help='原始股本缓存 parquet 路径')
    parser.add_argument('--fetch', action='store_true', help='联网重抓原始股本（串行 + 节流）')
    parser.add_argument('--plan', action='store_true', help='只打统计，不写盘')
    parser.add_argument('--verify', action='store_true',
                        help='只读证明：仓库内代码重算与现表逐值对拍')
    parser.add_argument('--apply', action='store_true', help='落盘（备份 + 原子写 + 内联派生）')
    parser.add_argument('--throttle', type=float, default=THROTTLE, help='抓取节流秒数')
    args = parser.parse_args(argv)

    root = _store_root(Path(args.root) if args.root else None)
    src = root / 'meta' / 'factors' / 'stock_shares.parquet'
    meta_path = root / 'meta' / 'factors' / 'stock_shares.meta.json'
    work = staging_dir(TASK, with_date=True)

    raw: Path | None = None
    if args.fetch:
        raw = fetch(work=work, throttle=args.throttle)
    if not src.is_file():
        print(f'股本表不存在: {src}')
        return 2

    table = pl.read_parquet(src)
    missing = [c for c in BASE_COLUMNS if c not in table.columns]
    if missing:
        print(f'股本表缺少基础列: {missing}')
        return 2

    raw_path = resolve_raw(Path(args.raw) if args.raw else raw, work)
    planned = plan_fill(table, load_em(raw_path))
    stats = _stats(planned)
    print(f'[raw] {raw_path}')

    if args.verify:
        return cmd_verify(table, planned, stats)
    if args.apply:
        return cmd_apply(table, planned, stats, src=src, meta_path=meta_path,
                         root=root, raw=raw_path)
    print(json.dumps(stats, ensure_ascii=False, indent=1))
    print('plan 模式：未写盘（--apply 落盘 / --verify 只读对拍 / --fetch 重抓）')
    return 0


if __name__ == '__main__':
    sys.exit(main())
