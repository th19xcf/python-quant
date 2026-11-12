#!/usr/bin/env python3

"""把带日期的快照序列合并成单条 ``*_history.parquet`` 正式路径（G-098）。

迁移范围（两族 14 个文件，均为已登记的正式路径）::

    meta/st_status/st_status_<YYYYMMDD>.parquet  → meta/st_status/st_status_history.parquet
    meta/industry/industry_<YYYYMMDD>.parquet    → meta/industry/industry_history.parquet

判定口径（DG-4：日期后缀同属版本标记，正式路径不得带版本）：

- 每族只留一条无日期路径，**不留「最新截面」副本**（两套并存即 DG-1 缺陷）；
- 旧文件按 DG-13 整体移入仓库根 ``_backup/meta/<语义>_<日期>/``，移入前后逐文件
  SHA256 必须相等（证明是移动不是重写）；
- 合并只做「按日期纵向拼接」，不重算任何值：保留全部历史行，只丢弃「同一语义另存
  一份最新截面」这种副本形态，且不与既有行重复（``(code, as_of)`` 唯一）。

DG-20 四项证据由本工具逐项断言并打印：行数守恒、逐值对拍（既有非空值改动数 0）、
受影响文件 SHA256、抽样断言。任一不成立即退出码 1，不落盘。

用法::

    .venv\\Scripts\\python.exe tools\\migrate_snapshot_history.py            # 只校验并写候选
    .venv\\Scripts\\python.exe tools\\migrate_snapshot_history.py --apply    # 移旧入 _backup + 落正式
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_parquet_atomic  # noqa: E402
from src.data.snapshot_manifest import write_snapshot_manifest  # noqa: E402

TASK = 'g098_snapshot_history_20261113'

#: 两族的迁移规格；``prefix`` 同时用于发现源文件与重算目录 manifest。
FAMILIES: tuple[dict[str, Any], ...] = (
    {
        'key': 'st_status',
        'rel': 'meta/st_status',
        'prefix': 'st_status_',
        'out': 'st_status_history.parquet',
        'date_col': 'as_of',
        'key_cols': ('code', 'as_of'),
        'semantics': (
            '单表日期快照序列：每行是「某标的某日的 ST 三态截面」，'
            '按 as_of 纵向累积、只增不重写；正式路径不带日期，取哪一天一律由 '
            'as_of 列决定。历史 ST 判定仍以 meta/st/st_intervals.parquet 区间表为准，'
            '本表只作当日横截面核对'
        ),
        'authority': (
            'docs/数据权威路径清单.md 登记的 meta/st_status/st_status_history.parquet；'
            '选择函数 src/data/risk_flags.py::load_snapshot（取 as_of 不晚于目标日的'
            '最大 as_of 那一层）'
        ),
        'consumers': [
            'src/data/risk_flags.py::load_snapshot / snapshot_days（as_of 不晚于目标日）',
            'src/data/st_history.py::_snapshot_series 按 as_of 升序逐层读（防未来函数）',
            'src/advice/risk_filters.py load_st_snapshot',
            'src/data/quality/clean_set.py 取 as_of == 2026-11-03 那一层（已登记陈旧截面缺陷）',
            'src/data/quality/freshness_config.json 的 latest_as_of_distinct / parquet_max',
            'tools/build_universe_layers.py::_snapshot_comparison 取最大 as_of 做 L3 对拍',
        ],
        'produced_by': 'src/data/risk_flags.py::build_snapshot（按 as_of 覆盖同日后合并落盘）',
    },
    {
        'key': 'industry',
        'rel': 'meta/industry',
        'prefix': 'industry_',
        'out': 'industry_history.parquet',
        'date_col': 'as_of',
        'key_cols': ('code', 'as_of'),
        'semantics': (
            '单表日期快照序列：每行是「某标的某日的通达信行业分类」，'
            '按 as_of 纵向累积、只增不重写；正式路径不带日期。'
            '不是行业 PIT，历史行业归属仍须走 meta/industry_pit/ 区间表'
        ),
        'authority': (
            'docs/数据权威路径清单.md 登记的 meta/industry/industry_history.parquet；'
            '选择函数 src/data/industry.py::load_snapshot（取 as_of 不晚于目标日的'
            '最大 as_of 那一层）'
        ),
        'consumers': [
            'src/data/industry.py::load_snapshot / snapshot_days（as_of 不晚于目标日）',
            'src/advice/risk_filters.py 推荐/风控逐日行业分散度上限',
            'src/data/services/finance_service.py get_industry',
            'src/data/quality/extended_builders.py::_industry_snapshots 逐层抽取 '
            'meta/industry_pit/industry_pit.parquet',
            'tools/research/build_analysis_monthly.py 研究侧同一选择函数',
            'tools/single_window_diag.py --industry 默认取 as_of == 2026-11-04 那一层',
        ],
        'produced_by': 'src/data/industry.py::build_snapshot（按 as_of 覆盖同日后合并落盘）',
    },
)


def sha256(path: Path, chunk: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(chunk), b''):
            digest.update(block)
    return digest.hexdigest()


def _day_from_name(path: Path, prefix: str) -> date | None:
    stem = path.stem
    if not stem.startswith(prefix):
        return None
    token = stem[len(prefix):]
    if len(token) < 8 or not token[:8].isdigit():
        return None
    try:
        return datetime.strptime(token[:8], '%Y%m%d').date()
    except ValueError:
        return None


def discover(directory: Path, prefix: str) -> list[tuple[date, Path]]:
    """列出带日期的源文件（排除 ``*_history.parquet`` 这类已迁移目标）。"""
    found: list[tuple[date, Path]] = []
    for path in sorted(directory.glob(f'{prefix}*.parquet')):
        day = _day_from_name(path, prefix)
        if day is None:
            continue
        found.append((day, path))
    return sorted(found)


def _assert_sources(family: dict[str, Any], sources: list[tuple[date, Path]]) -> dict[str, Any]:
    """源文件自洽性：同构、as_of 与文件名一致、份内 code 唯一、as_of 无空值。"""
    if not sources:
        raise SystemExit(f'{family["key"]}: 未发现带日期的源文件，迁移无事可做')
    date_col = family['date_col']
    frames: list[tuple[date, Path, pl.DataFrame]] = []
    schemas: dict[str, list[str]] = {}
    for day, path in sources:
        frame = pl.read_parquet(path)
        schemas.setdefault('|'.join(frame.columns), []).append(path.name)
        days = frame[date_col].unique().to_list()
        if len(days) != 1 or days[0] != day:
            raise SystemExit(
                f'{path.name}: 表内 {date_col} 应为单一值 {day}，实测 {days[:5]}')
        if frame[date_col].null_count():
            raise SystemExit(f'{path.name}: {date_col} 存在空值')
        dup = frame.select('code').is_duplicated().sum()
        if dup:
            raise SystemExit(f'{path.name}: code 份内重复 {dup} 行')
        frames.append((day, path, frame))
    if len(schemas) != 1:
        raise SystemExit(f'{family["key"]}: 源文件 schema 不一致 {list(schemas.values())}')
    return {
        'files': [
            {
                'path': path.relative_to(ROOT).as_posix() if path.is_relative_to(ROOT) else str(path),
                'day': day.isoformat(),
                'rows': frame.height,
                'bytes': path.stat().st_size,
                'sha256': sha256(path),
                'columns': frame.columns,
            }
            for day, path, frame in frames
        ],
        'schema': frames[0][2].columns,
        'total_rows': sum(frame.height for _, _, frame in frames),
    }


def merge(family: dict[str, Any], sources: list[tuple[date, Path]]) -> pl.DataFrame:
    """按日期顺序纵向拼接，断言 ``(code, as_of)`` 唯一，并按 ``(as_of, code)`` 排序。"""
    frames = [pl.read_parquet(path) for _, path in sources]
    merged = pl.concat(frames, how='vertical')
    key_cols = list(family['key_cols'])
    dup = merged.select(key_cols).is_duplicated().sum()
    if dup:
        raise SystemExit(f'{family["key"]}: 合并后 {key_cols} 重复 {dup} 行')
    return merged.sort(key_cols)


def verify_against_sources(
    family: dict[str, Any], merged: pl.DataFrame, sources: list[tuple[date, Path]],
) -> dict[str, Any]:
    """逐源文件对拍：行数、逐值（含空值）、既有非空值改动数必须为 0。"""
    date_col = family['date_col']
    key_cols = list(family['key_cols'])
    value_cols = [col for col in merged.columns if col not in key_cols]
    report: list[dict[str, Any]] = []
    for day, path in sources:
        src = pl.read_parquet(path)
        part = merged.filter(pl.col(date_col) == day).select(src.columns)
        if part.height != src.height:
            raise SystemExit(f'{path.name}: 合并后该日 {part.height} 行 ≠ 源 {src.height} 行')
        left = part.sort('code')
        right = src.sort('code')
        if not left.equals(right):
            diff = [
                col for col in src.columns
                if not left[col].equals(right[col])
            ]
            raise SystemExit(f'{path.name}: 逐值对拍不一致，差异列 {diff}')
        joined = part.join(src, on=key_cols, how='inner', suffix='_src')
        if joined.height != src.height:
            raise SystemExit(f'{path.name}: 主键 join 丢行 {src.height} -> {joined.height}')
        changed = 0
        for col in value_cols:
            both = pl.col(col).is_not_null() & pl.col(f'{col}_src').is_not_null()
            changed += joined.filter(both & (pl.col(col) != pl.col(f'{col}_src'))).height
        if changed:
            raise SystemExit(f'{path.name}: 既有非空值改动 {changed} 格（应为 0）')
        report.append({
            'file': path.name,
            'day': day.isoformat(),
            'rows': src.height,
            'nulls_src': int(sum(src[col].null_count() for col in src.columns)),
            'nulls_merged': int(sum(part[col].null_count() for col in part.columns)),
            'value_changed_non_null': changed,
        })
    return {'per_file': report}


def assert_conservation(
    family: dict[str, Any], sources: list[tuple[date, Path]], merged: pl.DataFrame,
) -> dict[str, Any]:
    """行数守恒：合并行数 = 各源文件行数之和；逐日行数逐一相等。"""
    per_day: list[dict[str, Any]] = []
    total = 0
    for day, path in sources:
        rows = pl.read_parquet(path, columns=['code']).height
        part = merged.filter(pl.col(family['date_col']) == day).height
        if rows != part:
            raise SystemExit(f'{path.name}: 该日行数 {part} ≠ 源 {rows}')
        per_day.append({'day': day.isoformat(), 'source_rows': rows, 'merged_rows': part})
        total += rows
    if total != merged.height:
        raise SystemExit(f'{family["key"]}: 行数不守恒 {total} ≠ {merged.height}')
    return {'sum_source_rows': total, 'merged_rows': merged.height, 'delta': merged.height - total,
            'per_day': per_day}


def sample_assertions(family: dict[str, Any], merged: pl.DataFrame,
                      sources: list[tuple[date, Path]]) -> dict[str, Any]:
    """抽样断言：最新一层覆盖、每层 code 数、逐日唯一键抽查。"""
    date_col = family['date_col']
    days = sorted(merged[date_col].unique().to_list())
    latest = days[-1]
    latest_rows = merged.filter(pl.col(date_col) == latest).height
    last_src = sources[-1][1]
    last_rows = pl.read_parquet(last_src, columns=['code']).height
    if latest_rows != last_rows:
        raise SystemExit(f'{family["key"]}: 最新层 {latest_rows} 行 ≠ 源 {last_src.name} {last_rows} 行')
    probe_rows = []
    for day, path in (sources[0], sources[-1]):
        src = pl.read_parquet(path)
        first_code = src['code'][0]
        hit = merged.filter(
            (pl.col(date_col) == day) & (pl.col('code') == first_code))
        if hit.height != 1:
            raise SystemExit(f'{path.name}: 抽样 {first_code} 命中 {hit.height} 行（应为 1）')
        probe_rows.append({'day': day.isoformat(), 'code': first_code,
                           'row_equals_source': hit.select(src.columns).equals(
                               src.filter(pl.col('code') == first_code))})
    return {
        'days': [str(day) for day in days],
        'latest_day': str(latest),
        'latest_rows': latest_rows,
        'probes': probe_rows,
    }


def move_legacy(family: dict[str, Any], sources: list[tuple[date, Path]],
                directory: Path, backup_dir: Path) -> list[dict[str, Any]]:
    """把正式目录现有内容整体移入 ``_backup``，逐件证明移动前后 SHA256 相等。"""
    backup_dir.mkdir(parents=True, exist_ok=True)
    moved: list[dict[str, Any]] = []
    for path in sorted(directory.iterdir()):
        if not path.is_file():
            continue
        target = backup_dir / path.name
        if target.exists():
            raise SystemExit(f'备份落点已存在，拒绝覆盖: {target}')
        before = {'sha256': sha256(path), 'bytes': path.stat().st_size,
                  'inode': path.stat().st_ino}
        os.replace(path, target)
        after = {'sha256': sha256(target), 'bytes': target.stat().st_size,
                 'inode': target.stat().st_ino}
        if before != after:
            raise SystemExit(f'移动前后不一致: {path.name} {before} -> {after}')
        moved.append({'name': path.name, 'from': str(path), 'to': str(target), **after})
    expected = {path.name for _, path in sources} | {'manifest.json'}
    missing = expected - {item['name'] for item in moved}
    if missing:
        raise SystemExit(f'{family["key"]}: 以下文件未移入 _backup: {sorted(missing)}')
    return moved


def _write_manifest(family: dict[str, Any], store: Path) -> Path:
    """按迁移后的单表重算目录 manifest（复用既有 snapshot_manifest 写入器）。"""
    return write_snapshot_manifest(
        store, store / family['rel'],
        prefix=family['prefix'],
        dataset=f'{family["key"]}_history',
        semantics=family['semantics'],
        source=('本地名称快照 meta/names.json 派生；本地无名称时用远程 stock_basic 兜底'
                if family['key'] == 'st_status'
                else '通达信本地 cfg（tdxhy.cfg 分类 + tdxzs.cfg/tdxzs3.cfg 名称）解析的当日行业分类'),
        authority=family['authority'],
        path_pattern=f'{family["rel"]}/{family["out"]}',
        consumers=family['consumers'],
        produced_by=family['produced_by'],
        as_of_column=family['date_col'],
    )


def run(store: Path, staging_root: Path, apply: bool) -> dict[str, Any]:
    staging = staging_root / TASK
    staging.mkdir(parents=True, exist_ok=True)
    (staging / 'failed.csv').write_text('family,stage,reason\n', encoding='utf-8')
    progress: list[dict[str, Any]] = []
    summary: dict[str, Any] = {}
    for family in FAMILIES:
        directory = store / family['rel']
        sources = discover(directory, family['prefix'])
        logger.info(f'[{family["key"]}] 源文件 {len(sources)} 份')
        source_audit = _assert_sources(family, sources)
        merged = merge(family, sources)
        compare = verify_against_sources(family, merged, sources)
        conservation = assert_conservation(family, sources, merged)
        probes = sample_assertions(family, merged, sources)
        candidate = staging / family['key'] / family['out']
        candidate.parent.mkdir(parents=True, exist_ok=True)
        write_parquet_atomic(merged, candidate)
        if not pl.read_parquet(candidate).equals(merged):
            raise SystemExit(f'{family["key"]}: 候选回读与内存不一致')
        progress.append({'family': family['key'], 'stage': 'candidate_written',
                         'rows': merged.height, 'path': str(candidate)})
        entry: dict[str, Any] = {
            'sources': source_audit,
            'conservation': conservation,
            'value_compare': compare,
            'probes': probes,
            'candidate': {'path': str(candidate), 'rows': merged.height,
                          'bytes': candidate.stat().st_size, 'sha256': sha256(candidate)},
        }
        if apply:
            backup_dir = ROOT / '_backup' / 'meta' / f'{family["key"]}_20261113'
            entry['moved'] = move_legacy(family, sources, directory, backup_dir)
            formal = directory / family['out']
            write_parquet_atomic(merged, formal)
            if sha256(formal) != entry['candidate']['sha256']:
                raise SystemExit(f'{family["key"]}: 正式文件与候选 SHA256 不等')
            progress.append({'family': family['key'], 'stage': 'applied',
                             'path': str(formal)})
            entry['formal'] = {'path': str(formal), 'bytes': formal.stat().st_size,
                               'sha256': sha256(formal)}
            entry['manifest'] = str(_write_manifest(family, store))
            entry['manifest_sha256'] = sha256(Path(entry['manifest']))
        summary[family['key']] = entry
        logger.info(f'[{family["key"]}] 合并 {merged.height} 行，'
                    f'阈值校验全部通过{"，已落正式" if apply else "，仅写候选"}')
    (staging / 'assertions.json').write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    (staging / 'progress.json').write_text(
        json.dumps({'task': TASK, 'applied': apply, 'steps': progress},
                   ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='带日期快照序列 → 单表 history（G-098）')
    parser.add_argument('--store', default=str(ROOT / 'data/local_store'))
    parser.add_argument('--staging-root', default=str(ROOT / '_staging'))
    parser.add_argument('--apply', action='store_true',
                        help='把旧文件整体移入 _backup 并落正式（默认只写 _staging 候选）')
    args = parser.parse_args(argv)
    summary = run(Path(args.store), Path(args.staging_root), args.apply)
    for key, entry in summary.items():
        print(f'{key}: {entry["conservation"]["merged_rows"]} 行 / '
              f'候选 {entry["candidate"]["sha256"][:16]}…'
              + (f' / 正式 {entry["formal"]["sha256"][:16]}…' if 'formal' in entry else ''))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
