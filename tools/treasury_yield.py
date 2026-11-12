#!/usr/bin/env python3

"""国债 10 年期收益率（``cn_treasury_yield_10y``）的正式采集 / 构建 / 发布入口。

固化自三支一次性脚本，口径、源、字段一律不变，只做搬迁与整理：

- ``_staging/state_inputs_20261110/collect.py``        中债分年 + 东财全量（``treasury_*``）
- ``_staging/state_inputs_20261110/build.py``          A 段：分段组装 + 双源对拍
- ``_staging/derived_catchup_20261112/publish_macro.py``（treasury 部分：DG-11 + 追加发布）

口径（逐条对应旧脚本，未改一字）：

- 单列 ``y10``，单位 ``%``（旧 collect.py 的 note 即 ``unit=%``），不涉及亿→元换算；
- **切换日** ``SWITCH = 2006-03-01``（旧 build.py L18「中债口径最早可得日」）：
  ``date < SWITCH`` 全用东财；``date >= SWITCH`` 以中债为主，中债缺日才用东财补，
  补入行 ``source`` 仍标东财（旧 build.py L64-70）；
- 中债按年分片 ``treasury_chinabond_<year>.parquet``，年份 2006..2026（旧 L45）；
- 中债分片合并 ``unique(subset='date', keep='first')`` 后 ``sort('date')``（旧 L27-30）。

**缺口判定**：中债某年分片的「应有交易日集合」取 ``000001.SH`` 交易日 ∩ ``>= SWITCH``。
2006-01-04..2006-02-28 那 33 个交易日按口径本就由东财承担，若按整年要求会把 2006 年
分片永远判成 ``gap`` 而每次重取。三态 ``missing`` / ``gap`` / ``ok``；旧脚本的
``if target.exists(): continue`` / ``return`` 会让分片一旦落地永不刷新，源端补发布一天
即永久滞后。

东财侧是「一次请求返回全量」，不存在分片缺口，故每次 collect 都重取（旧脚本的
``exists → return`` 正是这次要修掉的那条）。

子命令::

    python tools/treasury_yield.py collect [--dry-run] [--years ...] [--refresh-current]
    python tools/treasury_yield.py build [--strict]
    python tools/treasury_yield.py publish [--yes]

``publish`` 默认 dry-run，只打印待追加行与备份落点；加 ``--yes`` 才落
``data/local_store/meta/macro/``。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Iterable

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_parquet_atomic, write_text_atomic  # noqa: E402
from src.data.staging import staging_dir_for_store  # noqa: E402

STORE = ROOT / 'data' / 'local_store'
TARGET_DIR = STORE / 'meta' / 'macro'
TARGET = TARGET_DIR / 'cn_treasury_yield_10y.parquet'
TARGET_CSV = TARGET_DIR / 'cn_treasury_yield_10y.csv'
MANIFEST = TARGET_DIR / 'manifest.json'
BACKUP_ROOT = ROOT / '_backup'
CALENDAR = STORE / 'quotes' / 'index' / 'sh' / '000001.SH.parquet'

#: 中债口径最早可得日；此日之前只能用东财（旧 build.py L18）
SWITCH = date(2006, 3, 1)
#: 中债分年抓取范围（旧 collect.py L45 ``range(2006, 2027)``）
CB_YEARS = tuple(range(2006, 2027))
#: 序列起点（东财全量最早日）
SERIES_START = date(2005, 1, 4)

CB_SOURCE = 'chinabond:bond_china_yield'
EM_SOURCE = 'eastmoney:bond_zh_us_rate'

SLEEP = 1.5
COLS = ['date', 'y10', 'source']
VALUE_COLS = ['y10']


# ---------------------------------------------------------------- 断点记录
@dataclass
class Progress:
    """``progress.csv`` / ``failed.csv``（DG-7：staging 必须可断点、可留失败清单）。"""

    base: Path
    progress: Path = field(init=False)
    failed: Path = field(init=False)

    def __post_init__(self) -> None:
        self.base.mkdir(parents=True, exist_ok=True)
        self.progress = self.base / 'progress.csv'
        self.failed = self.base / 'failed.csv'
        for p, header in ((self.progress, 'step,status,rows,start,end,note,ts\n'),
                          (self.failed, 'step,reason,detail,ts\n')):
            if not p.exists():
                p.write_text(header, encoding='utf-8', newline='\n')

    def ok(self, step: str, rows: int, start='', end='', note: str = '') -> None:
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.progress, 'a', encoding='utf-8', newline='\n') as fh:
            fh.write(f'{step},ok,{rows},{start},{end},{note},{ts}\n')

    def skip(self, step: str, note: str) -> None:
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.progress, 'a', encoding='utf-8', newline='\n') as fh:
            fh.write(f'{step},skip,0,,,{note},{ts}\n')

    def fail(self, step: str, reason: str, detail: object) -> None:
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        text = str(detail).replace('\n', ' ')[:300]
        with open(self.failed, 'a', encoding='utf-8', newline='\n') as fh:
            fh.write(f'{step},{reason},{text},{ts}\n')


# ---------------------------------------------------------------- 缺口判定
def trading_days(calendar: Path = CALENDAR) -> list[date]:
    """本地交易日历（``000001.SH`` 全部交易日期）。"""
    if not calendar.is_file():
        raise FileNotFoundError(f'交易日历缺失，无法求缺口集合: {calendar}')
    days = pl.read_parquet(calendar, columns=['date'])['date'].unique().sort().to_list()
    return [d for d in days if d >= SERIES_START]


def cb_expected(days: Iterable[date]) -> dict[int, list[date]]:
    """中债分片应有覆盖：``000001.SH`` 交易日 ∩ ``>= SWITCH``，按年分组。"""
    out: dict[int, list[date]] = {}
    for d in days:
        if d < SWITCH or d.year not in CB_YEARS:
            continue
        out.setdefault(d.year, []).append(d)
    return out


def shard_state(part: Path, expected: list[date]) -> str:
    """``missing``（无分片）/ ``gap``（有分片但缺应有交易日）/ ``ok``。"""
    if not part.is_file():
        return 'missing'
    have = set(pl.read_parquet(part, columns=['date'])['date'].to_list())
    return 'ok' if all(d in have for d in expected) else 'gap'


def shard_status(parts: Path, expected: dict[int, list[date]],
                 years: Iterable[int] | None = None) -> dict[int, str]:
    return {y: shard_state(parts / f'treasury_chinabond_{y}.parquet', expected[y])
            for y in sorted(expected) if years is None or y in set(years)}


# ---------------------------------------------------------------- 取数
def fetch_chinabond_year(year: int) -> pl.DataFrame:
    """中债官方 «中债国债收益率曲线» 10 年期，单位 %。"""
    import akshare as ak

    raw = ak.bond_china_yield(f'{year}0101', f'{year}1231')
    if raw is None or len(raw) == 0:
        raise ValueError('empty frame')
    gov = raw[raw['曲线名称'] == '中债国债收益率曲线'].copy()
    if len(gov) == 0:
        raise ValueError('no 中债国债收益率曲线')
    frame = pl.from_pandas(gov[['日期', '10年']]).rename({'日期': 'date', '10年': 'y10'})
    return frame.with_columns(
        pl.col('date').cast(pl.Utf8).str.strptime(pl.Date, '%Y-%m-%d'),
        pl.col('y10').cast(pl.Float64),
    ).drop_nulls('y10').sort('date')


def fetch_eastmoney() -> pl.DataFrame:
    """东财 «中国国债收益率10年» 全量，单位 %。"""
    import akshare as ak

    raw = ak.bond_zh_us_rate('20050101')
    if raw is None or len(raw) == 0:
        raise ValueError('empty frame')
    frame = pl.from_pandas(raw[['日期', '中国国债收益率10年']]).rename(
        {'日期': 'date', '中国国债收益率10年': 'y10'})
    return frame.with_columns(
        pl.col('date').cast(pl.Utf8).str.strptime(pl.Date, '%Y-%m-%d'),
        pl.col('y10').cast(pl.Float64),
    ).drop_nulls('y10').sort('date')


def collect_chinabond(parts: Path, expected: dict[int, list[date]],
                      fetch: Callable[[int], pl.DataFrame] = fetch_chinabond_year, *,
                      years: Iterable[int] | None = None, refresh_current: bool = False,
                      dry_run: bool = False, progress: Progress | None = None,
                      sleep: float = SLEEP) -> dict[int, str]:
    """按三态判定补抓 / 重取中债分年分片；返回抓取前的判定。"""
    parts.mkdir(parents=True, exist_ok=True)
    status = shard_status(parts, expected, years)
    newest_year = max(expected) if expected else None
    for year, state in status.items():
        part = parts / f'treasury_chinabond_{year}.parquet'
        if state == 'ok' and not (refresh_current and year == newest_year):
            print(f'treasury_chinabond_{year} skip（已覆盖应有交易日 {len(expected[year])} 天）')
            if progress:
                progress.skip(f'treasury_chinabond_{year}', f'ok {len(expected[year])} days')
            continue
        if dry_run:
            print(f'treasury_chinabond_{year} would-fetch（{state}）')
            continue
        try:
            frame = fetch(year)
        except Exception as exc:  # noqa: BLE001
            print(f'treasury_chinabond_{year} FAIL {type(exc).__name__}: {exc}')
            if progress:
                progress.fail(f'treasury_chinabond_{year}', type(exc).__name__, exc)
            time.sleep(sleep * 2)
            continue
        frame.write_parquet(part)
        have = set(frame['date'].to_list())
        still = [d for d in expected[year] if d not in have]
        span = f'{str(frame["date"].min())}..{str(frame["date"].max())}'
        print(f'treasury_chinabond_{year} {state}->fetched rows={frame.height} '
              f'{span} still_missing={len(still)}')
        if progress:
            progress.ok(f'treasury_chinabond_{year}', frame.height,
                        str(frame['date'].min()), str(frame['date'].max()),
                        f'{state} still_missing={len(still)}')
        time.sleep(sleep)
    return status


def collect_eastmoney(parts: Path, fetch: Callable[[], pl.DataFrame] = fetch_eastmoney, *,
                      dry_run: bool = False, progress: Progress | None = None) -> int:
    """东财全量每次重取（单请求返回全历史，无分片缺口可判）。"""
    parts.mkdir(parents=True, exist_ok=True)
    if dry_run:
        print('treasury_eastmoney would-fetch（全量单请求）')
        return 0
    try:
        frame = fetch()
    except Exception as exc:  # noqa: BLE001
        print(f'treasury_eastmoney FAIL {type(exc).__name__}: {exc}')
        if progress:
            progress.fail('treasury_eastmoney', type(exc).__name__, exc)
        return 0
    frame.write_parquet(parts / 'treasury_eastmoney.parquet')
    em_span = f'{str(frame["date"].min())}..{str(frame["date"].max())}'
    print(f'treasury_eastmoney fetched rows={frame.height} {em_span}')
    if progress:
        progress.ok('treasury_eastmoney', frame.height,
                    str(frame['date'].min()), str(frame['date'].max()), 'full refetch')
    return frame.height


# ---------------------------------------------------------------- 组装
def assemble(parts: Path) -> tuple[pl.DataFrame, dict]:
    """三段组装：``< SWITCH`` 全东财；``>= SWITCH`` 中债为主、东财补缺。"""
    shards = sorted(parts.glob('treasury_chinabond_*.parquet'))
    if not shards:
        raise FileNotFoundError(f'中债分片缺失，无法组装: {parts}')
    em_path = parts / 'treasury_eastmoney.parquet'
    if not em_path.is_file():
        raise FileNotFoundError(f'东财全量缺失，无法组装: {em_path}')
    cb = (pl.concat([pl.read_parquet(p).select('date', 'y10') for p in shards])
          .unique(subset='date', keep='first').sort('date'))
    em = pl.read_parquet(em_path).select('date', 'y10').sort('date')

    # 双源重叠日对拍（旧 build.py L35-52），只报告中位数/极值，不参与取值
    j = cb.join(em, on='date', how='inner', suffix='_em')
    diffs = [abs(float(a) - float(b))
             for a, b in zip(j['y10'].to_list(), j['y10_em'].to_list(), strict=True)
             if a is not None and b is not None]
    crosscheck = {
        'overlap_days': j.height,
        'n_abs_gt_1e-6': sum(1 for v in diffs if v > 1e-6),
        'n_abs_gt_1e-4': sum(1 for v in diffs if v > 1e-4),
        'n_abs_gt_1e-3': sum(1 for v in diffs if v > 1e-3),
        'max_abs_diff': max(diffs) if diffs else None,
        'mean_abs_diff': statistics.fmean(diffs) if diffs else None,
        'median_abs_diff': statistics.median(diffs) if diffs else None,
    }

    pre = em.filter(pl.col('date') < SWITCH).with_columns(pl.lit(EM_SOURCE).alias('source'))
    post_cb = cb.filter(pl.col('date') >= SWITCH).with_columns(pl.lit(CB_SOURCE).alias('source'))
    post_fill = (em.filter(pl.col('date') >= SWITCH).join(cb, on='date', how='anti')
                 .with_columns(pl.lit(EM_SOURCE).alias('source')))
    treasury = pl.concat([pre, post_cb, post_fill]).select(COLS).sort('date')

    assert treasury.height == pre.height + post_cb.height + post_fill.height, '三段重叠'
    assert treasury['date'].n_unique() == treasury.height, '组装后仍有重复日期'
    meta = {
        'chinabond_shards': len(shards),
        'chinabond_rows': cb.height,
        'eastmoney_rows': em.height,
        'segments': {'pre_eastmoney': pre.height, 'post_chinabond': post_cb.height,
                     'post_eastmoney_fill': post_fill.height},
        'crosscheck': crosscheck,
        'switch': str(SWITCH),
    }
    return treasury, meta


def coverage_report(treasury: pl.DataFrame, calendar: Path = CALENDAR) -> dict:
    """相对 ``000001.SH`` 交易日历的覆盖度（区间取本序列首末日）。"""
    cal = pl.read_parquet(calendar, columns=['date']).unique().sort('date')
    start, end = treasury['date'].min(), treasury['date'].max()
    window = cal.filter(pl.col('date').is_between(start, end))
    missing = window.join(treasury.select('date'), on='date', how='anti').sort('date')
    return {
        'rows': treasury.height,
        'start': str(start),
        'end': str(end),
        'calendar_days_in_window': window.height,
        'missing_vs_calendar': [str(d) for d in missing['date'].to_list()],
        'by_source': {r['source']: r['len'] for r in
                      treasury.group_by('source').len().sort('source').iter_rows(named=True)},
    }


# ---------------------------------------------------------------- 发布
def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open('rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def plan_publish(cand: pl.DataFrame, old: pl.DataFrame) -> dict:
    """追加计划（不落盘）：只取 ``date > 本地末行`` 的行，既有行一律不动。"""
    max_old = old['date'].max()
    new = cand.filter(pl.col('date') > max_old)
    return {
        'max_old': str(max_old),
        'rows_before': old.height,
        'rows_to_add': new.height,
        'rows_after': old.height + new.height,
        'add_dates': sorted({str(d) for d in new['date'].to_list()}),
        'add_rows': new.select(COLS).to_dicts(),
        'frame': new,
    }


def publish(cand: pl.DataFrame, *, yes: bool = False, stamp: str | None = None,
            target: Path = TARGET, target_csv: Path = TARGET_CSV,
            manifest_path: Path = MANIFEST, backup_root: Path = BACKUP_ROOT,
            report_path: Path | None = None) -> dict:
    """DG-11 备份（唯一时间戳位）→ 原子追加 → 断言 → 刷新 manifest。"""
    old = pl.read_parquet(target)
    plan = plan_publish(cand, old)
    if not yes:
        print(json.dumps({k: v for k, v in plan.items() if k != 'frame'},
                         ensure_ascii=False, indent=2, default=str))
        print('dry-run：未写任何文件（加 --yes 才发布）')
        return {'dry_run': True, **{k: v for k, v in plan.items() if k != 'frame'}}

    stamp = stamp or datetime.now().strftime('%Y%m%d_%H%M%S')
    # DG-13 路径口径：meta/ 层快照落 _backup/meta/<语义>_<日期>/
    backup = backup_root / 'meta' / f'macro_treasury_{stamp}'
    assert not backup.exists(), f'备份目录已存在，禁止复用: {backup}'
    backup.mkdir(parents=True)

    new = plan['frame']
    final = pl.concat([old, new], how='vertical_relaxed').sort('date')
    joined = final.join(old, on='date', how='inner', suffix='_old')
    report: dict = {
        'stamp': stamp,
        'backup_dir': str(backup),
        'rows_before': old.height,
        'rows_after': final.height,
        'added': new.height,
        'added_dates': plan['add_dates'],
        'value_changed': {c: int((~joined[c].eq_missing(joined[f'{c}_old'])).sum())
                          for c in VALUE_COLS},
        'source_changed': int((joined['source'] != joined['source_old']).sum()),
        'nulls_before': {c: int(old[c].null_count()) for c in old.columns},
        'nulls_after': {c: int(final[c].null_count()) for c in final.columns},
        'added_nulls': {c: int(new[c].null_count()) for c in new.columns},
    }
    assert all(v == 0 for v in report['value_changed'].values()), '既有行 y10 被改动'
    assert report['source_changed'] == 0, '既有行 source 被改动'
    for c in final.columns:
        delta = report['nulls_after'][c] - report['nulls_before'][c]
        assert delta == report['added_nulls'][c], f'{c} 空值增量与新增行不符'

    moved = {}
    for p in (target, target_csv, manifest_path):
        if not p.exists():
            continue
        before = sha256(p)
        shutil.move(str(p), backup / p.name)
        assert sha256(backup / p.name) == before, f'{p.name} 备份后 SHA 不一致'
        moved[p.name] = before
    report['dg11_moved_sha256'] = moved

    write_parquet_atomic(final, target)
    write_text_atomic(final.write_csv(), target_csv)
    report['final_sha256'] = sha256(target)

    manifest = json.loads((backup / 'manifest.json').read_text(encoding='utf-8'))
    manifest['as_of'] = str(final['date'].max())
    manifest['generated_at'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    manifest['produced_by'] = 'tools/treasury_yield.py（collect / build / publish）'
    manifest['files']['cn_treasury_yield_10y.parquet'].update(
        rows=final.height, coverage_end=str(final['date'].max()),
        sha256=report['final_sha256'], bytes=target.stat().st_size)
    meta = manifest['tables']['cn_treasury_yield_10y']
    seg = meta['source_segments'][0]
    seg['rows'] = int((final['source'] == CB_SOURCE).sum())
    seg['range'] = f'{SWITCH} ~ {final["date"].max()}'
    cover = coverage_report(final)
    meta['coverage_check'] = (
        f'{cover["start"]}~{cover["end"]} 区间内，000001.SH 的交易日共 '
        f'{cover["calendar_days_in_window"]} 天，缺失 {len(cover["missing_vs_calendar"])} 天')
    limits = [x for x in meta['known_limits'] if not x.startswith('国债 10Y 最新可得日')]
    limits.append(f'国债 10Y 最新可得日 {final["date"].max()}（采集于 '
                  f'{manifest["generated_at"]}）；该日之后无行表示源端尚未发布或尚未运行 collect')
    meta['known_limits'] = limits
    manifest['last_publish'] = {
        'tool': 'tools/treasury_yield.py publish',
        'stamp': stamp,
        'before_sha256': moved.get(target.name),
        'after_sha256': report['final_sha256'],
        'added_rows': new.height,
        'backup_dir': str(backup),
    }
    write_text_atomic(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n', manifest_path)

    report['ok'] = True
    if report_path:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str),
                               encoding='utf-8', newline='\n')
    print(json.dumps({k: report[k] for k in
                      ('rows_before', 'rows_after', 'added', 'added_dates')},
                     ensure_ascii=False, default=str))
    return report


# ---------------------------------------------------------------- CLI
def _parts_dir() -> Path:
    return staging_dir_for_store(STORE, 'treasury_yield') / 'parts'


def _work_dir() -> Path:
    return staging_dir_for_store(STORE, 'treasury_yield')


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest='cmd', required=True)

    c = sub.add_parser('collect', help='取数到 _staging/treasury_yield/parts/')
    c.add_argument('--dry-run', action='store_true', help='只打印缺口计划，不发请求')
    c.add_argument('--years', type=int, nargs='*', help='只处理指定年份分片')
    c.add_argument('--refresh-current', action='store_true', help='强制刷新最新年份分片')
    c.add_argument('--skip-eastmoney', action='store_true', help='跳过东财全量重取')

    b = sub.add_parser('build', help='分段组装 + 覆盖度校验 → 候选表')
    b.add_argument('--strict', action='store_true', help='覆盖度缺失非 0 时返回 2')

    p = sub.add_parser('publish', help='DG-11 备份 + 原子追加 + 刷新 manifest')
    p.add_argument('--yes', action='store_true', help='真正写 data/local_store（默认 dry-run）')
    p.add_argument('--stamp', help='备份时间戳（默认当前时间，禁止复用已存在目录）')

    args = ap.parse_args(argv)
    parts, work = _parts_dir(), _work_dir()

    if args.cmd == 'collect':
        progress = Progress(work)
        expected = cb_expected(trading_days())
        collect_chinabond(parts, expected, years=args.years,
                          refresh_current=args.refresh_current,
                          dry_run=args.dry_run, progress=progress)
        if not args.skip_eastmoney:
            collect_eastmoney(parts, dry_run=args.dry_run, progress=progress)
        return 0

    if args.cmd == 'build':
        treasury, meta = assemble(parts)
        cover = coverage_report(treasury)
        work.mkdir(parents=True, exist_ok=True)
        cand_path = work / 'cand_treasury_yield_10y.parquet'
        treasury.write_parquet(cand_path)
        (work / 'build_verify.json').write_text(
            json.dumps({'meta': meta, 'coverage': cover}, ensure_ascii=False, indent=2),
            encoding='utf-8', newline='\n')
        print(json.dumps({'rows': treasury.height, 'segments': meta['segments'],
                          'by_source': cover['by_source'],
                          'missing_vs_calendar': len(cover['missing_vs_calendar'])},
                         ensure_ascii=False))
        if args.strict and cover['missing_vs_calendar']:
            return 2
        return 0

    cand = pl.read_parquet(_work_dir() / 'cand_treasury_yield_10y.parquet')
    publish(cand, yes=args.yes, stamp=args.stamp,
            report_path=(_work_dir() / f'publish_{args.stamp or "now"}.json')
            if args.yes else None)
    return 0


if __name__ == '__main__':
    sys.exit(main())
