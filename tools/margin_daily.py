#!/usr/bin/env python3

"""沪深两融日频表（``cn_market_margin_daily``）的正式采集 / 构建 / 发布入口。

固化自六支一次性脚本，口径、源、字段一律不变，只做搬迁与整理：

- ``_staging/state_inputs_20261110/collect.py``       上交所分年 + 深交所东财主干
- ``_staging/state_inputs_20261110/fill_sz_gaps.py``  深交所官方单日补缺
- ``_staging/state_inputs_20261110/build.py``         B 段：汇总 + 覆盖度对拍
- ``_staging/sz_margin_sec_20261110/build_fill.py``   深市融券两列空值回填（字段级）
- ``_staging/sz_margin_sec_20261110/build_fix_0808.py`` 2024-08-08 整行零值缺陷修复
- ``_staging/derived_catchup_20261112/publish_macro.py``（两融部分：DG-11 + 追加发布）

子命令::

    python tools/margin_daily.py collect [--dry-run] [--years 2026] [--skip-sz]
    python tools/margin_daily.py build
    python tools/margin_daily.py publish [--yes]

``publish`` 默认 dry-run，只打印待追加行与备份落点；加 ``--yes`` 才落
``data/local_store/meta/macro/``。

与旧脚本的关键差异（本次修复）：旧 ``collect.py`` 用 ``if target.exists(): continue``
做断点，分片一旦落地就永不刷新，源端补发布一天即永久滞后。现改为按
「应有交易日集合 − 已有日期集合」求缺：缺日补、覆盖不全的分片重取、
``--refresh-current`` 可强制刷新当年分片。日历以 ``000001.SH`` 交易日为准
（与 ``build.py`` 的覆盖度参照同一份数据）。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
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
TARGET = TARGET_DIR / 'cn_market_margin_daily.parquet'
TARGET_CSV = TARGET_DIR / 'cn_market_margin_daily.csv'
MANIFEST = TARGET_DIR / 'manifest.json'
BACKUP_ROOT = ROOT / '_backup'
CALENDAR = STORE / 'quotes' / 'index' / 'sh' / '000001.SH.parquet'
#: 深交所官方逐日汇总（1,322 个交易日，原生 亿元/亿股）。不可低成本重建（1,322 次串行
#: 请求），按 DG-32 小输入规则进 Git；``_staging/sz_margin_sec_20261110/parts/`` 同内容保留。
SZ_OFFICIAL_DAYS = ROOT / 'docs' / 'evidence' / 'sz_margin_sec_20261110' / 'szse_gap_days.jsonl'
#: 东财深市整行零值缺陷日；仅该行 4 个金额格按官方回填
DEFECT_ROW = date(2024, 8, 8)
DEFECT_COLS = ('fin_buy', 'fin_balance', 'sec_balance', 'total_balance')

#: 两融序列起点；早于此日不要求覆盖
SERIES_START = date(2010, 3, 31)

SLEEP = 1.5
COLS = ['date', 'fin_buy', 'fin_balance', 'sec_sell_qty', 'sec_qty', 'sec_balance',
        'total_balance']
VALUE_COLS = COLS[1:]

SSE_COLS = {'信用交易日期': 'date', '融资余额': 'fin_balance', '融资买入额': 'fin_buy',
            '融券余量': 'sec_qty', '融券余量金额': 'sec_balance',
            '融券卖出量': 'sec_sell_qty', '融资融券余额': 'total_balance'}
SZ_MACRO_COLS = {'日期': 'date', '融资买入额': 'fin_buy', '融资余额': 'fin_balance',
                 '融券卖出量': 'sec_sell_qty', '融券余量': 'sec_qty',
                 '融券余额': 'sec_balance', '融资融券余额': 'total_balance'}
SZ_GAP_MAP = {'融资买入额': 'fin_buy', '融资余额': 'fin_balance', '融券卖出量': 'sec_sell_qty',
              '融券余量': 'sec_qty', '融券余额': 'sec_balance', '融资融券余额': 'total_balance'}


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


def expected_by_year(days: Iterable[date]) -> dict[int, list[date]]:
    out: dict[int, list[date]] = {}
    for d in days:
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
    return {y: shard_state(parts / f'margin_sse_{y}.parquet', expected[y])
            for y in sorted(expected) if years is None or y in set(years)}


# ---------------------------------------------------------------- 取数
def fetch_sse_year(year: int) -> pl.DataFrame:
    import akshare as ak

    raw = ak.stock_margin_sse(f'{year}0101', f'{year}1231')
    if raw is None or len(raw) == 0:
        raise ValueError('empty frame')
    frame = pl.from_pandas(raw[list(SSE_COLS)]).rename(SSE_COLS)
    return frame.with_columns(
        pl.col('date').cast(pl.Utf8).str.strptime(pl.Date, '%Y%m%d'),
        *[pl.col(c).cast(pl.Float64) for c in VALUE_COLS],
    ).drop_nulls('fin_balance').sort('date')


def fetch_sz_macro() -> pl.DataFrame:
    import akshare as ak

    raw = ak.macro_china_market_margin_sz()
    if raw is None or len(raw) == 0:
        raise ValueError('empty frame')
    frame = pl.from_pandas(raw[list(SZ_MACRO_COLS)]).rename(SZ_MACRO_COLS)
    return frame.with_columns(
        pl.col('date').cast(pl.Utf8).str.strptime(pl.Date, '%Y-%m-%d'),
        *[pl.col(c).cast(pl.Float64) for c in VALUE_COLS],
    ).drop_nulls('fin_balance').sort('date')


def fetch_sz_day(day: date) -> dict:
    import akshare as ak

    raw = ak.stock_margin_szse(day.strftime('%Y%m%d'))
    if raw is None or len(raw) == 0:
        raise ValueError('empty frame')
    rec: dict = {'date': str(day), 'unit': '100M_yuan'}
    for c in raw.columns:
        v = raw.iloc[0][c]
        rec[str(c)] = None if v is None else float(v)
    return rec


# ---------------------------------------------------------------- collect
def collect_sse(parts: Path, expected: dict[int, list[date]],
                fetch: Callable[[int], pl.DataFrame] = fetch_sse_year, *,
                years: Iterable[int] | None = None, refresh_current: bool = False,
                dry_run: bool = False, sleep: float = SLEEP,
                progress: Progress | None = None) -> dict[int, str]:
    """按缺口重取上交所分年分片，返回本次判定结果（按年）。"""
    status = shard_status(parts, expected, years)
    parts.mkdir(parents=True, exist_ok=True)
    newest_year = max(expected) if expected else None
    for year, state in status.items():
        part = parts / f'margin_sse_{year}.parquet'
        if state == 'ok' and not (refresh_current and year == newest_year):
            if progress:
                progress.skip(f'margin_sse_{year}', 'covered')
            print(f'margin_sse_{year} skip（已覆盖应有交易日）')
            continue
        if dry_run:
            print(f'margin_sse_{year} would-fetch（{state}）')
            continue
        try:
            frame = fetch(year)
        except Exception as exc:  # noqa: BLE001
            if progress:
                progress.fail(f'margin_sse_{year}', 'fetch_error', f'{type(exc).__name__}: {exc}')
            print(f'margin_sse_{year} FAIL {type(exc).__name__}: {exc}')
            time.sleep(sleep * 2)
            continue
        frame.write_parquet(part)
        missing_after = [d for d in expected[year]
                         if d not in set(frame['date'].to_list())]
        span = f'{str(frame["date"].min())}..{str(frame["date"].max())}'
        if progress:
            progress.ok(f'margin_sse_{year}', len(frame), frame['date'].min(),
                        frame['date'].max(),
                        f'{state}; unit=yuan src=SSE official; still_missing={len(missing_after)}')
        print(f'margin_sse_{year} {state}->fetched rows={len(frame)} '
              f'{span} still_missing={len(missing_after)}')
        time.sleep(sleep)
    return status


def collect_sz_macro(parts: Path, fetch: Callable[[], pl.DataFrame] = fetch_sz_macro, *,
                     dry_run: bool = False, progress: Progress | None = None) -> int:
    """深交所东财主干：单次全量，每次重取（不做 exists→skip）。"""
    part = parts / 'margin_szse_macro.parquet'
    if dry_run:
        print(f'margin_szse_macro would-fetch（现 {part.name} '
              f'{"存在" if part.is_file() else "缺失"}）')
        return 0
    try:
        frame = fetch()
    except Exception as exc:  # noqa: BLE001
        if progress:
            progress.fail('margin_szse_macro', 'fetch_error', f'{type(exc).__name__}: {exc}')
        print(f'margin_szse_macro FAIL {type(exc).__name__}: {exc}')
        return 0
    parts.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(part)
    if progress:
        progress.ok('margin_szse_macro', len(frame), frame['date'].min(), frame['date'].max(),
                    'unit=yuan src=eastmoney macro')
    print(f'margin_szse_macro rows={len(frame)} '
          f'{str(frame["date"].min())}..{str(frame["date"].max())}')
    return len(frame)


def sz_gap_days(sh_dates: Iterable[date], sz_dates: Iterable[date],
                done: Iterable[str] = ()) -> list[date]:
    """深市缺口日 = 沪市已有 − 东财主干已有 − ��已完成。"""
    done_set = {str(d) for d in done}
    have = set(sz_dates)
    return sorted(d for d in set(sh_dates) - have if str(d) not in done_set)


def collect_sz_gaps(parts: Path, fetch: Callable[[date], dict] = fetch_sz_day, *,
                    done: Iterable[str] = (), sh_dates: Iterable[date] | None = None,
                    dry_run: bool = False, sleep: float = SLEEP,
                    max_empty_run: int = 5,
                    progress: Progress | None = None) -> list[date]:
    """深交所官方单日补缺，逐日追加 ``parts/szse_official_days.jsonl``。"""
    out = parts / 'szse_official_days.jsonl'
    sz_part = parts / 'margin_szse_macro.parquet'
    if sh_dates is None:
        sh_dates = []
        for p in sorted(parts.glob('margin_sse_*.parquet')):
            sh_dates.extend(pl.read_parquet(p, columns=['date'])['date'].to_list())
    sz_dates = (pl.read_parquet(sz_part, columns=['date'])['date'].to_list()
                if sz_part.is_file() else [])
    todo = sz_gap_days(sh_dates, sz_dates, done)
    print(f'深市缺口 {len(todo)} 天: {[str(d) for d in todo[:8]]}'
          f'{" ..." if len(todo) > 8 else ""}')
    if dry_run:
        return todo
    parts.mkdir(parents=True, exist_ok=True)
    empty_run = 0
    ok = fail = 0
    for d in todo:
        step = f'szse_official_day_{d:%Y%m%d}'
        try:
            rec = fetch(d)
        except Exception as exc:  # noqa: BLE001
            if progress:
                progress.fail(step, 'fetch_error', f'{type(exc).__name__}: {exc}')
            fail += 1
            time.sleep(sleep * 2)
            continue
        if not rec:
            empty_run += 1
            if progress:
                progress.fail(step, 'empty', 'empty frame')
            if empty_run >= max_empty_run:
                print('连续空返回达上限，停止压源')
                break
            time.sleep(sleep)
            continue
        empty_run = 0
        with open(out, 'a', encoding='utf-8', newline='\n') as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + '\n')
        if progress:
            progress.ok(step, 1, d, d, 'szse official single-day, unit=100M yuan')
        ok += 1
        time.sleep(sleep)
    print(f'深市补缺完成 ok={ok} fail={fail}')
    return todo


# ---------------------------------------------------------------- build
def assemble(parts: Path) -> tuple[pl.DataFrame, dict]:
    """合并沪深两市 → 候选表；口径与旧 build.py 的 B 段一致。"""
    sse_parts = sorted(parts.glob('margin_sse_*.parquet'))
    if not sse_parts:
        raise FileNotFoundError(f'上交所分片缺失: {parts}/margin_sse_*.parquet')
    sse = (pl.concat([pl.read_parquet(p) for p in sse_parts])
           .unique(subset='date', keep='first').sort('date'))
    sz = pl.read_parquet(parts / 'margin_szse_macro.parquet')
    sh = sse.select(COLS).with_columns(pl.lit('sh').alias('market'),
                                       pl.lit('sse:stock_margin_sse').alias('source'))

    gap_path = parts / 'szse_official_days.jsonl'
    gap_rows = ([json.loads(x) for x in gap_path.read_text(encoding='utf-8').splitlines()
                 if x.strip()] if gap_path.is_file() else [])
    if gap_rows:
        gap = pl.DataFrame([{
            'date': date.fromisoformat(r['date']),
            **{v: (None if r.get(k) is None else float(r[k]) * 1e8)
               for k, v in SZ_GAP_MAP.items()},
        } for r in gap_rows]).with_columns(pl.col('date').cast(pl.Date)).sort('date')
    else:
        gap = pl.DataFrame(schema={c: (pl.Date if c == 'date' else pl.Float64) for c in COLS})
    szz = pl.concat([
        sz.select(COLS).with_columns(
            pl.lit('sz').alias('market'),
            pl.lit('eastmoney:macro_china_market_margin_sz').alias('source')),
        gap.select(COLS).with_columns(
            pl.lit('sz').alias('market'),
            pl.lit('szse:stock_margin_szse').alias('source')),
    ]).unique(subset='date', keep='first').sort('date')
    margin = (pl.concat([sh, szz]).select(['date', 'market', 'source'] + VALUE_COLS)
              .sort(['market', 'date']))
    meta = {'sse_parts': len(sse_parts), 'sse_rows': sse.height, 'sz_rows': sz.height,
            'sz_gap_rows': gap.height}
    return margin, meta


def coverage_report(margin: pl.DataFrame, calendar: Path = CALENDAR) -> dict:
    """相对 ``000001.SH`` 交易日历的覆盖度（与旧 build.py 的 C 段同一参照）。"""
    cal = pl.read_parquet(calendar, columns=['date']).unique().sort('date')
    report: dict = {}
    for m in ('sh', 'sz'):
        sub = margin.filter(pl.col('market') == m)
        span = cal.filter(pl.col('date').is_between(sub['date'].min(), sub['date'].max()))
        miss = span.join(sub.select('date').unique(), on='date', how='anti').sort('date')
        report[m] = {
            'rows': sub.height,
            'start': str(sub['date'].min()),
            'end': str(sub['date'].max()),
            'missing_vs_calendar': [str(d) for d in miss['date'].to_list()],
            'nulls': {c: int(sub[c].null_count()) for c in VALUE_COLS},
            'source': {r['source']: r['len'] for r in
                       sub.group_by('source').len().sort('source').iter_rows(named=True)},
        }
    return report


# ---------------------------------------------------------------- 字段级回填
def apply_official_patches(margin: pl.DataFrame,
                          days_path: Path = SZ_OFFICIAL_DAYS) -> tuple[pl.DataFrame, dict]:
    """深市融券两列空值回填 + 2024-08-08 整行零值缺陷修复（口径与已入库结果一致）。

    来源：``szse:stock_margin_szse`` 逐日汇总（原生 亿元/亿股，×1e8）。
    只补空值、只改缺陷行 4 格，未命中一律 ``otherwise(原值)``（DG-31 显式兜底）。
    """
    if not days_path.is_file():
        raise FileNotFoundError(f'深市官方逐日输入缺失（不可重建）: {days_path}')
    raw = [json.loads(x) for x in days_path.read_text(encoding='utf-8').splitlines() if x.strip()]
    off = pl.DataFrame([{
        'date': date.fromisoformat(r['date']),
        **{f'off_{k}': (None if r.get(k) is None else float(r[k]) * 1e8)
           for k in ('sec_qty', 'sec_sell_qty', *DEFECT_COLS)},
    } for r in raw]).with_columns(pl.col('date').cast(pl.Date)).unique(
        subset='date', keep='first').sort('date')

    df = margin.join(off, on='date', how='left')
    for col in ('sec_qty', 'sec_sell_qty'):
        df = df.with_columns(
            pl.when(pl.col('market') == 'sz')
            .then(pl.when(pl.col(col).is_null() & pl.col(f'off_{col}').is_not_null())
                  .then(pl.col(f'off_{col}')).otherwise(pl.col(col)))
            .otherwise(pl.col(col)).alias(col))
    for col in DEFECT_COLS:
        df = df.with_columns(
            pl.when((pl.col('market') == 'sz') & (pl.col('date') == DEFECT_ROW)
                    & pl.col(f'off_{col}').is_not_null())
            .then(pl.col(f'off_{col}')).otherwise(pl.col(col)).alias(col))

    out = df.select(margin.columns).sort(['market', 'date'])
    assert out.height == margin.height
    joined = out.join(margin, on=['date', 'market'], how='inner', suffix='_old')
    changed = {c: int((~joined[c].eq_missing(joined[f'{c}_old'])).sum()) for c in VALUE_COLS}
    stats = {
        'official_days': off.height,
        'official_range': [str(off['date'].min()), str(off['date'].max())],
        'changed_by_column': changed,
        'defect_row': str(DEFECT_ROW),
        'defect_cols_changed': {c: changed[c] for c in DEFECT_COLS},
        'rows_still_null_sec': int(out.filter(
            (pl.col('market') == 'sz') & pl.col('sec_qty').is_null()).height),
    }
    # 只允许两类改动：融券两列的空值回填（行数不定但两列必须相等）、缺陷行 4 格各 1 处
    assert changed['sec_qty'] == changed['sec_sell_qty'], f'两列回填数不一致: {changed}'
    assert changed['sec_qty'] <= off.height, f'回填数超出官方覆盖天数: {changed}'
    assert all(changed[c] == 1 for c in DEFECT_COLS), f'缺陷行回填格数不等于 1: {changed}'
    untouched = [c for c in VALUE_COLS
                 if c not in ('sec_qty', 'sec_sell_qty', *DEFECT_COLS)]
    assert all(changed[c] == 0 for c in untouched), f'回填改动了目标外单元格: {changed}'
    return out, stats


# ---------------------------------------------------------------- publish
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
        'add_rows': new.select(['date', 'market', 'fin_buy', 'fin_balance']).to_dicts(),
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
    backup = backup_root / 'meta' / f'macro_margin_{stamp}'
    assert not backup.exists(), f'备份目录已存在，禁止复用: {backup}'
    backup.mkdir(parents=True)

    new = plan['frame']
    final = pl.concat([old, new], how='vertical_relaxed').sort(['market', 'date'])
    joined = final.join(old, on=['date', 'market'], how='inner', suffix='_old')
    report: dict = {
        'stamp': stamp,
        'backup_dir': str(backup),
        'rows_before': old.height,
        'rows_after': final.height,
        'added': new.height,
        'added_dates': plan['add_dates'],
        'value_changed': {c: int((~joined[c].eq_missing(joined[f'{c}_old'])).sum())
                          for c in VALUE_COLS},
        'nulls_before': {c: int(old[c].null_count()) for c in old.columns},
        'nulls_after': {c: int(final[c].null_count()) for c in final.columns},
        'added_nulls': {c: int(new[c].null_count()) for c in new.columns},
    }
    assert all(v == 0 for v in report['value_changed'].values()), '既有行被改动'
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
    manifest['produced_by'] = 'tools/margin_daily.py（collect / build / publish）'
    manifest['files']['cn_market_margin_daily.parquet'].update(
        rows=final.height, coverage_end=str(final['date'].max()),
        sha256=report['final_sha256'], bytes=target.stat().st_size)
    meta = manifest['tables']['cn_market_margin_daily']
    sh_rows = int((final['market'] == 'sh').sum())
    sz_rows = int((final['market'] == 'sz').sum())
    meta['source_segments']['sh'][0]['rows'] = sh_rows
    meta['source_segments']['sz'][0]['rows'] = int(
        (final['source'] == 'eastmoney:macro_china_market_margin_sz').sum())
    meta['source_segments']['sz'][1]['rows'] = int(
        (final['source'] == 'szse:stock_margin_szse').sum())
    cover = coverage_report(final)
    meta['coverage_check'] = (
        f'沪深各 {sh_rows}/{sz_rows} 行，区间 {final["date"].min()}~{final["date"].max()}；'
        f'相对 000001.SH 交易日历缺失 sh={len(cover["sh"]["missing_vs_calendar"])} 天、'
        f'sz={len(cover["sz"]["missing_vs_calendar"])} 天')
    limits = [x for x in meta['known_limits']
              if not x.startswith('沪深两融最新可得日')]
    limits.append(f'沪深两融最新可得日 {final["date"].max()}（采集于 '
                  f'{manifest["generated_at"]}）；该日之后无行表示源端尚未发布或尚未运行 collect')
    meta['known_limits'] = limits
    manifest['last_publish'] = {
        'tool': 'tools/margin_daily.py publish',
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
    return staging_dir_for_store(STORE, 'margin_daily') / 'parts'


def _work_dir() -> Path:
    return staging_dir_for_store(STORE, 'margin_daily')


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest='cmd', required=True)

    c = sub.add_parser('collect', help='取数到 _staging/margin_daily/parts/')
    c.add_argument('--dry-run', action='store_true', help='只打印缺口计划，不发请求')
    c.add_argument('--years', type=int, nargs='*', help='只处理指定年份分片')
    c.add_argument('--skip-sz', action='store_true', help='跳过深交所两条链路')
    c.add_argument('--refresh-current', action='store_true', help='强制刷新最新年份分片')

    b = sub.add_parser('build', help='合并 + 覆盖度校验 → 候选表')
    b.add_argument('--strict', action='store_true', help='覆盖度缺失非 0 时返回 2')

    p = sub.add_parser('publish', help='DG-11 备份 + 原子追加 + 刷新 manifest')
    p.add_argument('--yes', action='store_true', help='真正写 data/local_store（默认 dry-run）')
    p.add_argument('--stamp', help='备份时间戳（默认当前时间，禁止复用已存在目录）')

    args = ap.parse_args(argv)
    parts, work = _parts_dir(), _work_dir()

    if args.cmd == 'collect':
        progress = Progress(work)
        expected = expected_by_year(trading_days())
        collect_sse(parts, expected, years=args.years, refresh_current=args.refresh_current,
                    dry_run=args.dry_run, progress=progress)
        if not args.skip_sz:
            collect_sz_macro(parts, dry_run=args.dry_run, progress=progress)
            done = []
            gap_path = parts / 'szse_official_days.jsonl'
            if gap_path.is_file():
                done = [json.loads(x)['date'] for x in
                        gap_path.read_text(encoding='utf-8').splitlines() if x.strip()]
            collect_sz_gaps(parts, done=done, dry_run=args.dry_run, progress=progress)
        return 0

    if args.cmd == 'build':
        margin, meta = assemble(parts)
        margin, patch = apply_official_patches(margin)
        cover = coverage_report(margin)
        work.mkdir(parents=True, exist_ok=True)
        cand_path = work / 'cand_market_margin.parquet'
        margin.write_parquet(cand_path)
        (work / 'build_verify.json').write_text(
            json.dumps({'meta': meta, 'patches': patch, 'coverage': cover},
                       ensure_ascii=False, indent=2),
            encoding='utf-8', newline='\n')
        print(json.dumps({'rows': margin.height, 'by_market': {
            m: cover[m]['rows'] for m in ('sh', 'sz')},
            'missing_vs_calendar': {m: len(cover[m]['missing_vs_calendar'])
                                    for m in ('sh', 'sz')}}, ensure_ascii=False))
        if args.strict and any(cover[m]['missing_vs_calendar'] for m in ('sh', 'sz')):
            return 2
        return 0

    cand = pl.read_parquet(_work_dir() / 'cand_market_margin.parquet')
    publish(cand, yes=args.yes, stamp=args.stamp,
            report_path=(_work_dir() / f'publish_{args.stamp or "now"}.json')
            if args.yes else None)
    return 0


if __name__ == '__main__':
    sys.exit(main())
