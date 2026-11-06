#!/usr/bin/env python3

r"""外部依赖数据契约的本地构建器（停复牌 / 行业 PIT / 指数成分历史）。

背景
====
``extended_contracts.py`` 只负责**登记缺口**，不产生数据，因此三项契约长期
停留在 ``missing``。本模块补上生产侧：把能从本地既有资产严格派生出来的部分
真正做出来，派不出来的部分如实标注不完整——绝不用当前快照回填历史，那正是
前视偏差的来源。

三份产物
--------
- **停复牌日表** ``meta/suspension/suspension_daily.parquet``
  口径：``交易日历 − 该标的实际有 bar 的交易日``。通达信 ``.day`` 不为停牌日
  生成记录，缺口即停牌；但与源端漏数不可区分，故给出 ``confidence``。
  区间取每只标的自身的 ``[首个 bar, 末个 bar]``，可声明 ``complete=true``。
- **行业 PIT 区间表** ``meta/industry_pit/industry_pit.parquet``
  口径：对 ``meta/industry/industry_YYYYMMDD.parquet`` 日快照做变更点抽取，
  只写生效日与失效日。本地快照只有最近几天，故 ``complete=false``，契约
  如实保持 ``partial``。
- **指数成分进出区间表** ``meta/index_membership/membership_intervals.parquet``
  口径：每日抓一次中证指数官方成分快照（``snapshots/``），再对快照序列做
  进出差分。首份快照的纳入日未知 -> ``in_date_known=false``，不假装知道。

约定
====
- 全部默认 **dry-run**，``--commit`` 才写 ``data/**``；联网还需
  ``--allow-remote``。
- 写入一律 ``.tmp`` + ``os.replace``，产物与覆盖声明同时落盘。
- 只新增 ``meta/`` 下的派生文件，不改行情/因子等原始资产。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import polars as pl
from loguru import logger
from polars.exceptions import PolarsError

from src.data.local_store.paths import default_store_root
from src.data.quality.extended_contracts import CONTRACTS
from src.data.trading_calendar import get_calendar

_SPEC = {spec.key: spec for spec in CONTRACTS}

#: 停复牌表默认覆盖的资产类型（指数不会停牌，故排除）。
DEFAULT_ASSET_CLASSES: tuple[str, ...] = ('stock', 'fund')
#: 默认抓取的指数（akshare 代码, ts_code）。
DEFAULT_INDICES: tuple[tuple[str, str], ...] = (
    ('000300', '000300.SH'),
    ('000905', '000905.SH'),
    ('000852', '000852.SH'),
    ('000016', '000016.SH'),
)
_EXCHANGE_TO_MARKET = {
    '上海证券交易所': 'SH',
    '深圳证券交易所': 'SZ',
    '北京证券交易所': 'BJ',
}

_SUSPENSION_SCHEMA: dict[str, Any] = {
    'ts_code': pl.String, 'trade_date': pl.Date, 'asset_class': pl.String,
    'market': pl.String, 'is_suspended': pl.Boolean, 'run_id': pl.String,
    'run_len': pl.Int32, 'run_pos': pl.Int32, 'confidence': pl.String,
    'source': pl.String, 'detected_at': pl.Date,
}
_INDUSTRY_SCHEMA: dict[str, Any] = {
    'ts_code': pl.String, 'effective_date': pl.Date, 'end_date': pl.Date,
    'industry_code': pl.String, 'industry_name': pl.String,
    'level': pl.String, 'source': pl.String,
}
_MEMBERSHIP_SCHEMA: dict[str, Any] = {
    'interval_id': pl.String, 'index_code': pl.String, 'ts_code': pl.String,
    'in_date': pl.Date, 'out_date': pl.Date, 'in_date_known': pl.Boolean,
    'out_date_known': pl.Boolean, 'source': pl.String,
}


def _path(root: Path, key: str) -> Path:
    return root / _SPEC[key].path


def _coverage_path(root: Path, key: str) -> Path:
    return root / _SPEC[key].coverage_path


def _frame(rows: list[tuple[Any, ...]], schema: dict[str, Any]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema, orient='row')


def _write_frame(path: Path, frame: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    frame.write_parquet(tmp)
    os.replace(tmp, path)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + '\n',
                   encoding='utf-8')
    os.replace(tmp, path)


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _quote_dates(path: Path) -> list[date]:
    """读取单个行情文件的 ``date`` 列（缺失/损坏返回空列表）。"""
    try:
        frame = pl.read_parquet(path, columns=['date'])
    except (OSError, ValueError, TypeError, PolarsError) as exc:
        logger.warning(f'行情日期列读取失败 {path.name}: {exc}')
        return []
    return [d for d in frame['date'].to_list() if isinstance(d, date)]


def _runs(positions: np.ndarray) -> list[tuple[int, int]]:
    """把升序位置数组切成连续段 ``[(start, end), ...]``（闭区间）。"""
    if positions.size == 0:
        return []
    runs: list[tuple[int, int]] = []
    start = previous = int(positions[0])
    for raw in positions[1:]:
        value = int(raw)
        if value == previous + 1:
            previous = value
            continue
        runs.append((start, previous))
        start = previous = value
    runs.append((start, previous))
    return runs


def build_suspension(
    root: Path | str, *, as_of: date | None = None,
    asset_classes: Sequence[str] = DEFAULT_ASSET_CLASSES,
    commit: bool = False,
) -> dict[str, Any]:
    """由「交易日历 − 实际 bar 日」派生停复牌日表（只写停牌行）。"""
    store = Path(root)
    index_path = store / 'meta' / 'daily_index.parquet'
    if not index_path.is_file():
        raise FileNotFoundError(f'缺少行情索引，无法派生停牌表: {index_path}')

    calendar = get_calendar(store, as_of=as_of)
    horizon = calendar.authoritative_end or (
        calendar.dates[-1] if calendar.dates else None)
    if horizon is None:
        raise ValueError('交易日历为空，无法派生停牌表')
    position_of = {day: pos for pos, day in enumerate(calendar.dates)}

    index = pl.read_parquet(index_path).filter(
        pl.col('data_type').is_in(list(asset_classes)))
    if index.is_empty():
        raise ValueError(f'无 {list(asset_classes)} 类标的，无法派生停牌表')

    from src.data.quality.stale_status import stale_codes  # noqa: PLC0415

    stale = stale_codes(store)
    detected_at = as_of or date.today()
    limit = position_of[horizon]

    rows: list[tuple[Any, ...]] = []
    covered = 0
    skipped = 0
    for item in index.iter_rows(named=True):
        ts_code = str(item['ts_code'])
        data_type = str(item['data_type'])
        market = str(item['market']).lower()
        source_path = store / 'quotes' / data_type / market / f'{ts_code}.parquet'
        days = _quote_dates(source_path) if source_path.is_file() else []
        seq = sorted({position_of[d] for d in days if d in position_of})
        if not seq:
            skipped += 1
            continue
        covered += 1
        low, high = seq[0], min(seq[-1], limit)
        if high <= low:
            continue
        expected = np.arange(low, high + 1, dtype=np.int64)
        missing = np.setdiff1d(expected, np.asarray(seq, dtype=np.int64))
        if missing.size == 0:
            continue
        confidence = 'low' if ts_code in stale else 'high'
        for run_no, (start, end) in enumerate(_runs(missing), start=1):
            run_len = int(end - start + 1)
            run_id = f'{ts_code}#{run_no}'
            for offset, pos in enumerate(range(int(start), int(end) + 1), start=1):
                rows.append((
                    ts_code, calendar.dates[pos], data_type, market, True,
                    run_id, run_len, offset, confidence, 'quote_gap',
                    detected_at,
                ))

    frame = _frame(rows, _SUSPENSION_SCHEMA)
    low_confidence = (int(frame.filter(pl.col('confidence') == 'low').height)
                      if frame.height else 0)
    summary: dict[str, Any] = {
        'dataset': 'suspension',
        'codes_covered': covered,
        'codes_skipped': skipped,
        'suspended_rows': frame.height,
        'suspended_codes': frame['ts_code'].n_unique() if frame.height else 0,
        'low_confidence_rows': low_confidence,
        'stale_codes': len(stale),
        'calendar_source': calendar.source,
        'start': calendar.dates[0].isoformat() if calendar.dates else None,
        'end': horizon.isoformat(),
        'committed': bool(commit),
    }
    if not commit:
        return summary

    _write_frame(_path(store, 'suspension'), frame)
    _write_json(_coverage_path(store, 'suspension'), {
        'dataset': 'suspension',
        'method': 'derived_from_quote_gap',
        'authority': 'derived',
        'start': summary['start'],
        'end': summary['end'],
        'complete': True,
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'calendar_source': calendar.source,
        'codes_covered': covered,
        'codes_skipped': skipped,
        'suspended_rows': frame.height,
        'suspended_codes': summary['suspended_codes'],
        'low_confidence_rows': low_confidence,
        'caveats': [
            '停牌与源端漏数在行情缺口上不可区分：confidence=low 的标的'
            '（stale_status 已登记停更）缺口可靠性较低',
            '交易决策应只用 confidence=high 的子集；low 子集只适合'
            '「该日无成交」这类宽松判断',
            '指数标的未纳入（指数不停牌）；基金仅覆盖本地有行情者',
            '缺口口径依赖统一交易日历，基准指数日历退化时口径随之退化',
            '本表只登记停牌行，查询时「无记录」等价于「当日有 bar」',
        ],
    })
    logger.info(f'[extended] 停复牌表已落盘 {frame.height} 行，'
                f'覆盖 {covered} 只标的（跳读 {skipped}）')
    return summary


def _industry_snapshots(root: Path) -> list[tuple[date, Path]]:
    """``industry_YYYYMMDD[_vN].parquet`` -> [(日期, 路径)]，同日取最后写入。"""
    directory = root / 'meta' / 'industry'
    if not directory.is_dir():
        return []
    by_day: dict[date, Path] = {}
    for path in sorted(directory.glob('industry_*.parquet')):
        token = path.stem.split('_', 1)[1][:8]
        try:
            day = datetime.strptime(token, '%Y%m%d').date()
        except ValueError:
            continue
        current = by_day.get(day)
        if current is None or path.stat().st_mtime_ns >= current.stat().st_mtime_ns:
            by_day[day] = path
    return sorted(by_day.items())


def build_industry_pit(root: Path | str, *, commit: bool = False) -> dict[str, Any]:
    """把行业日快照抽成 PIT 区间（只在变更点切段，不做历史外推）。"""
    store = Path(root)
    snapshots = _industry_snapshots(store)
    if not snapshots:
        raise FileNotFoundError('meta/industry 下没有 industry_YYYYMMDD.parquet 快照')

    history: dict[str, list[tuple[date, str, str, str, str]]] = {}
    for day, path in snapshots:
        try:
            frame = pl.read_parquet(path, columns=[
                'code', 'industry_l1', 'industry_l2', 'industry_code', 'source'])
        except (OSError, ValueError, TypeError, PolarsError) as exc:
            logger.warning(f'行业快照不可读，跳过 {path.name}: {exc}')
            continue
        for row in frame.iter_rows(named=True):
            raw_code = str(row['industry_code'] or 'unknown')
            if raw_code == 'unknown' or not raw_code.startswith('T'):
                continue
            history.setdefault(str(row['code']), []).append((
                day, raw_code, str(row['industry_l1'] or 'unknown'),
                str(row['industry_l2'] or 'unknown'), str(row['source'] or ''),
            ))

    rows: list[tuple[Any, ...]] = []
    for code, items in history.items():
        items.sort(key=lambda item: item[0])
        for level, prefix in (('L1', 3), ('L2', 5)):
            open_day: date | None = None
            open_code = open_name = open_source = ''
            current = ''
            for day, raw_code, l1, l2, source in items:
                code_at_level = raw_code[:prefix]
                if code_at_level == current:
                    continue
                if open_day is not None:
                    rows.append((code, open_day, day, open_code, open_name,
                                 level, open_source))
                open_day = day
                open_code = code_at_level
                open_name = l1 if level == 'L1' else l2
                open_source = source
                current = code_at_level
            if open_day is not None:
                rows.append((code, open_day, None, open_code, open_name,
                             level, open_source))

    frame = _frame(rows, _INDUSTRY_SCHEMA)
    first_day, last_day = snapshots[0][0], snapshots[-1][0]
    summary: dict[str, Any] = {
        'dataset': 'industry_pit',
        'snapshots': len(snapshots),
        'codes': len(history),
        'intervals': frame.height,
        'start': first_day.isoformat(),
        'end': last_day.isoformat(),
        'committed': bool(commit),
    }
    if not commit:
        return summary

    _write_frame(_path(store, 'industry_pit'), frame)
    _write_json(_coverage_path(store, 'industry_pit'), {
        'dataset': 'industry_pit',
        'method': 'snapshot_change_points',
        'authority': 'derived',
        'start': summary['start'],
        'end': summary['end'],
        # 只有最近若干天快照，之前的历史行业未知，不得声明完整
        'complete': False,
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'snapshots': len(snapshots),
        'intervals': frame.height,
        'incomplete_reason': (
            f'本地行业快照仅 {len(snapshots)} 份（{summary["start"]} 起），'
            '此前历史行业无权威来源；PIT 查询早于该日应返回空，'
            '不得用当前行业回填'
        ),
        'caveats': [
            '行业口径为通达信行业（T 代码树），非申万/中信，跨口径不可混用',
            'industry_code=unknown 的标的不入表，避免把缺失当成某一级行业',
            '快照缺失的交易日不做插值，变更生效日只精确到快照日',
        ],
    })
    logger.info(f'[extended] 行业 PIT 区间表已落盘 {frame.height} 段，'
                f'{len(snapshots)} 份快照，覆盖 {len(history)} 只标的')
    return summary


def _fetch_membership_snapshot(index_code: str) -> tuple[pl.DataFrame, date] | None:
    """抓取单只指数的官方成分快照（需 akshare；失败返回 None 而非空表）。"""
    try:
        import akshare as ak  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - 环境缺依赖时降级
        logger.error(f'[extended] 缺少 akshare，无法抓取指数成分: {exc}')
        return None
    try:
        frame = ak.index_stock_cons_csindex(symbol=index_code)
    except (OSError, ValueError, RuntimeError, TypeError) as exc:
        logger.error(f'[extended] {index_code} 成分抓取失败: {exc}')
        return None
    if frame is None or len(frame) == 0:
        logger.error(f'[extended] {index_code} 成分返回空表')
        return None

    rows: list[tuple[Any, ...]] = []
    snapshot_date: date | None = None
    for record in frame.to_dict('records'):
        raw_code = str(record.get('成分券代码') or '').strip()
        market = _EXCHANGE_TO_MARKET.get(str(record.get('交易所') or '').strip())
        if not (raw_code.isdigit() and len(raw_code) == 6 and market):
            continue
        try:
            day = datetime.strptime(str(record.get('日期') or '')[:10],
                                    '%Y-%m-%d').date()
        except ValueError:
            day = snapshot_date or date.today()
        snapshot_date = snapshot_date or day
        rows.append((index_code, f'{raw_code}.{market}', day, 'csindex'))
    if not rows or snapshot_date is None:
        logger.error(f'[extended] {index_code} 成分解析后无有效行')
        return None
    return pl.DataFrame(rows, schema={
        'index_code': pl.String, 'ts_code': pl.String,
        'snapshot_date': pl.Date, 'source': pl.String,
    }, orient='row'), snapshot_date


def _load_membership_snapshots(
    snapshot_dir: Path,
) -> dict[date, dict[str, set[str]]]:
    snapshots: dict[date, dict[str, set[str]]] = {}
    if not snapshot_dir.is_dir():
        return snapshots
    for path in sorted(snapshot_dir.glob('snapshot_*.parquet')):
        try:
            frame = pl.read_parquet(path).select(
                ['snapshot_date', 'index_code', 'ts_code'])
        except (OSError, ValueError, TypeError, PolarsError) as exc:
            logger.warning(f'成分快照不可读，跳过 {path.name}: {exc}')
            continue
        for day, index_code, ts_code in frame.iter_rows():
            resolved = _as_date(day)
            if resolved is None:
                continue
            snapshots.setdefault(resolved, {}).setdefault(
                str(index_code), set()).add(str(ts_code))
    return snapshots


def _intervals_for_index(
    index_code: str, ordered: Sequence[tuple[date, dict[str, set[str]]]],
) -> list[tuple[Any, ...]]:
    """对单只指数的快照序列做进出差分，返回区间行。"""
    timeline = [(day, snapshot.get(index_code, set())) for day, snapshot in ordered]
    universe: set[str] = set()
    for _day, members in timeline:
        universe |= members

    def _known(position: int) -> bool:
        """前一份快照与当前快照相邻 -> 变更日可精确到当前快照日。"""
        if position <= 0:
            return False
        return (timeline[position][0] - timeline[position - 1][0]).days == 1

    rows: list[tuple[Any, ...]] = []
    for ts_code in sorted(universe):
        in_date: date | None = None
        in_pos = 0
        for pos, (day, members) in enumerate(timeline):
            present = ts_code in members
            if present:
                if in_date is None:
                    in_date, in_pos = day, pos
                continue
            if in_date is None:
                continue
            rows.append((
                f'{index_code}:{ts_code}:{in_date:%Y%m%d}_{in_pos}', index_code,
                ts_code, in_date, day, _known(in_pos), _known(pos), 'csindex',
            ))
            in_date = None
        if in_date is not None:
            rows.append((
                f'{index_code}:{ts_code}:{in_date:%Y%m%d}_{in_pos}', index_code,
                ts_code, in_date, None, _known(in_pos), False, 'csindex',
            ))
    return rows


def build_index_membership(
    root: Path | str, *, indices: Iterable[tuple[str, str]] = DEFAULT_INDICES,
    allow_remote: bool = False, commit: bool = False,
) -> dict[str, Any]:
    """抓取指数成分快照，并对快照序列做进出差分。"""
    store = Path(root)
    snapshot_dir = _path(store, 'index_membership').parent / 'snapshots'
    fetched = 0
    if allow_remote:
        for symbol, ts_code in indices:
            result = _fetch_membership_snapshot(symbol)
            if result is None:
                continue
            frame, snapshot_date = result
            path = snapshot_dir / (
                f'snapshot_{ts_code.replace(".", "_")}_{snapshot_date:%Y%m%d}.parquet')
            if commit:
                _write_frame(path, frame)
            fetched += 1
            logger.info(f'[extended] {ts_code} 成分 {frame.height} 只'
                        f'（{snapshot_date}）{"已落盘" if commit else "dry-run 未落盘"}')
    elif not snapshot_dir.is_dir():
        logger.warning('[extended] 未给 --allow-remote 且无本地成分快照，'
                       '指数成分契约保持 missing')

    snapshots = _load_membership_snapshots(snapshot_dir)
    if not snapshots:
        return {'dataset': 'index_membership', 'snapshots': 0, 'intervals': 0,
                'fetched': fetched, 'committed': bool(commit)}

    ordered = sorted(snapshots.items())
    index_codes = sorted({code for snapshot in snapshots.values()
                          for code in snapshot})
    rows: list[tuple[Any, ...]] = []
    for index_code in index_codes:
        rows.extend(_intervals_for_index(index_code, ordered))

    frame = _frame(rows, _MEMBERSHIP_SCHEMA)
    first_day, last_day = ordered[0][0], ordered[-1][0]
    summary: dict[str, Any] = {
        'dataset': 'index_membership',
        'snapshots': len(ordered),
        'indices': len(index_codes),
        'intervals': frame.height,
        'open_intervals': (int(frame.filter(pl.col('out_date').is_null()).height)
                           if frame.height else 0),
        'fetched': fetched,
        'start': first_day.isoformat(),
        'end': last_day.isoformat(),
        'committed': bool(commit),
    }
    if not commit:
        return summary

    _write_frame(_path(store, 'index_membership'), frame)
    _write_json(_coverage_path(store, 'index_membership'), {
        'dataset': 'index_membership',
        'method': 'csindex_daily_snapshot_diff',
        'authority': 'official_snapshot',
        'start': summary['start'],
        'end': summary['end'],
        'complete': False,
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'snapshots': len(ordered),
        'indices': index_codes,
        'intervals': frame.height,
        'incomplete_reason': (
            f'只有 {len(ordered)} 份官方成分快照（{summary["start"]} 起）；'
            '首份快照的纳入日未知，已标 in_date_known=false'
        ),
        'caveats': [
            'in_date 记录「首次观测到在成分内」的快照日，不等于真实纳入日；'
            'in_date_known=false 表示纳入日只能界定在相邻快照之间',
            '快照间断期间发生的进出无法定位到精确日期',
            'PIT 查询早于 coverage.start 应返回空，禁止用最新成分回填历史',
        ],
    })
    logger.info(f'[extended] 指数成分区间表已落盘 {frame.height} 段，'
                f'{len(ordered)} 份快照 / {len(index_codes)} 只指数')
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='扩展数据契约产物构建（默认 dry-run）')
    parser.add_argument('target', nargs='?', default='all',
                        choices=('all', 'suspension', 'industry-pit',
                                 'membership'),
                        help='要构建的产物，缺省 all')
    parser.add_argument('--root', default=None, help='列存根目录（缺省用配置默认值）')
    parser.add_argument('--as-of', default=None, help='观察日 YYYY-MM-DD')
    parser.add_argument('--asset-classes', nargs='*',
                        default=list(DEFAULT_ASSET_CLASSES),
                        help='停牌表覆盖的资产类型')
    parser.add_argument('--allow-remote', action='store_true',
                        help='允许联网抓取指数成分快照')
    parser.add_argument('--commit', action='store_true',
                        help='真正写入 data/**；缺省只打印计划')
    parser.add_argument('--json', action='store_true', help='以 JSON 输出')
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    root = Path(args.root) if args.root else default_store_root()
    as_of = (datetime.strptime(args.as_of[:10], '%Y-%m-%d').date()
             if args.as_of else None)
    payload: dict[str, Any] = {}

    if args.target in ('all', 'suspension'):
        payload['suspension'] = build_suspension(
            root, as_of=as_of, asset_classes=tuple(args.asset_classes),
            commit=args.commit)
    if args.target in ('all', 'industry-pit'):
        payload['industry_pit'] = build_industry_pit(root, commit=args.commit)
    if args.target in ('all', 'membership'):
        payload['index_membership'] = build_index_membership(
            root, allow_remote=args.allow_remote, commit=args.commit)

    if args.json:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + '\n')
    else:
        for key, item in payload.items():
            logger.info(f'[extended] {key}: {item}')
        if not args.commit:
            logger.info('[extended] dry-run：加 --commit 才会写入 data/**')
    return 0


if __name__ == '__main__':
    sys.exit(main())
