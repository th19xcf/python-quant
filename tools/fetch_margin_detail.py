#!/usr/bin/env python3

"""拉取个股融资融券明细（tushare ``margin_detail``），供「个股两融买点」研究。

存储
====
- 正式层：``data/local_store/meta/margin_detail/margin_detail_YYYYMM.parquet``
  （**按自然月分区**）。理由：3 年约 36 个文件，日增量只重写当月一份
  （约 7 万行、秒级），既不产生上千个日文件拖累 manifest / freshness 的
  glob，也不必为了一个交易日重写整年。
- 原始响应缓存：``tmp/margin_detail_raw/YYYYMMDD.parquet``；
- 进度：``tmp/margin_detail_state.json``（已完成交易日），断点续跑只补缺口；
- 幂等：合并后按 ``(trade_date, ts_code)`` 去重，``--refresh`` 才重拉。

schema（10 列，tushare 原字段）::

    trade_date(date) / ts_code(str) / rzye(float, 融资余额)
    / rzmre(float, 融资买入额) / rzche(float, 融资偿还额)
    / rqye(float, 融券余额) / rqmcl(float, 融券卖出量)
    / rqchl(float, 融券偿还量) / rzrqye(float, 融资融券余额)
    / rqyl(float, 融券余量)

用法::

    python tools/fetch_margin_detail.py --probe 5      # 探数，不写正式层
    python tools/fetch_margin_detail.py --start 2023-11-21
    python tools/fetch_margin_detail.py --offline      # 只用缓存
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_parquet_atomic, write_text_atomic  # noqa: E402
from src.data.manifest_refresh import sha256_file  # noqa: E402
from src.data import margin_quality  # noqa: E402
from src.data.trading_calendar import trading_dates  # noqa: E402

STORE = ROOT / 'data' / 'local_store'
OUT_DIR = STORE / 'meta' / 'margin_detail'
RAW_DIR = ROOT / 'tmp' / 'margin_detail_raw'
STATE_FILE = ROOT / 'tmp' / 'margin_detail_state.json'
TOKEN_FILE = ROOT / '.secrets' / 'tushare_token.txt'
MANIFEST = STORE / 'meta' / 'manifest.json'

COLUMNS = ('trade_date', 'ts_code', 'rzye', 'rzmre', 'rzche',
           'rqye', 'rqmcl', 'rqchl', 'rzrqye', 'rqyl')
REQUEST_SLEEP = 0.35          # 200 次/分钟上限 → 3.5 次/秒留余量
MIN_ROWS, MAX_ROWS = 10, 7000   # 2010 年两融试点期每日仅约 40 只标的


class FetchError(RuntimeError):
    pass


def _token() -> str:
    import os  # noqa: PLC0415

    token = (os.environ.get('TS_TOKEN') or '').strip()
    if not token and TOKEN_FILE.is_file():
        token = TOKEN_FILE.read_text(encoding='utf-8').strip()
    if not token:
        raise FetchError('TS_TOKEN 未设置，且 .secrets/tushare_token.txt 不存在或为空')
    return token


def _pro() -> Any:
    import tushare as ts  # noqa: PLC0415

    return ts.pro_api(_token())


def _retry(fn, *args: Any, attempts: int = 5, **kwargs: Any) -> Any:
    last: Exception | None = None
    for attempt in range(attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last = exc
            # 限流按整分钟恢复，退避要盖过一个配额窗口
            time.sleep(62.0 if '频率' in str(exc) else 2.0 * (attempt + 1))
    raise FetchError(f'tushare 调用连续失败：{last}')


def _normalize(raw: pl.DataFrame, day: date) -> pl.DataFrame:
    missing = [column for column in COLUMNS if column not in raw.columns]
    if missing:
        raise FetchError(f'{day} 返回缺列: {missing}；实际列 {raw.columns}')
    date_expr = pl.coalesce([
        pl.col('trade_date').cast(pl.String)
        .str.strptime(pl.Date, '%Y%m%d', strict=False),
        pl.col('trade_date').cast(pl.String).str.slice(0, 10)
        .str.to_date(strict=False),
    ]).alias('trade_date')
    frame = raw.select(list(COLUMNS)).with_columns(
        date_expr,
        pl.col('ts_code').cast(pl.String),
        *[pl.col(column).cast(pl.Float64) for column in COLUMNS[2:]],
    ).filter(pl.col('trade_date') == pl.lit(day))
    return _validate(frame, day)


def _validate(frame: pl.DataFrame, day: date) -> pl.DataFrame:
    """落盘前门禁：行数在合理区间，关键字段非空，余额口径不整列为空。"""
    if not MIN_ROWS <= frame.height <= MAX_ROWS:
        raise FetchError(f'{day} 行数异常: {frame.height}（合理区间 '
                         f'{MIN_ROWS}~{MAX_ROWS}），不落盘')
    for column in ('trade_date', 'ts_code'):
        if frame[column].null_count():
            raise FetchError(f'{day} {column} 存在空值，不落盘')
    if frame['rzye'].null_count() / frame.height > 0.1:
        raise FetchError(f'{day} rzye 空值占比过高，不落盘')
    if frame['ts_code'].n_unique() != frame.height:
        raise FetchError(f'{day} ts_code 重复，不落盘')
    return frame


def _raw_path(day: date) -> Path:
    return RAW_DIR / f'{day:%Y%m%d}.parquet'


def _month_path(month: str) -> Path:
    return OUT_DIR / f'margin_detail_{month}.parquet'


def _fetch_day(pro: Any, day: date, *, refresh: bool) -> pl.DataFrame:
    path = _raw_path(day)
    if path.is_file() and not refresh:
        return _normalize(pl.read_parquet(path), day)
    raw = _retry(pro.margin_detail, trade_date=f'{day:%Y%m%d}')
    frame = _normalize(pl.DataFrame(raw), day)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(frame, path)
    return frame


def _merge_month(frame: pl.DataFrame) -> Path:
    month = frame['trade_date'][0].strftime('%Y%m')
    path = _month_path(month)
    existing = pl.read_parquet(path) if path.is_file() else None
    merged = frame if existing is None else pl.concat([existing, frame])
    merged = (merged.unique(subset=['trade_date', 'ts_code'], keep='last')
              .sort(['trade_date', 'ts_code']))
    path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(merged, path)
    return path


def _load_state() -> tuple[set[str], dict[str, str]]:
    if not STATE_FILE.is_file():
        return set(), {}
    payload = json.loads(STATE_FILE.read_text(encoding='utf-8'))
    return set(payload.get('done') or []), dict(payload.get('failed') or {})


def _save_state(done: set[str], failed: dict[str, str] | None = None) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(json.dumps({'done': sorted(done),
                                  'failed': failed or {}},
                                 ensure_ascii=False), STATE_FILE)


def _register_manifest(paths: list[Path]) -> None:
    if not MANIFEST.is_file():
        return
    payload = json.loads(MANIFEST.read_text(encoding='utf-8'))
    files = payload.setdefault('files', [])
    index = {item.get('path'): item for item in files}
    for path in paths:
        rel = path.relative_to(STORE).as_posix()
        entry = index.get(rel)
        if entry is None:
            entry = {'path': rel}
            files.append(entry)
            index[rel] = entry
        entry.update({
            'rows': pl.read_parquet(path).height,
            'bytes': path.stat().st_size,
            'sha256': sha256_file(path),
            'columns': pl.read_parquet(path).columns,
            'note': '个股两融明细（自然月分区）；由 tools/fetch_margin_detail.py 生成',
        })
    payload['generated_at'] = date.today().isoformat()
    write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2), MANIFEST)


def _describe(frame: pl.DataFrame, day: date, cost: float) -> str:
    return (f'{day} rows={frame.height} 股票={frame["ts_code"].n_unique()} '
            f'rzye非空={frame.height - frame["rzye"].null_count()} '
            f'rqye非空={frame.height - frame["rqye"].null_count()} '
            f'耗时={cost:.2f}s')


def _open_days(pro: Any, start: date, end: date) -> list[date]:
    """用 tushare 交易日历取开市日，避免把节假日当成缺数。"""
    cal = pl.DataFrame(_retry(pro.trade_cal, exchange='SSE',
                              start_date=f'{start:%Y%m%d}',
                              end_date=f'{end:%Y%m%d}', is_open='1'))
    return sorted(
        date.fromisoformat(str(value)[:4] + '-' + str(value)[4:6]
                           + '-' + str(value)[6:8])
        for value in cal['cal_date'].to_list())


def probe(count: int) -> int:
    pro = _pro()
    end = date.today()
    days = _open_days(pro, end.replace(year=end.year - 1), end)[-count:]
    frame = pl.DataFrame()
    for day in days:
        started = time.monotonic()
        raw = _retry(pro.margin_detail, trade_date=f'{day:%Y%m%d}')
        frame = _normalize(pl.DataFrame(raw), day)
        print(_describe(frame, day, time.monotonic() - started), flush=True)
    print(f'列: {list(frame.columns)}')
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', default='2023-11-21')
    parser.add_argument('--end', default=date.today().isoformat())
    parser.add_argument('--probe', type=int, default=0)
    parser.add_argument('--refresh', action='store_true')
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--audit', action='store_true',
                        help='只重建逐日完整性标记，不抓数')
    args = parser.parse_args(argv)

    if args.audit:
        marks = margin_quality.build_completeness(STORE)
        bad = marks.filter(~pl.col('is_complete'))
        print(f'完整性标记 {marks.height} 天，不完整 {bad.height} 天：'
              f'{bad["trade_date"].to_list()}')
        return 0

    if args.probe:
        return probe(args.probe)

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    pro = None if args.offline else _pro()
    days = (_open_days(pro, start, end) if pro is not None
            else list(trading_dates(start, end, ROOT)))
    # 当日数据在收盘后才逐步入库，默认只补到昨天，避免落半天的残缺日。
    today = date.today()
    days = [day for day in days if day < today]
    done, failed = _load_state()
    todo = [day for day in days
            if args.refresh or day.isoformat() not in done] if not args.offline \
        else [day for day in days if _raw_path(day).is_file()
              and day.isoformat() not in done]
    if not todo:
        print(f'无待补交易日（共 {len(days)} 个，已完成 {len(done)}）')
        return 0
    started = time.monotonic()
    touched: list[Path] = []
    for index, day in enumerate(todo, 1):
        try:
            frame = _fetch_day(pro, day, refresh=args.refresh)
        except Exception as exc:  # noqa: BLE001
            # 失败批次不推进进度、不写正式层，下一轮自动重跑
            failed[day.isoformat()] = str(exc)[:200]
            _save_state(done, failed)
            print(f'[FAIL] {day}: {exc}', flush=True)
            continue
        if not args.dry_run:
            touched.append(_merge_month(frame))
            done.add(day.isoformat())
            failed.pop(day.isoformat(), None)
            _save_state(done, failed)
        if index % 10 == 0 or index == len(todo):
            elapsed = time.monotonic() - started
            eta = elapsed / index * (len(todo) - index)
            print(f'[{index}/{len(todo)}] {day} rows={frame.height} '
                  f'已用 {elapsed:.0f}s，剩余约 {eta:.0f}s', flush=True)
        if pro is not None:
            time.sleep(REQUEST_SLEEP)
    if touched and not args.dry_run:
        _register_manifest(sorted(set(touched)))
        marks = margin_quality.build_completeness(STORE)
        bad = marks.filter(~pl.col('is_complete'))
        if bad.height:
            print(f'[WARN] 不完整交易日 {bad.height} 天（已写入 '
                  f'completeness.parquet，下游按日截面应剔除）：'
                  f'{bad["trade_date"].to_list()}', flush=True)
    print(f'完成 {len(todo)} 个交易日；失败 {len(failed)} 个'
          f'{"：" + ",".join(sorted(failed)) if failed else ""}；'
          f'正式层 {OUT_DIR}；请求数 {len(todo)}')
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
