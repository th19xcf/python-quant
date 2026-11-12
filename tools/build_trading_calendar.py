#!/usr/bin/env python3

r"""重建 ``meta/trading_calendar/official_future.parquet``（官方未来交易日历）。

为什么需要本脚本
================
``official_future.parquet`` 此前只有文件、没有生成入口（``docs/跨机每日同步清单.md``
标注「外部录入，无代码生成入口」），一旦要往前推进或跨年更新就只能凭记忆手工录入。
本脚本把口径固化：**只保存行情尚未发生的官方交易日**，可反复重跑。

数据来源
========
``akshare.tool_trade_date_hist_sina()``（新浪财经交易日历接口，1990-12-19 起；
交易所公布次年休市安排后，该接口会把次年年末之前的日期一并给出）。
接口不可用时可先用 ``--input`` 落一份 CSV/Parquet 快照，再用同一脚本离线重放。

口径
====
- ``quote_authoritative_end``：本地基准指数（``000001.SH``，缺失时 ``000300.SH``）
  最后一根 bar 的日期，代表「已发生行情」的边界；
- ``official_future.parquet``：严格晚于该边界、且不晚于官方日历末日的交易日，
  供停牌/缺口构建在未来窗口内确认哪天真的开市；
- 该文件**不影响** ``quote_authoritative_end``：已发生行情之外的日期不能被当成
  停牌证据（见 ``coverage.json`` 的 note）。

更新时机
========
- 每个交易日行情落盘后（或每周）重跑一次，窗口起点会随基准 index 推进自动前移；
- **每年 12 月**交易所公布次年休市安排后必须重跑，否则跨年后仍会用量旧日历；
- 日常无需人工维护，文件内容由「官方日历 + 本地基准 index」确定性推导。

用法::

    .venv\\Scripts\\python.exe tools\\build_trading_calendar.py              # dry-run
    .venv\\Scripts\\python.exe tools\\build_trading_calendar.py --apply      # 写盘
    .venv\\Scripts\\python.exe tools\\build_trading_calendar.py --check      # 与现文件比对
    .venv\\Scripts\\python.exe tools\\build_trading_calendar.py \\
        --input research\\calendar\\trade_dates.csv --apply                  # 离线重放

幂等：同一份官方日历 + 同一个 ``quote_authoritative_end`` + 同一个 ``--fetched-at``
会产出逐行一致的内容；内容未变化时 ``--apply`` 不重写文件。
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from loguru import logger
from polars.exceptions import PolarsError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.data.atomic_write import write_parquet_atomic, write_text_atomic  # noqa: E402
from src.data.trading_calendar import (  # noqa: E402
    BENCHMARK_CODES,
    configured_holidays,
)

ENDPOINT = 'akshare:tool_trade_date_hist_sina'
DATASET = 'official_future_trading_calendar'
NOTE = (
    '官方接口只给到 official_end；此后仍为工作日近似且 approx_from 后移。'
    'quote_authoritative_end 不因本文件改变：停牌/缺口构建只能用已发生行情日，'
    '否则会把未来日期误判为停牌。'
)


def _as_date(value: Any) -> date | None:
    """把 date/datetime/字符串/pandas.Timestamp 统一成 date。"""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        for fmt in ('%Y-%m-%d', '%Y%m%d'):
            try:
                return datetime.strptime(text[:10], fmt).date()
            except ValueError:
                continue
    return None


def _index_path(store_root: Path, ts_code: str) -> Path:
    market = ts_code.rsplit('.', 1)[-1].lower() if '.' in ts_code else 'sh'
    return store_root / 'quotes' / 'index' / market / f'{ts_code}.parquet'


def read_quote_authoritative_end(store_root: Path) -> date | None:
    """本地基准指数最后一根 bar 的日期（无可用文件时返回 None）。"""
    for code in BENCHMARK_CODES:
        path = _index_path(store_root, code)
        if not path.is_file():
            continue
        try:
            frame = pl.read_parquet(path, columns=['date'])
        except (OSError, ValueError, PolarsError) as exc:
            logger.warning(f'[calendar] 基准指数读取失败 {path}: {exc}')
            continue
        values = [_as_date(v) for v in frame['date'].to_list()]
        parsed = [v for v in values if v is not None]
        if parsed:
            return max(parsed)
    return None


def load_official_calendar(input_path: Path | None) -> list[date]:
    """读取官方交易日历：本地快照优先，否则调用 akshare。"""
    if input_path is not None:
        if input_path.suffix.lower() == '.parquet':
            frame = pl.read_parquet(input_path)
        else:
            frame = pl.read_csv(input_path, try_parse_dates=True)
        values = frame['trade_date'].to_list()
    else:
        import akshare as ak  # noqa: PLC0415

        frame = ak.tool_trade_date_hist_sina()
        values = frame['trade_date'].tolist()

    parsed = {_as_date(v) for v in values}
    dates = sorted(d for d in parsed if d is not None)
    if not dates:
        raise SystemExit('官方交易日历为空，检查 --input 或 akshare 返回')
    return dates


def weekday_window(
    start: date, end: date, holidays: Iterable[date]
) -> tuple[date, ...]:
    """闭区间 [start, end] 内的工作日（剔除节假日配置）。"""
    blocked = frozenset(holidays)
    if end < start:
        return ()
    out: list[date] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5 and cursor not in blocked:
            out.append(cursor)
        cursor += timedelta(days=1)
    return tuple(out)


def build_frame(
    official: list[date], quote_end: date
) -> tuple[pl.DataFrame, dict[str, Any]]:
    """按口径裁剪官方日历并生成 coverage 声明。"""
    official_end = official[-1]
    future = [d for d in official if d > quote_end]
    frame = pl.DataFrame({'trade_date': future}, schema={'trade_date': pl.Date})

    approx = weekday_window(quote_end + timedelta(days=1), official_end,
                            configured_holidays())
    approx_set, official_set = set(approx), set(future)
    coverage = {
        'dataset': DATASET,
        'endpoint': ENDPOINT,
        'quote_authoritative_end': quote_end.isoformat(),
        'official_end': official_end.isoformat(),
        'official_future_days': len(future),
        'weekday_approx_days_same_window': len(approx),
        'weekday_only_dates': [d.isoformat() for d in sorted(approx_set - official_set)],
        'official_only_dates': [
            d.isoformat() for d in sorted(official_set - approx_set)
        ],
        'note': NOTE,
    }
    return frame, coverage


def _read_existing_dates(path: Path) -> list[date] | None:
    if not path.is_file():
        return None
    try:
        frame = pl.read_parquet(path, columns=['trade_date'])
    except (OSError, ValueError, PolarsError) as exc:
        logger.warning(f'[calendar] 现有文件读取失败 {path}: {exc}')
        return None
    return [d for d in (_as_date(v) for v in frame['trade_date'].to_list())
            if d is not None]


def _backup(path: Path) -> Path:
    """覆盖前把原文件复制到 ``_backup/trading_calendar/``，不删除任何历史副本。"""
    backup_dir = ROOT / '_backup' / 'trading_calendar'
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup = backup_dir / f'{path.stem}.{stamp}{path.suffix}'
    index = 1
    while backup.exists():
        backup = backup_dir / f'{path.stem}.{stamp}_{index}{path.suffix}'
        index += 1
    shutil.copy2(path, backup)
    return backup


def write_outputs(
    frame: pl.DataFrame,
    coverage: dict[str, Any],
    output_dir: Path,
    store_root: Path,
) -> bool:
    """写 parquet + coverage.json；内容一致时跳过写盘（幂等）。"""
    parquet_path = output_dir / 'official_future.parquet'
    coverage_path = output_dir / 'coverage.json'
    text = json.dumps(coverage, ensure_ascii=False, indent=2) + '\n'

    existing_dates = _read_existing_dates(parquet_path)
    same_parquet = existing_dates == frame['trade_date'].to_list()
    same_coverage = (
        coverage_path.is_file()
        and coverage_path.read_text(encoding='utf-8') == text
    )
    if same_parquet and same_coverage:
        logger.info('[calendar] 内容一致，跳过写入（幂等）')
        return False

    output_dir.mkdir(parents=True, exist_ok=True)
    if parquet_path.is_file() and not same_parquet:
        logger.info(f'[calendar] 原文件已备份: {_backup(parquet_path)}')
    if not same_parquet:
        write_parquet_atomic(frame, parquet_path)
        logger.info(f'[calendar] 已写入 {len(frame)} 个交易日: {parquet_path}')
    if not same_coverage:
        write_text_atomic(text, coverage_path)
        logger.info(f'[calendar] coverage 已写入: {coverage_path}')
    return True


def _print_summary(
    frame: pl.DataFrame,
    coverage: dict[str, Any],
    quote_end: date,
    output_dir: Path,
) -> None:
    print(f'quote_authoritative_end: {quote_end.isoformat()}')
    print(f'official_end:            {coverage["official_end"]}')
    print(f'未来交易日:              {len(frame)}')
    print(f'工作日近似同窗口:        {coverage["weekday_approx_days_same_window"]}')
    print(f'仅官方有的日期:          {len(coverage["official_only_dates"])}')
    print(f'仅工作日近似有:          {len(coverage["weekday_only_dates"])}')
    if len(frame):
        print(f'区间: {frame["trade_date"].min()} → {frame["trade_date"].max()}')
    existing = _read_existing_dates(output_dir / 'official_future.parquet')
    if existing is None:
        print('现有文件: 不存在（--apply 将新建）')
        return
    existing_valid = [d for d in existing if d is not None]
    generated = frame['trade_date'].to_list()
    if existing_valid == generated:
        print(f'与现有文件对齐: 是（{len(existing_valid)} 行逐行一致）')
    else:
        print(f'与现有文件对齐: 否（现有 {len(existing_valid)} 行 / 重建 {len(generated)} 行）')
        missing = sorted(set(existing_valid) - set(generated))
        extra = sorted(set(generated) - set(existing_valid))
        for day in missing:
            print(f'  - 现有有、重建无: {day}')
        for day in extra:
            print(f'  + 重建有、现有无: {day}')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='重建官方未来交易日历')
    parser.add_argument('--store-root', default=None, help='本地列存根目录')
    parser.add_argument('--input', default='', help='官方日历快照 CSV/Parquet（缺省调用 akshare）')
    parser.add_argument('--output-dir', default='',
                        help='输出目录（默认 <store-root>/meta/trading_calendar）')
    parser.add_argument('--fetched-at', default='',
                        help='coverage.json 的 fetched_at（默认当前时间，格式 YYYY-MM-DDTHH:MM:SS）')
    parser.add_argument('--quote-end', default='',
                        help='覆盖 quote_authoritative_end（默认取本地基准 index 最后一根 bar），'
                             '用于离线复现历史版本')
    parser.add_argument('--apply', action='store_true', help='真正写盘；缺省只打印摘要')
    parser.add_argument('--check', action='store_true',
                        help='与现有文件逐行比对，不一致返回 1')
    args = parser.parse_args(argv)

    store_root = (
        Path(args.store_root) if args.store_root
        else ROOT / 'data' / 'local_store'
    )
    output_dir = (
        Path(args.output_dir) if args.output_dir
        else store_root / 'meta' / 'trading_calendar'
    )

    if args.quote_end:
        quote_end = _as_date(args.quote_end)
        if quote_end is None:
            logger.error(f'[calendar] 无法解析 --quote-end: {args.quote_end}')
            return 2
    else:
        quote_end = read_quote_authoritative_end(store_root)
    if quote_end is None:
        logger.error(
            f'[calendar] 未找到基准指数行情，无法确定 quote_authoritative_end: {store_root}'
        )
        return 1

    official = load_official_calendar(Path(args.input) if args.input else None)
    frame, coverage = build_frame(official, quote_end)
    coverage['fetched_at'] = (
        args.fetched_at or datetime.now().replace(microsecond=0).isoformat()
    )
    # 字段顺序与现有 coverage.json 保持一致
    ordered = {
        'dataset': coverage['dataset'],
        'endpoint': coverage['endpoint'],
        'fetched_at': coverage['fetched_at'],
        'quote_authoritative_end': coverage['quote_authoritative_end'],
        'official_end': coverage['official_end'],
        'official_future_days': coverage['official_future_days'],
        'weekday_approx_days_same_window': coverage['weekday_approx_days_same_window'],
        'weekday_only_dates': coverage['weekday_only_dates'],
        'official_only_dates': coverage['official_only_dates'],
        'note': coverage['note'],
    }

    _print_summary(frame, ordered, quote_end, output_dir)

    if args.check:
        existing = _read_existing_dates(output_dir / 'official_future.parquet')
        if existing is None:
            print('--check: 现有文件不存在')
            return 1
        return 0 if existing == frame['trade_date'].to_list() else 1

    if not args.apply:
        print('dry-run：未写盘（加 --apply 执行）')
        return 0

    write_outputs(frame, ordered, output_dir, store_root)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
