#!/usr/bin/env python3
r"""期货日线采集：按交易日增量追加到单合约文件（tushare `fut_daily`，单连接串行 + 节流）。

两种模式
========
- ``incremental``（默认，日更用）：先刷新 ``fut_basic`` 白名单，再从已覆盖末日之后
  逐个交易日拉取（1 天 1 次请求），按 ``(ts_code, trade_date)`` 去重后追加进
  ``<staging>/daily/<exchange>/<ts_code>.parquet``。已覆盖的交易日直接跳过；
  某日失败即停压退出（退出码 3），该日不写进度，下次重跑从该日续。
- ``full``：逐合约拉全历史再合并（首建或回补历史缺口；997 只约 22 分钟）。
  不再「文件存在就整只跳过」——已存在则按 ``trade_date`` 比对后追加。

幂等
====
交易日粒度去重：同一交易日重跑新增 0 行，且文件不被重写（``mtime`` 不变）。

留证
====
- ``daily/<exchange>/<ts_code>.parquet``：单合约日线（存在则追加）
- ``contracts_<exchange>.csv``：合约白名单（``build_futures_continuous.py`` 的输入）
- ``progress_days.csv``：逐交易日一行（trade_date, rows, added, contracts, status, fetched_at）
- ``failed_days.csv``：失败交易日逐条登记

用法::

    .venv\\Scripts\\python.exe tools/fetch_futures_daily.py                  # 续采到最新交易日
    .venv\\Scripts\\python.exe tools/fetch_futures_daily.py --since 20261111 --until 20261120
    .venv\\Scripts\\python.exe tools/fetch_futures_daily.py --mode full --market CFFEX
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import tushare as ts

ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_STAGING = ROOT / '_staging' / 'multi_asset_tier2_20261110'

#: 纳入采集的品种（其余品种不落盘，避免把 3,600 个无关合约全拉一遍）
VARIETIES = {
    'CFFEX': ('IF', 'IH', 'IC', 'IM', 'T', 'TF', 'TS', 'TL'),
    'SHFE': ('AG',),
    'INE': ('SC',),
}
#: ts_code 后缀 → 落盘目录
SUFFIX_DIR = {'CFX': 'cffex', 'SHF': 'shfe', 'INE': 'ine'}

#: 命中任一关键字即判定为限流/封禁，立即停压
RATE_LIMIT_HINTS = ('每分钟', '每天', '超过访问', '频率', 'limit', 'too many', 'forbidden')

DEFAULT_THROTTLE = 1.3
MAX_CONSECUTIVE_FAILURES = 3


def _staging_dir() -> Path:
    override = os.environ.get('QUANT_FUTURES_STAGING')
    return Path(override) if override else _DEFAULT_STAGING


def _pro():
    token_file = ROOT / '.secrets' / 'tushare_token.txt'
    return ts.pro_api(token_file.read_text(encoding='utf-8').strip())


def _append_csv(path: Path, row: dict[str, object], header: list[str]) -> None:
    new = not path.exists()
    with open(path, 'a', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        if new:
            writer.writeheader()
        writer.writerow(row)


def _merge_contract(path: Path, df: pd.DataFrame) -> tuple[int, int]:
    """把 df 追加进单合约文件：按 ``trade_date`` 比对，已有日期不重写。

    返回 ``(新增行数, 合并后总行数)``；新增 0 行时不落盘。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    df = df.sort_values('trade_date')
    if path.exists():
        old = pd.read_parquet(path)
        have = set(old['trade_date'].astype(str))
        new = df[~df['trade_date'].astype(str).isin(have)]
        if new.empty:
            return 0, len(old)
        merged = pd.concat([old, new], ignore_index=True)
    else:
        new = df
        merged = df
    merged = merged.drop_duplicates(subset=['ts_code', 'trade_date'], keep='last')
    merged = merged.sort_values('trade_date').reset_index(drop=True)
    # TODO(write_dataset): 落 staging/daily，路径不在 quotes_futures 契约内
    merged.to_parquet(path, index=False)
    return len(new), len(merged)


def _file_max_date(path: Path) -> str:
    """从 parquet 行组统计读 trade_date 最大值（不加载整表）。"""
    try:
        import pyarrow.parquet as pq  # noqa: PLC0415

        meta = pq.read_metadata(path)
        idx = meta.schema.names.index('trade_date')
        best = None
        for group in range(meta.num_row_groups):
            stats = meta.row_group(group).column(idx).statistics
            if stats is not None and stats.max is not None:
                value = str(stats.max)
                best = value if best is None else max(best, value)
        if best:
            return best
    except Exception:  # noqa: BLE001
        pass
    return str(pd.read_parquet(path, columns=['trade_date'])['trade_date'].max())


def _covered_through(daily_dir: Path) -> str | None:
    """三个交易所都覆盖到的最晚交易日（任一交易所缺文件视为未覆盖）。

    优先用 ``coverage.json`` 缓存；任一分片文件比缓存新则回退逐件读行组统计。
    """
    cache = daily_dir.parent / 'coverage.json'
    if cache.exists():
        try:
            payload = json.loads(cache.read_text(encoding='utf-8'))
            stamp = cache.stat().st_mtime
            newest = max((path.stat().st_mtime for path in daily_dir.rglob('*.parquet')),
                         default=0.0)
            if newest <= stamp and payload.get('covered_through'):
                return str(payload['covered_through'])
        except Exception:  # noqa: BLE001
            pass
    maxes: list[str] = []
    for exchange in VARIETIES:
        files = sorted((daily_dir / exchange.lower()).glob('*.parquet'))
        if not files:
            return None
        maxes.append(max(_file_max_date(path) for path in files))
    return min(maxes) if maxes else None


def load_contracts(pro, exchange: str, *, throttle: float, staging: Path) -> pd.DataFrame:
    """取该交易所全部普通合约，按品种白名单过滤。"""
    time.sleep(throttle)
    basic = pro.fut_basic(exchange=exchange, fut_type='1')
    if basic is None or basic.empty:
        raise RuntimeError(f'fut_basic({exchange}) 返回空，先排查链路再接数据')
    keep = basic[basic['fut_code'].isin(VARIETIES[exchange])].copy()
    keep = keep.sort_values(['fut_code', 'delist_date', 'ts_code']).reset_index(drop=True)
    staging.mkdir(parents=True, exist_ok=True)
    keep.to_csv(staging / f'contracts_{exchange.lower()}.csv', index=False, encoding='utf-8')
    return keep


def _open_days(pro, start: str, end: str, *, throttle: float) -> list[str]:
    if start > end:
        return []
    time.sleep(throttle)
    try:
        cal = pro.trade_cal(exchange='CFFEX', start_date=start, end_date=end, is_open='1')
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f'trade_cal 异常，先排查链路：{exc}') from exc
    if cal is None or cal.empty:
        # 区间内没有开市日（周末/节假日），不是链路故障
        print(f'[SKIP] {start}~{end} 无开市日')
        return []
    return sorted(str(x) for x in cal['cal_date'].tolist() if str(x).isdigit())


def _ok_days(progress: Path) -> set[str]:
    if not progress.exists():
        return set()
    frame = pd.read_csv(progress, dtype=str)
    return set(frame.loc[frame['status'] == 'ok', 'trade_date'].astype(str))


def fetch_incremental(pro, markets: list[str], *, throttle: float, staging: Path,
                      since: str | None, until: str) -> dict[str, int]:
    """按交易日批量拉取并追加；单日失败即停压，不推进该日进度。"""
    daily = staging / 'daily'
    progress = staging / 'progress_days.csv'
    failed = staging / 'failed_days.csv'
    prog_header = ['trade_date', 'rows', 'added', 'contracts', 'status', 'fetched_at']
    fail_header = ['trade_date', 'reason', 'detail', 'checked_at']

    allowed: dict[str, str] = {}
    for market in markets:
        contracts = load_contracts(pro, market, throttle=throttle, staging=staging)
        for code in contracts['ts_code'].astype(str):
            allowed[code] = SUFFIX_DIR[code.rsplit('.', 1)[-1]]

    covered = _covered_through(daily)
    if since:
        start = since
    elif covered:
        start = (datetime.strptime(covered, '%Y%m%d').date() + timedelta(days=1)).strftime('%Y%m%d')
    else:
        start = (date.today() - timedelta(days=60)).strftime('%Y%m%d')
    days = _open_days(pro, start, until, throttle=throttle)
    if since is None:
        days = [d for d in days if covered is None or d > covered]
    done = _ok_days(progress)
    days = [d for d in days if d not in done]

    stats = {'days': len(days), 'rows': 0, 'added': 0, 'empty': 0, 'error': 0}
    for day in days:
        time.sleep(throttle)
        now = datetime.now().isoformat(timespec='seconds')
        try:
            frame = pro.fut_daily(trade_date=day)
        except Exception as exc:  # noqa: BLE001
            msg = f'{type(exc).__name__}: {exc}'
            stats['error'] += 1
            _append_csv(failed, {'trade_date': day, 'reason': 'exception', 'detail': msg,
                                 'checked_at': now}, fail_header)
            print(f'[ERR] {day} {msg[:120]}')
            if any(hint in msg.lower() for hint in RATE_LIMIT_HINTS):
                print('[STOP] 命中限流/封禁关键字，立即停压，不重试。')
            sys.exit(3)

        if frame is None or frame.empty:
            stats['empty'] += 1
            _append_csv(failed, {'trade_date': day, 'reason': 'empty', 'detail': '',
                                 'checked_at': now}, fail_header)
            print(f'[EMPTY] {day}')
            continue

        frame = frame.copy()
        frame['ts_code'] = frame['ts_code'].astype(str)
        frame = frame[frame['ts_code'].isin(allowed)].copy()
        # 伪合约（IF.CFX / TL0.CFX 等主力连续与指数序列）不在 fut_basic 白名单内，此处一并剔除
        if frame.empty:
            stats['empty'] += 1
            _append_csv(failed, {'trade_date': day, 'reason': 'empty', 'detail': 'no whitelisted contract',
                                 'checked_at': now}, fail_header)
            print(f'[EMPTY] {day} (no whitelisted contract)')
            continue

        added = 0
        try:
            for code, group in frame.groupby('ts_code'):
                target = daily / allowed[code] / f'{code}.parquet'
                new_rows, _ = _merge_contract(target, group)
                added += new_rows
        except Exception as exc:  # noqa: BLE001
            stats['error'] += 1
            msg = f'{type(exc).__name__}: {exc}'
            _append_csv(failed, {'trade_date': day, 'reason': 'write', 'detail': msg,
                                 'checked_at': now}, fail_header)
            print(f'[ERR] {day} 落盘失败 {msg[:120]}；该日不计进度')
            sys.exit(3)

        stats['rows'] += len(frame)
        stats['added'] += added
        _append_csv(progress, {'trade_date': day, 'rows': len(frame), 'added': added,
                               'contracts': frame['ts_code'].nunique(), 'status': 'ok',
                               'fetched_at': now}, prog_header)
        print(f'[OK] {day} rows={len(frame)} added={added}')

    (staging / 'coverage.json').write_text(json.dumps(
        {'covered_through': _covered_through(daily),
         'updated_at': datetime.now().isoformat(timespec='seconds')},
        ensure_ascii=False, indent=2), encoding='utf-8')
    return stats


def fetch_contracts(pro, markets: list[str], *, throttle: float, staging: Path,
                    limit: int | None) -> dict[str, int]:
    """full 模式：逐合约拉全历史，按 trade_date 比对后追加。"""
    daily = staging / 'daily'
    progress = staging / 'progress.csv'
    failed = staging / 'failed.csv'
    prog_header = ['ts_code', 'rows', 'first_date', 'last_date', 'status', 'fetched_at']
    fail_header = ['ts_code', 'reason', 'detail', 'checked_at']

    stats = {'total': 0, 'appended': 0, 'skipped': 0, 'empty': 0, 'error': 0}
    consecutive_fail = 0
    for market in markets:
        contracts = load_contracts(pro, market, throttle=throttle, staging=staging)
        todo = contracts['ts_code'].tolist()
        if limit is not None:
            todo = todo[:limit]
        stats['total'] += len(todo)
        for idx, code in enumerate(todo, 1):
            time.sleep(throttle)
            now = datetime.now().isoformat(timespec='seconds')
            try:
                frame = pro.fut_daily(ts_code=code)
            except Exception as exc:  # noqa: BLE001
                msg = f'{type(exc).__name__}: {exc}'
                stats['error'] += 1
                consecutive_fail += 1
                _append_csv(failed, {'ts_code': code, 'reason': 'exception', 'detail': msg,
                                     'checked_at': now}, fail_header)
                print(f'[ERR] {code} {msg[:120]}')
                if any(hint in msg.lower() for hint in RATE_LIMIT_HINTS):
                    print('[STOP] 命中限流/封禁关键字，立即停压，不重试。')
                    sys.exit(3)
                if consecutive_fail >= MAX_CONSECUTIVE_FAILURES:
                    print(f'[STOP] 连续 {consecutive_fail} 次失败，停压退出。')
                    sys.exit(3)
                continue
            consecutive_fail = 0
            if frame is None or frame.empty:
                stats['empty'] += 1
                _append_csv(failed, {'ts_code': code, 'reason': 'empty', 'detail': '',
                                     'checked_at': now}, fail_header)
                print(f'[EMPTY] {code}')
                continue
            frame = frame.sort_values('trade_date').reset_index(drop=True)
            target = daily / SUFFIX_DIR[code.rsplit('.', 1)[-1]] / f'{code}.parquet'
            new_rows, total = _merge_contract(target, frame)
            if new_rows:
                stats['appended'] += 1
            else:
                stats['skipped'] += 1
            _append_csv(progress, {
                'ts_code': code, 'rows': total,
                'first_date': str(frame['trade_date'].iloc[0]),
                'last_date': str(frame['trade_date'].iloc[-1]),
                'status': 'ok', 'fetched_at': now,
            }, prog_header)
            if idx % 25 == 0 or idx == len(todo):
                print(f'[{market}] {idx}/{len(todo)} appended={stats["appended"]} '
                      f'unchanged={stats["skipped"]} empty={stats["empty"]} err={stats["error"]}')
    return stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='期货日线采集（交易日增量追加）')
    parser.add_argument('--mode', choices=('incremental', 'full'), default='incremental',
                        help='默认 incremental（按交易日追加）；full 为逐合约全历史合并')
    parser.add_argument('--market', action='append', choices=sorted(VARIETIES),
                        help='可重复；缺省为全部')
    parser.add_argument('--throttle', type=float, default=DEFAULT_THROTTLE,
                        help='每次请求前的休眠秒数（默认 1.3）')
    parser.add_argument('--since', default=None, help='起始交易日 YYYYMMDD（显式指定则不跳过已覆盖日）')
    parser.add_argument('--until', default=date.today().strftime('%Y%m%d'),
                        help='截止交易日 YYYYMMDD（默认今天）')
    parser.add_argument('--limit', type=int, default=None, help='full 模式只取前 N 个合约（调试用）')
    parser.add_argument('--staging', default=None, help='staging 目录覆盖（测试用）')
    args = parser.parse_args(argv)

    staging = Path(args.staging) if args.staging else _staging_dir()
    markets = args.market or sorted(VARIETIES)
    pro = _pro()
    print(f'=== futures {args.mode} staging={staging} markets={markets} ===')
    if args.mode == 'incremental':
        stats = fetch_incremental(pro, markets, throttle=args.throttle, staging=staging,
                                  since=args.since, until=args.until)
    else:
        stats = fetch_contracts(pro, markets, throttle=args.throttle, staging=staging,
                                limit=args.limit)
    print(f'[SUMMARY] {stats}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
