#!/usr/bin/env python3

"""财务三表 + 日频估值本地采集入口。

用法：
    .venv\\Scripts\\python.exe tools\\collect_financials.py
    .venv\\Scripts\\python.exe tools\\collect_financials.py --only statements
    .venv\\Scripts\\python.exe tools\\collect_financials.py --merge-only
    .venv\\Scripts\\python.exe tools\\collect_financials.py --valuation-limit 50
    .venv\\Scripts\\python.exe tools\\collect_financials.py --only valuation --since 2026-11-07 --until 2026-11-09

说明：
- 默认采集 2016Q1 至最近一个已结束季度；
- 分片落 ``data/local_store/meta/financials/parts``，中断后重跑自动续传；
- ``--since``/``--until`` 为估值区间增量：只落区间内的行，已补完该区间的代码
  按 ``progress.json`` 断点跳过，中断重跑不重写全历史；``--force`` 仍是全历史重写，
  两者互斥；
- 不写 MySQL、不改现有库结构。
"""

# ruff: noqa: T201 - CLI 面向终端输出

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.financials.collector import (  # noqa: E402
    FinancialsCollector,
    RetryPolicy,
    periods_between,
)
from src.data.financials.store import (  # noqa: E402
    coverage,
    financials_dir,
    merge_parts,
)
from src.database.db_manager import DatabaseManager  # noqa: E402
from src.database.models.stock import StockBasic  # noqa: E402
from src.utils.config import Config  # noqa: E402


def latest_period_end(today: date) -> date:
    """最近一个已结束的报告期（未结束的季度不计）。"""
    for month, day in ((12, 31), (9, 30), (6, 30), (3, 31)):
        candidate = date(today.year, month, day)
        if candidate <= today:
            return candidate
    return date(today.year - 1, 12, 31)


def load_universe() -> tuple[dict[str, str], list[str]]:
    """从 MySQL 读取 ``symbol -> ts_code`` 与在市标的清单（只读）。"""
    config = Config()
    db_manager = DatabaseManager(config)
    db_manager.connect()
    try:
        session = db_manager.get_session()
        rows = session.query(StockBasic.ts_code, StockBasic.symbol, StockBasic.status).all()
    finally:
        db_manager.disconnect()
    symbols: dict[str, str] = {}
    listed: list[str] = []
    for ts_code, symbol, status in rows:
        if not ts_code or not symbol:
            continue
        symbols[str(symbol).zfill(6)] = str(ts_code)
        if status == 'L':
            listed.append(str(ts_code))
    return symbols, sorted(listed)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='财务三表 + 日频估值本地采集')
    parser.add_argument('--start', default='2016-01-01', help='起始日（含），默认 2016-01-01')
    parser.add_argument('--end', default='', help='结束日（含），默认最近已结束季度末')
    parser.add_argument('--only', default='all',
                        help='all|statements|bj|valuation|merge，默认 all')
    parser.add_argument('--workers', type=int, default=6, help='估值/北交所并发数')
    parser.add_argument('--force', action='store_true', help='忽略已有分片重采')
    parser.add_argument('--valuation-limit', type=int, default=0,
                        help='仅采前 N 只估值（调试用，0 表示全量）')
    parser.add_argument('--since', default='',
                        help='估值区间增量起点（含，YYYY-MM-DD）：只落该日之后的行，'
                             '按 progress.json 断点续跑，不重写全历史')
    parser.add_argument('--until', default='',
                        help='估值区间增量终点（含，YYYY-MM-DD），仅与 --since 同用，'
                             '缺省为今天')
    parser.add_argument('--merge-only', action='store_true', help='只合并现有分片')
    parser.add_argument('--no-merge', action='store_true', help='采集后不合并')
    return parser.parse_args()


def parse_range(args: argparse.Namespace) -> tuple[date | None, date | None]:
    """解析并校验 ``--since`` / ``--until``（与 ``--force`` 互斥）。"""
    since = date.fromisoformat(args.since) if args.since else None
    until = date.fromisoformat(args.until) if args.until else None
    if args.force and since is not None:
        raise SystemExit('--force 与 --since 语义冲突：--force 是全历史重写，'
                         '--since 是区间增量')
    if until is not None and since is None:
        raise SystemExit('--until 需要与 --since 一起使用')
    if since is not None and args.only not in {'valuation'}:
        raise SystemExit('--since/--until 目前只支持 --only valuation')
    if since is not None and until is not None and since > until:
        raise SystemExit(f'--since({since}) 不能晚于 --until({until})')
    return since, until


def main() -> int:
    args = parse_args()
    since, until = parse_range(args)
    started = time.perf_counter()
    store_root = PROJECT_ROOT / 'data' / 'local_store'
    config = Config()
    retry = RetryPolicy(
        max_retries=int(getattr(config.data, 'retry_max_retries', 3)),
        delay=float(getattr(config.data, 'retry_delay', 1.0)),
        backoff_factor=float(getattr(config.data, 'retry_backoff_factor', 2.0)),
        max_delay=float(getattr(config.data, 'retry_max_delay', 10.0)),
    )
    collector = FinancialsCollector(store_root, workers=args.workers, retry=retry)

    if args.merge_only:
        stats = collector.merge_all()
        print(json.dumps({'merge': stats}, ensure_ascii=False, indent=2))
        return 0

    start = date.fromisoformat(args.start)
    end = (date.fromisoformat(args.end) if args.end
           else latest_period_end(date.today()))
    periods = periods_between(start, end)
    print(f'报告期 {len(periods)} 个: {periods[0]} ~ {periods[-1]}')

    symbols, listed = load_universe()
    print(f'股票清单: symbol 映射 {len(symbols)}，在市 {len(listed)}')

    bj_codes = [code for code in listed if code.endswith('.BJ')]
    results: dict[str, object] = {}
    if args.only in {'all', 'statements'}:
        results['statements'] = collector.collect_statements(
            periods, symbols, force=args.force).__dict__
    if args.only in {'all', 'bj'}:
        results['bj_balance'] = collector.collect_bj_balance(
            periods, symbols, force=args.force).__dict__
        results['bj_statements'] = collector.collect_bj_statements(
            bj_codes, start_period=periods[0]).__dict__
    if args.only in {'all', 'valuation'}:
        codes = listed[:args.valuation_limit] if args.valuation_limit else listed
        results['valuation'] = collector.collect_valuation(
            codes, force=args.force, since=since, until=until).__dict__
        if since is not None and not args.no_merge:
            # 区间增量只动估值表，合并也只重建 valuation_daily，不重写三表
            results['merge'] = {'valuation': merge_parts('valuation', store_root)}
    if args.only == 'parent':
        results['parent'] = collector.backfill_parent_net_profit(periods).__dict__
    if args.only in {'all', 'merge', 'parent'} and not args.no_merge:
        results['merge'] = collector.merge_all()

    results['coverage'] = {table: coverage(table, store_root) for table in
                           ('income', 'balance', 'cashflow', 'valuation')}
    results['elapsed_s'] = round(time.perf_counter() - started, 2)
    results['store_dir'] = str(financials_dir(store_root))
    print(json.dumps(results, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
