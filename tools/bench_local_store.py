"""本地列存验收基准：单票冷/热取数、批量取数、磁盘/文件统计。

默认沿用迁移前基准区间（2021-10-19 ~ 2026-10-19、批量 200 只），
可用 --start / --end / --codes 覆盖，便于复用为通用基准。

用法：
    .venv\\Scripts\\python.exe tools\\bench_local_store.py
    .venv\\Scripts\\python.exe tools\\bench_local_store.py `
        --start 2022-01-01 --end 2026-01-01 --codes 600519.SH,000001.SZ
"""
# ruff: noqa: T201  # 基准脚本面向终端输出

import argparse
import statistics
import sys
import time
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.data_cache import global_data_cache  # noqa: E402
from src.data.local_store import get_local_quote_store, reset_local_stores  # noqa: E402
from src.data.managers.data_fetcher import DataFetcher  # noqa: E402
from src.utils.config import Config  # noqa: E402

IDX_CODE = '000001.SH'
IDX_START, IDX_END = '1990-01-01', '2026-11-03'
DEFAULT_START, DEFAULT_END = '2021-10-19', '2026-10-19'
DEFAULT_LIMIT = 200


def pct(values, q):
    return statistics.quantiles(values, n=100)[q - 1] if len(values) > 1 else values[0]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='本地列存验收基准')
    parser.add_argument('--start', default=DEFAULT_START,
                        help=f'批量区间起始日（默认 {DEFAULT_START}）')
    parser.add_argument('--end', default=DEFAULT_END,
                        help=f'批量区间结束日（默认 {DEFAULT_END}）')
    parser.add_argument('--codes', default='',
                        help='逗号分隔的股票代码；缺省自动挑选覆盖整段区间的前 '
                             f'{DEFAULT_LIMIT} 只在市标的')
    return parser.parse_args()


def select_codes(store, start: str, end: str, limit: int = DEFAULT_LIMIT) -> list[str]:
    """自动挑选覆盖 [start, end] 整段区间的在市标的。"""
    lo, hi = date.fromisoformat(start), date.fromisoformat(end)
    codes = []
    for (kind, code) in sorted(store._load_meta()):
        if kind != 'stock':
            continue
        cov = store.coverage('stock', code)
        if cov is None or cov.start > lo or cov.end < hi:
            continue
        codes.append(code)
        if len(codes) == limit:
            break
    return codes


def main(start: str, end: str, codes_arg: str) -> None:
    config = Config()
    reset_local_stores()
    store = get_local_quote_store(config)
    stats = store.stats()
    print(f'STORE symbols={stats["symbols"]} files={stats["files"]} '
          f'rows={stats["rows"]:,} disk_mb={stats["disk_bytes"] / 1024 / 1024:.2f} '
          f'by_type={stats["by_type"]}')

    fetcher = DataFetcher(config, db_manager=None)
    # 单票全历史：冷（每次清空 DataCache）与热（缓存命中）
    cold, rows = [], 0
    for _ in range(5):
        global_data_cache.clear()
        t0 = time.perf_counter()
        df = fetcher.get_index_data(IDX_CODE, IDX_START, IDX_END)
        elapsed = (time.perf_counter() - t0) * 1000
        cold.append(elapsed)
        rows = df.height
    t0 = time.perf_counter()
    hot_df = fetcher.get_index_data(IDX_CODE, IDX_START, IDX_END)
    hot = (time.perf_counter() - t0) * 1000
    print(f'COLD rows={rows} n=5 min={min(cold):.2f}ms p50={statistics.median(cold):.2f}ms '
          f'max={max(cold):.2f}ms')
    print(f'HOT rows={hot_df.height} elapsed={hot:.3f}ms')

    # 批量日线取数：--codes 指定，或缺省挑选覆盖整段区间的在市标的
    codes = ([c.strip().upper() for c in codes_arg.split(',') if c.strip()]
             if codes_arg else select_codes(store, start, end))
    global_data_cache.clear()
    t0 = time.perf_counter()
    batch = fetcher.get_stock_batch(codes, start, end, '1d', 'hfq')
    elapsed = time.perf_counter() - t0
    batch_rows = sum(df.height for df in batch.values())
    print(f'BATCH requested={len(codes)} hit={len(batch)} rows={batch_rows:,} '
          f'elapsed={elapsed:.3f}s ({elapsed * 1000 / max(len(batch), 1):.2f}ms/票)')

    global_data_cache.clear()
    for code, df in batch.items():
        assert not df.is_empty()
    print('BATCH integrity: all frames non-empty')

    # 基线：同一批标的、同一区间，强制走通达信（改造前路径）
    from src.data.tdx_handler import TdxHandler

    store.read_daily = lambda *a, **k: None
    tdx_fetcher = DataFetcher(config, db_manager=None)
    tdx_fetcher.register_source(TdxHandler(config, None))
    global_data_cache.clear()
    t0 = time.perf_counter()
    tdx_rows = 0
    for code in codes:
        df = tdx_fetcher.get_stock_data(code, start, end, '1d', 'hfq')
        tdx_rows += df.height
    tdx_elapsed = time.perf_counter() - t0
    print(f'BASELINE_TDX requested={len(codes)} rows={tdx_rows:,} '
          f'elapsed={tdx_elapsed:.3f}s ({tdx_elapsed * 1000 / len(codes):.2f}ms/票)')


if __name__ == '__main__':
    args = parse_args()
    main(args.start, args.end, args.codes)
