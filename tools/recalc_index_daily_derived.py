"""幂等重算 index_daily 的 pre_close / change / pct_chg（已按 TDX 回刷 close）。

昨收取「同标的上一交易日」的通达信 .day 收盘价，不用表内上一行——该表存在
整段缺失的交易日，用表内上一行会跨缺口错位。写库前导出全表备份并记 SHA256；
遇 1205/1213 重试一次即停，不 KILL 连接。
"""

import csv
import hashlib
import sys
from collections.abc import Callable
from datetime import date
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.index_daily_derived import recalc_derived_rows  # noqa: E402
from src.data.tdx_day_parser import (  # noqa: E402
    build_day_file_path,
    read_recent_records,
    resolve_market,
)
from src.utils.config import get_config  # noqa: E402

REPORT_DIR = Path(r'd:\code\python\quant\reports\index_daily_derived_20261102')
BATCH = 200


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fetch_rows(conn) -> list[dict]:
    rows = conn.execute(text(
        'SELECT id, ts_code, trade_date, `close`, pre_close, `change`, pct_chg '
        'FROM index_daily ORDER BY ts_code, trade_date')).mappings().all()
    return [dict(row) for row in rows]


def _backup(conn, name: str) -> tuple[int, str]:
    cols = [r[0] for r in conn.execute(text(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'index_daily' "
        "ORDER BY ORDINAL_POSITION")).all()]
    rows = conn.execute(text('SELECT * FROM index_daily ORDER BY id')).all()
    out = REPORT_DIR / name
    with out.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    digest = _sha256(out)
    print(f'备份 {out.name}: rows={len(rows)} sha256={digest}')
    return len(rows), digest


def _tdx_prev_close_provider(tdx_root: Path) -> Callable[[str, date], float | None]:
    """按通达信 .day 记录序列返回同标的上一交易日收盘价（带缓存）。"""
    cache: dict[str, dict[date, float]] = {}

    def _load(ts_code: str) -> dict[date, float]:
        if ts_code in cache:
            return cache[ts_code]
        code = ts_code.split('.')[0]
        path = build_day_file_path(tdx_root, resolve_market(code, ts_code), code)
        mapping: dict[date, float] = {}
        if path.exists():
            records = read_recent_records(path, None)
            for prev, cur in zip(records, records[1:], strict=False):
                mapping[cur['date']] = float(prev['close'])
        else:
            print(f'  TDX 文件缺失，{ts_code} 全部跳过: {path}')
        cache[ts_code] = mapping
        return mapping

    def _provider(ts_code: str, trade_date: date) -> float | None:
        return _load(ts_code).get(trade_date)

    return _provider


def _diff_stats(old: dict[int, dict], new: list[dict], field: str) -> str:
    deltas: list[float] = []
    changed = null_old = 0
    for row in new:
        prev = old[row['id']][field]
        if prev is None and row[field] is None:
            continue
        if prev is not None and row[field] is not None and row[field] == prev:
            continue
        changed += 1
        if prev is None:
            null_old += 1
        else:
            deltas.append(row[field] - prev)
    up = sum(1 for d in deltas if d > 0)
    down = sum(1 for d in deltas if d < 0)
    worst = max((abs(d) for d in deltas), default=0.0)
    return (f'{field}: 改动 {changed} 行（升 {up} / 降 {down} / 原值 NULL '
            f'{null_old}），max|Δ|={worst!r}')


def _write(engine, updates: list[dict]) -> None:
    sql = text(
        'UPDATE index_daily SET pre_close = :pre_close, '
        '`change` = :change, pct_chg = :pct_chg, updated_at = NOW() '
        'WHERE id = :id')

    def _run() -> None:
        with engine.begin() as conn:
            conn.execute(text('SET SESSION lock_wait_timeout = 60'))
            conn.execute(text('SET SESSION innodb_lock_wait_timeout = 60'))
            for start in range(0, len(updates), BATCH):
                conn.execute(sql, updates[start:start + BATCH])

    try:
        _run()
    except OperationalError as e:
        code = getattr(e.orig, 'args', [None])[0]
        if code in (1205, 1213):
            print(f'锁等待 {code}，重试一次')
            _run()
        else:
            raise


def main() -> None:
    config = get_config()
    db = config.database
    url = URL.create(
        drivername='mysql+pymysql', username=db.username, password=db.password,
        host=db.host, port=db.port, database=db.database,
        query={'charset': db.charset},
    )
    engine = create_engine(url, poolclass=sa.pool.NullPool)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    with engine.connect() as conn:
        rows, digest = _backup(conn, 'index_daily_before_tdx_derived.csv')
        before = _fetch_rows(conn)

    prev_close_of = _tdx_prev_close_provider(Path(config.data.tdx_data_path))
    computed = recalc_derived_rows(before, prev_close_of)
    old_by_id = {row['id']: row for row in before}
    changed = [row for row in computed if any(
        row[field] != old_by_id[row['id']][field]
        for field in ('pre_close', 'change', 'pct_chg'))]
    print(f'总行数 {len(before)}，有权威昨收可重算 {len(computed)}，'
          f'需写回 {len(changed)}')
    for field in ('pre_close', 'change', 'pct_chg'):
        print(_diff_stats(old_by_id, computed, field))

    if changed:
        _write(engine, changed)
    print(f'写回完成 {len(changed)} 行（备份 SHA256 {digest}）')

    with engine.connect() as conn:
        after = _fetch_rows(conn)
        for row in after:
            if row['pre_close'] is None or row['close'] is None:
                continue
            expected = prev_close_of(row['ts_code'], row['trade_date'])
            if expected is None:
                continue
            if (row['pre_close'] != expected
                    or row['change'] != row['close'] - expected
                    or abs(row['pct_chg'] - (
                        (row['close'] - expected) / expected * 100
                        if expected else 0.0)) > 0):
                raise RuntimeError(
                    f"派生字段不自洽: {row['ts_code']} {row['trade_date']} "
                    f'close={row["close"]} pre_close={row["pre_close"]} '
                    f'expected_pre={expected}')
    print(f'自洽性校验通过：{len(after)} 行 pre_close/change/pct_chg 与 '
          '权威昨收一致')
    engine.dispose()


if __name__ == '__main__':
    main()
