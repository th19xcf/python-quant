"""幂等补全 index_daily 缺失交易日（数据源：本地通达信指数 .day）。

- 只插入「TDX 有、库内没有」的行；既有键跳过，绝不改既有行；
- 写库前导出全表 CSV 并记 SHA256；遇 1205/1213 重试一次即停，不 KILL 连接；
- 重跑时缺失为 0，不产任何写入。

用法：.venv\\Scripts\\python.exe tools\\fill_index_daily_gaps.py
"""

import csv
import hashlib
import sys
from datetime import date, datetime
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import sessionmaker

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.data.index_daily_fill import (  # noqa: E402
    build_missing_rows,
    contiguous_segments,
    missing_trade_dates,
)
from src.data.tdx_day_parser import (  # noqa: E402
    build_day_file_path,
    read_recent_records,
    resolve_market,
)
from src.database.models.index import IndexDaily  # noqa: E402
from src.utils.config import get_config  # noqa: E402

CODES = ('000001.SH', '399001.SZ', '399006.SZ')
REPORT_DIR = Path(r'd:\code\python\quant\reports\index_daily_fill_20261102')
BATCH = 500


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _tdx_records(root: Path, ts_code: str) -> list[dict]:
    code = ts_code.split('.')[0]
    path = build_day_file_path(
        root, resolve_market(code, ts_code), code)
    records = read_recent_records(path, None)
    closes = [r['close'] for r in records]
    dup_dates = len(records) - len({r['date'] for r in records})
    print(f'{ts_code}: TDX {path.name} {len(records)} 条 '
          f'{records[0]["date"]}~{records[-1]["date"]} '
          f'close 区间 {min(closes)}~{max(closes)} 重复日期={dup_dates}')
    return records


def _backup(engine) -> str:
    with engine.connect() as conn:
        cols = [r[0] for r in conn.execute(text(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'index_daily' "
            'ORDER BY ORDINAL_POSITION')).all()]
        rows = conn.execute(text('SELECT * FROM index_daily ORDER BY id')).all()
    path = REPORT_DIR / 'index_daily_before_fill.csv'
    with path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    digest = _sha256(path)
    print(f'备份 {path.name}: rows={len(rows)} sha256={digest}')
    return digest


def _insert(engine, rows: list[dict]) -> int:
    session = sessionmaker(bind=engine)()

    def _run() -> int:
        inserted = 0
        for start in range(0, len(rows), BATCH):
            batch = rows[start:start + BATCH]
            session.execute(text('SET SESSION lock_wait_timeout = 60'))
            session.execute(text('SET SESSION innodb_lock_wait_timeout = 60'))
            session.add_all([IndexDaily(**row) for row in batch])
            session.commit()
            inserted += len(batch)
        return inserted

    try:
        return _run()
    except OperationalError as exc:
        code = getattr(exc.orig, 'args', [None])[0]
        if code in (1205, 1213):
            print(f'锁等待 {code}，重试一次')
            session.rollback()
            return _run()
        raise
    except IntegrityError as exc:
        session.rollback()
        raise RuntimeError(f'插入冲突（既有键被改？）：{exc}') from exc
    finally:
        session.close()


def main() -> None:
    config = get_config()
    db = config.database
    url = URL.create(
        drivername='mysql+pymysql', username=db.username, password=db.password,
        host=db.host, port=db.port, database=db.database,
        query={'charset': db.charset},
    )
    engine = create_engine(url, poolclass=sa.pool.NullPool)
    root = Path(config.data.tdx_data_path)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    tdx: dict[str, list[dict]] = {}
    db_dates: dict[str, list] = {}
    db_map: dict[str, dict] = {}
    with engine.connect() as conn:
        for code in CODES:
            tdx[code] = _tdx_records(root, code)
            rows = conn.execute(text(
                'SELECT trade_date, `open`, high, low, `close`, pre_close, '
                '`change`, pct_chg, vol, amount FROM index_daily '
                'WHERE ts_code = :c ORDER BY trade_date'), {'c': code}).mappings().all()
            db_dates[code] = [r['trade_date'] for r in rows]
            db_map[code] = {r['trade_date']: dict(r) for r in rows}

    plan: dict[str, list[dict]] = {}
    for code in CODES:
        tdx_dates = [r['date'] for r in tdx[code]]
        missing = missing_trade_dates(tdx_dates, db_dates[code])
        rows = build_missing_rows(code, tdx[code], db_dates[code])
        assert len(rows) == len(missing), (
            f'{code} 生成行数 {len(rows)} != 缺口数 {len(missing)}')
        plan[code] = rows

        first_db = db_dates[code][0] if db_dates[code] else None
        last_db = db_dates[code][-1] if db_dates[code] else None
        leading = sum(1 for d in missing if first_db and d < first_db)
        trailing = sum(1 for d in missing if last_db and d > last_db)
        inside = len(missing) - leading - trailing
        print(f'\n=== {code} ===')
        print(f'TDX {len(tdx_dates)} 天；库内 {len(db_dates[code])} 行'
              f'（{first_db}~{last_db}）；缺口 {len(missing)} 天'
              f'（区间前 {leading} / 区间内 {inside} / 区间后 {trailing}）')
        segments = contiguous_segments(missing)
        print(f'连续片段 {len(segments)} 段')
        for start, end in segments[:12]:
            count = sum(1 for d in missing if start <= d <= end)
            print(f'  {start} ~ {end} 共 {count} 天')
        if len(segments) > 12:
            print(f'  ... 其余 {len(segments) - 12} 段略')

    total = sum(len(rows) for rows in plan.values())
    print(f'\n合计待插入 {total} 行')
    if total == 0:
        print('无缺口，脚本幂等退出（未写库）')
        return

    digest = _backup(engine)
    inserted = _insert(engine, [row for rows in plan.values() for row in rows])
    print(f'实际插入 {inserted} 行（备份 SHA256 {digest}）')

    # ---- 补后校验 ----
    with engine.connect() as conn:
        print('\n=== 补后校验 ===')
        all_dates: dict[str, list] = {}
        for code in CODES:
            n = conn.execute(text(
                'SELECT COUNT(*) FROM index_daily WHERE ts_code = :c'),
                {'c': code}).scalar()
            dates = conn.execute(text(
                'SELECT trade_date FROM index_daily WHERE ts_code = :c '
                'ORDER BY trade_date'), {'c': code}).scalars().all()
            all_dates[code] = list(dates)
            missing_after = missing_trade_dates(
                [r['date'] for r in tdx[code]], all_dates[code])
            print(f'{code}: 行数 {len(db_dates[code])} -> {n}；'
                  f'剩余缺口 {len(missing_after)}')
            assert len(missing_after) == 0, f'{code} 仍有缺口'

        dup = conn.execute(text(
            'SELECT COUNT(*) FROM (SELECT ts_code, trade_date FROM index_daily '
            'GROUP BY ts_code, trade_date HAVING COUNT(*) > 1) t')).scalar()
        print(f'全表重复组: {dup}')
        assert dup == 0

        # 补入行逐字段与 TDX 比对 + pre_close 校验（整表取回后内存比对）
        for code in CODES:
            by_date = {r['date']: r for r in tdx[code]}
            prev_close = {
                tdx[code][i]['date']: tdx[code][i - 1]['close']
                for i in range(1, len(tdx[code]))
            }
            stored_all = {
                row['trade_date']: dict(row)
                for row in conn.execute(text(
                    'SELECT trade_date, `open`, high, low, `close`, pre_close, '
                    '`change`, pct_chg, vol, amount FROM index_daily '
                    'WHERE ts_code = :c'), {'c': code}).mappings()
            }
            worst_close = worst_vol = worst_amount = 0.0
            bad_prev = bad_formula = 0
            for row in plan[code]:
                stored = stored_all[row['trade_date']]
                rec = by_date[row['trade_date']]
                worst_close = max(worst_close, abs(stored['close'] - rec['close']))
                worst_vol = max(worst_vol, abs(stored['vol'] - rec['volume']))
                if stored['amount'] is not None and rec['amount'] is not None:
                    worst_amount = max(
                        worst_amount, abs(stored['amount'] - rec['amount']))
                expected = prev_close.get(row['trade_date'])
                if expected is None:
                    if any(stored[f] is not None
                           for f in ('pre_close', 'change', 'pct_chg')):
                        bad_prev += 1
                elif stored['pre_close'] != expected:
                    bad_prev += 1
                if expected is not None and (
                        stored['change'] != stored['close'] - expected
                        or stored['pct_chg'] != (
                            (stored['close'] - expected) / expected * 100
                            if expected else 0.0)):
                    bad_formula += 1
            print(f'{code}: 补入行 close max|Δ|={worst_close!r} '
                  f'vol max|Δ|={worst_vol!r} amount max|Δ|={worst_amount!r} '
                  f'pre_close 不符={bad_prev} 派生公式不符={bad_formula}')
            assert bad_prev == 0
            assert bad_formula == 0

        # 既有行未变：与备份 CSV 逐 id 比对 close/vol/amount
        backup = REPORT_DIR / 'index_daily_before_fill.csv'
        with backup.open(encoding='utf-8', newline='') as f:
            before = {int(r['id']): r for r in csv.DictReader(f)}
        changed = 0
        for row in conn.execute(text(
                'SELECT id, `close`, vol, amount FROM index_daily')).mappings():
            old = before.get(row['id'])
            if old is None:
                continue
            for field in ('close', 'vol', 'amount'):
                old_value = old[field]
                new_value = row[field]
                if old_value == '' and new_value is None:
                    continue
                if old_value == '' or new_value is None:
                    changed += 1
                    break
                if float(old_value) != float(new_value):
                    changed += 1
                    break
        print(f'既有行 close/vol/amount 变动: {changed} 行')
        assert changed == 0

        # 抽样（含缺口边界与序列首行）
        print('\n抽样补入行（TDX 原值 vs 库内）:')
        samples = [
            ('000001.SH', date(1990, 12, 19)),  # TDX 首行，无昨收
            ('000001.SH', date(2023, 1, 3)),    # 缺口后首行
            ('399001.SZ', date(1991, 4, 3)),    # TDX 首行，无昨收
            ('399001.SZ', date(2024, 2, 28)),   # 库内起始日前最后一个缺口行
            ('399006.SZ', date(2026, 10, 30)),  # 库内结束日之后的尾部补行
        ]
        for code, trade_date in samples:
            rec = {r['date']: r for r in tdx[code]}.get(trade_date)
            if rec is None:
                print(f'  {code} {trade_date} 不在 TDX，跳过')
                continue
            stored = conn.execute(text(
                'SELECT trade_date, `close`, pre_close, vol, amount '
                'FROM index_daily WHERE ts_code = :c AND trade_date = :d'),
                {'c': code, 'd': trade_date}).mappings().one()
            print(f'  {code} {stored["trade_date"]} '
                  f'close={stored["close"]} (TDX {rec["close"]}) '
                  f'pre_close={stored["pre_close"]} '
                  f'vol={stored["vol"]} (TDX {rec["volume"]}) '
                  f'amount={stored["amount"]} (TDX {rec["amount"]})')
    engine.dispose()
    print(f'\n补全完成 {datetime.now():%Y-%m-%d %H:%M:%S}')


if __name__ == '__main__':
    main()
