#!/usr/bin/env python3
"""stock_dividend 指数代码记录清理脚本（dry-run 默认，确认后加 --execute）。

只允许写 stock_dividend 表：先统计、导出 CSV，再按阈值校验后删除。

安全校验说明：命中集合必须只含指数代码。旧实现
`non_index_hits = [c for c in index_codes if not is_index_code(c)]`
在已按 is_index_code 过滤的 index_codes 上再取反，该列表恒为空，保护永不触发。
现改为 `find_non_index_hits`：先对全表代码按 is_index_code 取反得到「非指数集合」，
再与命中集合求交，构造违规命中时会被识别并中止。
"""
# ruff: noqa: T201  # CLI 脚本面向终端输出

import csv
import sys
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.database.db_manager import DatabaseManager  # noqa: E402
from src.database.models.stock import StockDividend  # noqa: E402
from src.utils.code_converter import is_index_code  # noqa: E402
from src.utils.config import Config  # noqa: E402

MAX_ROWS = 200
PROJECT_ROOT = Path(__file__).resolve().parents[1]

CSV_COLUMNS = [
    'id', 'ts_code', 'symbol', 'name', 'dividend_year', 'report_date',
    'record_date', 'ex_date', 'pay_date', 'cash_div', 'share_div',
    'total_div', 'rights_issue_price', 'rights_issue_ratio',
]


def _row_dict(record) -> dict:
    return {column: getattr(record, column, None) for column in CSV_COLUMNS}


def find_non_index_hits(all_codes: Iterable[str], hit_codes: Iterable[str]) -> list[str]:
    """从全表非指数代码集合反查，返回命中集合里混入的非指数代码。

    Args:
        all_codes: 全表全部代码，用于按 is_index_code 取反建立非指数集合
        hit_codes: 本次计划命中的代码

    Returns:
        命中集合与非指数集合的交集（正常应为空）
    """
    non_index = {code for code in all_codes if not is_index_code(code)}
    return sorted(set(hit_codes) & non_index)


def self_check() -> None:
    """证明安全校验确实会触发：构造一条非指数命中，必须被识别出来。"""
    all_codes = ['000001.SH', '399001.SZ', '600519.SH']
    hits = ['000001.SH', '600519.SH']
    detected = find_non_index_hits(all_codes, hits)
    assert detected == ['600519.SH'], f'非指数命中未被识别: {detected}'
    print(f'安全自检：命中 {hits} 中识别出非指数 {detected}（保护可触发）')


def main(execute: bool) -> int:
    config = Config()
    db_manager = DatabaseManager(config)
    db_manager.connect()
    session = db_manager.get_session()
    if session is None:
        print('ERROR: 无法获取数据库会话')
        return 1

    try:
        self_check()

        codes = [row[0] for row in session.query(StockDividend.ts_code).distinct().all()]
        index_codes = sorted(code for code in codes if is_index_code(code))
        non_index_hits = find_non_index_hits(codes, index_codes)

        per_code = {
            code: session.query(StockDividend).filter(StockDividend.ts_code == code).count()
            for code in index_codes
        }
        total = sum(per_code.values())

        print(f'命中指数代码 {len(index_codes)} 个，记录 {total} 条')
        for code, count in sorted(per_code.items()):
            print(f'  {code}: {count}')

        if total == 0:
            print('无需清理')
            return 0

        records = session.query(StockDividend).filter(
            StockDividend.ts_code.in_(index_codes)
        ).all()
        if len(records) != total:
            print(f'ABORT: 统计 {total} 条与实际查询 {len(records)} 条不一致，不删除')
            return 2

        PROJECT_ROOT.joinpath('logs').mkdir(exist_ok=True)
        stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_path = PROJECT_ROOT / 'logs' / f'stock_dividend_index_backup_{stamp}.csv'
        with open(csv_path, 'w', newline='', encoding='utf-8-sig') as fp:
            writer = csv.DictWriter(fp, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(_row_dict(record) for record in records)
        print(f'备份已导出: {csv_path}（{len(records)} 行）')

        if non_index_hits:
            print(f'ABORT: 命中集合含非指数代码 {non_index_hits}，不删除')
            return 2
        if total > MAX_ROWS:
            print(f'ABORT: 命中 {total} 条超过阈值 {MAX_ROWS}，不删除')
            return 2

        if not execute:
            print('DRY-RUN 完成，未删除任何记录（确认后加 --execute）')
            return 0

        deleted = session.query(StockDividend).filter(
            StockDividend.ts_code.in_(index_codes)
        ).delete(synchronize_session=False)
        session.commit()
        print(f'已删除 {deleted} 条 stock_dividend 指数记录')
        return 0
    finally:
        session.close()


if __name__ == '__main__':
    sys.exit(main('--execute' in sys.argv))
