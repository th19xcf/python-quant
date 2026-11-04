#!/usr/bin/env python3

"""生成「不可用标的」排除清单（只读）。

产出 ``data/local_store/meta/universe/universe_exclusions.parquet`` 与
``universe_exclusions.json``。行情 parquet、MySQL 均只读。

用法::

    .venv\\Scripts\\python.exe tools\\build_universe_exclusions.py
    .venv\\Scripts\\python.exe tools\\build_universe_exclusions.py --no-db --no-tdx
"""

import argparse
import sys
from pathlib import Path

from loguru import logger
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.data.local_store.store import resolve_store_root  # noqa: E402
from src.data.quality.tradable_universe import build_exclusions  # noqa: E402
from src.database.db_manager import DatabaseManager  # noqa: E402
from src.utils.config import get_config  # noqa: E402

BASIC_TABLES = {
    'stock': 'SELECT ts_code FROM stock_basic',
    'index': 'SELECT ts_code FROM index_basic',
    'fund': 'SELECT ts_code FROM fund_basic',
}


def _load_basic_tables() -> dict[str, list[str]]:
    cfg = get_config()
    dbm = DatabaseManager(cfg)
    dbm.connect()
    session = dbm.get_session()
    result: dict[str, list[str]] = {}
    for asset_class, sql in BASIC_TABLES.items():
        result[asset_class] = [row[0] for row in session.execute(text(sql)).all()]
    logger.info('基础表已读取: {}', {k: len(v) for k, v in result.items()})
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description='生成不可用标的排除清单（只读）')
    parser.add_argument('--store-root', default=None, help='本地列存根目录')
    parser.add_argument('--tdx-root', default=None, help='通达信 vipdoc 目录')
    parser.add_argument('--output', default=None, help='输出目录（默认 meta/universe）')
    parser.add_argument('--as-of', default=None, help='数据快照日（默认取 daily_index 最大末日）')
    parser.add_argument('--no-db', action='store_true', help='跳过基础表（不生成 no_quotes/no_basic_info）')
    parser.add_argument('--no-tdx', action='store_true', help='跳过 TDX（不生成内部缺口/截断两类）')
    args = parser.parse_args()

    cfg = get_config()
    store_root = Path(args.store_root) if args.store_root else resolve_store_root(cfg)
    tdx_root = None
    if not args.no_tdx:
        raw = args.tdx_root or getattr(cfg.data, 'tdx_data_path', '')
        if raw:
            tdx_root = Path(raw)
        else:
            logger.warning('未配置通达信路径且未传 --tdx-root，跳过缺口/截断两类')

    basics = None
    if not args.no_db:
        try:
            basics = _load_basic_tables()
        except SQLAlchemyError as exc:
            logger.warning('基础表读取失败，跳过 no_quotes/no_basic_info: {}', exc)

    summary = build_exclusions(
        store_root,
        tdx_root=tdx_root,
        basic_tables=basics,
        output_dir=args.output,
        as_of=args.as_of,
    )
    logger.info(
        '完成: {} 只标的有排除记录；block={} / all={}；健康覆盖率 {}%',
        summary['excluded_union']['all_reason_codes'],
        summary['excluded_union']['blocking_codes'],
        summary['excluded_union']['all_reason_codes'],
        round(summary['coverage_after_exclusion']['healthy_by_audit_definition']
              ['all']['code_retention'] * 100, 1),
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
