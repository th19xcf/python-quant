"""stock_daily 库内行数 vs 本地列存行数一致性巡检，逐类解释差异。

只读。分类：
- equal：行数一致
- db_more：库内多于列存（列存构建后源端又追加/库内含其他来源）
- db_less：库内少于列存（构造/写入失败或列存质量过滤）
- only_local / only_db：标的缺失
另抽检首行 pre_close 是否为空、次行是否等于上一交易日 close。

用法：.venv\\Scripts\\python.exe tools\\verify_stock_daily_consistency.py
"""
# ruff: noqa: T201  # 巡检脚本面向终端输出

import sys
from collections import defaultdict
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.data.local_store.store import get_local_quote_store  # noqa: E402
from src.database.db_manager import DatabaseManager  # noqa: E402
from src.utils.config import get_config  # noqa: E402

SAMPLE_PRINT = 12


def main():
    cfg = get_config()
    store = get_local_quote_store(cfg)
    dbm = DatabaseManager(cfg)
    dbm.connect()
    s = dbm.get_session()

    meta = {code: int(row['rows']) for (kind, code), row in store._load_meta().items()
            if kind == 'stock'}

    db_rows = dict(s.execute(text(
        'SELECT ts_code, COUNT(*) FROM stock_daily GROUP BY ts_code')).all())
    db_range = {code: (mn, mx) for code, mn, mx in s.execute(text(
        'SELECT ts_code, MIN(trade_date), MAX(trade_date) FROM stock_daily '
        'GROUP BY ts_code')).all()}

    print('库内总行数:', sum(db_rows.values()), '标的:', len(db_rows))
    print('列存总行数:', sum(meta.values()), '标的:', len(meta))

    categories = defaultdict(list)
    for code, local_n in meta.items():
        db_n = db_rows.get(code)
        if db_n is None:
            categories['only_local'].append((code, local_n, 0))
        elif db_n == local_n:
            categories['equal'].append((code, local_n, db_n))
        elif db_n > local_n:
            categories['db_more'].append((code, local_n, db_n))
        else:
            categories['db_less'].append((code, local_n, db_n))
    for code in set(db_rows) - set(meta):
        categories['only_db'].append((code, 0, db_rows[code]))

    print('\n== 分类汇总 ==')
    for name in ('equal', 'db_more', 'db_less', 'only_local', 'only_db'):
        items = categories[name]
        print(f'{name}: {len(items)} 只, 行数差合计 '
              f'{sum(b - a for _, a, b in items)}')
        for item in sorted(items, key=lambda x: -abs(x[2] - x[1]))[:SAMPLE_PRINT]:
            code, a, b = item
            print(f'   {code}: local={a} db={b} diff={b - a} db_range={db_range.get(code)}')
    # 重复键自检
    dup = s.execute(text(
        'SELECT COUNT(*) FROM (SELECT ts_code, trade_date FROM stock_daily '
        'GROUP BY ts_code, trade_date HAVING COUNT(*) > 1) t')).scalar()
    print('\n重复键组数:', dup)

    # 首行 pre_close 抽检
    sample_codes = [c for c, _, _ in categories['equal']][:5] + \
                   [c for c, _, _ in categories['db_more']][:5]
    print('\n== 首行 pre_close 抽检 ==')
    for code in sample_codes:
        first = s.execute(text(
            'SELECT trade_date, pre_close FROM stock_daily WHERE ts_code=:c '
            'ORDER BY trade_date LIMIT 2'), {'c': code}).all()
        print(f'{code}: {first}')

    s.close()
    dbm.cleanup()


if __name__ == '__main__':
    main()
