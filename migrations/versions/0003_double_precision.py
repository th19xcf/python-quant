"""价格与复权因子列改用双精度（MySQL DOUBLE）

Revision ID: 0003_double_precision
Revises: 0002_sync_indexes
Create Date: 2026-11-02

背景：stock_daily / stock_adj_factor 的价格与复权因子列原为 MySQL float
（4 字节单精度），落库后引入 ~1e-7 相对误差，复权因子连乘会进一步放大，
影响回测收益与信号。本迁移把这些列改为 DOUBLE（8 字节，对应 SQLAlchemy
Double / Python float64）。

幂等：升级/回滚都先读 information_schema 的实际列类型，已符合目标类型时跳过，
可重复执行；只改列类型，保留原有 NULL 约束、DEFAULT 与 COMMENT。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0003_double_precision'
down_revision: str | None = '0002_sync_indexes'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# (表名, 列名, 默认值子句, 注释)
_COLUMNS: tuple[tuple[str, str, str, str], ...] = (
    ('stock_daily', 'open', 'DEFAULT NULL', '开盘价'),
    ('stock_daily', 'high', 'DEFAULT NULL', '最高价'),
    ('stock_daily', 'low', 'DEFAULT NULL', '最低价'),
    ('stock_daily', 'close', 'DEFAULT NULL', '收盘价'),
    ('stock_daily', 'qfq_factor', 'DEFAULT 1', '前复权因子'),
    ('stock_daily', 'hfq_factor', 'DEFAULT 1', '后复权因子'),
    ('stock_adj_factor', 'qfq_factor', 'DEFAULT NULL', '前复权因子'),
    ('stock_adj_factor', 'hfq_factor', 'DEFAULT NULL', '后复权因子'),
)


def _column_types(table_names: Sequence[str]) -> dict[tuple[str, str], str]:
    """读取 information_schema 中这些表的实际列类型（小写，如 float / double）。"""
    bind = op.get_bind()
    stmt = sa.text(
        "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE "
        "FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table"
    )
    result: dict[tuple[str, str], str] = {}
    for table in table_names:
        for row in bind.execute(stmt, {'table': table}).all():
            result[(str(row[0]), str(row[1]))] = str(row[2]).lower()
    return result


def _modify_columns(target_type: str, current_type: str) -> None:
    """把仍为 current_type 的列改为 target_type；无待改列时不做任何 ALTER。"""
    tables = sorted({table for table, _, _, _ in _COLUMNS})
    types = _column_types(tables)

    by_table: dict[str, list[str]] = {}
    for table, column, default_sql, comment in _COLUMNS:
        if types.get((table, column)) == current_type:
            by_table.setdefault(table, []).append(
                f"MODIFY COLUMN `{column}` {target_type.upper()} {default_sql} COMMENT '{comment}'"
            )

    for table, clauses in by_table.items():
        # 共享库上避免无限等待 metadata lock：最多 60 秒，超时直接报错退出，
        # 不把 ALTER 留在队列里阻塞其他读写。
        op.execute("SET SESSION lock_wait_timeout = 60")
        op.execute(f"ALTER TABLE `{table}` " + ", ".join(clauses))


def upgrade() -> None:
    _modify_columns(target_type='DOUBLE', current_type='float')


def downgrade() -> None:
    _modify_columns(target_type='FLOAT', current_type='double')
