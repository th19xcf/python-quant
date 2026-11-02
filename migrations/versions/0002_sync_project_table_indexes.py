"""同步项目表索引与 stock_basic 表注释

Revision ID: 0002_sync_indexes
Revises: 0001_baseline
Create Date: 2026-11-02

0001 基线使用 create_all(checkfirst=True)，已存在的表不会补建索引；
本迁移补齐模型声明的 8 个索引，并同步 stock_basic 表注释。
所有操作均做存在性判断，兼容“基线已建全量索引”的全新数据库。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0002_sync_indexes'
down_revision: str | None = '0001_baseline'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEXES: tuple[tuple[str, str, list[str]], ...] = (
    ('stock_adj_factor', 'idx_stock_adj_factor_trade_date', ['trade_date']),
    ('stock_adj_factor', 'idx_stock_adj_factor_ts_code_trade_date', ['ts_code', 'trade_date']),
    ('stock_daily', 'idx_stock_daily_trade_date', ['trade_date']),
    ('stock_daily', 'idx_stock_daily_ts_code_trade_date', ['ts_code', 'trade_date']),
    ('stock_dividend', 'idx_stock_dividend_dividend_year', ['dividend_year']),
    ('stock_dividend', 'idx_stock_dividend_ts_code', ['ts_code']),
    ('stock_minute', 'idx_stock_minute_trade_time', ['trade_time']),
    ('stock_minute', 'idx_stock_minute_ts_code_freq_trade_time', ['ts_code', 'freq', 'trade_time']),
)


def _existing_indexes(table_name: str) -> set[str]:
    inspector = sa.inspect(op.get_bind())
    return {index['name'] for index in inspector.get_indexes(table_name)}


def upgrade() -> None:
    for table_name, index_name, columns in _INDEXES:
        if index_name not in _existing_indexes(table_name):
            op.create_index(index_name, table_name, columns, unique=False)

    op.execute("ALTER TABLE stock_basic COMMENT = '股票基本信息表'")


def downgrade() -> None:
    for table_name, index_name, columns in reversed(_INDEXES):
        if index_name in _existing_indexes(table_name):
            op.drop_index(index_name, table_name=table_name)

    op.execute("ALTER TABLE stock_basic COMMENT = ''")
