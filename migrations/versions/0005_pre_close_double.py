"""stock_daily.pre_close 改用双精度（MySQL DOUBLE）

Revision ID: 0005_pre_close_double
Revises: 0004_merge_heads
Create Date: 2026-11-02

背景：0003 已把 stock_daily 的 OHLC 与复权因子列改为 DOUBLE，但昨收价
pre_close 仍为 FLOAT。昨收价直接参与涨跌停价计算（Decimal 取整到分），
单精度尾差会在边界价位造成 ±0.01 的判定漂移，因此一并对齐为 DOUBLE。

注意：MySQL 的 ALTER ... MODIFY COLUMN 只改列类型、不重写既有值；
本迁移只负责 DDL，存量数据的 float32 尾数需由单独的重刷作业
（按数据源原始价重新写入，字段口径与 upsert 写库路径一致）清除。

幂等：先读 information_schema 实际列类型，已是 double 时跳过，
可重复执行；保留 NULL 约束、DEFAULT 与 COMMENT。

版本号保持 ≤32 字符：alembic_version.version_num 默认为 VARCHAR(32)。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0005_pre_close_double'
down_revision: str | None = '0004_merge_heads'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = 'stock_daily'
_COLUMN = 'pre_close'


def _current_column_type() -> str | None:
    """读取 information_schema 中 pre_close 的实际列类型（小写）。"""
    bind = op.get_bind()
    stmt = sa.text(
        "SELECT COLUMN_TYPE FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table AND COLUMN_NAME = :column"
    )
    row = bind.execute(stmt, {'table': _TABLE, 'column': _COLUMN}).first()
    return str(row[0]).lower() if row else None


def _modify(target_type: str, current_type: str) -> None:
    """仅在列仍为目标之外的精度时执行 ALTER，避免重复变更。"""
    if _current_column_type() == current_type:
        # 共享库上避免无限等待 metadata lock：最多 60 秒，超时报错退出，
        # 不把 ALTER 留在队列里阻塞其他读写。
        op.execute('SET SESSION lock_wait_timeout = 60')
        op.execute(
            f"ALTER TABLE `{_TABLE}` MODIFY COLUMN `{_COLUMN}` "
            f"{target_type.upper()} DEFAULT NULL COMMENT '昨收价'"
        )


def upgrade() -> None:
    _modify(target_type='DOUBLE', current_type='float')


def downgrade() -> None:
    _modify(target_type='FLOAT', current_type='double')
