"""index_daily.amount 改用双精度（MySQL DOUBLE），并明确 NULL 语义

Revision ID: 0008_index_amount_double
Revises: 0007_units_minute_index
Create Date: 2026-11-02

背景：index_daily.amount 原为 MySQL FLOAT（4 字节单精度）。指数成交额量级
可达 1e12，float32 在该量级的可表示间隔约 1.3e5，落库后相对误差约 1e-7；
升为 DOUBLE（8 字节）后，后续写入可按 Python float64 精度保存。

注意（同 0005_pre_close_double）：ALTER ... MODIFY COLUMN 只改列类型，
不重写既有值；存量行的 float32 尾数仍然保留，需由按源端重新写入才能清除。

NULL 语义：amount 允许为空，NULL 表示「上游未提供成交额」（例如 AkShare
的 stock_zh_index_daily 只给价格与成交量，没有成交额），不得用 0 冒充，
避免下游统计把缺失当成真实零成交。

幂等：先读 information_schema 的实际列类型与注释，已是 double 且注释一致
时跳过，可重复执行；保留 NULL 约束与 DEFAULT。

版本号保持 ≤32 字符：alembic_version.version_num 默认为 VARCHAR(32)。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0008_index_amount_double'
down_revision: str | None = '0007_units_minute_index'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = 'index_daily'
_COLUMN = 'amount'
_COMMENT = '成交额(元)，NULL=上游未提供'
_COMMENT_LEGACY = '成交额(元)'


def _current() -> tuple[str | None, str]:
    """读取 information_schema 中 amount 的实际列类型（小写）与注释。"""
    bind = op.get_bind()
    row = bind.execute(sa.text(
        "SELECT COLUMN_TYPE, COLUMN_COMMENT FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table "
        "AND COLUMN_NAME = :column"
    ), {'table': _TABLE, 'column': _COLUMN}).first()
    return (str(row[0]).lower(), str(row[1] or '')) if row else (None, '')


def _set_lock_timeouts() -> None:
    """限制元数据锁与行锁等待，超时报错退出，不 KILL 连接。"""
    op.execute('SET SESSION lock_wait_timeout = 60')
    op.execute('SET SESSION innodb_lock_wait_timeout = 60')


def _modify(target_type: str, current_type: str, comment: str) -> None:
    """仅在当前类型为 current_type（或注释不一致）时执行 ALTER。"""
    current, current_comment = _current()
    if current == current_type or (current == target_type and current_comment != comment):
        _set_lock_timeouts()
        op.execute(
            f"ALTER TABLE `{_TABLE}` MODIFY COLUMN `{_COLUMN}` "
            f"{target_type.upper()} DEFAULT NULL COMMENT '{comment}'"  # noqa: S608
        )


def upgrade() -> None:
    _modify(target_type='DOUBLE', current_type='float', comment=_COMMENT)


def downgrade() -> None:
    _modify(target_type='FLOAT', current_type='double', comment=_COMMENT_LEGACY)
