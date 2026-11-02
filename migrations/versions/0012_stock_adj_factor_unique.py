"""stock_adj_factor 业务唯一约束 (ts_code, trade_date)

Revision ID: 0012_stock_adj_factor_unique
Revises: 0011_index_daily_double
Create Date: 2026-11-02

背景：复权因子写入已从「先 SELECT 再 add_all」的非原子查重切换到
upsert_rows（MySQL ON DUPLICATE KEY UPDATE / SQLite ON CONFLICT DO UPDATE）。
原子写法依赖唯一索引命中冲突分支，否则并发写入仍可能产生重复行。

加约束前先只读检查 (ts_code, trade_date) 重复组：有重复则中止迁移并报错，
不静默择一；无重复才创建。约束名幂等（已存在同名索引则跳过；已存在同列
唯一索引也跳过），可重复执行。

downgrade：仅删除本迁移创建的同名索引。

版本号保持 ≤32 字符：alembic_version.version_num 默认为 VARCHAR(32)。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0012_stock_adj_factor_unique'
down_revision: str | None = '0011_index_daily_double'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = 'stock_adj_factor'
_NAME = 'uq_stock_adj_factor_ts_code_trade_date'
_COLUMNS = ('ts_code', 'trade_date')


def _set_lock_timeouts() -> None:
    """限制行锁与元数据锁等待，超时报错退出，不 KILL 连接。"""
    op.execute('SET SESSION lock_wait_timeout = 60')
    op.execute('SET SESSION innodb_lock_wait_timeout = 60')


def _index_state() -> dict[str, tuple[bool, str]]:
    """返回 {索引名: (是否唯一, 逗号连接的列顺序)}。"""
    bind = op.get_bind()
    rows = bind.execute(sa.text(
        "SELECT INDEX_NAME, NON_UNIQUE, "
        "GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) "
        "FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table "
        "GROUP BY INDEX_NAME, NON_UNIQUE"
    ), {'table': _TABLE}).all()
    return {
        str(name): (int(non_unique) == 0, str(columns))
        for name, non_unique, columns in rows
    }


def _duplicate_groups() -> int:
    """统计按 (ts_code, trade_date) 分组后行数 > 1 的组数。"""
    col_sql = ', '.join(f'`{column}`' for column in _COLUMNS)
    bind = op.get_bind()
    return int(bind.execute(sa.text(
        f'SELECT COUNT(*) FROM (SELECT {col_sql} FROM `{_TABLE}` '  # noqa: S608
        f'GROUP BY {col_sql} HAVING COUNT(*) > 1) t'
    )).scalar() or 0)


def upgrade() -> None:
    duplicates = _duplicate_groups()
    if duplicates:
        raise RuntimeError(
            f'{_TABLE} 按 {_COLUMNS} 仍有 {duplicates} 个重复组，'
            '加唯一约束前必须先处理，已中止迁移'
        )

    state = _index_state()
    if _NAME in state:
        return
    if any(is_unique and cols == ','.join(_COLUMNS)
           for is_unique, cols in state.values()):
        return

    _set_lock_timeouts()
    col_sql = ', '.join(f'`{column}`' for column in _COLUMNS)
    op.execute(
        f'ALTER TABLE `{_TABLE}` ADD CONSTRAINT `{_NAME}` UNIQUE ({col_sql})'
    )


def downgrade() -> None:
    _set_lock_timeouts()
    if _NAME in _index_state():
        op.execute(f'ALTER TABLE `{_TABLE}` DROP INDEX `{_NAME}`')
