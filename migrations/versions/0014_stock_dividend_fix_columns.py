"""stock_dividend 增加人工修正列 cash_div_fix / share_div_fix

Revision ID: 0014_stock_dividend_fix_columns
Revises: 0013_fund_daily_unique_keys
Create Date: 2026-11-10

背景：本地分红快照支持 `*_fix` 覆盖列（优先级 no_ex_rights > *_fix > 原值，
见 src/data/adj_factor_calculator.py），但数据库表缺这两列，远程刷新
（build.py 的 refresh_remote_snapshot）重建快照时会把修正抹掉。

本迁移只做 ALTER TABLE ADD COLUMN（可空、无默认值），不改写、不删除任何既有行：
既有 53,974 行的新列均为 NULL，等价于"未修正"。
幂等：列已存在则跳过；lock_wait_timeout 超时报错退出。

downgrade 仅删除本迁移新增的两列。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0014_stock_dividend_fix_columns'
down_revision: str | None = '0013_fund_daily_unique_keys'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = 'stock_dividend'
_COLUMNS = (
    ('cash_div_fix', '每股派现修正值(元)，非空时覆盖 cash_div'),
    ('share_div_fix', '每股送转修正值(股)，非空时覆盖 share_div'),
)


def _set_lock_timeouts() -> None:
    op.execute('SET SESSION lock_wait_timeout = 60')
    op.execute('SET SESSION innodb_lock_wait_timeout = 60')


def _existing_columns(table: str) -> set[str]:
    bind = op.get_bind()
    rows = bind.execute(sa.text(
        'SELECT COLUMN_NAME FROM information_schema.COLUMNS '
        'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table'
    ), {'table': table}).all()
    return {str(r[0]) for r in rows}


def upgrade() -> None:
    existing = _existing_columns(_TABLE)
    for name, comment in _COLUMNS:
        if name in existing:
            continue
        _set_lock_timeouts()
        op.execute(
            f'ALTER TABLE `{_TABLE}` ADD COLUMN `{name}` FLOAT NULL '
            f"COMMENT '{comment}'"
        )


def downgrade() -> None:
    existing = _existing_columns(_TABLE)
    for name, _ in _COLUMNS:
        if name not in existing:
            continue
        _set_lock_timeouts()
        op.execute(f'ALTER TABLE `{_TABLE}` DROP COLUMN `{name}`')
