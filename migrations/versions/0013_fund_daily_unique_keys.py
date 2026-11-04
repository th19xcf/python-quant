"""fund_daily / closed_fund_daily 唯一键与日期索引

Revision ID: 0013_fund_daily_unique_keys
Revises: 0012_stock_adj_factor_unique
Create Date: 2026-11-03

依据 docs/基金日线唯一键与索引补齐评审.md 的结论执行（核查时两表均为 0 行、
无业务唯一键、无二级索引）：
- 唯一键 (ts_code, trade_date)：供 upsert_rows 的
  ON DUPLICATE KEY UPDATE / ON CONFLICT DO UPDATE 命中冲突分支；
- trade_date 二级索引：服务按交易日横截面扫描。

执行前只读检查重复组；若有重复则中止迁移并报错，不静默择一删除。
索引名幂等：已存在同名索引则跳过；已存在同列唯一索引也跳过唯一键创建。

downgrade 仅删除本迁移创建的索引，不删除数据。

版本号保持 ≤32 字符：alembic_version.version_num 默认为 VARCHAR(32)。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0013_fund_daily_unique_keys'
down_revision: str | None = '0012_stock_adj_factor_unique'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES = {
    'fund_daily': (
        'uk_fund_daily_code_date',
        'idx_fund_daily_trade_date',
    ),
    'closed_fund_daily': (
        'uk_closed_fund_daily_code_date',
        'idx_closed_fund_daily_trade_date',
    ),
}


def _set_lock_timeouts() -> None:
    """限制行锁与元数据锁等待，超时报错退出，不 KILL 连接。"""
    op.execute('SET SESSION lock_wait_timeout = 60')
    op.execute('SET SESSION innodb_lock_wait_timeout = 60')


def _index_state(table: str) -> dict[str, tuple[bool, str]]:
    """返回 {索引名: (是否唯一, 逗号连接的列顺序)}。"""
    bind = op.get_bind()
    rows = bind.execute(sa.text(
        "SELECT INDEX_NAME, NON_UNIQUE, "
        "GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) "
        "FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table "
        "GROUP BY INDEX_NAME, NON_UNIQUE"
    ), {'table': table}).all()
    return {
        str(name): (int(non_unique) == 0, str(columns))
        for name, non_unique, columns in rows
    }


def _duplicate_groups(table: str) -> int:
    bind = op.get_bind()
    return int(bind.execute(sa.text(
        f'SELECT COUNT(*) FROM (SELECT ts_code, trade_date FROM `{table}` '  # noqa: S608
        'GROUP BY ts_code, trade_date HAVING COUNT(*) > 1) t'
    )).scalar() or 0)


def upgrade() -> None:
    for table, (unique_name, date_index_name) in _TABLES.items():
        duplicates = _duplicate_groups(table)
        if duplicates:
            raise RuntimeError(
                f'{table} 按 (ts_code, trade_date) 仍有 {duplicates} 个重复组，'
                '加唯一约束前必须先处理，已中止迁移'
            )

        state = _index_state(table)
        if unique_name not in state and not any(
                is_unique and cols == 'ts_code,trade_date'
                for is_unique, cols in state.values()):
            _set_lock_timeouts()
            op.execute(
                f'ALTER TABLE `{table}` ADD CONSTRAINT `{unique_name}` '
                'UNIQUE (`ts_code`, `trade_date`)'
            )

        if date_index_name not in _index_state(table):
            _set_lock_timeouts()
            op.execute(
                f'CREATE INDEX `{date_index_name}` ON `{table}` (`trade_date`)'
            )


def downgrade() -> None:
    _set_lock_timeouts()
    for table, (unique_name, date_index_name) in _TABLES.items():
        state = _index_state(table)
        if date_index_name in state:
            op.execute(f'ALTER TABLE `{table}` DROP INDEX `{date_index_name}`')
        if unique_name in state:
            op.execute(f'ALTER TABLE `{table}` DROP INDEX `{unique_name}`')
