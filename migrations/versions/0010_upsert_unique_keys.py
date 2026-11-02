"""为原子 upsert 补业务唯一约束

Revision ID: 0010_upsert_unique_keys
Revises: 0009_index_daily_dedup
Create Date: 2026-11-02

背景：upsert_rows 已由「批量 SELECT -> 逐行 INSERT」改为原子写法
（MySQL ``INSERT ... ON DUPLICATE KEY UPDATE`` / SQLite ``ON CONFLICT DO UPDATE``）。
后者依赖唯一索引才能命中冲突分支，否则同一批次/跨批次重复键仍会插入重复行。

本迁移加约束的表（依据实际写入语义，不盲目全加）：

- index_daily (ts_code, trade_date)：指数日线身份，且 0009 已完成去重；
- stock_daily (ts_code, trade_date)：股票日线身份，写入方 akshare/baostock 均按此键 upsert；
- stock_minute (ts_code, freq, trade_time)：分钟线身份，freq 不可缺省
  （原 upsert 键缺 freq，会把 1min/5min 同时间戳互相覆盖，已同步修正代码）。

未加约束的表及原因：

- stock_basic / index_basic / fund_basic / closed_fund_basic：已有 ts_code 唯一约束；
- stock_adj_factor：天然键 (ts_code, trade_date) 且当前 0 重复，但写入方
  AdjFactorCalculator._batch_insert 仍是另一套「查询 -> add_all」逻辑、只捕获
  OSError/RuntimeError，加唯一索引会把并发竞态变成未捕获的 IntegrityError；
  待其切换到 upsert_rows 后再单独加；
- fund_daily / closed_fund_daily：无写入方、0 行，暂不加。

幂等：同名索引已存在则跳过；已存在同列的唯一索引（任意名字）也跳过；
可重复执行。加约束前逐表检查重复组，仍有重复则报错中止，不静默择一。

downgrade：删除本迁移创建的同名索引（只删名字匹配的，不动既有索引）。

版本号保持 ≤32 字符：alembic_version.version_num 默认为 VARCHAR(32)。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0010_upsert_unique_keys'
down_revision: str | None = '0009_index_daily_dedup'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# (表名, 约束/索引名, 列顺序)
_TARGETS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ('index_daily', 'uq_index_daily_ts_code_trade_date',
     ('ts_code', 'trade_date')),
    ('stock_daily', 'uq_stock_daily_ts_code_trade_date',
     ('ts_code', 'trade_date')),
    ('stock_minute', 'uq_stock_minute_ts_code_freq_trade_time',
     ('ts_code', 'freq', 'trade_time')),
)


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


def _duplicate_groups(table: str, columns: tuple[str, ...]) -> int:
    """统计按业务键分组后行数 > 1 的组数。"""
    col_sql = ', '.join(f'`{column}`' for column in columns)
    bind = op.get_bind()
    return int(bind.execute(sa.text(
        f'SELECT COUNT(*) FROM (SELECT {col_sql} FROM `{table}` '  # noqa: S608
        f'GROUP BY {col_sql} HAVING COUNT(*) > 1) t'
    )).scalar() or 0)


def _ensure_unique(table: str, name: str, columns: tuple[str, ...]) -> None:
    duplicates = _duplicate_groups(table, columns)
    if duplicates:
        raise RuntimeError(
            f'{table} 按 {columns} 仍有 {duplicates} 个重复组，'
            '加唯一约束前必须先处理，已中止迁移'
        )

    state = _index_state(table)
    if name in state:
        return
    target_columns = ','.join(columns)
    if any(is_unique and cols == target_columns
           for is_unique, cols in state.values()):
        # 已存在同列唯一索引（别的名字），不必重复创建
        return

    _set_lock_timeouts()
    col_sql = ', '.join(f'`{column}`' for column in columns)
    op.execute(
        f'ALTER TABLE `{table}` ADD CONSTRAINT `{name}` UNIQUE ({col_sql})'
    )


def upgrade() -> None:
    for table, name, columns in _TARGETS:
        _ensure_unique(table, name, columns)


def downgrade() -> None:
    _set_lock_timeouts()
    for table, name, _columns in _TARGETS:
        if name in _index_state(table):
            op.execute(f'ALTER TABLE `{table}` DROP INDEX `{name}`')
