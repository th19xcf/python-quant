"""index_daily 价格与成交量列改用双精度（MySQL DOUBLE）

Revision ID: 0011_index_daily_double
Revises: 0010_upsert_unique_keys
Create Date: 2026-11-02

背景：index_daily 的 open/high/low/close/pre_close/change/pct_chg/vol 原为
MySQL FLOAT（4 字节单精度）。指数点位（~3000）与成交量（~1e8 手）在该精度下
可表示间隔分别为 ~2e-4 与 ~8，落库会带来 ~1e-7 相对误差；升为 DOUBLE 后，
后续写入按 Python float64 精度保存。

注意（同 0005/0008）：ALTER ... MODIFY COLUMN 只改列类型、不重写存量值，
存量行的 float32 尾数仍保留；这些列的权威源（本地 TDX .day）在迁移后按源值
回刷，才能把差异降到「来源差异」级别。amount 已是 DOUBLE（0008），本次不碰。

幂等：先读 information_schema 的实际列类型，已是 double 的列跳过，可重复执行；
逐列保留原有 NULL/NOT NULL、DEFAULT 与 COMMENT。

downgrade：同样的方式改回 FLOAT。

版本号保持 ≤32 字符：alembic_version.version_num 默认为 VARCHAR(32)。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0011_index_daily_double'
down_revision: str | None = '0010_upsert_unique_keys'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = 'index_daily'
_COLUMNS: tuple[str, ...] = (
    'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol',
)


def _set_lock_timeouts() -> None:
    """限制元数据锁与行锁等待，超时报错退出，不 KILL 连接。"""
    op.execute('SET SESSION lock_wait_timeout = 60')
    op.execute('SET SESSION innodb_lock_wait_timeout = 60')


def _column_states() -> dict[str, tuple[str, str, str | None, str]]:
    """返回 {列名: (实际类型小写, IS_NULLABLE, DEFAULT, COMMENT)}。"""
    placeholders = ', '.join(f':c{i}' for i in range(len(_COLUMNS)))
    params = {'table': _TABLE}
    params.update({f'c{i}': column for i, column in enumerate(_COLUMNS)})
    bind = op.get_bind()
    rows = bind.execute(sa.text(
        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, "  # noqa: S608
        "COLUMN_COMMENT FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table "
        f"AND COLUMN_NAME IN ({placeholders})"
    ), params).all()
    return {
        str(name): (str(column_type).lower(), str(nullable),
                    None if default is None else str(default), str(comment or ''))
        for name, column_type, nullable, default, comment in rows
    }


def _change_type(target_type: str) -> None:
    """把仍是另一精度类型的列一次性 MODIFY 到 target_type。"""
    states = _column_states()
    clauses = []
    for column in _COLUMNS:
        state = states.get(column)
        if state is None:
            continue
        current_type, nullable, default, comment = state
        if current_type == target_type.lower():
            continue
        nullability = 'NULL' if nullable == 'YES' else 'NOT NULL'
        if default is None:
            default_sql = ' DEFAULT NULL' if nullable == 'YES' else ''
        else:
            default_sql = f' DEFAULT {default}'
        comment_sql = comment.replace("'", "''")
        clauses.append(
            f'MODIFY COLUMN `{column}` {target_type.upper()}{default_sql} '
            f'{nullability} COMMENT \'{comment_sql}\''
        )
    if not clauses:
        return
    _set_lock_timeouts()
    op.execute(f'ALTER TABLE `{_TABLE}` ' + ', '.join(clauses))


def upgrade() -> None:
    _change_type('DOUBLE')


def downgrade() -> None:
    _change_type('FLOAT')
