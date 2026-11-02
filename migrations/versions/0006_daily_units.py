"""stock_daily 成交量/成交额单位对齐 K 线契约

Revision ID: 0006_daily_units
Revises: 0005_pre_close_double
Create Date: 2026-11-02

背景：K 线契约规定 stock_daily.vol 单位为「手」（1 手 = 100 股）、
amount 单位为「元」。实测 600000.SH 存量 22 行（2024-01-02 ~ 2024-01-31）
的 vol 与本地 TDX .day 原始股本数完全相等（比值 1.0000），即库里存的是
「股」而非「手」；amount 与 TDX 元值一致（比值 0.999998~1.000002），
数据本身正确，只是模型/列注释误写成「千元」。

本迁移做两件事：
1. 存量数据修正：vol 由「股」改为「手」（vol = vol / 100）。
   存量 22 行全部为整数股本，整除无精度损失；amount 不动。
2. 列注释对齐契约：vol='成交量(手，1手=100股)'、amount='成交额(元)'。

幂等性：MySQL 的 DDL 会隐式提交，无法与 DML 组成原子事务。这里用
vol 列注释中的契约标记「1手=100股」判断数据是否已换算：
- 注释无标记 → 视为仍是旧口径，执行一次除法并改写注释；
- 注释已有标记 → 跳过数据除法（可安全重复执行）。
正常路径下 alembic_version 保证本迁移只执行一次，标记仅用于中断恢复。
执行前已导出 stock_daily 全表 CSV 并记录 SHA256。

并发：共享库上避免无限等待元数据锁/行锁，会话级超时 60 秒，
超时报错退出，不 KILL 任何连接；遇 1205/1213 由调用方重试一次即停。

版本号保持 ≤32 字符：alembic_version.version_num 默认为 VARCHAR(32)。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0006_daily_units'
down_revision: str | None = '0005_pre_close_double'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = 'stock_daily'
_VOL_COMMENT = '成交量(手，1手=100股)'
_AMOUNT_COMMENT = '成交额(元)'
# 契约标记：出现在 vol 列注释中即表示「股→手」已完成
_VOL_MARKER = '1手=100股'
_VOL_COMMENT_LEGACY = '成交量(手)'
_AMOUNT_COMMENT_LEGACY = '成交额(千元)'


def _column_comments() -> dict[str, str]:
    """读取 stock_daily 各列的实际注释（小写列名 -> 注释）。"""
    bind = op.get_bind()
    rows = bind.execute(sa.text(
        "SELECT COLUMN_NAME, COLUMN_COMMENT FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table"
    ), {'table': _TABLE}).all()
    return {str(name).lower(): (comment or '') for name, comment in rows}


def _set_lock_timeouts() -> None:
    """限制元数据锁与行锁等待，避免在共享库上无限排队。"""
    op.execute('SET SESSION lock_wait_timeout = 60')
    op.execute('SET SESSION innodb_lock_wait_timeout = 60')


def _alter_comment(column: str, comment: str) -> None:
    """只改列注释，列类型/可空性/默认值保持不变（vol、amount 均为 FLOAT NULL）。"""
    op.execute(
        f"ALTER TABLE `{_TABLE}` MODIFY COLUMN `{column}` "
        f"FLOAT DEFAULT NULL COMMENT '{comment}'"
    )


def upgrade() -> None:
    _set_lock_timeouts()
    comments = _column_comments()

    # 数据修正：仅在注释还没有契约标记时执行一次，避免重复除法
    if _VOL_MARKER not in comments.get('vol', ''):
        op.execute(
            "UPDATE `stock_daily` SET `vol` = `vol` / 100 WHERE `vol` IS NOT NULL"
        )
        comments = _column_comments()

    if comments.get('vol') != _VOL_COMMENT:
        _alter_comment('vol', _VOL_COMMENT)
    if comments.get('amount') != _AMOUNT_COMMENT:
        _alter_comment('amount', _AMOUNT_COMMENT)


def downgrade() -> None:
    _set_lock_timeouts()
    comments = _column_comments()

    # 回退数据：注释仍带契约标记说明数据是「手」，乘回 100 恢复旧口径
    if _VOL_MARKER in comments.get('vol', ''):
        op.execute(
            "UPDATE `stock_daily` SET `vol` = `vol` * 100 WHERE `vol` IS NOT NULL"
        )
        comments = _column_comments()

    if comments.get('vol') != _VOL_COMMENT_LEGACY:
        _alter_comment('vol', _VOL_COMMENT_LEGACY)
    if comments.get('amount') != _AMOUNT_COMMENT_LEGACY:
        _alter_comment('amount', _AMOUNT_COMMENT_LEGACY)
