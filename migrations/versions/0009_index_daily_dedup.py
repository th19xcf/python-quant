"""清理 index_daily 重复 (ts_code, trade_date) 行

Revision ID: 0009_index_daily_dedup
Revises: 0008_index_amount_double
Create Date: 2026-11-02

取证（2026-11-02，实测 2401 行）：
- 000001.SH：482 个重复日期（964 行），399001.SZ：57 个（114 行），
  399006.SZ：57 个（114 行），合计 596 组、多余 596 行；
- 596/596 组的 close/vol/amount/pre_close/pct_chg 完全一致（0 组有值差异）；
- 重复行 id 间隔普遍很大（595/596 组 gap>1），created_at 相差数秒，
  即两次并发/紧邻的批量写入各插入了一份，属 upsert「先 SELECT 再 INSERT」
  非原子造成的同源重复，不是多来源口径冲突，也不是 ts_code 重写；
- 表上仅有 PRIMARY(id)，无 (ts_code, trade_date) 唯一约束。

保留规则：每组保留 updated_at 最新的一行，时间相同则保留 id 最大的一行；
因组内值完全一致，删除不丢数据。

幂等：重复执行时删除 0 行；删除后校验每组唯一，仍有重复则报错中止。

唯一约束：本次不加 (ts_code, trade_date) UNIQUE。upsert_rows 采用
「批量预加载 -> 逐行 INSERT」，并发下若加唯一索引会把静默重复变成
1062 异常，而调用方仅捕获连接类异常，整批写入会中断。要加需先把
upsert 改为原子 INSERT ... ON DUPLICATE KEY UPDATE，另行评估。

版本号保持 ≤32 字符：alembic_version.version_num 默认为 VARCHAR(32)。
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0009_index_daily_dedup'
down_revision: str | None = '0008_index_amount_double'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = 'index_daily'


def _set_lock_timeouts() -> None:
    """限制行锁等待，超时报错退出，不 KILL 连接。"""
    op.execute('SET SESSION lock_wait_timeout = 60')
    op.execute('SET SESSION innodb_lock_wait_timeout = 60')


def _duplicate_groups() -> int:
    bind = op.get_bind()
    return int(bind.execute(sa.text(
        "SELECT COUNT(*) FROM (SELECT ts_code, trade_date FROM index_daily "
        "GROUP BY ts_code, trade_date HAVING COUNT(*) > 1) t"
    )).scalar() or 0)


def upgrade() -> None:
    _set_lock_timeouts()
    # 保留 updated_at 最新者；时间相同或均为 NULL 时保留 id 最大者
    op.execute(
        "DELETE t1 FROM `index_daily` t1 "
        "JOIN `index_daily` t2 "
        "ON t1.`ts_code` = t2.`ts_code` AND t1.`trade_date` = t2.`trade_date` "
        "AND ("
        "  t1.`updated_at` < t2.`updated_at` "
        "  OR (t1.`updated_at` IS NULL AND t2.`updated_at` IS NOT NULL) "
        "  OR (t1.`updated_at` <=> t2.`updated_at` AND t1.`id` < t2.`id`)"
        ")"
    )
    remaining = _duplicate_groups()
    if remaining:
        raise RuntimeError(
            f'index_daily 去重后仍存在 {remaining} 个重复组，已中止迁移'
        )


def downgrade() -> None:
    # 去重是不可逆的数据清理，无法也不应恢复已删除的重复行；
    # 仅重置会话级锁等待设置，空实现保持迁移可回退到版本号。
    _set_lock_timeouts()
