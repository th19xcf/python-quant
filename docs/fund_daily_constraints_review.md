# fund_daily / closed_fund_daily 唯一键与索引补齐（待评审，未执行）

> 状态：**待评审**。本文档只描述建议的 DDL，未在任何环境执行，也未新增 alembic 迁移。
> 执行前置：确认业务写入方已按 `(ts_code, trade_date)` 幂等 upsert，避免唯一键冲突把批量写入打断。

## 1. 现状（2026-11-03 核查）

| 项 | fund_daily | closed_fund_daily |
|---|---|---|
| 行数 | 0 | 0 |
| 主键 | `id`（auto_increment） | `id`（auto_increment） |
| 业务唯一键 | **无** | **无** |
| 二级索引 | **无**（仅主键） | **无**（仅主键） |
| 全仓写入方 | 无 | 无 |

`ts_code` 为 `VARCHAR(20) NOT NULL`，`trade_date` 为 `DATE NOT NULL`。
其余字段（OHLC、pre_close、change、pct_chg、vol、amount、created_at、updated_at）
与 `stock_daily` 口径一致，未发现类型冲突。

## 2. 建议 DDL（MySQL）

### 2.1 变更前检查重复（只读）

```sql
SELECT ts_code, trade_date, COUNT(*) AS cnt
FROM fund_daily
GROUP BY ts_code, trade_date
HAVING cnt > 1
ORDER BY cnt DESC
LIMIT 100;

SELECT ts_code, trade_date, COUNT(*) AS cnt
FROM closed_fund_daily
GROUP BY ts_code, trade_date
HAVING cnt > 1
ORDER BY cnt DESC
LIMIT 100;
```

两表当前均为 0 行，预期结果为空；若未来已写入数据且查出重复，必须先停止
DDL，按下节“重复清理”处理后再添加唯一键。

### 2.2 添加唯一键与按日期检索索引

```sql
-- fund_daily
ALTER TABLE fund_daily
  ADD UNIQUE KEY uk_fund_daily_code_date (ts_code, trade_date),
  ADD KEY idx_fund_daily_trade_date (trade_date);

-- closed_fund_daily
ALTER TABLE closed_fund_daily
  ADD UNIQUE KEY uk_closed_fund_daily_code_date (ts_code, trade_date),
  ADD KEY idx_closed_fund_daily_trade_date (trade_date);
```

说明：
- 唯一键列顺序 `(ts_code, trade_date)`，同时可作为“单标的按日期范围”查询的最左前缀索引。
- 单独 `trade_date` 索引服务“按交易日横截面扫描”（如批量回测、市场宽度）。
- 不新增 `symbol` 等冗余列，避免与唯一键职责重叠。

### 2.3 重复清理（仅当 2.1 查出重复时）

```sql
-- 先导出重复行备份，再按 id 保留最新一条
CREATE TABLE fund_daily_dup_backup AS
SELECT * FROM fund_daily
WHERE (ts_code, trade_date) IN (
  SELECT ts_code, trade_date FROM (
    SELECT ts_code, trade_date, COUNT(*) c
    FROM fund_daily GROUP BY ts_code, trade_date HAVING c > 1
  ) t
);

DELETE f FROM fund_daily f
JOIN (
  SELECT ts_code, trade_date, MAX(id) AS keep_id
  FROM fund_daily GROUP BY ts_code, trade_date HAVING COUNT(*) > 1
) k ON f.ts_code = k.ts_code AND f.trade_date = k.trade_date
WHERE f.id <> k.keep_id;
```

`closed_fund_daily` 同理替换表名。清理前必须完成备份表创建与行数核对。

## 3. 影响面

| 对象 | 影响 |
|---|---|
| 读路径 | 无行为变化；按 `ts_code + trade_date` 的查询可利用唯一键最左前缀，横截面查询走新二级索引 |
| 写路径 | 未来若实现 `fund_daily` 落库，必须使用 `INSERT ... ON DUPLICATE KEY UPDATE` 或先查后写，禁止裸 `INSERT` |
| 数据量 | 两表当前 0 行，`ALTER TABLE` 为元数据级操作，秒级完成；无长事务/锁表风险 |
| 回测/画图 | 本地列存已按 Parquet 单文件存储基金数据，不依赖这两张表；本次 DDL 不改变任何运行时结果 |

## 4. 回滚

```sql
-- fund_daily
ALTER TABLE fund_daily
  DROP INDEX uk_fund_daily_code_date,
  DROP INDEX idx_fund_daily_trade_date;

-- closed_fund_daily
ALTER TABLE closed_fund_daily
  DROP INDEX uk_closed_fund_daily_code_date,
  DROP INDEX idx_closed_fund_daily_trade_date;
```

回滚只删除索引，不删除任何数据行。若执行过 2.3 的重复清理，需从
`fund_daily_dup_backup` / `closed_fund_daily_dup_backup` 回灌。

## 5. 为什么没有直接执行

- 本次任务约束明确要求“不跑 alembic、不改表结构”，因此只交付待评审 SQL。
- 项目迁移体系（`src/database/migrations/`）要求与代码写入方同步上线；当前两表
  无写入方，单独加唯一键会让“未来写入实现”缺少配套的 upsert 代码约束。
  建议与“基金日线落库”需求一并评审、同一批次执行。
