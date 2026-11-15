# stock_dividend「少 1,432 行」归因报告

- 生成时间：2026-11-06 16:40
- 结论：**不是数据丢失，是 2026-11-05 两次有意的「预案行去重」，共删除 1,429 行，
  全过程有 sha256 链与逐行清单可追溯。不需要恢复。**

## 1. 疑问的由来

盘点时对比发现：

| 对象 | 行数 | 文件 |
| --- | ---: | --- |
| 备份 `stock_dividend.20261105_114725.parquet` | 61,464 | `data/local_store/_backup/` |
| 当前 `factors/stock_dividend.parquet` | 60,032 | 生产表 |
| 差 | **1,432** | 原因未明 |

## 2. 真因：两次删除操作

`logs/dividend_delete_plan_rows_20261105/` 保留了完整审计件：

| 时间 | 动作 | 前 | 后 | 删除 |
| --- | --- | ---: | ---: | ---: |
| 11-05 15:09 | 删除 `ex_date` 为空的预案行 | 61,461 | 60,033 | 1,428 |
| 11-05 15:15 | 单独删除 300043.SZ 的计划行（0.03） | 60,033 | 60,032 | 1 |

`delete_result.json` 明确记录 `deleted: 1428, added: 0, changed_columns: 0`，
逐行清单 `deleted_rows_executed.csv`（1,428 行）与
`deleted_rows_executed_300043.csv`（1 行）可复现。

### sha256 链完整闭合

```
before  59f885179124418f0f77fa1f5c59954390ca9e175846183a53a968fb8ae05282  61,461 行
  ↓ 第一轮删除 1,428 行
        4cd2e04cfc7b42965ef5a1d1482d418b2c9a58ddb30e23b27b6d36d2b426017a  60,033 行
  ↓ 第二轮删除 1 行
        ccbbc04848c2d23d417701f3b2a380b4e6680015bea4258c0e4fad2b49a7388f  60,032 行  ← 与当前生产表字节一致
```

当前文件 sha256 实测 `ccbbc048…`，与最后一步 `after_sha256` 完全相同 → 该表自
2026-11-05 15:15 起未被再次改动，删除结果就是当前状态。

备份（11:47）比 before 快照（13:37）多 3 行，是 11:47~13:37 之间另有修正，
与本次疑问无关。

## 3. 删除是否正确：五项校验

| 校验项 | 结果 |
| --- | --- |
| 删除清单能否在 before 快照按 `row_idx` 定位 | 1,429 / 1,429 命中 |
| 删除行的 `ex_date` 是否为空 | **1,429 / 1,429 为空** |
| 删除行中「同股同年度已无任何 `ex_date` 非空实施行」的孤儿 | **0** |
| before 的非空 `ex_date` 行（59,361）与 current 是否逐键相等 | **完全相同** |
| `ex_date` 空行数变化 | 2,100 → 671，差 1,429（= 删除量） |
| 行数守恒 | 61,461 − 1,429 = 60,032 ✅ |

即：**每一条被删的预案行，同一标的同一年度都还有 `ex_date` 非空的实施行存在**，
删掉的是同一分红事件的重复预案记录（`progress` 为空、`report_period` 为空的
sweep 抓取残留），不是唯一事件。

## 4. 影响评估

- **复权计算零影响**：`src/data/adj_factor_calculator.py` 与
  `derive_total_dividend.derive()` 都只处理 `ex_date` 非空的事件行；`ex_date`
  为空的行本来就不参与因子与派现总额派生。
- **查询影响**：若下游需要「已公告未实施」的预案清单，被删的这批属于与实施行
  重复的历史残留，不构成信息缺失。
- **当前保留的 671 行空 `ex_date` 是真预案**：其中 530 行为 2026 年度，
  `progress` 为「董事会决议通过 / 预披露」，属尚未实施的真实事件，未被误删。

## 5. 处置建议

1. **不恢复**——删除口径正确，恢复只会把重复预案重新引入。
2. 备份文件 `data/local_store/_backup/stock_dividend.*.parquet`（5 份，约 13.5 MB）
   可以清理；若需留档，保留 `logs/dividend_delete_plan_rows_20261105/` 下的
   `stock_dividend.before.parquet` 与 `stock_dividend.before_300043.parquet` 即可
   （这两份是删除前后对照的唯一凭据）。

## 6. 复现命令

```bash
# 校验删除行是否全部 ex_date 为空、是否存在孤儿事件
.venv/Scripts/python.exe artifacts/verify_dividend_deletion2.py

# 校验 before 与 current 的非空 ex_date 行完全相等
.venv/Scripts/python.exe -c "
import polars as pl
b=pl.read_parquet('logs/dividend_delete_plan_rows_20261105/stock_dividend.before.parquet')
c=pl.read_parquet('data/local_store/factors/stock_dividend.parquet')
k=['ts_code','ex_date','cash_div','share_div','dividend_year']
print(b.filter(pl.col('ex_date').is_not_null()).select(k).sort(k).equals(
      c.filter(pl.col('ex_date').is_not_null()).select(k).sort(k)))"
```
