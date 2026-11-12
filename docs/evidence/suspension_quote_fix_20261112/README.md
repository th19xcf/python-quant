# 停牌表 23 条「有成交却记停牌」回写（2026-11-12）

## 结论

`suspension_daily.parquet` 中 23 条 `is_suspended=true` 的记录，其 (ts_code, trade_date)
在 `quotes/stock/**` 里存在 `volume>0` 且 `amount>0` 的真实 bar，原判定被行情反证。
本次把这 23 条改为 `is_suspended=false`，`source` 由 `quote_gap` 改为
`bar_present_prev_quote_gap`（语义：行情 bar 存在，原判定来自行情缺口反推）。

| 项 | 值 |
|---|---|
| 行数 | 987,194 → 987,194（守恒） |
| `is_suspended=true` 行 | 987,194 → 987,171（-23） |
| 改动行（含 source） | 23 |
| 非目标行逐值改动 | 0 |
| 逐列 null 计数 | 前后全为 0，相等 |
| SHA256 | `72dfdbff…61e9` → `73418a62…f771` |
| 回滚快照 | `_backup/meta/suspension_daily_20261112/suspension_daily.parquet` |

## 分布

- `000001.SZ` 22 条：1991-05-06、1992-01-31、1992-02-11、1992-02-12、1992-02-17、
  1992-03-06、1992-03-26、1992-03-31、1992-06-15、1992-07-15、1992-07-27、
  1992-07-28、1992-08-31、1992-11-20、1992-12-08、1992-12-10、1992-12-11、
  1992-12-15、1993-02-18、1995-01-03、1996-01-02、1996-02-09
- `000002.SZ` 1 条：1992-04-20（volume 1,521 / amount 11,423,000）

逐条数值见 `suspension_rebutted_rows.csv`（含改前 source、run_id/run_pos/run_len/
confidence/detected_at 与当日行情 close/volume/amount）。

## 口径与追溯

- 其余字段原样保留：`run_id / run_pos / run_len / confidence / detected_at` 仍是原缺口
  派生结果，加上 `source` 里的 `prev_quote_gap` 前缀，原 `quote_gap` 事实不会丢失。
- 停牌并集 `src/data/quality/suspension_union.py` 已改为只采纳 `is_suspended != False`
  的行；`false` 行仅作留档，不参与停牌判定、不影响 L2 可交易层。
- `manifest.json` 增加 `corrections` 块；`coverage.json` 增加 `rebutted_rows` 并把
  「本表只登记停牌行」的说明改为「查询需按 `is_suspended != False` 过滤」。
- 本表由「交易日历 − 有 bar」反推，若将来整表重建，这 23 条会按当前行情自然不再产生；
  本次按任务要求保留行并改为 false，以维持行数守恒与可追溯性。

## 第二段：因子表补行与 000002.SZ 日期错位

### stock_adj_factor

| 项 | 值 |
|---|---|
| 行数 | 18,129,870 → 18,129,892（+22） |
| 新增 | `000001.SZ` 22 天，`qfq_factor` / `hfq_factor` 取两侧相邻行同值 |
| 迁移 | `000002.SZ` `1992-04-18`（周六）→ `1992-04-20`（周一），值不变（1 删 1 增） |
| 非目标行逐值改动 | 0（逐列 null 计数前后全 0） |
| SHA256 | `0196f654…ae7c` → `be29372d…81d0` |

22 天逐日断言「前一存在行 == 后一存在行」再取值，等价于确认该区间无除权事件；
若某日两侧不等，脚本直接断言失败、不落盘。

### 000002.SZ 1992-04-18 的判定依据（不是「缺行」而是「日期错位」）

- 因子表现状：`stock_adj_factor` / `stock_turnover` 记 `1992-04-18`（周六），
  两表各 8,515 行；行情 `000002.SZ` 记 `1992-04-20`（周一），也是 8,515 行。
- 换手率反推流通股 = `volume / turn`，`1992-04-14`~`1992-04-23` 恒为 **6,782.1806**
  （8 个正常交易日全部一致）；`04-18` 行 `turn = 0.2242641548`，
  `0.2242641548 × 6,782.1806 = 1,521`，与行情 `1992-04-20` 的 `volume = 1,521`、
  `amount = 11,423,000` 完全对齐。
- `adj_factor` 在 `04-16`、`04-17`、`04-18`、`04-21`、`04-22` 均为同一对
  `qfq=0.010085071507931177` / `hfq=1.7073091178650852`，区间无除权事件。
- 结论：同一根 bar 的日期被写成周六，处置为把 `04-18` 行原值迁到 `04-20`，
  不猜任何数值；迁移后两边日期集合一致（各 8,515 行）。

### stock_turnover

| 项 | 值 |
|---|---|
| 行数 | 18,055,750 → 18,055,751（+1） |
| 新增 | `000001.SZ 1991-09-30`，`turn = 0.0` |
| 迁移 | `000002.SZ 1992-04-18` → `1992-04-20`，`turn` / `source` / `fetched_at` 原值保留 |
| 非目标行逐值改动 | 0 |
| SHA256 | `eea4bf81…3f66` → `7f90eda1…a184` |

`1991-09-30` 的行情 bar 存在但 `volume = 0`、`amount = 0`，换手率按派生口径
`volume / float_share` 得 0；`source` 记 `derived:quotes_volume+float_share`。
这是全表唯一 `turn == 0` 的行（原文 18,055,750 行全部 `turn > 0`），
因为派生函数只对 `volume > 0` 的 bar 生成记录。之所以仍然补这一行：
该日 bar 确实存在（不是停牌缺口），补 0 让「有 bar 就有换手率记录」在本例成立；
若后续改为「零成交日不落行」，需要同步改 `src/data/local_store/turnover.py`
的过滤口径，不能只删这一行。

逐条数值见 `factor_backfill_rows.csv`。

## 第三段：六层原位掩码重建 + `searchsorted` 映射缺陷

### 3.1 正式层表掩码结果

只替换 23 个 (ts_code, date) 的 `in_layer`，值取自修复后全量重建候选
`_staging/layers_fixcheck_20261112`；其余行逐值不动（DG-31 显式 `otherwise(in_layer)` 兜底）。

| 层 | 行数 | 改动行 | 非目标改动 | null 计数 | SHA256 前 → 后 |
|---|---|---|---|---|---|
| `l0_all_history` | 18,922,964 | 0 | 0 | 0 | `f1b1a58c…178e`（未变） |
| `l1_listed_120d` | 18,922,964 | 0 | 0 | 0 | `999330f1…cb5d`（未变） |
| `l2_tradable` | 18,922,964 | 22 | 0 | 0 | `706a13d1…7baa` → `1f09ca97…7803` |
| `l3_risk_excluded` | 18,922,964 | 22 | 0 | 0 | `9994d4cd…ebb7` → `9604b549…a3ea` |
| `l4_liquid` | 18,922,964 | 11 | 0 | 0 | `42ff9a9d…a922` → `813f821c…24a5` |
| `l5_pool` | 18,922,964 | 11 | 0 | 0 | `7200fa5c…782d` → `8f109b9a…98d3` |

回滚快照 `_backup/meta/universe_layers_20261112/`（6 张 parquet + manifest.json）。
逐条结果见 `layer_rebuild_rows.csv`（6 层 × 23 键 = 138 行）。

目标行判定（`stage3_verify.json` 从正式文件重读）：

- `l2_tradable` / `l3_risk_excluded`：22 条为 `true`；`000001.SZ 1991-05-06` 仍为 `false`
  —— 该日是上市首日，L1「上市满 120 个交易日」未满足，与本次修复无关。
- `l4_liquid` / `l5_pool`：11 条为 `true`（1992-06-15 起至 1993-02-18 的 11 天 adv20 ≥ 2,000 万）；
  其余 12 条 `false` 是因为 1991-1992 早期成交额未达阈值，不是停牌导致。

### 3.2 全量重算对照（只报告，不写盘）

`_staging/layers_oldcheck_20261112`（原代码 + 回写前停牌表）vs
`_staging/layers_fixcheck_20261112`（修复后代码 + 回写后停牌表），差异逐条在
`_staging/suspension_fix_20261112/layer_full_recompute_diff.csv`：

| 层 | 行数 | true 行 旧 → 新 | 差异行 | 目标 23 键 | 09-30 映射缺陷 | 其它 |
|---|---|---|---|---|---|---|
| l0 | 18,922,966 | 18,922,966 → 18,922,966 | 0 | 0 | 0 | 0 |
| l1 | 18,922,966 | 18,223,740 → 18,223,740 | 0 | 0 | 0 | 0 |
| l2 | 18,922,966 | 17,442,528 → 17,442,549 | 23 | 22 | 1 | 0 |
| l3 | 18,922,966 | 16,785,767 → 16,785,788 | 23 | 22 | 1 | 0 |
| l4 | 18,922,966 | 13,333,775 → 13,333,786 | 13 | 11 | 0 | 2 |
| l5 | 18,922,966 | 13,198,838 → 13,198,849 | 13 | 11 | 0 | 2 |

> 全量重建的网格是 18,922,966 行，比正式表多 2 行（上游 `stock_basic` 漂移）；
> 正因如此本次只做原位掩码，不整表替换。

**`000001.SZ 1991-09-30`（映射缺陷，`l2`: true → false）**：行情里 1991-09-28（周六）、
1991-09-29（周日）各有一根 bar，原代码 `searchsorted(side='left')` 把两根都顶到 09-30，
再经 `unique(keep='first')` 保留 09-28 那根（volume 1,086），真实的 09-30 bar
（volume 0 / amount 0）被丢弃，于是判成「有成交」。修复后非交易日 bar 直接丢弃，
09-30 只剩真实 bar（volume 0）→ `traded=False` → `l2=False`。

**`000009.SZ 1992-10-12 / 1993-01-18`（l4/l5 各 1 次翻转，属修复的连带影响）**：
该股有 32 根非交易日 bar。旧映射把它们顶到下一交易日，27 处与真 bar 撞车时
「先到的非交易日 bar」取胜、真实成交额被替换，另有 3 处顶到了本来没有 bar 的交易日
（1991-09-30、1991-11-11、1992-02-03）；修复后这 32 根直接丢弃。
`adv20` 是 20 日滚动均值，32 处成交额替换会向后传播约 20 个交易日、共 227 个
`cal_idx` 的 adv20 与原来不同，其中恰好 2 处跨过 2,000 万阈值：

| 日期 | 旧 adv20 | 新 adv20 | 旧 l4 | 新 l4 |
|---|---|---|---|---|
| 1992-10-12 | 20,167,550 | 19,995,600 | true | false |
| 1993-01-18 | 19,882,550 | 20,257,450 | false | true |

即「其余标的受影响」只有这一只、这两行，且**均为旧映射用错成交额造成的偏差**，
方向与量级都可逐值复算。正式表本次未改这两行（只在报告里说明，是否随下次
层重建一起收敛由层构建任务决定）。

### 3.3 代码修复与用例

- [build_universe_layers.py](file:///e:/code/python/quant/tools/build_universe_layers.py#L171-L225)
  的 `_quote_panel` 增加日期精确匹配：`cal_array[cal_idx] == day_values` 不成立的 bar
  直接丢弃，不再向后顶；`unique(keep='first')` 仅用于同一交易日的重复保护。
- 新增 [test_universe_layers_quote_mapping.py](file:///e:/code/python/quant/tests/test_universe_layers_quote_mapping.py)：
  ① 周日 bar 不得顶到周一（周一真实 bar 的 `traded` 保持 false）；
  ② 只有周日 bar 时下一交易日不产生行；③ 超日历末日的 bar 不产生越界 `cal_idx`。

### 3.4 下游一致性（DG-23）

真实读路径 = 消费者按 `date`/`ts_code` lazy scan 正式路径：

```
pl.scan_parquet('data/local_store/meta/universe/layers/l2_tradable.parquet')
  .filter(ts_code == '000001.SZ' & date == 1996-02-09)  →  1 行, in_layer = true
```

同时 23 个目标键全部用同一 `scan_parquet` 路径读出并与期望逐条比对通过；
`manifest.json` 的 `files[*].rows/sha256/bytes` 与磁盘实测一致（6/6）。

### 3.5 本次明确不动的 220 / 219 行非交易日因子记录

`stock_adj_factor` 220 行、`stock_turnover` 219 行的 `trade_date` 不在交易日历上，
集中在 16 只 1991-1993 年的深市老股：

| 年份 | adj_factor | turnover |
|---|---|---|
| 1991 | 178 | 177 |
| 1992 | 30 | 30 |
| 1993 | 12 | 12 |

| 标的 | adj | turnover | 标的 | adj | turnover |
|---|---|---|---|---|---|
| 000002.SZ | 47 | 47 | 000011.SZ | 2 | 2 |
| 000004.SZ | 44 | 43 | 000012.SZ | 9 | 9 |
| 000001.SZ | 37 | 37 | 000014.SZ | 1 | 1 |
| 000005.SZ | 34 | 34 | 000016.SZ | 2 | 2 |
| 000009.SZ | 32 | 32 | 000017.SZ | 3 | 3 |
| 000006.SZ | 2 | 2 | 000018.SZ | 1 | 1 |
| 000007.SZ | 2 | 2 | 000020.SZ | 2 | 2 |
| 000008.SZ | 1 | 1 | 000505.SZ | 1 | 1 |

逐代码核对：adj_factor 的这 220 行与该股行情里「日期不在交易日历」的 bar **逐日一一对应**
（16 只全部一致，`0` 处偏差）；turnover 少 1 行是 `000004.SZ` 有一根 bar `volume=0`
未生成换手率记录。

为什么不能与本次一起改：这批记录不是「日期写错」，而是「TDX 原始字节里就有这批
bar、因子表照抄了 bar 日期」。处置方向取决于口径选择——按 TDX 原始字节保留，
还是按交易日规则清洗（丢弃或并入下一交易日）；后者与前一段 `000009.SZ` 的
`adv20` 连带影响同源，会同时改变因子表行数与层构建输入，属于「改变既定口径」，
须先出决策材料再动（DG-29 第三类）。本次只报告分布，不改任何一行。

## 复现

```powershell
.venv\Scripts\python.exe 2026-11-12\suspension-quote-fix\work\stage1_fix_suspension.py
.venv\Scripts\python.exe 2026-11-12\suspension-quote-fix\work\stage2_fix_factors.py
.venv\Scripts\python.exe 2026-11-12\suspension-quote-fix\work\stage3_patch_layers.py
.venv\Scripts\python.exe 2026-11-12\suspension-quote-fix\work\stage3_compare_layers.py
.venv\Scripts\python.exe 2026-11-12\suspension-quote-fix\work\stage3_verify.py
```
