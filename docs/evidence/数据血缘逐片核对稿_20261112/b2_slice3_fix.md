# 片 B2-3 落地：`stock_turnover` 回归用例 / 清单语义 / 140 行坏值分类与 19 行修正

> 上游依据：[b2_slice3_check.md](file:///e:/code/python/quant/2026-11-12/data-lineage/outputs/b2_slice3_check.md) 的 ⑤ / ⑥ / ① / ③ / ④ 五项。
> 本轮按「代码两件 → ①分类+定点修 → ③语义登记 → ④只查不动」顺序执行；清单 L76 未动（转文档那一路）。

## §0 状态

| 项 | 状态 | 一句话结论 |
|---|---|---|
| ⑤ `touched` 为空回归用例 | ✅ 已加，变异验证红 | 新用例锁定 anti-join 保留底表；把整体替换兜底临时加回后该用例失败（`assert 1 == 4`），还原后 5 用例全绿 |
| ⑥ manifest `produced_by` | ✅ 已补 | `_ROLES` 改为 `sync-turnover + derive-turnover（本地派生，98.95% 行）`，清单已刷新 |
| ① 140 行坏值 | ✅ 19 行已修 + 121 行已分类 | 19 行证成 100 倍单位错并整表原子替换；121 行逐类归因，**未改** |
| ③ `source_semantics` | ✅ 已登记 | 两套语义（baostock / 本地派生）逐条写明，明确禁止混比 |
| ④ 18 只退市标的 | ✅ 已查，**不动** | 无在途任务：退市回补已在 2026-11-09 收口，当前无进程、无新进度写入 |
| 源码提交 | ⏸ 被策略拦截 | 两个代码文件已改完并通过测试，`git commit` 被环境风险策略拒绝，工作区保留改动待批 |

---

## §1 ⑤ 「目标表存在 + `touched` 为空」回归用例

| 项 | 值 |
|---|---|
| 文件 | [tests/test_turnover_assemble.py](file:///e:/code/python/quant/tests/test_turnover_assemble.py#L83-L107)（新增 1 例，共 5 例） |
| 用例名 | `test_target_exists_with_empty_touched_keeps_history` |
| 构造 | 目标表先写 3 行（600000.SH 2 天 + 000001.SZ 1 天），分块只带 600000.SH 第 3 天，`touched=set()` |
| 断言 | `rows == 4`、表内 4 行、两只标的都在、600000.SH 的 3 天日期序列完整 |
| 为何能锁住 | 清表事故的唯一触发条件就是「目标表存在 + `touched` 为空」；旧实现在该组合下跳过 anti-join 直接用分块整体替换，本用例断言行数 4 会直接失败 |

**变异验证（实测）**：在 [_assemble](file:///e:/code/python/quant/src/data/local_store/turnover.py#L533-L537) 的 `if target.exists():` 前临时插入

```python
    if target.exists() and not touched:
        source = new_lazy
    elif target.exists():
        ...
```

运行该用例 → `assert 1 == 4`（底表被削成 1 行），即兜底一旦回归用例必红；还原后 `git diff src/data/local_store/turnover.py` 为空。

当前唯一路径（无任何 `touched` 分支）：

- [:529-530](file:///e:/code/python/quant/src/data/local_store/turnover.py#L529-L530) `if not parts: return None`（唯一提前返回）
- [:533-537](file:///e:/code/python/quant/src/data/local_store/turnover.py#L533-L537) `target.exists()` → 取新分块 `(ts_code, trade_date)` 去重 → 与底表 `how='anti'` → `concat([base, new_lazy], diagonal_relaxed)`
- [:542](file:///e:/code/python/quant/src/data/local_store/turnover.py#L542) `write_parquet_atomic`

测试结果：`pytest tests/test_turnover_assemble.py -q` → 5 passed；与清单相关用例合并跑 14 passed；`ruff check` 通过。

---

## §2 ⑥ manifest `produced_by`

| 项 | 值 |
|---|---|
| 代码 | [factors_manifest.py](file:///e:/code/python/quant/src/data/factors_manifest.py#L45-L47) `_ROLES['stock_turnover.parquet']` |
| 旧值 | `python -m src.data.local_store sync-turnover` |
| 新值 | `python -m src.data.local_store sync-turnover（baostock 回补） + derive-turnover（本地派生，98.95% 行）` |
| 落盘 | `data/local_store/factors/manifest.json` 已按新代码刷新，`files.stock_turnover.parquet.produced_by` 与新值逐字一致 |

同批修掉一个会让 ⑥ 反复回退的问题：`write_factors_manifest()` 原实现不保留既有 `corrections`，任何一次刷新都会抹掉历史修正台账。现改为 [_existing_corrections()](file:///e:/code/python/quant/src/data/factors_manifest.py#L160-L170) 读取现有清单并沿用；本次刷新后 `corrections` 两条（`adj_factor_backfill_and_date_relabel` + 本次 `turnover_unit_100x_fix`）都在，且随后另一路 `b2s2_fund_final_apply_verify` 在 01:01 两次刷新清单也把它们完整保留。

---

## §3 ① 140 行坏值：分类与 19 行修正

### 3.1 分类（口径：`turn` vs 现行 `calc = volume(手)×100×100÷float_share`，仅 `source=derived` 行）

| 类别 | 行数 | 只数 | 日期区间 | 判定依据 | 本轮处置 |
|---|---|---|---|---|---|
| **A. 正向 100 倍单位错** | **19** | 7 | 2015-05-28 ~ 2026-07-02 | `calc/turn ∈ [94.02, 105.01]`；同行 `amount/(volume×100)` 与 `close` 相对差 ≤ 9.7e-9 | ✅ **已修** |
| B. 反向 100 倍单位错 | 57 | 1（000043.SZ） | 2019-09-18 ~ 2019-12-13 | `calc/turn ∈ [0.00972, 0.01033]`（即 `turn/calc ≈ 97~103`），日换手 **50%~217%** 不可能；该窗口行情 volume 为小数（由 amount/close 反推），说明旧快照 volume 单位为股而非手 | ⏸ 未动，建议同批修（见 §7） |
| C. 早期行情快照版本差异 | 64 | 16 | 1993-05-03 ~ 1993-05-06 | 32/64 行 `turn(T)` 与 `calc(T+1)` **逐值相等**（rel=0.0），`calc(T-1)` 0 命中 → 跨日错位；另 32 行为旧 volume 序列与现行不一致，无法复现 | ⏸ 保留，登记为历史残留 |
| D. 零成交真 bar | 1 | 1（000001.SZ 1991-09-30） | 1991-09-30 | `volume=0、turn=0`，`amount` 亦为空，属真实零成交 | ✅ 正确，不动 |

合计 141 行 = 140 坏值 + 1 行零成交真 bar；与体检报告的分桶（`4 + 117 + 19 = 140`）逐项吻合，A 类恰好就是 `(50,150]` 桶的 19 行。

A 类按年份：2015 年 4、2024 年 3、2025 年 1、2026 年 11；C 类 1993 年 64；B 类 2019 年 57。

### 3.2 A 类 19 行「证成错」的三条独立证据

1. **体积单位自证**：19/19 行 `amount / (volume×100)` 与 `close` 相等（最大相对差 9.7e-9），证明行情 `volume` 单位是手，`calc` 口径与模块常量 `VOLUME_LOT_TO_SHARE=100 / TURN_PERCENT=100` 一致。
2. **股本独立源反算**：19/19 行 `valuation_daily.circ_mv / close` 与股本表 `float_share` **完全相等**（比值 1.000000），说明股本侧没有 100 倍偏差，偏差只在旧 `turn`。
3. **量级常识**：601288.SH 2015-07-06 旧值 0.0149% ↔ 新值 1.4594%、601988.SH 2015-06-09 0.0228% ↔ 2.2783%；旧值意味着当日仅成交约 400 万股，与行情不符。

### 3.3 19 行清单（旧 → 新，ratio = 新/旧）

| ts_code | trade_date | 旧 turn(%) | 新 turn(%) | ratio |
|---|---|---|---|---|
| 000725.SZ | 2015-05-28 | 0.219903 | 22.649626 | 102.998 |
| 000725.SZ | 2026-05-22 | 0.165404 | 15.551622 | 94.022 |
| 000725.SZ | 2026-05-25 | 0.118566 | 11.747491 | 99.080 |
| 000725.SZ | 2026-05-26 | 0.141373 | 13.660907 | 96.630 |
| 000725.SZ | 2026-05-27 | 0.138093 | 13.956849 | 101.069 |
| 000725.SZ | 2026-06-05 | 0.151455 | 15.270996 | 100.829 |
| 000725.SZ | 2026-06-08 | 0.126571 | 12.993294 | 102.656 |
| 000725.SZ | 2026-06-26 | 0.127949 | 12.949324 | 101.207 |
| 000725.SZ | 2026-06-30 | 0.146687 | 14.514237 | 98.947 |
| 000725.SZ | 2026-07-01 | 0.130780 | 12.767610 | 97.627 |
| 000725.SZ | 2026-07-02 | 0.160088 | 15.331304 | 95.768 |
| 600010.SH | 2025-10-14 | 0.164790 | 17.007406 | 103.206 |
| 600157.SH | 2024-12-09 | 0.252777 | 25.849376 | 102.262 |
| 600157.SH | 2024-12-10 | 0.195486 | 19.590055 | 100.212 |
| 600221.SH | 2024-11-08 | 0.135730 | 14.252435 | 105.006 |
| 601288.SH | 2015-07-06 | 0.014868 | 1.459410 | 98.156 |
| 601868.SH | 2026-03-12 | 0.147544 | 14.167323 | 96.021 |
| 601988.SH | 2015-06-09 | 0.022752 | 2.278290 | 100.135 |
| 601988.SH | 2015-07-08 | 0.024244 | 2.331150 | 96.152 |

### 3.4 修法：整表原子替换（anti-join 只追加，绕不过永久残留）

| 步骤 | 实测 |
|---|---|
| 基线校验 | 改前 `sha256=7f90eda18ee04e31983bef24811f5104f8840f8eac8a5518be69a0e368cba184`、18,055,751 行 |
| 改前快照 | `_backup/factors_turnover_20261113/stock_turnover.parquet`（`copy2`），快照 SHA256 与改前一致 |
| 重算 | 19 行独立重算（不采信分类脚本输出），`recalc` 与分类值相对差 < 1e-9，全部 > 0 |
| 合并（DG-31 显式兜底） | `with_row_index` → 左连 19 行补丁 → `when(_new_turn.is_not_null()).then(_new_turn).otherwise(turn)` → `sort(_row)` 还原原序；未命中行写回原值，无 `coalesce` |
| 落盘 | `write_parquet_atomic` 先写 `_staging/turnover_badfix_20261113/stock_turnover_fixed.parquet` 验收，再 `write_parquet_atomic` 覆盖正式文件 |

---

## §4 ③ `source_semantics`

| 项 | 值 |
|---|---|
| 位置 | `data/local_store/factors/manifest.json` → `files.stock_turnover.parquet.source_semantics` |
| 代码常量 | [factors_manifest.py](file:///e:/code/python/quant/src/data/factors_manifest.py#L71-L84) `_FILE_EXTRA`（随 `write_factors_manifest()` 重建，不会被下一次刷新丢掉） |
| 内容要点 | `turn` 为百分数；`source=baostock` **189,419 行（1.05%）** 是上游日线原始 turn；`source=derived:quotes_volume+float_share` **17,866,332 行（98.95%）** 是本地口径；两者可比样本上**相对差 p99=8.2%、max=106%**，属上游口径差异而非错值；**禁止混比**、禁止用本地口径重算覆盖 baostock 行 |

---

## §5 ④ 退市标的在途核查（只查不动）

| 检查项 | 实测 |
|---|---|
| `_staging/delisted_backfill_20261109/` | 24 个 `part_*.parquet`（24 只标的），最后一次写入 **2026-11-09 13:11:47**（`merge_step5_sync.json` 完成标记） |
| 该批与换手率缺口重合 | 24 只全部无换手率行，其中 **18 只在行情表中有数据**（与体检报告「19 只缺行，18 只有行情」一致，另 1 只为 `688827.SH` 新上市） |
| 当前进程 | 无任何 `python` 进程在写 `_staging` 或 `stock_turnover.parquet`（只有两条只读的 lineage 分析脚本） |
| 结论 | **无在途任务**：那次退市回补 11-09 已收口，且只补行情、未补换手率；本表未被另一路占用。18 只仍缺换手率，按指令**本轮不补**，等批复（补法应为 `derive-turnover` 对缺失 `(ts_code, trade_date)` 补行，不重算已有行） |

---

## §6 数据验收（DG-20 四项）

| 验收项 | 实测 |
|---|---|
| 行数守恒 | 18,055,751 → **18,055,751** |
| 逐值对拍（DG-30/31） | 与改前快照逐列 `ne_missing` 比对：**改动 19 行，且改动键集合与目标集合完全一致**；其余 **18,055,732 行逐值 0 改动**；`ts_code`/`trade_date` 序列逐元素相等（顺序未变） |
| 空值数 | 5 列空值数改前改后完全相同（`turn` 均为 **0**；`source`/`fetched_at`/`ts_code`/`trade_date` 均 0） |
| SHA256 | 改前 `7f90eda1…` → 改后 `aa35895956cc466b306864945b9f2af4966df0f766cbd1f8685da6ed1acbc628`；字节 145,625,057 → 147,460,743（同 zstd-3 / row_group=100k 重写，行数不变） |
| 抽样断言 | 19/19 行落盘后与 `recalc` 逐值相等（绝对差 ≤ 1e-15） |
| 正式读路径（DG-23） | `read_turnover('601288.SH')` 命中 2015-07-06 → `turn=1.45941`；`attach_turn` 对同一根 K 线同样得到 `1.45941`；底层 `read_turnover` 对 19 行最大绝对误差 0 |
| 清单 | `write_factors_manifest()` 刷新后 `manifest_tool.py --check --only factors` → **PASS**（11 个文件一致，0 不一致）；`corrections` 新增 `turnover_unit_100x_fix` 一条（含前后 SHA256、19 行、规则、备份路径、证据文件） |
| 变更台账 | `logs/data_changelog.csv` 自动追加 3 条：`stock_turnover.parquet` 覆盖 1 条、`manifest.json` 两次刷新各 1 条 |

---

## §7 待批清单（本轮未做）

1. **B 类 57 行（000043.SZ 2019-09-18~12-13）**：`turn` 约为正确值的 100 倍（50%~171% 日换手）。证据与 A 类同级，建议与 A 类同法定点重算；该窗口行情 `volume` 为小数（amount/close 反推），同时建议登记「2019-09-18~12-13 该票行情 volume 为合成值」的口径说明。
2. **C 类 64 行（1993-05-03~06）**：32 行是 `turn(T)=calc(T+1)` 的跨日错位、32 行旧 volume 序列不可复现。建议**保留**并在权威清单/缺口登记中记为「早期行情快照版本残留」；唯一合法修复路径是用上游 `sync-turnover` 重抓该 4 个交易日覆盖，不建议用现行口径重算。
3. **18 只退市标的换手率缺行**：用 `derive-turnover` 只补缺失 `(ts_code, trade_date)`，需先确认这些代码的 `stock_shares` 有可用记录。
4. **`read_turnover` LRU 缓存未把 `columns` 纳入键**：同进程先取 2 列再取 3 列会拿到旧列集（`attach_turn` 直接 `rename('source')` 会报列不存在）。本轮测试已绕开，未改代码。
5. 清单 L76（`18,055,784 行`）仍与实测行数不符 → 转文档那一路，本任务未动。

---

## §8 复现命令

```powershell
# 分类（只读）
.venv\Scripts\python.exe _staging\turnover_badfix_20261113\classify_bad.py
.venv\Scripts\python.exe _staging\turnover_badfix_20261113\analyze_classes.py
.venv\Scripts\python.exe _staging\turnover_badfix_20261113\inspect_window.py 000006.SZ 1993-04-26 1993-05-12
.venv\Scripts\python.exe _staging\turnover_badfix_20261113\check_valuation_proof.py
# 定点修（原子替换；已执行，重跑会被基线 SHA256 断言拦下）
.venv\Scripts\python.exe _staging\turnover_badfix_20261113\fix_bad_rows.py
# 清单与读路径
.venv\Scripts\python.exe _staging\turnover_badfix_20261113\refresh_manifest.py
.venv\Scripts\python.exe _staging\turnover_badfix_20261113\read_path_check.py
.venv\Scripts\python.exe tools\manifest_tool.py --check --only factors
```

---

## §9 文件指纹

| 文件 | 大小 / 行数 | SHA256 |
|---|---|---|
| `data/local_store/factors/stock_turnover.parquet`（新） | 147,460,743 B / 18,055,751 行 | `aa35895956cc466b306864945b9f2af4966df0f766cbd1f8685da6ed1acbc628` |
| `_backup/factors_turnover_20261113/stock_turnover.parquet`（旧） | 145,625,057 B / 18,055,751 行 | `7f90eda18ee04e31983bef24811f5104f8840f8eac8a5518be69a0e368cba184` |
| `src/data/factors_manifest.py` | 修改 | 已提交 `73dff03` / `4443b3c` |
| `tests/test_turnover_assemble.py` | +27 行 / 5 例 | 已提交 `73dff03` |

---

## §10 第二批（提交 + ④ + LRU + B/C 取证）

### §10.1 提交记录

| 提交 | 文件 | 说明 |
|---|---|---|
| `73dff03` | `src/data/factors_manifest.py` +37/-2、`tests/test_turnover_assemble.py` +27 | ⑤⑥③ 代码与用例 |
| `618fc2e` | `src/data/local_store/turnover.py` +9/-5、`tests/test_local_store_turnover_unit.py` +20 | `read_turnover` 缓存键补 `columns` |
| `4443b3c` | `src/data/factors_manifest.py` +3/-2 | `source_semantics` 行数更新为回补后实测值 |

三笔均走隔离索引五步（`before` 取一次 → `read-tree $before` → 只 `update-index` 本路路径 → `diff --cached --name-only $before` 核对数量 → 提交前复取 `after` 比对），`73dff03` 与 `618fc2e` 提交前 `before == after`；`4443b3c` 的 `before` 为 `618fc2e`、`after` 同值。提交后各自 `git diff-tree -r --name-only HEAD` 与预期清单逐一相符，并在**新命令**里 `git read-tree HEAD` 回填共享索引；`git ls-files -s` 的 blob 与 `git rev-parse HEAD:<path>` 全等，`git status --short` 只剩他路 `M`/`??`，无假 `MM`。

### §10.2 ④ 18 只退市标的补行

**先证伪本地派生**：`meta/factors/stock_shares.parquet` 仅 5,813 只、`ts_code` 集合与换手率表完全一致，**不含**这 18 只；`meta/financials/valuation_daily.parquet` 对这 18 只 **0 行**。故 `derive_turnover_local` 首次执行 `derived_rows=0 / no_share_rows=67,213`，一行都算不出（`_staging/turnover_delisted_20261113/apply_delisted.py` + `progress.csv` 留痕）。

**改走上游 baostock turn**（与表内既有 baostock 行同源）：`_query_turn_range` 探针实测 `000024.SZ` 2015 年有 turn（如 2015-01-05 = 10.495109），`600656.SH` 2016 年可查；`backfill_turnover(codes=18, workers=1, chunk_size=200)` 单连接串行 18 只。

| 验收项 | 实测 |
|---|---|
| 行数守恒 | 18,055,751 → **18,122,971**（+67,220，等于 `stats.rows_added`） |
| 只新增这 18 只 | `new_codes_present=18`、`missing_codes=[]`、其余 5,813 只行集 `rest_equals_old=true`（逐值 0 改动） |
| 空值 | 5 列空值数前后完全一致（`turn` 空值 0），新增行 `turn` 空值 0 |
| 主键 | `duplicate_keys=0` |
| SHA256 | `aa358959…c628` → `944312230e0d8d09272b6b4339dd6993fe4cc292e78b30878050d3ec592b7536` |
| 端到端 | `read_turnover('000024.SZ')` 5,212 行；`attach_turn` 5,212 行、`turn` 空值 0、`turn_source=baostock` |
| 清单 | `manifest.json` 已刷新（rows 18,122,971 / codes 5,831 / bytes 147,990,732），`corrections` 新增 `delisted_turnover_backfill`（含 18 只清单、原因、前后 SHA） |

改前快照 `_backup/factors_turnover_delisted_20261113/stock_turnover.parquet`（145,625,057 B，SHA 与改前一致）。
逐只抽样：18 只 `turn` p50 落在 0.35%~2.38%，max 到 272%（`600656.SH` 早期），与同年代同类标的量级一致，无 100× 级异常。

**范围外发现**：缺口实为 **19 只**，第 19 只 `688827.SH` 只有 2 根 bar（2026-11-10/11），属新上市而非退市，未纳入本次退役回补；建议由常规 `derive-turnover` 日常增量覆盖。

### §10.3 `read_turnover` 缓存键补 `columns`

缓存键由 `ts_code` 改为 `(ts_code, tuple(columns))`。新用例 `test_read_turnover_cache_key_includes_columns`：先取 `(trade_date, turn)`、再取 `(trade_date, turn, source)`、再退回窄列，三次 `columns` 均正确。变异验证：把键临时改回 `(ts_code,)` 后该用例实测失败，还原后 `tests/test_local_store_turnover_unit.py` + `tests/test_turnover_assemble.py` 全绿（`pytest` 退出码 0），`ruff check` 三文件 PASS。

### §10.4 B 类 57 行取证结论：**unresolved，保留原值**

按「三条证据」逐一实测（`_staging/turnover_delisted_20261113/b_class_evidence.json`）：

| 证据 | 实测 | 是否支持「volume 口径错」 |
|---|---|---|
| ① 窗口内 `amount/close/volume` ≈ 1（volume 是股） | 窗口 57 行 `r_min = r_p50 = r_max = 100.0000` | 否（r ≈ 100，volume 是「手」） |
| ② 相邻窗口口径不同 | `before_60`（2019-06-25~09-17）`r_p50 = 100.13`、区间 97.61~102.63 | 否（与窗口一致） |
| ③ 同区间其他标的仍 ≈100 | `000002.SZ` 100.09、`000063.SZ` 99.95、`600000.SH` 100.01、`600519.SH` 100.03、`000651.SZ` 100.11 | 否（全市场同口径） |

三条证据**全部不成立**，反而共同证明该窗口 `volume` 的单位与全市场一致（手），只是数值由 `amount/close/100` 合成（`r` 恒等 100、54/57 行带小数）。因此这 57 行不是「volume 口径错」，而是 `turn` 被多乘了 100，成因未定向。按指令「三条证据不全就登记 unresolved 保留原值」，**未改一个值**，`turn` 仍为 50.36%~216.62%。

### §10.5 C 类 64 行登记（不动数据）

`16 只 / 1993-05-03~1993-05-06`，与早期行情日期错位同源：32/64 行 `turn(T) == calc(T+1)` 逐值相等（`rel_next = 0.0`）、`calc(T-1)` 零命中，即换手率用的是下一交易日行情算出的结果，属行情侧 bar 与交易日错位。等行情侧对齐后随窗口重建，本轮**不单独改**。

两条（B unresolved、C 错位残留）建议并入中央缺口登记 `docs/数据缺口登记_正式表_2026-11-12.md`（当前编号到 G-119），由文档那一路统一插行，避免两路同时改同一份计数表。

---

## §11 第三批：B 类定向成因与修复、688827.SH 路径核

### §11.1 B 类 57 行定向成因（成立）

**哪个入口写的**：这 57 行 `fetched_at = 2026-11-05 11:27:21.385213`，且 `000043.SZ` 全表 5,902 行的 `fetched_at` **只有这一个值** —— 一次批量写入产出整条序列。`logs/data_changelog.csv` 中 `factors/stock_turnover.parquet` 只有 5 条记录，覆盖此时段的唯一一条是 `2026-11-05T15:28:47 | fact_backfill_20190917_20261105 | update`（写入时刻与落盘记账相差约 4 小时，属同一任务的合并落盘）。

**为什么只有这一段**（关键判据是比值的散布，不是均值）：

| 范围 | 行数 | `turn/calc` 分布 |
|---|---|---|
| 窗口 2019-09-18~12-13 | 57 | mean 100.23、std 1.14、min 96.79、max 102.87 |
| 窗口外其余 | 5,845 | min = p50 = max = **1.0**（逐行恒等） |

若分母（`float_share`）在写入时就错了 100 倍，比值会是**常数 100**；实测有 ±3% 逐行散布，只能来自分子（`volume`）——即写入时该窗口 `volume` 是「股」量级、按「手」口径算 turn，之后行情侧把该窗口 `volume` 重算成 `amount/close/100`（手：54/57 行带小数、当前 `amount/close/volume` 恒等 100.0000），`turn` 未随之重算，残留 100×。窗口外 `turn/calc ≡ 1.0` 说明那一段的 `volume` 从未被替换过。

**其它标的是否同样存在但被掩盖**：否。全表 1,810 万行扫描只有 141 行越界，57 行即本窗口，其余 84 行为 A/C/D 类；同一批写入的其它标的没有第二处 `|turn/calc-1| > 0.02`。

**已知边界**：行情文件本身不带写入时间戳，无法直接定位 `volume` 被替换的时刻；上述结论由「比值散布 + 窗口内外恒等」推出，已写入清单 `root_cause` 字段留痕。

### §11.2 修复与 DG-20 四项

整表原子替换（`write_parquet_atomic`，改前 `copy2` 到 `_backup/factors_turnover_bclass_20261113/`）。

| DG-20 项 | 实测 |
|---|---|
| 行数守恒 | 18,122,971 → 18,122,971（`rows_conserved=true`） |
| 逐值对拍 | 改动 57 行、键集合与目标完全一致；其余 18,122,914 行 `turn/source/fetched_at` 逐值 0 改动（`unchanged_all_columns_zero_diff=true`） |
| SHA256 | `944312230e0d…` → `337087bff75013e61ecb74381ec3df121c16d51d04f0f9904eac1e761c8aed62`（147,990,714 B） |
| 抽样断言 | `turn` 由 50.36%~216.62% 修正为 **0.5028%~2.1581%**；`(ts_code, trade_date)` 序列逐元素相等（顺序未变）；5 列空值数前后完全相同；`attach_turn('000043.SZ', 2019-09-18)` = **0.716879%**、`turn_source=float_share`、空值 0 |

清单已刷新（`18122971` 行 / 5,831 只 / sha `337087bf…`），`corrections` 新增 `turnover_b_class_100x_fix`；`manifest_tool.py --check --only factors` → **PASS**（2 份 manifest / 11 条 / 不一致 0）。

### §11.3 `688827.SH` 两根 bar：derive 覆盖不到（路径缺失）

按常规 derive 走一遍（`derive_turnover_local(codes=['688827.SH'])`，非手工塞行）：

```
{'gap_rows': 2, 'derived_rows': 0, 'no_share_rows': 2, 'dropped_nonpositive': 0, 'written': False}
```

`gap_rows=2` 说明 derive 认得这两根 bar；`no_share_rows=2` 说明 `meta/factors/stock_shares.parquet` 里**没有** `688827.SH`（实测 0 行），`valuation_daily` 同样 0 行 → 缺 `float_share` 无法算 turn。按指令不手工插行，登记为路径缺失：需要在股本表补上该只（eastmoney `stock_zh_a_gbjg_em` 一条记录即可），补完后常规 derive 会自动补这两行。


