# B1 片4 收口：事件与分红族四处体检问题 + changelog 补记

日期：2026-11-12　范围：`src/data/events.py`、`data/local_store/factors/manifest.json`、`data/local_store/factors/stock_dividend.parquet`（只读定性）、`backups/critical_assets/`（只查）、`logs/data_changelog.csv`

## 结论摘要

| 序号 | 事项 | 结论 | 落点 |
|---|---|---|---|
| 一 | `events` replace 分支守恒断言 | **已补齐**，三项断言 + 反例自测全部跑通 | `src/data/events.py` L305-L365、L655-L678；`tests/test_data_events.py::TestReplaceConservation` |
| 二 | `factors/manifest.json` 的 `fund_meta.json` 陈旧 + `produced_by` 错配 | **已修正**；实测 4,234 B，原记 4,112 B 与两处回报都不对 | `data/local_store/factors/manifest.json`（旧档 `_backup/factors_manifest_20261112/`） |
| 三 | `stock_dividend` 10 行「漏计」 | **定性反转为 2 行真漏计 + 8 行对拍口径问题**；未改数据、未重建因子 | 本文件第三节；脚本在 `_staging/b1_slice4_fix_20261112/` |
| 四 | `backups/critical_assets/.../events.parquet` 回刷 | **查实无需回刷**，已与正式表逐字节一致 | 见第四节 |
| 五 | `logs/data_changelog.csv` 补记 `163503.SZ` | **已追加 1 行**，既有行零改动 | 见第五节 |

---

## 一、`events` 的 replace 分支补守恒断言

### 改动

- [events.py](file:///E:/code/python/quant/src/data/events.py#L305-L365) 新增 `_row_key` 与 `_assert_replace_conservation`；[events.py](file:///E:/code/python/quant/src/data/events.py#L655-L678) 的 replace 分支在**落盘前**调用它。
- 新增统计键：`existing_retained`（保留未改）、`existing_affected`（保留但内容被改写）、`existing_replaced_out`（被替换掉）。`existing_affected` 不再写死 0。
- replace 丢弃/改写存量行时各打一条 warning，不再静默丢历史。

### 三项断言（DG-31 口径）

1. **行数守恒**：`merged.height == incoming.height`；且存量行必须被归入上述三类，`保留未改 + 保留改值 + 被替换掉 == 存量行数`。只算 `event_id` 交集就当年守恒的实现会在这里失败。
2. **改动行数**：同 `event_id` 内容被改写的既有行逐行计入，计数直接回填 `stats['existing_affected']`。
3. **null 计数**：逐列断言 `写盘空值数 == 待写空值数`（写路径不得凭空造 null）且 `写盘空值数 <= 存量空值数`（**空值从 0 变成 N 必然失败**）。

### 反例自测（证明断言真会失败，不是空过）

| 反例 | 构造 | 实际抛错 |
|---|---|---|
| 写路径造 null | 待写帧 title 无空值，写盘帧把 title 置 null | `RuntimeError: replace 写路径为 title 造出了空值：待写 0 行空，写盘 1 行空` |
| 端到端 0→N | 先导入正常 1 行，再用缺 `title` 列的规范化表走 replace | `RuntimeError: replace 使 title 的空值从 0 行增加到 1 行（空值从 0 变成 N 即判定为写回缺陷）`，且落盘 `events.parquet` SHA256 前后一致（`2e3a64874625…`）＝正式文件未被写坏 |
| 行数不守恒 | 写盘帧凭空多一行 | `RuntimeError: replace 写盘行数不守恒：待写 1 行，实际 2 行` |

### 验证

```
.venv\Scripts\python.exe -m pytest tests\test_data_events.py tests\test_events_source_marking.py -q
# 30 passed
.venv\Scripts\python.exe -m ruff check src\data\events.py tests\test_data_events.py
# All checks passed
```

---

## 二、`factors/manifest.json` 的 `fund_meta.json` 陈旧记录

### 字节数裁决

实测 `data/local_store/factors/fund_meta.json`：

| 指标 | 实测值 |
|---|---|
| 字节 | **4,234 B** |
| SHA256 | `4fe7e823307d4e4b2cc7fc19f07ce7ab83c95726cb2747771652a3b9036fe7bc` |
| mtime | 2026-11-12T22:34:01（即 `tools/reconcile_p2_metadata.py` 那次改写） |

磁盘上既没有 4,112 B（manifest 原记）、也没有 4,324 B（一处回报）的版本，**两处回报与 manifest 三者互不相同，以实测 4,234 B 为准**。

### 留档与写盘

| 对象 | 字节 | SHA256 |
|---|---|---|
| 旧 manifest（留档 `_backup/factors_manifest_20261112/manifest.json`） | 10,168 | `bb947cb854993801f7e8bc869a96001e691fde042cd1758b91ec26c5099b2676` |
| 新 manifest | 10,369 | `acea7bcbbac4f4e95ae2064e7e452dc6bb2675ed76134463dce6b933db21d564` |

- 留档由 `copy2` 完成，留档与改写前逐字节一致（SHA 相同）。
- 写盘走 `src/data/atomic_write.py::write_text_atomic`（DG-9）。
- 未整份重生成：`collect_factors_manifest` 不产出 `corrections`，整份重写会丢调账证据；改为只替换可机读记账字段，`corrections` / `authority` / `source` 原键保留。
- 改写前先证明该 JSON 往返序列化逐字节等同（10,168 B 完全一致），因此本次改动范围可穷举。

### 改动内容

1. `files['fund_meta.json']`：`bytes` 4,112→4,234、`sha256`→`4fe7e823…`、`mtime`→`2026-11-12T22:34:01`。
2. `generated_at` → `2026-11-12T23:55:36`。
3. `produced_by` 错配修正（`src/data/factors_manifest.py::_ROLES` 同步改，避免下次重生成又写回旧值）：

| 条目 | 原 produced_by | 修正为 |
|---|---|---|
| `fund_adj_factor.parquet` | `tools/build_fund_corporate_actions.py` | `src/data/local_store/fund_factors.py::rebuild_fund_factors` |
| `fund_dividend.parquet` | 同上 | `src/data/local_store/fund_factors.py::build_fund_store` |
| `fund_share_change.parquet` | 同上 | `fund_factors.py::build_fund_store + tools/fund_split_repair.py（追加缺失事件）` |
| `fund_meta.json` | 同上 | `fund_factors.py::rebuild_fund_factors + tools/reconcile_p2_metadata.py（修正计数）` |
| `fund_covered_codes.txt`（**第 5 项，超出清单 4 项，见第七节披露**） | 同上 | `fund_factors.py::rebuild_fund_factors` |

依据：`tools/build_fund_corporate_actions.py` 全文只写 `meta/fund_corporate_actions_2019plus.parquet` 与它自己的 manifest（L265-L302），不产出上述任一文件；`fund_factors.py` L475/L481/L513/L592 分别写 `fund_adj_factor.parquet` / `fund_covered_codes.txt` / `fund_meta.json` / `fund_dividend.parquet` + `fund_share_change.parquet`。

### 验证

```
.venv\Scripts\python.exe tools\manifest_tool.py --check --only factors
# PASS：11 条记录 / 不一致 0 / 无法定位 0 / 缺失 0
```

盘上漂移扫描（改写前）结果：**仅 `fund_meta.json` 一个条目**字节/SHA 不符，其余 10 条全对；`totals` 未变（6 张 parquet / 39,226,790 行 / 161,590,536 B）。

---

## 三、`stock_dividend` 10 行「漏计」的定性

### 结论：10 行里 8 行不是缺陷，真漏计只有 2 行

体检按「每个 `(ts_code, ex_date)` 取一行参数」与行情 `pre_close` 对拍。实际上这 8 行在表内**各有两条同日记录**（一条 `audit_fix_20261110`、一条 `mysql_remote`），而消费方口径是**同日求和**：

- [adj_factor_calculator.py](file:///E:/code/python/quant/src/data/adj_factor_calculator.py) `merge_same_day_events`（L90-L103）按 `(ts_code, ex_date)` 对 `cash_div` / `share_div` / 配股参数求和；
- [corporate_actions.py](file:///E:/code/python/quant/src/backtest/engine/corporate_actions.py#L202-L228) `_parse_one` 同样按 `ex_date` 累加。

求和后重建的参考价与行情 `pre_close` **逐分不差（残差 0.000000）**，因子级相对误差 0.0。

### 方法学澄清：`pre_close` 不是独立证据

[tdx_handler.py](file:///E:/code/python/quant/src/data/tdx_handler.py#L738-L747) 的 `pre_close_t = close_{t-1} * qfq_factor_{t-1} / qfq_factor_t`——行情 `pre_close` 由本仓因子链派生，用它去对拍分红表是**循环论证**（只能证明因子链与自身自洽）。因此本片改用**当日真实 high/low**（源端原始价，不受因子链影响）做 ±10% 涨跌停区间校验，作为独立判据。

### 逐行定性

| # | 标的 | 除权日 | 表内行数 | 表内现值（有效层） | 同日合计 | 重建 ref | 行情 pre_close | ref 残差 | 因子级相对误差 | 涨跌停带（按表内重建） | 判定 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 300760.SZ | 2025-05-29 | 2 | 0.560 + 1.410 | 现金 1.970 | 228.540 | 228.540 | 0.000000 | 0.000% | [205.69, 251.39] 容 226.41~229.77 | 非缺陷 |
| 2 | 601966.SH | 2024-06-14 | 2 | 0.286 + 0.091 | 现金 0.377 | 18.303 | 18.303 | 0.000000 | 0.000% | [16.47, 20.13] 容 18.16~18.48 | 非缺陷 |
| 3 | 000667.SZ | 2006-02-20 | **1** | 送转 0.387 / 现金 0 | 送转 0.387 | 3.482336 | 3.215573 | **+0.266763** | **+8.296%** | 跌停 3.13，当日 low **3.12 破位** | **真漏计** |
| 4 | 600015.SH | 2003-11-04 | 2 | 0.223 + 0.130 | 现金 0.353 | 6.867 | 6.867 | 0.000000 | 0.000% | [6.18, 7.55] 容 6.91~7.19 | 非缺陷 |
| 5 | 601318.SH | 2018-06-07 | 2 | 0.200 + 1.000 | 现金 1.200 | 62.610 | 62.610 | 0.000000 | 0.000% | [56.35, 68.87] 容 63.02~64.54 | 非缺陷 |
| 6 | 601898.SH | 2024-08-20 | 2 | 0.442 + 0.113 | 现金 0.555 | 12.965 | 12.965 | 0.000000 | 0.000% | [11.67, 14.26] 容 12.64~13.05 | 非缺陷 |
| 7 | 600317.SH | 2014-06-16 | 2 | 0.329 + 0.200（现金）、2.0（送转） | 现金 0.529 / 送转 2.0 | 2.470333 | 2.470333 | 0.000000 | 0.000% | [2.22, 2.72] 容 2.44~2.72 | 非缺陷 |
| 8 | 601828.SH | 2023-07-24 | 2 | 0.046 + 0.034 | 现金 0.080 | 4.880 | 4.880 | 0.000000 | 0.000% | [4.39, 5.37] 容 4.87~4.96 | 非缺陷 |
| 9 | 300163.SZ | 2015-09-25 | **1** | 送转 2.0 / 现金 0 | 送转 2.0 | 9.973333 | 9.940000 | **+0.033333** | **+0.335%** | 跌停 8.98，当日 high=low=close **8.95 破位** | **真漏计** |
| 10 | 000738.SZ | 2013-06-14 | 2 | 0.021 + 0.041 | 现金 0.062 | 12.048 | 12.048 | 0.000000 | 0.000% | [10.84, 13.25] 容 11.90~12.24 | 非缺陷 |

因子级相对误差 = 按表内重建的 `calc_dividend_event_factor` 与交付快照 `qfq_factor` 跳变之比减 1；残差 = 重建 ref − 行情 `pre_close`。

### 待修清单（仅登记，本次不改数据、不重建因子）

| 标的 | 除权日 | 表内现值 | 应有值 | 证据强度 | 涉及消费者 |
|---|---|---|---|---|---|
| `300163.SZ` | 2015-09-25 | `cash_div=0.0`、`share_div=2.0`（1 行，`mysql_remote`） | `cash_div=0.1`（`share_div=2.0` 不变） | **强**：29.82/3 = 9.94 精确闭合，且 8.95 恰为该 ref 的跌停价（一字跌停） | 见下方清单 |
| `000667.SZ` | 2006-02-20 | `share_div=0.387`、`cash_div=0.0`（1 行，`sina_verified`） | 最可能 `share_div=0.5`（10 送 5，ref=3.22）；现有证据只能把 share 限定在 **[0.3935, 0.5445]**，**精确值待实施公告原文核定，不猜** | 中：涨跌停只给出区间下界（0.387 会破跌停 0.01 元），不能唯一定值 | 同上 |

冻结期影响（供后续决策）：

- 若按当前表重建因子，`000667.SZ` 2006-02-20 之前全部前复权价量/收益/均线/指标整体偏移 **+8.296%**（`0.72098053` vs 交付 `0.66575014`）；`300163.SZ` 偏移 **+0.335%**（`0.33333333` vs 交付 `0.33221925`）。
- 交付快照当前与交易所口径一致，**但与表不自洽**：表↔因子必须同批修，只改分红表会让两边分叉。派生层冻结期间不做任何一侧的单独修改。

消费者清单（均已核实读有效层且同日求和）：

| 文件 | 用法 |
|---|---|
| [adj_factor_calculator.py](file:///E:/code/python/quant/src/data/adj_factor_calculator.py#L90-L110) | qfq/hfq 因子计算，`merge_same_day_events` 同日求和 |
| [build.py](file:///E:/code/python/quant/src/data/local_store/build.py#L123-L163) | `rebuild-factors` / `_factor_arrays` 本地因子重建 |
| [tdx_handler.py](file:///E:/code/python/quant/src/data/tdx_handler.py#L380-L387) | 读 `*_fix` 修正列 + L738-L747 派生 `pre_close` |
| [corporate_actions.py](file:///E:/code/python/quant/src/backtest/engine/corporate_actions.py#L202-L228) | 回测内权益账本，按 `ex_date` 累加 |
| [data_manager.py](file:///E:/code/python/quant/src/data/data_manager.py#L83) | 分红快照装载 |
| [dividend_marker.py](file:///E:/code/python/quant/src/ui/dividend_marker.py) | 图表分红标记 |
| [replay.py](file:///E:/code/python/quant/src/advice/replay.py#L565) | 读 `factors/stock_dividend.parquet` 复现 |

### 复算

```
.venv\Scripts\python.exe _staging\b1_slice4_fix_20261112\qualify_dividend_gaps.py   # 逐行反算，输出 ten_rows_qualified.json
.venv\Scripts\python.exe _staging\b1_slice4_fix_20261112\limit_band_check.py        # 涨跌停区间独立校验，输出 limit_band_check.json
```

---

## 四、`backups/critical_assets/` 的 `events.parquet` 快照

查实结果：**已与正式表逐字节一致，无需回刷**，因此未做 `copy2`（回刷只会写出同一份字节、无意义改动 mtime），也不存在新旧 SHA 差异。

| 侧 | 路径 | 字节 | 行数 | SHA256 | mtime |
|---|---|---|---|---|---|
| 正式 | `data/local_store/meta/events/events.parquet` | 3,482,917 | 185,750 | `7877d1fca3e44f2c5d7fbfb3509915cd09d40e6f6d4f73d8431f653435e3f309` | 2026-11-12 19:53:03 |
| 快照 | `backups/critical_assets/data/local_store/meta/events/events.parquet` | 3,482,917 | 185,750 | 同上 | 2026-11-12 19:53:03 |

- `backups/critical_assets/manifest.json`（`generated_at` 2026-11-12T23:51:38，396 文件 / 1,038,220,640 B）已登记该新值。
- 全仓 `backups/**` 下不存在 3,479,009 B 的 events 快照；另两份分别是 11-09 回滚前 `2,464,110 B`（`.../meta/events/_backup/events.20261109_220042_pre_rollback_122883.parquet`）与 11-07 审计备份 `663,078 B`（`.../reports/data_audit_2026-11-07/p1_events_backup/events.parquet`）。
- 结论：「落后 97 行（3,479,009 vs 3,482,917）」是 23:51:38 快照重生成**之前**的状态，现已自然收敛。

---

## 五、`logs/data_changelog.csv` 补记 `163503.SZ`

追加 1 行（只追加，既有行零改动）：

```text
run_20261112_g066_fix,2026-11-12T19:18:51,g066_163503_fix_20261112,update,data/local_store/quotes/fund/sz/163503.SZ.parquet,127068,ee445181b35600e92b0861b7012e8a936fe3651cd91a175e38bfaec150fa455f,d876e88e1120133d065757895b3a2a01e3e331dd395c617b274dcaab889f5801,agent,<reason>
```

- `timestamp` 取修复当时（修复文件 mtime 2026-11-12 19:18:51），非记账时点。
- `reason`：`2007-10-15~16 折算日除权参考价修复当时未记账；写入折算锚点 1.00246 并把 2007-10-17 的 pre_close 接续到该值（当日 pct_chg 由 -48.01% 修正为 +1.45%）；依据 2026-11-12/gap-rootcause/work/apply_g066_result.json 与 apply_g066_163503.py 留证；sha256 由 D876E88E… 变 EE445181…；于 2026-11-13T00:00:26 事后补记`
- 写入前核对目标文件当前 SHA256 = `ee445181…`（与 `apply_g066_result.json` 的 `sha256_after` 一致），`sha256_before` = `sha256_backup` = `d876e88e…`。

自证三项：

| 断言 | 结果 |
|---|---|
| 原文件全部字节是追加后文件的前缀（既有行零改动） | ✅ true |
| 行数恰好 +1 | ✅ 10,332 → 10,333 |
| 字节增量恰等于新增行长度 | ✅ +627 B = 627 B |
| SHA256 | `6b8e18adcb8594acdc1bd6515f42cec2fa75d02f768a196de2d115413356b93d` → `a66f3e56f067f81f5df04031e11c4efc07ffda311a6e4d6e9d775ec0c75ef077` |

并发提示：该文件当时正被另一条会话追加（我开始前 10,328 行，落笔前已 10,332 行），本次用 `a` 模式单次写入并做前缀比对，确认没有插入他人行中间。

---

## 六、留档与产物位置

| 对象 | 路径 |
|---|---|
| 旧 factors manifest 留档 | `_backup/factors_manifest_20261112/manifest.json` |
| manifest 刷新脚本 + 断点 | `_staging/factors_manifest_refresh_20261112/`（`refresh.py`、`progress.csv`、`failed.csv` 空） |
| 分红定性脚本与输出 | `_staging/b1_slice4_fix_20261112/`（`qualify_dividend_gaps.py`、`ten_rows_qualified.json`、`limit_band_check.py`、`limit_band_check.json`、`two_rows_full.csv`） |
| changelog 追加自证 | `_staging/b1_slice4_fix_20261112/changelog_append.json` |

---

## 七、本次自行决定并执行的事项（供事后追认或推翻）

1. **`produced_by` 修了第 5 项** `fund_covered_codes.txt`（清单只点 4 项）。依据：该文件由 `fund_factors.py` L481 写，`tools/build_fund_corporate_actions.py` 全文零引用；与另外 4 项是同一处 `_ROLES` 错配、同一次改写，留着会让 manifest 改完仍存假记录。回滚方式：把该条 `produced_by` 改回 `tools/build_fund_corporate_actions.py`。
2. **第四件未执行 copy2 回刷**。依据：快照与正式表 SHA256 完全相同，回刷是空操作且会改 mtime 制造假变更。若需要形式上的回刷记录，可随时补做（结果不变）。
3. **未整份重生成 factors manifest**，改为只替换可机读记账字段。依据：`collect_factors_manifest` 不产出 `corrections`，整份重写会丢 2026-11-12 调账证据。
4. **分红定性改用涨跌停区间做独立校验**。依据：`pre_close` 由本仓因子链派生（`tdx_handler.py` L738-L747），不能自证分红表；high/low 是源端原始价，与因子链无关。
