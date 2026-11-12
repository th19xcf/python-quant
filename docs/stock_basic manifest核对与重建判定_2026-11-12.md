# stock_basic manifest 与实文件一致性核对（2026-11-12）

> 范围：`data/local_store/meta/stock_basic.parquet` 及其 manifest。未改 parquet 内容、未改 `src/`、未提交 Git。
> 复现脚本与明细：`_staging/stock_basic_manifest_20261112/`（`reconcile_stock_basic.py`、`rebuild_manifest.py`、
> `reconcile_report.json`、`rebuild_diff.csv`、`name_conflicts.csv`）。

## 0. 结论

1. **manifest 已按实文件重算并原子写入**：5,914 → **5,916 行**，唯一键 5,916，逐列空值计数、字段清单、
   文件 SHA256 与生成时刻全部由脚本读出，无手填值。
2. **全量重建实测差异 22 只 / 62 格（占 5,916 行的 0.37%）**，行集完全相同（0 只增、0 只减）。
   差异集中在 9 只名称、13 只退市日期链、1 只退市理由。
3. **不建议现在整表重建**：13 只日期差异属「行情文件 2026-11-11 新到、表未跟进」，方向正确但会改变
   退市剔除口径；9 只名称里有 **5 只会被 `names.json` 的 XD 标记 + 4 字截断劣化**（`皖新传媒`→`皖新传` 等）。
   需先修 `names.json` 截断问题或给构建器加「截断不覆盖」判定，再重建。
4. **688827.SH 名称冲突按 DG-18 登记为 unresolved**，未择一覆盖任何源；其余 10 条「公告改名早于
   `names.json`」的冲突一并登记，见 §2.3。

## 1. manifest 重建

| 项 | 旧 manifest | 新 manifest |
|---|---|---|
| `rows` | 5,914 | **5,916**（实文件 `DataFrame.height`） |
| `unique_ts_code` | 未记录 | **5,916**（`n_unique()`，重复代码 0 个） |
| 字段清单 / 类型 | 未记录 | 13 列，`columns` + `schema` |
| 逐列空值计数 | 未记录 | `null_counts` / `non_null_counts` / `distinct_counts` |
| `source_parquet_sha256` | `20c457…`（旧构建值，与实文件不符） | **`d3d90abd27628b8b43131e2f661d3a4d7a3fa01e761b51e770ebbc8e0071b9fe`** |
| manifest 自身 SHA256 | `33be347f9ff1fc5ff9c78725ff9482b4ad058f7bf301f9b02ed243bc37b5b612` | `41a8a5d764d7e9813c047b279dc2088316ce6ac230c356703aef08380643a3e0` |
| 生成时刻 | `2026-11-10T14:16:30` | `generated_at`（脚本运行时刻） |

- 空值计数（实测）：`name` 2、`name_basis` 2、`delist_date` / `delist_reason` / `delist_date_basis` 各 5,580，
  其余 7 列 0。空值数量与旧 manifest 的 `name_missing: 2` 一致，说明补 688827.SH 未引入新空值。
- 每个数值的来源逐项写在 manifest 的 `field_sources`；构建期证据计数（`quote_files`、`evidence`、
  `tcode_decisions`、`dropped_migration_old_codes` 等）无法从当前 parquet 反推，原样移入
  `provenance.previous_build` 并注明「描述 2026-11-10 那次构建，不代表当前文件」。
- 行数演进记在 `row_count_history`：manifest 记录 5,914 / 2026-11-12 改前实文件 5,915 / 现 5,916，
  即 **manifest 早在本轮改动前就已落后**，不是本轮才产生的偏差。
- 写入方式：`src.data.atomic_write.write_text_atomic`（临时文件 → fsync → `os.replace`），无 `.tmp` 残留。
- 改前快照：`_backup/meta/stock_basic_manifest_20261112_070513/`（parquet 与旧 manifest 各一份，
  两者 SHA256 与源文件一致）。
- **该 manifest 无任何代码消费者**：`src/` 全目录检索 `stock_basic.manifest` 命中 0 处，只有
  `tools/build_stock_basic.py` 写它；本次改动风险面限于审计留痕。

### 新 manifest 顶层字段

`manifest_schema_version`、`generated_at`、`generated_by`、`builder`、`source_parquet*`（路径/SHA256/字节/mtime）、
`rows`、`unique_ts_code`、`duplicate_ts_code`、`columns`、`schema`、`null_counts`、`non_null_counts`、
`distinct_counts`、`null_codes`、`value_counts`（status / delist_reason / 4 个 basis 列 / exchange / market / board）、
`date_ranges`、`field_sources`、`column_basis`（沿用构建口径说明）、`provenance`、`previous_manifest`、
`row_count_history`、`unresolved`。

## 2. 逐列一致性核对

口径：以 `quotes/stock/{sh,sz,bj}/*.parquet` 文件集合与首末 bar、`meta/names.json`、
`meta/name_history.parquet`、`meta/code_migration.parquet`、`tmp/names_raw/stock_basic_{L,D,P}.parquet`
（tushare 官方清单）独立复算每一列，不直接复用候选表结论。

| 列 | 不一致行数 | 结论 |
|---|---|---|
| `ts_code` | 0 | 与「行情文件名 ∪ 官方退市清单 ∪ 保留旧码 − 可覆盖 T 码」完全一致 |
| `name` | 9 | 见 §2.2 |
| `name_basis` | 0 | 与「names.json 命中 → names_json；否则 name_history」一致 |
| `list_date` | 0 | 首根 bar / 官方上市日规则全部一致 |
| `delist_date` | 13 | 见 §3.2（行情文件 2026-11-11 新到，表仍是官方摘牌日） |
| `status` | 0 | `lag_td` 与退市证据判定一致 |
| `delist_reason` | 1 | 601268.SH：表现记「退市」，按现行情+`code_migration`（601268→601399）应为「旧码迁移」 |
| `exchange` / `market` / `board` | 0 / 0 / 0 | 代码后缀/前缀推导全部一致 |
| `date_precision` | 13 | 随 `delist_date` 同批变化（`official` → `bar`） |
| `delist_date_basis` | 13 | 同上（`official_delist` → `quote_last_bar`） |

### 2.1 `raw/closure_evidence.parquet` 不是本表来源

实际路径 `meta/st/raw/closure_evidence.parquet`，18 行、列 `ts_code / close_date / evidence`，
`close_date` 是 **ST 撤销日**（2008–2012），不是退市日。18 只代码都在 stock_basic 内，但
**stock_basic 任何一列都不由它推导**，故不参与核对，只登记为「非本表来源」。

### 2.2 名称：现有表 vs 当前来源（9 条）

| ts_code | 表现值 | 当前来源值 | 判定 |
|---|---|---|---|
| 001381.SZ | 皇冠 | 皇冠新材（name_history 2026-11-03） | 表现值截断，重建可修正 |
| 600251.SH | 冠农股份 | 新疆冠农（name_history 2026-10-27） | 表现值陈旧，重建可修正 |
| 603112.SH | 华翔股 | 华翔股份 | 表现值截断，重建可修正 |
| 603680.SH | 今创集 | 今创集团 | 表现值截断，重建可修正 |
| 601801.SH | 皖新传媒 | `XD皖新传`→`皖新传` | **重建会劣化**（TDX 4 字截断 + 当日 XD 标记） |
| 601811.SH | 新华文轩 | `XD新华文`→`新华文` | **重建会劣化** |
| 601869.SH | 长飞光纤 | `XD长飞光`→`长飞光` | **重建会劣化** |
| 603309.SH | 维力医疗 | `XD维力医`→`维力医` | **重建会劣化** |
| 605033.SH | 美邦股份 | `XD美邦股`→`美邦股` | **重建会劣化** |

即现有表 4 处截断/陈旧、5 处比 `names.json` 更完整；`names.json` 本轮被刷新后混入了当日 XD 前缀与
4 字截断，直接按「names.json 优先」重建会把这 5 个正确全名改坏。

### 2.3 名称双源冲突：140 条，按 DG-18 登记 unresolved

`names.json`（TDX 快照，clean 后）与 `name_history` 末段（tushare 公告）不一致共 140 条，
分类（`name_conflicts.csv` 的 `classification` 列）：

| 分类 | 条数 | 说明 |
|---|---|---|
| `inferred_artifact` | 81 | name_history 末段是 `inferred:strip_G` 推断名（如 `深振业` vs `深振业Ａ`），非真冲突 |
| `tolerance_truncation` | 47 | 一方是另一方前缀（TDX 4 字截断，如 `华润新能` / `华润新能源`） |
| `st_wrapper` | 1 | 688053.SH：`ST思科瑞` vs `思科瑞`，ST 标记差异 |
| `direct_rename_conflict` | 11 | 公告改名早于 `names.json`，见下表 |

`direct_rename_conflict` 11 条（表内值均仍为旧名，**未改动**）：

| ts_code | names.json | name_history | 公告日 |
|---|---|---|---|
| 001236.SZ | 弘业期货 | 苏豪期货 | 2024-03-29 |
| 002203.SZ | 海亮股份 | 海亮材料 | 2026-09-24 |
| 300210.SZ | 森远股份 | 中科森远 | 2026-05-01 |
| 300351.SZ | 永贵电器 | 永贵智能 | 2026-09-09 |
| 300492.SZ | 华图山鼎 | 华图科技 | 2026-10-16 |
| 301015.SZ | 百洋医药 | 百洋制药 | 2026-10-21 |
| 600258.SH | 首旅酒店 | 首旅如家 | 2017-02-08 |
| 600962.SH | 国投中鲁 | 国投电子 | 2026-10-17 |
| 603030.SH | 全筑股份 | 云阙智算 | 2026-05-13 |
| 603268.SH | 松发股份 | 恒力重工 | 2026-11-07 |
| 603955.SH | 大千生态 | 灵动股份 | 2026-10-23 |

本任务标的 **688827.SH** 单独登记：`names.json` = `N钶锐`（clean 后 `钶锐`），`name_history` =
`钶锐锶`（ann_date 2026-11-10，`tushare_namechange`），表内取 `钶锐`（`name_basis=names_json`）。
两者相差一字、无法判定是 TDX 截断还是两个不同简称，**按 DG-18 保留本地存量、不择一覆盖**，
已写入新 manifest 的 `unresolved[0]`（`action: keep_local`）。

## 3. 整表重建实测差异

### 3.1 汇总

| 指标 | 实测 |
|---|---|
| 重建候选行数 | 5,916 |
| 仅现有有 | 0 |
| 仅重建有 | 0 |
| 差异代码数 | **22**（0.37%） |
| 差异单元格数 | **62** |

| 列 | 差异行数 | 性质 |
|---|---|---|
| `name` | 9 | 4 修正 + 5 劣化（§2.2） |
| `delist_date` | 13 | 官方摘牌日 → 末根 bar（§3.2） |
| `list_date_basis` / `delist_date_basis` / `date_precision` | 各 13 | 同上，`official` → `bar` |
| `delist_reason` | 1 | 601268.SH：退市 → 旧码迁移 |

### 3.2 13 只日期差异的成因

这 13 只的行情文件均在 **2026-11-11 14:05** 新建（此前无行情），表内仍是「无行情退市股」口径的
官方摘牌日；按构建器「有行情 = 末根 bar」的既定规则重建，日期会前移：

| ts_code | 表现 delist_date | 末根 bar | 首根 bar |
|---|---|---|---|
| 000033.SZ | 2017-07-07 | 2017-07-06 | 1994-01-03 |
| 000511.SZ | 2018-07-18 | 2018-07-17 | 1993-05-18 |
| 000594.SZ | 2015-07-13 | 2015-07-10 | 1996-03-20 |
| 000693.SZ | 2019-07-09 | 2019-07-08 | 1997-02-26 |
| 000979.SZ | 2018-12-28 | 2018-12-27 | 2000-06-16 |
| 002070.SZ | 2019-07-09 | 2019-07-08 | 2006-10-12 |
| 300372.SZ | 2017-08-28 | 2017-08-25 | 2014-01-27 |
| 600401.SH | 2019-07-12 | 2019-07-08 | 2003-09-24 |
| 600432.SH | 2018-07-13 | 2018-07-11 | 2003-09-05 |
| 600656.SH | 2016-05-13 | 2016-05-11 | 1990-12-19 |
| 600680.SH | 2019-05-23 | 2018-04-27 | 1993-10-18 |
| 600806.SH | 2018-07-13 | 2018-07-11 | 1994-01-03 |
| 601268.SH | 2015-05-21 | 2014-04-28 | 2010-02-02 |

其中 600680.SH / 601268.SH 差一年以上——摘牌前长期停牌，末根 bar 远早于官方摘牌日。这属口径选择：
「最后交易日」与「官方摘牌日」在停牌股上不等价，需明确下游要哪一个。

## 4. 是否整表重建

**结论：暂不重建。** 理由与代价：

1. **收益有限**：行集零差异，只影响 22/5,916 只；其中 13 只是「新到行情未跟进」，1 只是换码理由，
   4 只是名称修正。
2. **有明确劣化项**：重建会因 `names.json` 当日 XD 前缀 + 4 字截断，把 5 只正确全名改短
   （`皖新传媒`→`皖新传` 等）。现有 `merge_with_existing` 只在候选为空时兜底，不会阻止非空覆盖。
3. **改变历史口径**：13 只退市日从官方摘牌日前移到末根 bar，会影响所有按 `delist_date` 判退市/剔除的
   下游（`universe_filter.py`、`industry_universe.py`、`industry_pit.py`、`expectation.py`、
   `st_history.py`、`risk_flags.py`、`build_breadth_ma.py`、`quality/freshness_check.py` 覆盖度分母）。
   停牌股提前 1 年退出股票池是否符合预期，需要单独裁定。
4. **前置条件**（任一满足后再重建）：① 先修 `names.json` 截断（去掉 XD 后不足 4 字的名字重新取全名）；
   ② 或给构建器加「`names.json` 值是 `name_history` 前缀时不当覆盖」的判定并补单测；
   ③ 对 13 只停牌股的 `delist_date` 语义（末根 bar vs 官方摘牌日）给出显式口径。

若只做局部修正（不整表重建），建议最小动作是：用 `merge_with_existing` 的同款脚本只更新这 13 只的
日期链与 601268 的理由，不触碰名称列——需另行确认后再执行。

## 5. 留证与复现

```powershell
# 1. 逐列核对 + 全量重建差异（只读，约 1 分钟）
.venv\Scripts\python.exe _staging\stock_basic_manifest_20261112\reconcile_stock_basic.py

# 2. 从实文件重算 manifest（预览；加 --write 原子写入）
.venv\Scripts\python.exe _staging\stock_basic_manifest_20261112\rebuild_manifest.py
```

- 改前快照：`_backup/meta/stock_basic_manifest_20261112_070513/`
  - `stock_basic.parquet` SHA256 `d3d90abd27628b8b43131e2f661d3a4d7a3fa01e761b51e770ebbc8e0071b9fe`
  - `stock_basic.manifest.json` SHA256 `33be347f9ff1fc5ff9c78725ff9482b4ad058f7bf301f9b02ed243bc37b5b612`
- 改后：parquet 未改动（SHA256 同上）；新 manifest SHA256
  `41a8a5d764d7e9813c047b279dc2088316ce6ac230c356703aef08380643a3e0`。
- 下游冒烟：`src.data.local_store.expectation.listing_windows()` 实跑返回 5,916 个窗口，
  含 `688827.SH list_date=2026-11-10 / delist_date=None`，证明表可被真实读路径读到。

## 6. 本次自行决定并执行的事项

| 事项 | 依据 | 回滚方式 |
|---|---|---|
| 只重算 manifest、不重建 parquet | 任务约束「重建前先报差异」；且重建含 5 处名称劣化 | 无需回滚，parquet 未动 |
| 将构建期证据计数移入 `provenance.previous_build` | 这些值无法从实文件重算，留在顶层会被误读为当前值 | 从 `_backup/meta/stock_basic_manifest_20261112_070513/` 拷回旧 manifest |
| 名称冲突分类（inferred_artifact / truncation / ST / rename） | 140 条原始冲突中 129 条是格式或推断产物，逐条列会淹没真冲突 | 分类规则写在核对脚本 `_classify`，可改 |
| 未更新 `docs/数据权威路径清单.md` | 路径与口径未变，仅 manifest 内容刷新 | — |
