# B1 第六片 · ST 人工补充层基础层校准体检

- 派发对象：`meta/stock_basic.parquet`、`meta/st/st_intervals.parquet`、`meta/st/supplement/*.parquet`
- 口径：与前几片一致（① 写入口 ② manifest 一致性 ③ 权威源对拍 ④ 第二份同语义 ⑤ 权威清单登记）
- 性质：**只读**。未改代码、未写 `data/`、未改任何 parquet / manifest / 清单、未提交
- 实测时间：2026-11-12；行情基准为本地 `stock` 全量行情（末日 2026-11-11）
- 复现脚本：`2026-11-12/data-lineage/work/b1s6_supplement.py`、`b1s6_extra.py`、`b1s6_evidence.py`、`b1s6_replay.py`、`b1s6_negative.py`

---

## 0. 结论摘要

| 项 | 结论 |
|---|---|
| 批次归属 | 三表**只有 1 张在 B1**。`supplement` 两表在 B1 深度 0；`stock_basic` 属 **B3**（`governance_order.md` L122），`st_intervals` 属 **B4**（L141）→ 两张按指令跳过，只写归属与交接（§9、§10） |
| ① 写入口 | 两表同一正式入口 `tools/sync_st_name_history.py`（build / apply 两段），parquet 走原子写；**manifest 与 `unresolved.csv` 是裸写**（DG-9 不满足），`write_text_atomic` 已导入但全文件 0 调用。`fetched_at=now()` 使重跑不可逐字节复现 |
| ② manifest | 2/2 记录的 `rows` / `bytes` / `sha256` 与实测**全部一致**；`manifest_tool --check --only meta/st/supplement` → 1 份 / 2 条 / 不一致 0。但 `unresolved.csv` **未登记**进 files 段，且无「行情末日快照日」字段 → coverage 时效不可自证 |
| ③ 对拍 | 只读重算与现存表**逐值一致**：coverage 2,336/2,336、intervals 2/2、unresolved 9/9。负证据与 tushare 候选（1,111 只）、7 份快照累计 ST（199 只）**交集均为 0**。两处实质缺口：**2,323/2,334 条负证据的 `coverage_end` 停在 2026-11-09，落后行情 2 个交易日**；**15 只 / 17 段价格反推疑似 ST 段整段落在负证据窗口内**（弱证据冲突，待人工复核） |
| ④ 第二份同语义 | 无并存的"第二份正式"数据，`backups/critical_assets/` 与 `G:\git\python-quant\` 与正式表逐字节一致。`_backup/st_cov_window_20261110/` 是合规回滚稿（coverage 2,335 行，少 1 行）；`tmp/cov_sandbox/` 与它逐字节相同，属仓库内残留副本 |
| ⑤ 清单登记 | 11 条偏差：3 条口径错误（P1、P3、P4）、4 条数字与指向错误（P2、P5、P6、P10）、4 条结构性缺漏（P7、P8、P9、P11）。逐条见 §8，**未改动该文件** |
| 最重要一条 | 权威清单与 manifest 都写"补充层**覆盖**正式层"，实测代码是"**正式层优先、补充层只补缺**"（coverage `keep='first'`）与"按五元组去重后**并集**"（intervals）。2,336 只补充标的与官方覆盖**零交集**，即"覆盖"语义从未被执行过 |

---

## 1. 批次归属核对（先做，避免凑数）

| 表 | `table_inventory.csv` | `governance_order.md` | 归属判定 |
|---|---|---|---|
| `meta/st/supplement/supplement_intervals.parquet` | L44 `st`，`L3` / `base` | L72「人工补充 / supplement_intervals」，批内无派生边 → 深度 0 | **B1 ✓ 本片体检对象** |
| `meta/st/supplement/supplement_coverage.parquet` | 同上（未单独登记，随 `supplement_intervals` 一行） | 同上 | **B1 ✓ 本片体检对象** |
| `meta/stock_basic.parquet` | L24 `meta`，`L1` / `mixed` | L122「包含表：`stock_basic`（mixed，P0 之后）」、L127 须在 P0 完成后重算 | **B3 → 跳过**，交接见 §9 |
| `meta/st/st_intervals.parquet` | L42 `st`，`L3` / `mixed` | L141「B4 / 含人工覆盖层」，L143 依赖 `st_price_inferred` + `st_raw` + `st_status` + `supplement_intervals` | **B4 → 跳过**，交接见 §10 |

`stock_shares` / `stock_turnover` 未列入、未体检。

归属本身有一处矛盾，记入 P8：`table_inventory.csv` 把 `supplement_intervals` 标为 `L3`，`governance_order.md` 按派生边算出它的深度是 0（B1），而实测它读 `meta/st/st_coverage.parquet`（B5 级）——层级标签、依赖深度、真实上游三处互不一致。

---

## 2. 三件文件基础事实（实测）

| 文件（目录 + 文件名） | 行数 | 字节 | 列数 | SHA256（前 16） | 时间跨度 |
|---|---|---|---|---|---|
| `data/local_store/meta/st/supplement/supplement_intervals.parquet` | 2 | 2,781 | 7 | `f6a43084d7e380bc` | 2019-07-16 ~ 2025-12-31 |
| `data/local_store/meta/st/supplement/supplement_coverage.parquet` | 2,336 | 16,041 | 5 | `75f55307219b85c2` | 1990-12-19 ~ 2026-11-09 |
| `data/local_store/meta/st/supplement/unresolved.csv` | 9 | 1,132 | 4 | `ef4977642e207708` | — |

三件 mtime 同为 2026-11-10 19:47:51（同一次 build 落盘）。

列结构：

| 文件 | 列 |
|---|---|
| intervals | `ts_code, start_date, end_date, flag, source, precision, fetched_at` |
| coverage | `ts_code, coverage_start, coverage_end, source, fetched_at` |
| unresolved | `ts_code, reason, former_names, evidence` |

`supplement_intervals` 全部 2 行：

| ts_code | 区间 | flag | source | precision |
|---|---|---|---|---|
| `603810.SH` | 2019-07-16 ~ 2019-11-13 | `st` | `cninfo_notice_dated` | `announcement` |
| `920680.BJ` | 2025-04-30 ~ 2025-12-31 | `*st` | `cninfo_notice_dated` | `announcement` |

`supplement_coverage` 结构实测：2,336 行 / 2,336 只（主键 `ts_code`，重复 0，空值 0，`coverage_start >= coverage_end` 0 行）；来源 `em_name_history_clean` 2,334 + `cninfo_notice_dated` 2。`coverage_end` 分布：2026-11-09 占 2,325 行，其余 11 行分散在 2018-12-12 ~ 2026-09-14（退市/停牌标的）。

`unresolved.csv` 9 只：`600606.SH / 600658.SH / 600683.SH / 600775.SH / 600833.SH / 600837.SH / 600845.SH / 600855.SH / 600874.SH`，reason 全为 `st_name_without_dated_event`（曾用名里有 ST/PT 前缀，但检索不到可定日期的实施/撤销公告）。

---

## 3. ① 写入口

| 检查点 | 实测 |
|---|---|
| 正式入口 | `tools/sync_st_name_history.py`（子命令 `build` 重算落盘、`apply` 合并进 `st_intervals` / `st_coverage`），实现 `src/data/st_name_history.py`：`plan_supplement` L527-580、`build_supplement` L579-640 |
| 可反复跑 | √ 离线可跑：全部输入本地已备（em 登记册、st_coverage、行情、7 份快照、公告缓存），无需联网 |
| parquet 原子写 | √ 走本地 helper `_write_atomic`（L147-148）→ `write_parquet_atomic`，调用点 L610-611 |
| 覆盖正式表的方式 | **× 不走 DG-11**：`_write_atomic` 直接覆盖 `supplement/*.parquet`，未先把现有正式件移入 `_backup/`。现存 `_backup/st_cov_window_20261110/` 是人工另做的一次快照，不是 build 流程产物 |
| manifest 原子写 | **× 裸写**：`Path.write_text`（`_write_text` L184-186，调用 L636）。`write_text_atomic` 已在 L52 导入、全文件 0 处调用（死导入） |
| `unresolved.csv` 原子写 | **× 裸写**：`write_csv`（L615） |
| 跨文件事务声明 | **× 缺失**（DG-10）：一次 build 内 4 次独立写（2 parquet + 1 csv + 1 manifest），无写入顺序、无失败残留说明——若 parquet 成功而 manifest 失败，正式目录会留下"数据已换、manifest 未换"的不一致态 |
| 逐字节可复现 | **×**：`fetched_at = datetime.now()`（L285/L559/L573）与 `built_at` 写入产出，内容等价但字节必变，manifest 的 `sha256` 每次重跑都会变；即"可反复跑"成立、"可复现校验"不成立 |
| 上游输入（实测 5 项） | `meta/st/raw/em_org_basic.parquet`（24,876 行）／`meta/st/st_coverage.parquet`（过滤掉 supplement 来源后 3,553 行）／`stock` 全量行情池（现 5,832 只，build 时 5,818）／`meta/st_status/` 7 份快照（`st_status_20261103…20261112`）／`_staging/st_candidate_notices_cache_20261112/`（11 只公告分片 + `manifest.json` + `progress.json`）与 `_staging/st_em_pages_cache_20261112/`（50 页分片 + 3 件状态文件） |
| 断点与失败清单（DG-7） | 公告/分页缓存的 `manifest.json` 与 `progress.json` 都在 `_staging/` 内，合规；但 `candidate_state.json`（926 B）、`em_state.json`（607 B）落在**正式层** `meta/st/`，`candidate_failures.csv` / `em_failures.csv` 只有表头（0 失败）——同一任务的断点被拆到两层，未形成 DG-7 要求的单目录 `progress.* + failed.csv` |
| 跨层副作用 | `apply` 调 `st_history.build_store`，会整表重写 `st_intervals`（B4）与 `st_coverage`（B5）→ 本表写入口实际跨 B1/B4/B5 三层 |

---

## 4. ② manifest 一致性

`manifest_tool --check --only meta/st/supplement` → `1 份 manifest / 2 条记录 / 不一致 0`。

| 记录 | rows 声明 → 实测 | bytes 声明 → 实测 | sha256 前 16 声明 → 实测 |
|---|---|---|---|
| `meta/st/supplement/supplement_intervals.parquet` | 2 → 2 ✓ | 2,781 → 2,781 ✓ | `f6a43084d7e380bc` → 同 ✓ |
| `meta/st/supplement/supplement_coverage.parquet` | 2,336 → 2,336 ✓ | 16,041 → 16,041 ✓ | `75f55307219b85c2` → 同 ✓ |

标量字段复核：

| 字段 | 声明 | 实测 | 判 |
|---|---|---|---|
| `em_rows` | 24,876 | `em_org_basic.parquet` 24,876 行 | ✓ |
| `pool` | 5,818 | 现行情池 5,832 只（新增 14 只，含 688827.SH） | 历史值，非缺陷；但 manifest 未记"行情末日/构池快照日"，无法从记录判断时效 |
| `uncovered_before` | 2,345 | 同口径在现池（5,832 只）下为 2,346（official 覆盖 3,553 只、其中 67 只不在行情池内 → 池内已覆盖 3,486）；与构池时的 2,345 差 1 只，对应池扩容 | ✓ |
| `clean_negative_coverage` / `dated_candidate_coverage` | 2,334 / 2 | 2,334 / 2 | ✓ |
| `unresolved` / `unresolved_by_reason` | 9 / `{st_name_without_dated_event: 9}` | 9 / 同 | ✓ |
| `supplement_interval_rows` / `supplement_coverage_rows` | 2 / 2,336 | 2 / 2,336 | ✓ |

manifest 缺口两条：① `unresolved.csv` 未进 `files` 段（DG-3 要求逐文件记账，本目录 3 件只记 2 件）；② 无「行情末日快照日 / 构池时刻」字段，导致 §5 的 2,323 行时效问题无法从 manifest 自证（见 P11）。

---

## 5. ③ 权威源对拍

### 5.1 只读重算 vs 现存表（最强证据）

用当前代码、当前输入，**只调纯函数** `plan_supplement`（不落盘），分别以 `today=2026-11-09`（复现 build 时刻的行情状态）与 `today=2026-11-10` 重算：

| 对象 | 重算 | 现存 | 逐值差异 |
|---|---|---|---|
| coverage（键 `ts_code + coverage_start + coverage_end + source`），`today=2026-11-09` | 2,337 | 2,336 | **现存 2,336 行逐值全等**；重算多 1 行（`688827.SH`，现池扩容），"只在重算"1 行、"只在现存"0 行 |
| 同 coverage，`today=2026-11-10` | 2,337 | 2,336 | 2,324 行的 `coverage_end` 由 11-09 变 11-10（输入漂移，见 5.2），其余全等 |
| intervals（键 `ts_code + start + end + flag + source + precision`） | 2 | 2 | **0 / 0**（两个 `today` 下均零差异） |
| unresolved（代码 + reason） | 9 | 9 | **0**（两个 `today` 下均零差异） |

→ `supplement` 两表**完全可由当前代码 + 当前输入复现**。权威清单/manifest 把它记作"不可重建的人工资产"（P9），与实测不符：它不含任何人工录入内容，2 条区间来自 `cninfo` 定日公告，2,334 条负证据来自东财 F10 曾用名 + 行情窗口。

重算多出 1 行 `688827.SH`（现池 5,832 > 构池时 5,818，该股 2026-11-10 上市）：`today=11-09` 下出现 `coverage_start(11-10) > coverage_end(11-09)` 的反序窗口，说明 `plan_supplement` **没有 `start <= end` 断言**；`today=11-10` 下该行退化为 start==end 的单日窗口。仅时间回溯时显现，不影响现存表，但属应当补的边界断言。

### 5.2 行情窗口回验（coverage 的源头是否仍成立）

| 检查 | 结果 |
|---|---|
| `coverage_start` 是否等于个股行情首行 | **2,334 / 2,334 相等，差异 0**（起点 1990-12-19 ~ 2026-11-06） |
| `coverage_end` 是否等于个股行情末日 | **2,323 条落后 2 个交易日**：`coverage_end` 停在 2026-11-09，对应个股行情已到 2026-11-11。另 2 条行情末日就是 11-09，9 条为退市/停牌（与其行情末日相等，正确） |
| 影响 | 这 2,323 只在 2026-11-10、2026-11-11 两个交易日落到覆盖窗口之外 → 状态查询返回 `unknown`，正确答案应为 `non_st`。属"构建后行情刷新、补充层未重跑"，不是"无数据" |

### 5.3 独立源交叉检验（负证据是否假阴性）

| 独立源 | 规模 | 与 2,334 只负证据的交集 | 判 |
|---|---|---|---|
| `meta/st/raw/tushare_namechange_intervals.parquet`（名称变更 ST 候选段） | 2,458 行 / 1,111 只 | **0 只** | ✓ 无冲突 |
| `meta/st_status/st_status_20261103…20261112` 7 份快照累计 ST 代码 | 199 只 | **0 只** | ✓ 无冲突 |
| `meta/st/st_intervals.parquet` 全部 ST/`*st` 区间 | 2,304 行 | **0 只**（clean 集合内无任何 ST 段） | ✓ 无冲突 |
| `meta/st/st_price_inferred.parquet`（价格反推疑似段） | 1,351 行 | **15 只 / 17 段**，17/17 整段落在负证据窗口内，`confidence` 全 `low` | **弱证据冲突，需人工复核** |

17 段明细（`flag` 全 `suspected_st`、`confidence` 全 `low`）：

| ts_code | 疑似段 | ts_code | 疑似段 |
|---|---|---|---|
| `600063.SH` | 2011-02-10 ~ 2011-07-29 | `600664.SH` | 2007-01-08 ~ 2008-08-14 |
| `600133.SH` | 2006-12-25 ~ 2007-07-24 | `600682.SH` | 2006-12-22 ~ 2008-03-07 |
| `600135.SH` | 2006-12-21 ~ 2008-02-13 | `600688.SH` | 2006-10-25 ~ 2009-12-30 |
| `600211.SH` | 2006-12-21 ~ 2007-08-02 | `600688.SH` | 2010-10-26 ~ 2011-05-18 |
| `600283.SH` | 2004-11-04 ~ 2005-05-26 | `600688.SH` | 2012-12-04 ~ 2013-06-27 |
| `600379.SH` | 2006-12-25 ~ 2007-10-12 | `600865.SH` | 2007-01-12 ~ 2008-01-25 |
| `600517.SH` | 2009-09-24 ~ 2010-03-09 | `601567.SH` | 2011-06-27 ~ 2012-01-16 |
| `600643.SH` | 2007-01-05 ~ 2007-12-19 | `601908.SH` | 2012-09-14 ~ 2013-03-29 |
| — | — | `603085.SH` | 2025-06-05 ~ 2026-06-09 |

注意 `603085.SH` 的疑似段跨到 2026-06-09（最近一例），若成立则是负证据的近期假阴性。价格反推是弱证据（除权/停牌都会制造假阳性），本片不裁决，交 B4 统一裁定。

### 5.4 无法对拍的部分（必须写明）

| 项 | 原因 |
|---|---|
| 2,334 条负证据本身 | 单一来源（东财 F10 曾用名登记册 `em_org_basic.parquet`），无第二源可对拍。"名称从未出现 ST/PT 前缀 ⇒ 该股从未风险警示"是**推断**，不是公告直证 |
| 精度不可区分 | `supplement_coverage` **没有 `precision` 列**，2,334 条推断与 2 条公告直证在表内不可区分（对照 `st_intervals` 有 `precision`）→ 见 P4 |
| 公告原文 | 定日证据依赖 `_staging/st_candidate_notices_cache_20261112/`（11 件，`_staging/` 不入 Git、不在备份范围）；缓存一旦清理，2 条区间的举证链只剩 `fetched_at` 时间戳 |

---

## 6. supplement 与 `st_intervals` 一致性核对（点名项）

| 检查 | 结果 |
|---|---|
| 2 条补充区间是否进入 `st_intervals` | **2/2 均命中且各仅 1 行**，`ts_code / start_date / end_date / flag` 逐值相同，`source` 保留 `cninfo_notice_dated` |
| 两只在 `st_intervals` 里是否有重复或第二段 | 无。`603810.SH` 仅 1 段、`920680.BJ` 仅 1 段，无重叠、无相邻断裂 |
| 补充区间是否落在各自覆盖窗口内 | **0 冲突**（2/2 在 `coverage_start ~ coverage_end` 内） |
| 负证据与 ST 区间是否冲突 | **0 冲突**（2,334 只 clean 在 `st_intervals` 中无任何 ST 段） |
| 覆盖表合并结果 | `st_coverage` 5,889 行 = official 3,553（已剔除 supplement 来源的官方覆盖层）+ supplement 2,336；两者**无交集**，2,336 只全部以 supplement 来源独立存在 |
| 合并语义（代码实测） | coverage：`pl.concat([official, supp]).unique(subset=['ts_code'], keep='first')`（`st_history.py:1243-1245`）→ **official 在前、正式层优先，补充层只补缺**；intervals：`concat` 后仅按 `(ts_code,start,end,flag,source)` 去重（L1246-1247）→ **并集，不是覆盖** |
| 证据字段继承 | `supplement_intervals` 只有 7 列（无 `evidence` / `raw_fetched_at`），合并进 `st_intervals` 后这 2 行的 `evidence`、`raw_fetched_at` 为 null → 正式表里看不出对应公告编号，举证要回到 `_staging/` 缓存 |

结论：**一致性通过**（2/2 逐值一致、无重复、无冲突），但"覆盖关系"的登记口径与代码相反（P1），且证据字段在正式表中为空（§5.4）。

---

## 7. ④ 第二份同语义扫描

| 位置 | 行数 | 字节 | SHA256（前 16） | 判定 |
|---|---|---|---|---|
| `data/local_store/meta/st/supplement/`（正式） | 2 / 2,336 | 2,781 / 16,041 | `f6a43084d7e380bc` / `75f55307219b85c2` | 正式 |
| `backups/critical_assets/…/supplement/` | 2 / 2,336 | 同 | **与正式逐字节一致** | 合规快照 |
| `G:\git\python-quant\data\local_store\meta\st\supplement\` | 2 / 2,336 | 同 | **与正式逐字节一致** | 合规镜像 |
| `_backup/st_cov_window_20261110/supplement_intervals.parquet` | 2 | 2,781 | `091207a4ec1a2ad2` | 合规回滚稿（DG-11）；同尺寸不同 sha → 内容差异来自 `fetched_at` |
| `_backup/st_cov_window_20261110/supplement_coverage.parquet` | 2,335 | 15,466 | `cc6dc7c211455b7e` | 合规回滚稿；比正式**少 1 行** |
| `tmp/cov_sandbox/meta/st/supplement/`（两件） | 2 / 2,335 | 同 `_backup` | `091207a4…` / `cc6dc7c2…` | **仓库内残留副本**（`tmp/` 已 gitignore），与 `_backup` 稿逐字节相同、落后正式表 1 行 |

判定：**没有并存的第二份"正式"数据，也没有读 `_backup` / `tmp` 的 fallback**（读取集中在 `src/data/st_history.py`：L912 构造路径、L1242 合并）。`_backup` 与 `tmp` 两处旧稿均不可作为数据源。`tmp/cov_sandbox/` 属清理候选，本片不删（需点名路径后处理）。

---

## 8. ⑤ 权威清单登记缺陷逐条（`docs/数据权威路径清单.md`，本片不改）

| 编号 | 位置 | 现写什么 | 实测什么 | 建议 |
|---|---|---|---|---|
| **P1** | L67 | "合并顺序为「补充层覆盖正式层」" | coverage 是 `concat` 后 `unique(ts_code, keep='first')`，**official 在前 → 正式层优先**；intervals 是并集（仅五元组去重）。2,336 只补充标的与 official 覆盖**零交集**，"覆盖"从未发生 | 改为"补充层只补官方层未覆盖的标的与区间；覆盖表正式优先、区间表并集" |
| **P2** | L67 消费者列 | `st_name_history.py#L111-L116` | 那是**产出方**的路径常量函数；真正读侧是 `src/data/st_history.py:909-918`（`_load_supplement`）与 `:1242-1247`（合并） | 消费者改为指向 `st_history.py` 读侧 |
| **P3** | L67 | 未写 `coverage_end` 的口径与时效 | `coverage_end = min(个股行情末日, build 当日)`；当前 2,323/2,334 条停在 2026-11-09，行情已到 2026-11-11 | 补口径句 + 当前滞后天数 |
| **P4** | L67 | 未写精度与证据等级 | `supplement_coverage` 无 `precision` 列；2,334 条是"名称从未 ST"的**推断负证据**，2 条是公告直证，表内不可区分 | 补"负证据为推断、精度不可分"一句 |
| **P5** | L106 | "10 行 / 1,132 B" | **9 条数据行**（`1,132 B` 正确；含表头才是 10 行） | 改"9 行（另 1 行表头）" |
| **P6** | L106 | "reason 分 `no_quote_window` / `not_in_name_registry` 等" | 9 条**全部** `st_name_without_dated_event`，另两类本轮为 0 | 按实测改分布，或写"当前仅 1 类" |
| **P7** | L67 / L106 + manifest | `unresolved.csv` 未进 `supplement/manifest.json` 的 `files` 段 | 目录 3 件，manifest 只记 2 件（DG-3 要求逐文件 rows/bytes/sha256） | manifest 补第 3 条 |
| **P8** | `table_inventory.csv` L44 | `supplement_intervals` 标 `L3` / `base` | `governance_order.md` 算深度 0（B1）；实测它读 `st_coverage`（B5 级）→ 层级、深度、上游三者不一致 | 统一为"人工补充层，上游含 st_coverage（经 `official_coverage()` 过滤破环）" |
| **P9** | `lineage.json` supplement 节点 | `rows: null` / `files: null` / `writers_detected: []` / `derived_from: []` / `rebuildable: 不可重建（人工证据）` | 实测有正式 `tools/` 入口、2/2+2,336/2,336 逐值可复现、上游 ≥5 项（`em_org_basic` / `st_coverage` / 行情 / `st_status` 快照 / 公告缓存） | 补 rows/files/writer；`rebuildable` 改"可重建（依赖本地缓存）"，不可重建性只属于 `_staging` 公告缓存 |
| **P10** | `supplement/manifest.json` 的 `authority` 字段 | 引"清单 **L65**：唯一正式区间之外的人工补充层…" | L65 现是"ST 价格反推区间"；补充层登记实际在 **L67** | 改行号 L67 |
| **P11** | `supplement/manifest.json` | 无"行情末日 / 构池时刻"字段 | `pool=5,818` 已成历史值（现 5,832）；`coverage_end` 是否最新无法从 manifest 判定 | 补 `quote_last_date` / `pool_at_build` 两个可复算字段 |

对照（这条是对的）：L67 的"2 个区间 / 2,336 行覆盖"与实测完全一致。

---

## 9. 交接备忘 A · `meta/stock_basic.parquet`（B3，本片跳过）

| 项 | 实测 |
|---|---|
| 归属 | **B3**（`governance_order.md` L122 包含表、L127 要求 P0 完成后重算、L17 记其深度 2），前置条件是 P0 切断 `breadth_daily` 反向依赖 |
| P0 代码切断 | √ `tools/build_stock_basic.py` 全文件 `breadth` 仅 1 处命中且在注释（L212）；`load_trade_calendar` 改取 `src/data/trading_calendar.py` 的 `get_calendar(root).authoritative_dates()` |
| 现读日历是否真实交易日 | √ 实测 8,761 个权威日期，1990-12-19 ~ 2026-11-11，来源 `000001.SH`；尾部 262 个工作日为近似值且**不取**（日志明示），与 P0 提交自述一致 |
| 切断前后数据是否有差异 | 提交 `834d06d`（2026-11-12 23:09:53）自述"同一批输入仅替换日历来源，5,916 行 × 12 列改动 0 格（null 转哨兵后比较）"，并新增 `tests/test_stock_basic_calendar_source.py` 两条钉子。**本片未独立复现**：复现需重算并落盘，属 B3 范围（DG-5 要求候选落 `_staging/`） |
| 正式表现状 | 5,916 行 × **13 列**、71,429 B、sha256 `048335ccd9d81da7…`、mtime 2026-11-12 07:13:55；`status` 分布 L 5,580 / D 336；`list_date` max 2026-11-10、`delist_date` max 2026-10-27 |
| 数据是否被 P0 重算过 | 否。正式表 mtime（07:13:55）早于 P0 提交（23:09:53）；`_backup/stock_basic_20261112_204905/stock_basic.parquet` 与正式表同尺寸同 mtime，无变更可回滚 |
| 列口径漂移 | P0 对拍口径是 **12 列**，现表 **13 列**（多 `date_precision`）→ B3 重算时列集已不同，需重新确认对拍列清单 |
| manifest | **有**：`data/local_store/meta/stock_basic.manifest.json`（rows 5,916、`source_parquet_sha256` `048335cc…` 与实测一致、含逐列 `null_counts` / `field_sources` / `column_basis` / `row_count_history`）。但 `generated_by` 是 `_staging/stock_basic_manifest_20261112/rebuild_manifest.py` 一次性脚本，**不是 builder 内置** → 重算后 manifest 不会自动刷新 |

B3 开工前置清单：① 在 `_staging/` 重算并做 13 列逐值对拍（null 显式转哨兵）；② 重算后同步刷新 `meta/stock_basic.manifest.json`（当前生成器是一次性脚本，需决定并入 builder 还是重复手工跑）；③ 核 `date_precision` 语义与下游消费方。

---

## 10. 交接备忘 B · `meta/st/st_intervals.parquet`（B4，本片跳过）

| 项 | 实测 |
|---|---|
| 归属 | **B4**（`governance_order.md` L141 包含表「`st_intervals`（mixed，含人工覆盖层）」、L143 依赖 `st_price_inferred` + `st_raw` + `st_status` + `supplement_intervals`、L147 验收要求人工覆盖逐条留证） |
| 表现状 | 2,304 行 / 33,927 B / sha256 `efdc3303055cfd30…`，与 `meta/st/manifest.json` 记录一致。`manifest_tool --check --only meta/st`（前缀匹配到 6 份 manifest，含因前缀误入的 `meta/stock_basic.manifest.json`）→ 28 条记录 / 不一致 0 / sha256 格式告警 0 |
| 人工层与代码层边界（按 source 实测） | 代码推导层 2,102 行：`szse_change_name` 1,307 + `cninfo_notice` 790 + `snapshot_anchor` 5；**人工裁定覆盖层 200 行**：`tushare_namechange_reviewed`；人工补充层 2 行：`cninfo_notice_dated`（本片对象）。合计 2,304 ✓ |
| 覆盖层来源文件 | `data/local_store/meta/st/raw/reviewed_conflicts.parquet`（200 行 / 10 列 / 12,712 B / sha256 `224b87ddf77a1f82…`），构成说明为"203 段冲突裁定中新表胜出的 200 段" |
| 覆盖层优先级 | 最高。`apply_reviewed_conflicts`（`st_history.py:953`，调用 `:1269`）在快照合并与区间延伸**之后**强制套用；`apply_reviewed_coverage`（L1077，调用 L1279）逐只改写覆盖窗口；`reviewed_pending.parquet`（3 段证据不足）只复写到 `_staging`，不入正式表 |
| 覆盖层改动如何留证 | 由 `data/local_store/meta/st/raw/reviewed_conflicts.manifest.json` 承担：`source_report` 指向 `reports/st_g1_conflicts_20261110/g1_conflicts_final.csv`，逐文件记 rows/bytes/sha256/mtime，另记 precision 分布（announcement 56 / exact 143 / inferred 1）与 flag 分布（*st 80 / st 120），行内保留 `adjudication` 裁定列。三张人工表**无采集脚本**（人工录入），改动只能靠该 manifest 的 sha256 + mtime 判定 → 每次改人工层必须同步更新 3 个 sha256 与 `total_rows`，否则无法自证"改了哪一段" |
| 与 `supplement_intervals` 的接口 | B4 无条件 `concat` 本片 2 行，仅五元组去重；实测 2/2 各仅命中 1 行、无重复段、`evidence` 为 null（见 §6） |
| B4 需补的核对 | 200 段人工裁定与 2,102 行代码推导之间的"覆盖 → 是否产生断裂"逐段核对；本片只确认了补充层 2 行的接口一致性，未做人工层与代码层的逐段核对 |

---

## 11. 待办与去向

| # | 事项 | 去向 |
|---|---|---|
| 1 | 2,323 条负证据 `coverage_end` 落后行情 2 个交易日 → 重跑 `build` + `apply` | 需写 `data/`，不属只读片；建议排在本轮 B1 收口后的维护任务，或随 B4 一并重算 |
| 2 | 15 只 / 17 段价格反推疑似 ST 与负证据冲突 | 交 B4（`st_price_inferred` 与负证据的裁决在 B4 口径内） |
| 3 | 写入口缺陷：manifest 与 `unresolved.csv` 裸写、`write_text_atomic` 死导入、4 次写无事务声明、直接覆盖未走 DG-11 快照 | 代码修复任务（本片只读，未改） |
| 4 | `fetched_at=now()` 导致重跑不可逐字节复现，与 manifest 的 sha256 校验冲突 | 代码修复任务；建议改为传入固定 `run_at` 或把 `fetched_at` 移出产出表 |
| 5 | `plan_supplement` 缺 `start <= end` 断言 | 代码修复任务（与 #4 同批） |
| 6 | 权威清单 11 条（P1~P11） | 由另一路统一修改，本片已逐条给出"位置 / 现写 / 实测" |
| 7 | `tmp/cov_sandbox/` 残留副本（2 件，落后正式表 1 行） | 清理候选；需点名路径后再处理，本片未删 |
| 8 | B3 交接 3 项（重算对拍 / 补 manifest / `date_precision` 语义） | B3 批次 |
| 9 | B4 交接 2 项（人工层与代码层逐段核对 / 覆盖断裂检查） | B4 批次 |

---

## 12. 复现命令

```powershell
.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta/st/supplement
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s6_supplement.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s6_extra.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s6_evidence.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s6_replay.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s6_negative.py
git -C e:\code\python\quant grep -n "breadth" -- tools/build_stock_basic.py
```
