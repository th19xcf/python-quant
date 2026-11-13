# B2 第二片体检报告（合约/池/成分/宽度/基金因子）

- 体检日期：2026-11-13
- 批次来源：`2026-11-12/data-lineage/outputs/governance_order.md` 第 94-116 行，B2 = depth 1（15 张）
- 性质：**只读**。未改代码、未写 `data/`、未改任何 parquet / csv / manifest、未 git 提交
- 本片产出：本文件 + `2026-11-12/data-lineage/work/b2s2_{futures,etfpool,members,breadth,fundadj}.py`（只读脚本）与同名 `.json`（脚本 stdout）
- 工作区核对（成稿时实测 `git status --short`）：5 项 —— `M tests/test_free_float_fill.py`、`M tools/fill_free_float_shares.py`、`?? docs/evidence/cn_bond_index_member_probe_20261112/`、`?? docs/evidence/cn_bond_yield_tenor_probe_20261112/`、`?? docs/evidence/fund_f10_listdate_probe_20261112/`，**均为他路在途，非本片产出**；本片未新增/修改任何被 Git 跟踪的文件（`2026-11-12/**` 已 gitignore，5 个脚本与 5 个 JSON 不进入跟踪）

## 0. 本片取表清单、同片理由与排除清单

### 0.1 取了哪 5 张

按任务要求**从 B2 清单末尾倒序取 5 张**，逐张先比对 `b2_slice1_check.md`（该文件于 2026-11-13 00:08:02 生成，已读取）的取表声明后落定：

| 序 | B2 清单位次 | lineage 表 id | 族别 | 正式路径 | 行数 |
|---|---|---|---|---|---|
| 1 | 第 10 | `futures_multi_asset` | 期货合约/连续/移仓 | `data/local_store/meta/multi_asset/{futures_contracts,futures_continuous,futures_roll_events}.parquet` | 997 / 25,190 / 502 |
| 2 | 第 9 | `etf_equity_pool` | ETF 权益池（CSV） | `data/local_store/meta/pools/etf_equity_pool.csv` | 2,153 |
| 3 | 第 8 | `membership_intervals` | 指数成分区间 | `data/local_store/meta/index_members/membership_intervals.parquet` | 7,935 |
| 4 | 第 7 | `breadth_daily` | 市场宽度日表 | `data/local_store/meta/breadth/breadth_daily.parquet` | 8,824 |
| 5 | 第 6 | `fund_adj_factor` | 基金复权因子 | `data/local_store/factors/fund_adj_factor.parquet` | 2,974,925 |

### 0.2 为什么这 5 张同片

1. **同为 depth 1，且彼此 0 条派生边**：`breadth_daily`、`fund_adj_factor`、`membership_intervals`、`etf_equity_pool`、`futures_multi_asset` 在 `derivation_edges` 中两两之间没有边，`compute_depth.py` 的「同深度边 = 0」自证成立，故可同批并行、互不影响结论。
2. **上游全部是已收口或已独立的 B1 表**：`quotes_futures`、`quotes_fund`、`fund_dividend`、`fund_share_change`、`index_weight`、`quotes_stock` —— 不含 `stock_shares`、`st_intervals`、`stock_basic` 这三张仍在体检/修复窗口的上游（闸门 2 全体满足）。
3. **都是「一次派生」而非二次派生**：每张都能用 B1 层单一/少数上游逐值复算，符合 B2 的验收口径（除权残差 / 逐日对拍 / 结构重推）。
4. **落点连贯**：5 张分别在 `meta/multi_asset/`、`meta/pools/`、`meta/index_members/`、`meta/breadth/`、`factors/`，覆盖「目录多语义」「CSV 型正式表」「契约 coverage.json」「聚合数值表」「大表因子」五种形态，正好把 B1 七片没遇到的表形态补齐。

### 0.3 排除了哪些表（撞表 + 闸门 + 越界）

| 表 | 归属 | 排除原因 |
|---|---|---|
| `universe_price_jumps` | B2 / depth 1 | **撞表**：`b2_slice1_check.md` §0 已声明由第一片全量五检 → 跳过，不重复体检 |
| `anomalies` | B2 / depth 1 | **撞表**：同上（第一片含 `placeholder_codes` 缺失与 `coverage_gaps` 退役核账） |
| `implied_suspension` | B2 / depth 1 | **撞表**：同上 |
| `suspension_daily` | B2 / depth 1 | **撞表**：同上 |
| `stock_turnover` | B2 / depth 1（清单末位） | **闸门 2**：上游 `stock_shares.float_share`（G-077）仍在修复窗口，`governance_order.md` 第 112 行写「B1 的 G-077 未收口前不重算」；且 `data/local_store/factors/stock_turnover.parquet` mtime 2026-11-12 22:25:29、`_staging/turnover_audit_20261112/` 23:18 仍在动，属在检状态 → 挪到下一片。此项与第一片的排除理由一致 |
| `stock_adj_factor` | B2 / depth 1 | `governance_order.md` 第 114 行明确「三只标的留给在查任务，本批只标记状态不重复判定」，与在查任务同表不同片会产出两套状态 |
| `daily_index` / `ann_date_map` | B2 / depth 1 | 上游口径仍在其他路径处理（G-045 跟踪中），第一片也据此排除 |
| `name_history` | B2 / depth 1 | 上游 `st_raw` 无常规写入口、无 manifest（G-095），仍在体检中 |
| `listing_intervals` | B2 / depth 1 | 属日历/名录链，链内需与 `daily_index`、`name_history` 串行；本片不取 |
| B3 及以后的全部表（`etf_pool`、`fund_corp_actions`、`breadth_ma_daily`、`st_price_inferred`、`st_intervals`、`st_coverage`、`supplement_intervals`、`universe_exclusions` 等） | B3+ | **越界核对：一张未取**。本片 5 张全部落在 `governance_order.md` 第 96 行 B2 清单内 |

> 边界纪律执行情况：开工前读 `b2_slice1_check.md`（当时已生成），逐条比对其 §0 的 4 张取表；`stock_turnover` 因闸门 2 与第一片同判而一并挪后；剩余 5 张与第一片零重叠。

---

## 1. 结论先行

| # | 表 | 五检总体 | 最重的一条 |
|---|---|---|---|
| 1 | `futures_multi_asset` | ②③④ 通过（对拍命中 100%），① 通过，⑤ 有 3 处不符 | **血缘边方向反了**：`quotes_futures → futures_multi_asset` 实际是「同批兄弟产物」，真正的上游是 `_staging` 的 `daily/*.parquet` + `contracts_*.csv`（未登记） |
| 2 | `etf_equity_pool` | ①③④ 通过（复算 0 差异），② 基本通过（扁平 manifest），⑤ 有 2 处错记 | **存在仓库外第二写入口** `tmp/from_www_tmp_20261109/etf_pool_build.py:155-163`，`write_csv` 直写同一路径、非原子（DG-9） |
| 3 | `membership_intervals` | ②③④ 通过（结构重推 0 差异、官方快照 20/20 一致），① 登记错，⑤ 6 处 | **生产/消费 `out_date` 开闭区间不一致**：279 个 (指数,日期) 在 `out_date` 当日多纳入成分，平均 21.27 只、最多 100 只 |
| 4 | `breadth_daily` | ①④⑤ 基本通过，② 通过，**③ 不通过** | 按现行代码重算，共同日期 8,823 天里 `total_count` 6,819 天不等、`up_count` 5,801 天不等（最大差 45），差额集中在 ≤2019；下次 `build_full` 会改写这 6,819 行 |
| 5 | `fund_adj_factor` | ② 通过（0 不一致），① 通过（两写入口算法同源），③ **分级码历史段大面积不符**，④ 属同步镜像，⑤ 行数/只数全对但消费者漏 7 处 | **G-019「2,619 只 mismatch 0；行情有而因子无 0 行」与实测冲突**：实测 2,241 只行数不等、`561970.SH` 有行情零因子；且 175 个除权事件残差 >1e-2 |

**跨表共性缺陷（5/5 全中）**：`lineage.json` 的 `writers_detected` 对这批表普遍存在**假阳性**（docstring 行、写别的表的行、写 staging 候选的行被记成写正式表），且普遍**漏边**（代码读了未登记的上游）。这不是单表问题，是血缘图生成器 `build_lineage.py` 的判定口径问题，建议单列一条。

**另一条跨批结论**：`governance_order.md` 的 **P0 已完成**——`834d06d`（2026-11-12）已把 `tools/build_stock_basic.py` 的交易日历改取 `trading_calendar`，`breadth_daily → stock_basic` 反向边实测已不存在（`git grep breadth tools/build_stock_basic.py` 仅剩第 212 行 docstring 的历史说明）。`governance_order.md` 第 33-46 行的 P0 段与第 17 行的「唯一拓扑例外」均属过期描述，B3 的硬前置条件已解除。

---

## 2. `futures_multi_asset`

### 2.1 事实基线

| 表 | 行 | 字节 | SHA256(16) | mtime | 列 | 日期范围 |
|---|---|---|---|---|---|---|
| `futures_contracts` | 997 | 26,883 | `0c5d0e1a1af4efe6` | 2026-11-10 23:16:32 | 15 | list 2010-04-16~2026-11-02 / delist 2010-05-21~2029-08-31 |
| `futures_continuous` | 25,190 | 565,783 | `0d0798ad30bee526` | 2026-11-11 06:36:21 | 13 | 2010-04-16~2026-11-10 |
| `futures_roll_events` | 502 | 22,661 | `5203fb9e6231acbf` | 2026-11-10 23:16:32 | 11 | 2010-05-14~2026-11-06 |

10 品种（AG/IC/IF/IH/IM/SC/T/TF/TL/TS）；`ret` 空 10（每品种首行）、`settle_raw` 空 0、**`multiplier` 空 301**（SHFE 182 + INE 119 全空，上游同为空）。

### 2.2 ① 写入口

| 位置 | 性质 | 写入 | 方式 | 判定 |
|---|---|---|---|---|
| `tools/build_futures_continuous.py:305/306/307` | routine | 三张表各一次 | `write_parquet_atomic` 整表覆盖 | **唯一常规入口**；`--apply` 之外全流程 dry-run（`:290` 直接 return） |
| `_staging/multi_asset_tier2_20261110/apply_settle_g058.py:184` | oneoff | 仅 `futures_continuous` | 原子写 | G-058 补 `settle` 的补丁，已被常规入口吸收 |
| `tools/build_multi_asset_etf_pool.py:532/:560` | routine | `etf_pool.{csv,parquet}` | — | **不写本表** |
| `tools/fetch_etf_fee.py:593` | routine | `etf_fee` | — | **不写本表**；`:593` 是 docstring |

`lineage.json` 记 8 条 writers，实测 **4 条假阳性**（`:29` docstring 用法行、`etf_pool` 两处、`etf_fee` docstring），另**漏记** `:354` 的 manifest 写点。

### 2.3 ② manifest

`futures_manifest.json`（7,718 B）覆盖 **3/3** 表，rows / bytes / sha256 全等；「有记录无文件」0、「有文件无记录」0；`manifest_tool --check --only meta/multi_asset` → **PASS**（3 份 / 11 记录 / 不一致 0）。`coverage` 10 品种的 rows/first/last 与磁盘 0 差异，`roll_stat` 合计 502 与磁盘一致。
缺项：`files[]` 无 `columns`；manifest 自带 `built_at` 2026-11-10T23:16 却记录 11-11 之后的 sha（`files` 块被事后重算，重算者未定位）。
→ **`lineage.json` 的 `known_defects: "三张表无 manifest"` 已过期**。

### 2.4 ③ 对拍（命中 100%）

上游：`quotes/futures/{cffex,shfe,ine}/**` 997 个文件。

| 对拍 | 比对列 | 结果 |
|---|---|---|
| 合约集合 | `ts_code` | 双向差 **0**（997/997） |
| 元数据 | `list_date` / `delist_date` / `last_ddate` | 997/997 逐值相等（对 `_staging/.../contracts_*.csv`） |
| 归属 | `exchange` vs 目录名、`d_month` vs 代码数字位、品种前缀 | 各 997/997 |
| 连续表 | `close_raw` / `settle_raw` / `volume` / `oi` | 各 25,190/25,190 与 quotes 相等，未匹配 0 |
| 连续表复权链 | `close_adj` 链式复算 | 最大绝对差 **0.0** |
| 换月事件 | 1,004 个腿位全命中 quotes；`to_close` / `from_close_same_day` / `from_close_prev_day` / `from_oi` | 各 502/502；`return_pct`、`roll_gap_pct` 复算最大差 **0.0 / 0.0** |

可重建性：调模块函数重跑，cont / rolls 与正式表 `equals=True`；contracts 逐值全等但行序不同（`:82` 的 `.unique(keep='first')` 未带 `maintain_order`）。
G-058 现状：**逐合约层仍 21 行 `settle` 空**（IC/IF/IH 1901+1902、T1906/T1909、TF1909、TS1906/TS1909，均在 2019-01-18 / 02-13 / 02-15）；连续层 0。这 21 个 (合约,日期) 从未成为主力，不影响 continuous。

### 2.5 ④ 第二份

扫 `data/local_store/**` 共 10,868 parquet + 23 CSV，同语义命中仅本表 3 文件；`data/local_store/_backup` 不存在；仓库根 `_backup/futures_settle_g058_20261111/` 是回滚快照，非数据源。**0 条 DG-1 违规**。

### 2.6 ⑤ 登记 vs 实测

| 位置 | 现写什么 | 实测什么 | 判定 |
|---|---|---|---|
| 清单 L46 消费者 | `tools/build_futures_continuous.py` | 该脚本只写不读；全仓 0 读取点（仅字面量） | ❌ 中 |
| 清单 L44 | 「暂无代码消费者」 | `verify_futures_official.py:41-44` 实读 continuous | ❌ 中 |
| 清单 L43 定位 | 读在 `build_futures_continuous.py:49` | `:49` 是 `QUOTES_DIR` 常量，读在 `:95/:98` | ❌ 中 |
| 清单 L46 描述 | 「997 行：乘数…唯一依据」 | `multiplier` 空 301/997（30.2% 无法规模换算）；`per_unit` 997/997 有值却未被使用 | ⚠ 中 |

### 2.7 派生边对账

| 血缘声明 | 代码实测 | 判定 |
|---|---|---|
| `quotes_futures → futures_multi_asset`（evidence「常规」） | 代码**不读** quotes 层；读 `_staging/multi_asset_tier2_20261110/daily/{ex}/*.parquet` 的 `close/settle/vol/oi/trade_date`（`:47/:95/:98`）。`quotes/futures` 是同脚本 `:297-302` 的**写出目标** | **不一致 (a)：方向反了，二者是同批兄弟产物** |
| （缺）`contracts_{ex}.csv → futures_contracts` | 全部 15 列来自 `load_contracts`（`:71-88`） | **漏边 (b)** |
| （缺）`contracts.csv` 的 `fut_code/exchange/delist_date` → continuous 的 `variety/asset_class/exchange/contract_delist_date` | `:145-152`、`:164-177` 连接派生 | **漏边 (b)** |

### 2.8 缺陷

| # | 位置 | 现写什么 | 实测什么 | 严重度 |
|---|---|---|---|---|
| F-D1 | `lineage.json` edges | `quotes_futures → futures_multi_asset` | 代码读 staging daily；quotes 是同脚本写出目标 | 高 |
| F-D2 | `lineage.json` edges 缺边 | 只有 1 条入边 | 合约表全部来自 staging CSV；连续表品种/交易所/交割日来自 CSV 连接 | 高 |
| F-D3 | `lineage.json` writers | 8 条 | 真 3+1，假 4，漏 `:354` manifest 写点 | 中 |
| F-D4 | `lineage.json` 表记录 | rows=518 / files=null / 「三张表无 manifest」 | 26,689 行 / 3 文件 / manifest 3/3 一致 | 中 |
| F-D5 | `build_futures_continuous.py:354`、`:302` | `write_text` / `write_bytes` | 非原子写（DG-9） | 低 |
| F-D6 | `apply_settle_g058.py:203` | 快照落 `data/local_store/_backup/` | 实际在仓库根 `_backup/`；重跑会在 `data/` 内再造 `_backup`（DG-11） | 低 |
| F-D7 | `meta/multi_asset/` | 3 套语义 3 份 manifest | futures 未按 DG-3 命名 `manifest.json`；同目录 `manifest.json` 今日 00:12 仍被他路改写 | 低 |
| F-D8 | `governance_order.md:103` | 列在「基金链」行并注「独立」 | 内容纯期货，0 条基金/ETF 边 | 低 |
| F-D9 | DG-32 入层第①问 | — | `contracts` / `roll_events` 实测 **0 读取点**（只写） | 低 |

---

## 3. `etf_equity_pool`

### 3.1 事实基线

2,153 行（物理 2,154 行含表头）/ 329,929 B / SHA256 `e19ac3a2ae81a508` / mtime 2026-11-12 13:37:44 / 无 BOM、非 CRLF / 11 列 / 空值 0 / 1,338 只代码 / `first_date` 2005-02-23~2026-11-02 / `last_date` 全为 2026-11-04。
列：`tier, ts_code, name, list_date, rows, first_date, last_date, median_adv_120d, event_count, fake_bar_hits, in_pool_reason`
档位：`full` 1,338 / `std3y` 677 / `pool` 138，且 `pool ⊂ std3y ⊂ full` 成立；同代码跨档 11 列 0 处不一致。

### 3.2 ① 写入口

| 位置 | 性质 | 方式 | 判定 |
|---|---|---|---|
| `tools/build_etf_equity_pool.py`：`--apply`（:340-347）→ `_write_csv`（:236-241） | routine | `write_text_atomic`（DG-9 合规）；覆盖前 `copy2` 快照到 `_backup/meta/pools/etf_equity_pool_20261112/`（:341-345，实测存在，SHA `d2379430…`）；:244-259 有 DG-6 断言 | **唯一常规入口**；无参数实跑 `build_rows(STORE, 2026-11-04)` 得 2,153 行、与磁盘 **0 个单元格差异** → 非一次性脚本 |
| `tmp/from_www_tmp_20261109/etf_pool_build.py:155-163` | oneoff（未跟踪，`E:\www\tmp` 镜像） | `out.write_csv(POOL)` **直写、非原子** | **第二写入口**；其 `event_count` 不带 `ex_date ≤ as_of` 过滤、pool 档 reason 追加「含N根假bar」，当前表 0 行含该后缀 → 现行文件由仓库脚本产出 |
| `tools/build_multi_asset_etf_pool.py` | routine | `meta/multi_asset/etf_pool.{parquet,csv}` | **不写本路径** |

全仓无调度/编排调用；`logs/data_changelog.csv` 仅 1 条 `2026-11-05T14:10:33 create meta/pools/etf_equity_pool_20261104.csv`（task=`unattributed`），11-12 正名+重出 **0 条记录**。

### 3.3 ② manifest

`meta/pools/manifest.json`（8,015 B）的 `rows / bytes / sha256 / columns / tier_counts / ts_code_unique` **6/6 全一致**；无 `files` 数组（扁平结构）→ `manifest_tool --check --only meta/pools` 输出 PASS，但把 CSV 记为 1 条「不可核对」（机检通道不覆盖 CSV 行数）。双向差 0。
上游指纹漂移 2 处：`names.json` 磁盘 1,352,524 B / 11-12 16:11 vs 登记 1,352,109 B / 11-11；`quotes/fund` 登记「最新 159941.SZ」实际已到 `163503.SZ`（11-12 19:18）。
行数守恒核法（CSV 无 parquet metadata）：物理行 − 表头 = 2,153 = manifest `rows`，再用 `--check` 逐列复算。

### 3.4 ③ 对拍

| 对拍 | 比对列 | 结果 |
|---|---|---|
| 行情存在性 | 池内代码 vs `quotes/fund/**` | 1,338/1,338 全部有文件，缺 **0** |
| 覆盖区间 | `first_date` / `last_date` / `rows` | 逐只 **各 0 不符**（as_of = 2026-11-04） |
| 全表复算 | 11 列 | **0 差异**（人工成分 0 行） |
| 权益域 | 名称含 ETF 且非「联接」的 1,639 只 | 分类 equity 1,341 / cross 222 / bond 65 / gold 11；1,341 只权益中 3 只未入池，**全部因首行晚于 as_of**（158076.SZ 11-06、158081.SZ 11-05、561970.SH 11-11） |
| 伪 bar 折算 | `fake_bar_hits` | `fund_fake_bar_bars.csv` 14 行 / 13 只，池内 9 只逐只相符（mismatch 0）；另 4 只（159331/159529/159788/513600）为跨境 ETF，归属闭合 |
| 事件计数 | `event_count` | `fund_dividend` 3,413 行（≤as_of 3,405）+ `fund_share_change` 2,738 行（≤as_of 2,737）逐只一致 |

容差：CSV 全列零容差（整数计数 + 字符串 + 浮点中位数按 repr 相等）；命中率 100%。

### 3.5 ④ 第二份

| 候选 | 规模 | 判定 |
|---|---|---|
| `meta/multi_asset/etf_pool.parquet` / `.csv` | 795 行 × 48 列 | 列交集仅 8/11（本表独有 `median_adv_120d/event_count/in_pool_reason`，对方独有 39 列覆盖度指标）；代码交集 **424**、本表独有 914、对方独有 371（交集 31.7%）；域与档位口径不同（名称域+3 年+120 日中位 ≥1e8 vs `fund_basic.fund_type='ETF'` 含退市+60 日 ≥2e7/1e8）→ **不同语义，不构成 DG-1**（准入分层 vs 数据就绪度诊断，已分别登记 L35 / L34） |
| `meta/universe/layers/l5_pool.parquet` | `date/ts_code/in_layer` | 无 tier，非同语义 |

### 3.6 ⑤ 登记 vs 实测

| 位置 | 现写什么 | 实测什么 | 判定 |
|---|---|---|---|
| L35 主体 | 唯一路径、正名、`--check` 2,153 行 | 属实 | ✅ |
| L35 消费者列 | 把 `tmp/.../etf_pool_build.py` 列为消费者 | 它是**产出方**（只写不读） | ❌ 中 |
| L35 明细 | 列 5 只 / 8 个单元格 | 实为 6 只 / 9 个单元格（漏 `560130.SH` 1→0） | ⚠ 低 |
| L35 锚点 | `#L91` | 实际 L92 | ⚠ 低 |
| L89 | 伪 bar 清单「15 行」 | 14 条数据行 + 1 表头 | ⚠ 低 |

### 3.7 派生边对账

| upstream → 本表 | 读的列/层 | 代码位置 | 判定 |
|---|---|---|---|
| `quotes_fund` | `date`、`amount`，过滤 `date ≤ as_of`，尾 120 根中位数 | `build_etf_equity_pool.py:179-204` | ✅ 一致 |
| `fund_share_change` | `ts_code` + `ex_date`，仅 `ex_date ≤ as_of` 的行数 | `:127-138` | ✅ 一致（血缘未标过滤层） |
| `fund_dividend` | 同上 | `:127-138` | ✅ 一致 |
| （缺）`meta/names.json` | 名称（域名过滤 + 分类） | `:167-168, 181-182, 197` | **漏边** |
| （缺）`meta/fund_basic.parquet` | `name`（输出列优先取此） | `:170-173, 196` | **漏边** |
| （缺）`meta/anomalies/fund_fake_bar_bars.csv` | `ts_code` + `trade_date ≤ as_of` 计数 | `:141-151, 203` | **漏边** |
| `→ universe_layers` | `build_universe_layers.py:92` 仅入 `UPSTREAM_FILES`，`:400-407` 只记 exists/mtime/size | 不读任何列、不参与 L5 成员计算 | **出边性质被误述**（`known_defects` 说「被 L5 读取」不成立） |

### 3.8 缺陷

| # | 位置 | 现写什么 | 实测什么 | 严重度 |
|---|---|---|---|---|
| E-D1 | `tmp/from_www_tmp_20261109/etf_pool_build.py:155-163` | 未登记 | 仓库外第二写入口，`write_csv` 直写非原子；`event_count` 口径不同 | 高 |
| E-D2 | `lineage.json` edges | 3 条入边 | 另读 `names.json` / `fund_basic` / `fund_fake_bar_bars.csv` | 高 |
| E-D3 | `lineage.json` table.path | `meta/pools/etf_equity_pool*.csv` | 目录内仅 1 个 csv，无变体 | 中 |
| E-D4 | `lineage.json` writers | `writer_primary` 带「（一次性?）」、`writers_detected: []` | 常规入口，`--apply/--check` 齐备，复算 0 差异 | 中 |
| E-D5 | `lineage.json` known_defects | 「被 universe layers L5 读取」 | 只作上游指纹，不读列 | 中 |
| E-D6 | `meta/pools/manifest.json` | 扁平结构，无 `files` | `--check` 记「不可核对 1」 | 中 |
| E-D7 | `build_etf_equity_pool.py:316-321` | 缺省 as_of 取 510300.SH 末根 bar | = 2026-11-11；直接 `--apply` 得 2,158 行（full 1,341 / std3y 679），基线被静默替换且 manifest 不更新 | 中 |
| E-D8 | `logs/data_changelog.csv` | 仅 11-05 旧名 create 1 条 | 11-12 正名+重出 0 条，追溯断链 | 中 |
| E-D9 | 同上 `:269` / `:224,:226` | 快照目录用 `date.today()`；`list_date` 独立列 | 同日重跑覆盖同名快照；2,153 行 `list_date == first_date` 100% 相等，实为本地行情首行而非上市日 | 低 |
| E-D10 | `build_universe_layers.py:15` | `etf_equity_pool_<日期>.csv` | 已正名去日期 | 低 |

---

## 4. `membership_intervals`

### 4.1 事实基线

7,935 行 / 69,072 B / SHA256 `a65a70130bde0f36` / mtime 2026-11-12 16:54:47 / 8 列（`interval_id, index_code, ts_code, in_date, out_date, in_date_known, out_date_known, source`）/ 6 指数（000016、000300、000688、000852、000905、399006）/ 3,643 只 / `in_date` 2005-04-29~2026-10-30 / `out_date` 2005-07-29~2026-10-30。
`out_date` 空 **2,000**（25.2%，恰等于 6 指数当前成分数之和 50+300+50+1000+500+100=2,000）；`in_date_known=false` 1,986、`out_date_known=false` 2,000（与空值一一对应）；`source` 全为 `tushare.index_weight`；主键重复 0；`out_date ≤ in_date` 0 行；同 (指数,股票) 重叠区间 0 条（1,441 对为多段再纳入，最多 6 段）。

### 4.2 ① 写入口

| 位置 | 性质 | 方式 | 判定 |
|---|---|---|---|
| `src/data/quality/extended_builders.py:629` `_write_frame(...index_membership...)` → `:121` `write_parquet_atomic` | routine | 原子写、整表覆盖；`:630-654` 同笔写 `coverage.json` | **唯一常规入口** = `python -m src.data.quality.extended_builders membership --commit`（`:693-695`） |
| `tools/rebuild_index_constituents.py:854` | routine | 写 `meta/index_members/rebuild/intervals.parquet` | **不写权威路径**（`:13-15` 明示），且该目录当前不存在 |
| `tools/pull_index_weight.py:172-204` | routine | 只读本表算 sha256/行数并写 manifest 块 | 只读 |

### 4.3 ② manifest

`manifest.json`（4,098 B）的 `membership_intervals` 块与实测 8/8 全等（rows / codes / cols / columns / bytes / sha256 / in_date_range / out_date_range）；manifest mtime 14:59:46 早于表 16:54:47，但 sha 相同 ⇒ 16:54 的重建字节一致，不构成失效。
`coverage.json`（1,226 B）的 `start/end/periods/intervals/open_intervals/left_censored_intervals/indices` 与实测**全等，diffs 为空**。
`manifest_tool --check --only meta/index_members` → **PASS**（2 条 / 不一致 0 / 无法定位 0）。

### 4.4 ③ 对拍（三条，全部通过）

| 对拍 | 口径 | 结果 |
|---|---|---|
| 结构重推 | 用 `_load_weight_panels` + `_intervals_for_index` 从上游 375,727 行 / 259 期 / 6 指数重推 | 7,935 行，`frame_equal=true`，diff_rows **0**，skipped 0 |
| 逐期还原 | 1,126 个 (指数,期) 的区间还原成成分集合 vs 上游权重面板 | 不一致 **0** |
| 独立对拍（非循环论证） | 官方 csindex 日快照 20 份（4 指数 × 11-05/06/09/10/11） | 成分集合 **20/20 完全一致**（000300 各日 300、000852 各日 1,000） |

代码有效性：3,606/3,643 只在 `quotes/stock/**` 有文件，**37 只缺失**（SH 24 / SZ 13），含非股票码 `990018.SH`（上游 000300 有 18 期权重，2005-04-29~2006-10-31，本表如实收录 1 段）。
上游缺口继承：3 期低于名义规模（000300 2009-12-31 = 298；399006 2010-06-30 = 86、2010-07-30 = 93），实测上游面板本身即这些数 → 归因为上游覆盖不足，非本表错。

### 4.5 ④ 第二份

`meta/index_membership/` 目录实测**不存在**（目录与文件均 false）；`_backup/meta/legacy_tables_20261110/meta/index_membership/membership_intervals.parquet` 在（13,936 B / 1,850 行 / `out_date` 空 **1,850 = 100%**），SHA256 `031292238b71666616a667fed4539c057afb51d24722a0fea88f92f4cbc7e2ad` 与清单 L165 **逐字符一致**；`sina_in_dates.parquet` 不存在。
全库 `meta/**` + `factors/**` 84 个 parquet 按 schema `{index_code, ts_code, in_date}` 扫描 → 命中 **1** 个（即本表）。**无第二份，DG-1 合规**。

### 4.6 ⑤ 登记 vs 实测

| 位置 | 现写什么 | 实测什么 | 判定 |
|---|---|---|---|
| 清单 L24 | 「覆盖度见 `manifest.json` 的 `coverage` 键」 | 该键是 `index_weight_monthly` 的 expected_size/periods/rows；membership 的覆盖声明是 `coverage.json`（L99 自述）→ 同一文档两处口径冲突 | ❌ 中 |
| 清单 L24 | 链接 `extended_contracts.py#L90-L96` | 该区间属 `industry_pit` 契约尾部，index_membership 契约在 `:100-115` | ⚠ 低 |
| 清单 L58 / L261 | 快照「16 个文件 / 4 指数 × 11-05/06/09/10 / 7,400 行」 | `_staging/index_membership_snapshots_20261112/` 实测 **20 个文件 / 9,250 行 / 5 个日期**（增 11-11） | ⚠ 低 |

### 4.7 派生边对账

| upstream → 本表 | 读的列/层 | 代码位置 | 判定 |
|---|---|---|---|
| `index_weight` | `index_weight_monthly.parquet` 的 `index_code` / `ts_code` / `trade_date` 三列（**`weight` 不读**）；`index_code` 去后缀 `_normalize_index_code` | `:450-451`、`:588`、`:439-442` | ✅ 一致 |
| `index_basic` | **全程不碰** | 无 | **不一致：虚边**（血缘图多记一条依赖） |
| （缺）`quotes_stock` | 不读、无代码有效性 join | — | 无漏边，但**缺校验**（37 只无行情仍被当成分） |
| （缺）`daily_index` / `trading_calendar` | 不读（`trade_date` 已是月末最后交易日） | — | 无漏边 |

### 4.8 缺陷

| # | 位置 | 现写什么 | 实测什么 | 严重度 |
|---|---|---|---|---|
| M-D1 | `lineage.json` `writer_primary` / `writers_detected[0]` | 「指数成分重建脚本（`tools/rebuild_index_constituents.py` 一类）」；writers 列 `extended_builders.py:25` | 该脚本 `:13-15` 明示不写权威路径；真写盘在 `extended_builders.py:629`；`:25` 是模块 docstring 的 bullet | 高 |
| M-D2 | `extended_builders.py:493-495` vs `run_l1.py:147`、`run_domain.py:97` | 生产侧 `out_date` = 「首个不在册期」（开区间）；消费侧写 `out_date >= d`（闭区间） | 5,935 段在 `out_date` 当日面板**全部已不在册**（0/5,935 反例）→ 当日多纳入：**279 个 (指数,日期) 平均 21.27 只、最多 100 只**（000852 多次调样日） | 中高 |
| M-D3 | 本表 `ts_code` 取值面 | 3,643 只视作有效股票 | 37 只无 `quotes` 文件（92.9% 命中），`listing_intervals` / `universe_exclusions` 覆盖 **0/37** | 中 |
| M-D4 | `pull_index_weight.py:187` → manifest `produced_by` | `tools/rebuild_index_constituents.py` | 同 M-D1；另 manifest 顶层 `generated_at=2026-11-09` 早于块 `registered_at=2026-11-12` | 中 |
| M-D5 | `coverage.json` | `complete: true`，无缺口声明 | 3 期成分数低于名义规模（上游本身如此） | 低 |
| M-D6 | 清单 L24 / L24 锚点 / L58 / L261 | 见 §4.6 | 见 §4.6 | 低 |

---

## 5. `breadth_daily`

### 5.1 事实基线

8,824 行 / 135,009 B / SHA256 `322571b2c46f7ab0` / mtime 2026-11-12 20:59:26 / 10 列（`trade_date` Date + 9 列 Int64：`up_count, down_count, flat_count, total_count, limit_up_count, limit_down_count, new_high_250, new_low_250, adl`）/ `trade_date` 1990-12-19~2026-11-11 / 空值仅 `limit_up_count` 1、`limit_down_count` 1（均为 1990-12-19 首日无昨收）/ 日期无重复升序 / `adl == cumsum(up−down)` 恒等（max|Δ| = 0）/ **周末行 63** / `total_count == 0` 行 0。

> 以上为**体检时（重建前）**的基线。2026-11-13 00:27 已按 `build_full` 重建，现为 **8,823 行 / 136,200 B / SHA256 `20869903f724bbf5`**，逐项验收见 §11。

### 5.2 ① 写入口

| 位置 | 性质 | 方式 | 判定 |
|---|---|---|---|
| `src/data/services/breadth_service.py:353` `_write_parquet_atomic(frame, path)`（`:117-118` → `write_parquet_atomic`） | routine | 原子写、**整表覆盖**（`build_full` 与 `refresh` 共用）；同一函数 `:365-366` 写 `coverage.json` | **唯一常规入口**（惰性触发：`indicator_service.py:47`、`indicator_renderer.py:119`、`parallel_ops.py:37`；`regime.py:95-98` 直读 parquet） |
| `_staging/derived_1993b_20261113/layer3_breadth_daily_apply.py:203` | oneoff | 原子写 + `:214` 写 coverage | 其 `sha256_after` = 现表 SHA，证明**现表 = 当时 apply 后的 patched**，未被后续改写 |
| `tools/build_breadth_ma.py:584` | routine | `target = breadth_ma_daily.parquet`（`:116-117`） | **不写本表**（该文件只在本表 `:471` 读） |
| `_staging/.../layer3_prepare.py:64` | oneoff | 写 `breadth_daily_candidate.parquet` | **不写正式路径** |

补充：`python -m src.data.local_store build` **不含 breadth 步骤**（`build.py` 全文 0 处 breadth）。`refresh` 只重算 `trade_date > last−2天`，`<= cutoff` 原样保留 → 上游漂移被冻结在历史段（这正是 §5.4 差额的成因机制）。

### 5.3 ② manifest / coverage

`manifest.json`（5,927 B）顶层 sha 描述的是 `breadth_ma_daily`；本表以 `sibling_tables.files[0]` 记账：rows 8,824 ✓、cols 10 ✓、columns ✓、bytes 135,009 ✓、sha256 ✓、date_range ✓、coverage 块 5 字段与 `coverage.json` 全等 ✓；`manifest_tool --check --only meta/breadth` → 3 条 / 0 不一致 / **PASS**。
`coverage.json`（454 B）：rows / first / last / version 与磁盘一致 ✓，但 **无 `sha256` 键**（keys = `built_at, first/last_trade_date, note, patch_note, rolling_window, rows, version`）→ 不是「SHA 不等」，是**契约承诺的 SHA 根本不存在**。`built_at` 硬编码 `2026-11-13T00:00:00`（apply:211），晚于文件 mtime 约 3 小时。

### 5.4 ③ 对拍（**不通过**）

上游：`quotes/stock/{sh,sz,bj}/**` 5,832 个文件（剔除 superseded 2 只后 5,830 参与）。

| 对拍 | 结果 |
|---|---|
| 日期集合 | 行数相等（8,824）但**集合不等**：表有行情无 `1992-04-18`（周六）；行情有表内无 `1991-08-10`（周六） |
| 逐值重算（按 `_daily_from_quotes` 口径复刻，仅用原始 `close/pre_close/volume`；`close*qfq_factor` 只用于 250 日新高新低；ST 区间来自 `st_history` 判涨跌停幅度；无 ST 剔除、无上市未满 N 日剔除、无 `stock_basic` 过滤） | 共同日期 8,823 天：`up_count` 5,801 天不等（最大差 12，Σ\|Δ\| = 23,315）、`down_count` 5,572（13）、`flat_count` 1,381（4）、`total_count` **6,819（13，Σ\|Δ\| = 47,902）**、`limit_up` 2,965（**45**）、`limit_down` 2,001（40）、`new_high_250` 792（10）、`new_low_250` 1,016（8）→ 零容差命中率 `up` 34.2%、`total` 22.7% |

抽样（表 / 重算 / 绝对差）：`1993-05-04` 68/69/1；`1995-06-30` 261/266/5；`2007-05-30` 1,272/1,281/9（`limit_down` 895/**923**，Δ28）；`2015-06-15` 2,304/2,311/7；`2026-11-11` 5,574/5,574/0。

差额归因（**不判本表错值，只归因**）：
1. 年代分布：`total` 1990s 2,204 / 2000s 2,415 / 2010s 2,191 / **2020s 仅 11**（约 1,650 日），末次变动 2026-11-06、11-10。
2. 上游在 2026-11-12 当日被其他任务改写三次（13:09 坏 bar 剔除、20:00 `pre_close` 修补、20:29 1992 板块重排），而本表只吸收 17 只标的的「外科补丁」（7 个日期 + 删 2 行）。
3. 两套独立实现互证：本片重算与 `_staging/derived_1993b_20261113/layer3_breadth_daily_compare.json` 的残差逐列仅差 0~1 行（limit 两列 +1，来自 1990-12-19 的 null 口径）→ 重算口径可信，差额不来自复刻误差。
4. **后果**：下一次 `build_full` 会一次性改写这 6,819 行 `total_count`，属「上游已变、下游未跟」的冻结段，不是本表算法错。

容差：整数计数，零容差。

### 5.5 ④ 第二份

`data/local_store` 全树 parquet schema 扫描，同时含 `up_count/down_count/total_count` 的**只有本表 1 个**；`breadth_ma_daily.parquet`（8,654 行，列 `date/sample_size/above_ma20…`，无涨跌家数）是均线派生、非同语义；MySQL `market_breadth`（`stock.py:143 _save_market_breadth`）只写库、无本地落盘。**无第二份**。

### 5.6 ⑤ 登记 vs 实测

| 行 | 现写什么 | 实测什么 | 判定 |
|---|---|---|---|
| L28 锚点 | `#L340-L360` `refresh` | `refresh` 实为 380-406；340-360 是 `build_full` / `_write_snapshot` | ⚠ 低 |
| L28 消费者 | 只写 `refresh`、`src/advice/*` | 主消费方还有 `indicator_service:44-47`、`indicator_renderer:117-119`、`parallel_ops:34-37` | ⚠ 低 |
| L97 | 唯一「覆盖区间 + 口径 + **SHA**」声明 | `coverage.json` 无 `sha256` 键 | ❌ 中 |
| L97 | `build_breadth_ma.py:483` 读状态文件 | 483 是 `'state_file'` 字符串，真读在 490-491 | ⚠ 低 |
| L97 | 覆盖边界只认本文件、不从 min/max 反推 | coverage 1990-12-19 / 2026-11-11 与数据 min/max 完全相同，**无冲突** | ✅ 一致 |
| L138 | `python -m src.data.local_store build` 收口里的 `refresh`（`#L355`） | `build.py` 无 breadth 步骤；355 非写盘行（写盘 353、refresh 380） | ❌ 中 |

### 5.7 派生边对账

| 血缘声明 | 代码实测 | 判定 |
|---|---|---|
| `quotes_stock → breadth_daily` | `_scan_quotes:332-338` 读 `quotes/stock/*` 的 `date/ts_code/close/pre_close/volume/qfq_factor` | ✅ 边存在 |
| （缺）`st_history → breadth_daily` | `_st_pairs:177-201` 读 ST 区间 → `is_st` → 涨跌停幅度 | **漏边** |
| （缺）`code_migration → breadth_daily` | `_daily_from_quotes:234-236` 用 `load_entity_index().superseded` 剔 2 码 | **漏边** |
| （弱）`daily_index → breadth_daily` | `_local_max_date:158-165` 只判 stale，不影响数值 | 触发依赖，可接受 |
| `breadth_daily → stock_basic` | `build_stock_basic.py` 全文仅 `:212` docstring 提及；`load_trade_calendar` 已改取 `trading_calendar` | **边已不存在（P0 已完成）** |

**P0 结论（单列）**：`834d06d`（2026-11-12）已切断该反向边，`git grep breadth tools/build_stock_basic.py` 仅剩第 212 行 docstring 的历史说明，且新增 `tests/test_stock_basic_calendar_source.py` 两条钉子（源码级 + 行为级）。`governance_order.md` 第 17 行「唯一拓扑例外」与第 33-46 行 P0 段应改判为**已完成**，B3 重算 `stock_basic` 的硬前置已解除；`lineage.json` 保留该边属图未刷新。

### 5.8 缺陷

| # | 位置 | 现写什么 | 实测什么 | 严重度 |
|---|---|---|---|---|
| B-D1 | 表 vs `quotes/stock` | 血缘图 `reconciled: 与行情逐日对拍` | 共同日期 `up` 5,801/8,823、`total` 6,819 不一致，max\|Δ\| 45；系统性「重算 > 表」；下次 `build_full` 会改写 6,819 行 | 中 |
| B-D2 | `coverage.json` / L97 | 「覆盖区间 + 口径 + SHA」 | 无 `sha256` 键 | 中 |
| B-D3 | L138、`manifest.json` `produced_by` | build 收口调用 refresh | `build.py` 无 breadth；L355 非写盘行 | 中 |
| B-D4 | `lineage.json` writers | `build_breadth_ma.py:584`、`layer3_prepare.py:64` 写本表 | 前者写 `breadth_ma_daily`，后者写 staging 候选 | 中 |
| B-D5 | `lineage.json` `derived_from` | 仅 `quotes_stock` | 另有 `st_history`、`code_migration` 两条**数值**入边 | 中 |
| B-D6 | `lineage.json` known_defects / 边 | 被 `stock_basic` 当日历 | `834d06d` 已切断，图未刷新 | 中 |
| B-D7 | 表内日期集合 | — | `1992-04-18`（表独有）/ `1991-08-10`（行情独有）；63 个周末行仍在 | 低 |
| B-D8 | `coverage.built_at` | 2026-11-13T00:00:00 | 硬编码常量，晚于 mtime 3 小时 | 低 |
| B-D9 | `_write_snapshot:353` 与 `:365` | 两次独立原子写 | parquet 与 coverage 非事务（DG-10），第二步失败即不一致 | 低 |
| B-D10 | 1990-12-19 行 | `limit_up/limit_down` 为 null | 其余 8,823 行非空；拼接需注意 | 低 |

---

## 6. `fund_adj_factor`

### 6.1 事实基线

2,974,925 行 / 1,416,839 B / SHA256 `6d8b8c964014147d` / mtime 2026-11-11 20:29:09 / 4 列（`ts_code` String、`trade_date` Date、`qfq_factor` Float64、`hfq_factor` Float64）/ 2,619 只 / 2004-03-22~2026-11-11 / 每列空值 0 / 重复键 0 / 已按 `(ts_code, trade_date)` 排序 / `qfq ∈ [0.0015022, 104.4486]`、`hfq ∈ [0.009574, 665.696]`、1,717 只两侧恒 1.0。

### 6.2 ① 写入口与两入口等价性（`governance_order.md` 第 107 行点名项）

| 入口 | 位置 | 作用域 | 方式 |
|---|---|---|---|
| 常规 | `fund_factors.py:414 rebuild_fund_factors` → `_compute_code_factors:386` → `write_parquet_atomic:475` | 全部 2,619 只 | 整表原子写（zstd/lv3、row_group 100k）；随后 `:481` 写 sidecar、`:487` 刷新 quotes 因子列 |
| 修正 | `tools/fund_split_repair.py:288 apply_repair` → `recompute_code_factors:152` → `write_parquet_atomic:357` | 仅 `REPAIR_CODES`（`:89-90`，5 只），其余 `:354` 原样保留 | 整表原子写（读旧表 → 替换 5 只 → 全表重写） |
| oneoff | `_staging/{gap019_20261111,gap019_residual_20261111,gap150189_20261111,gap160136_20261111,quote_fix_20261111}/**.py` | 定向 | 均整表原子写；`gap150189` 对「事件表 + 因子表」两次原子写（跨文件非事务，DG-10）；`quote_fix/build_candidate.py:181` 只落 `_staging` |

**等价性结论（放行，但需补一条串行约束）**：两者**算法同源**——都走 `prepare_events_for_code` + `AdjFactorCalculator.calculate_factors(max_factor=FUND_FACTOR_MAX=1000)`，都是「累积后复权 `hfq` + 全段统一 `qfq`」；修正入口并非「按拆分日只重算某段」，而是对该只代码**整段历史重算**再 full-join 补旧行。差异只在两处：① 作用域 5 只 vs 2,619 只；② **副作用不等价**——常规入口写完刷新 quotes 副本与 sidecar/meta（`:487`/`:493`），修正入口两者都不做。因此约束为：**修正入口跑完必须补跑 quote 副本对齐**，否则 `factors/fund_adj_factor.parquet` 与 `quotes/fund/*.parquet` 的因子列脱钩（本次实测 0 差异，属随后已跑过全量 build，非机制保证）。

### 6.3 ② manifest

逐项全一致（bytes / sha256 / rows 2,974,925 / codes 2,619 / date_range / cols 4 / columns）；`manifest_tool --check --only factors` → **PASS**（11 条 / 0 不一致）；双向差 0（仅 `factors/manifest.json` 不登记自身）；无 `candidate/.bak`。
`factors/fund_meta.json` 的 `factor_rows` / `codes` 一致，但 `quote_factor_refresh = {targets: 4, changed_rows: 5,325}` —— 记录的是末次 oneoff 的 4 只，而非全量。

### 6.4 ③ 对拍

**(a) 除权参考价残差**（B2 派生层验收口径）：4,929 个事件（另 267 无前行、40 超区间、6 被 `max_factor` 上限丢弃）。

| 残差区间 | 事件数 | 占比 |
|---|---|---|
| ≤ 1e-3（三位小数报价舍入量级） | 4,700 | 95.4% |
| 1e-3 ~ 1e-2 | 54 | 1.1% |
| **> 1e-2** | **175** | 3.6% |
| 其中 > 0.1 | 20 | — |

最大残差 **0.7060404**（502025.SH 2020-02-06：事件 ratio 0.2503、实际台阶 0.4229）。175 条全部落在分级/结构化品种（150xxx 138、502xxx 18、其他 19），仅 3 条与行情自带 `pre_close` 台阶吻合。

**(b) 与上游事件对拍**：`fund_dividend` 3,359 条 —— 有跳变 3,345（99.58%）、无跳变 14、超区间 15、因子行缺失 39；反向全表 `|rel| ≥ 1e-4` 的跳变仅 **2 条无对应事件**（502020.SH 2019-10-28 +10.23%、502040.SH 2020-12-04 −3.37%）。`fund_share_change` 2,138 条 —— 有跳变 2,131、无跳变 7（含 150260.SZ 2020-06-11 ratio 1.9683 而台阶 1.0）。
另有 200 条分红事件的对齐日距 `ex_date` > 10 天（最大 1,427 天），全部由行情占位段造成（`prepare_events_for_code` 规则是「对齐到首个 ≥ `ex_date` 的交易日」，占位段把事件推到很后）。

**(c) 历史段可复算性**：用现行事件口径重算 1,027 只 —— 223,186 行差 > 1e-3、218,759 行 > 1e-2；174 只最大差 > 1e-3（150xxx 138、502xxx 18），最大 67.66（150304.SZ）。`502011.SH` 2020-07-09 台阶 1.031972 ≈ `prev_close/pre_close` 而非事件 2.0334 → 历史段是**旧口径存量**，非现行代码产物。

### 6.5 ④ 第二份

全库带因子列的只有 `quotes/stock`（5,861 文件）、`quotes/fund`（2,620）、`factors` 两表（本表与 `stock_adj_factor`）。
`quotes/fund` 2,620/2,620 带因子列，2,979,394 行按 as_of 对齐快照 **失配 0 行 0 只**（缺省 1.0）→ 当前是**同步镜像而非第二份**：写者 `store.py:929`（由 `fund_factors:487` 调用）、校验者 `store.py:1006`（接 `daily_data_gate.py:52`）；读侧 `read_file` 默认 `with_factors=False` 直接用镜像，`with_factors=True` 的调用点全是 stock。清单 L75「互为校验」与实测一致；**唯一漏口**是修正入口不刷新镜像。

### 6.6 ⑤ 登记 vs 实测

| 项 | 登记 | 实测 | 判定 |
|---|---|---|---|
| 行数 / 只数 / 区间 / 唯一性 | 2,974,925 / 2,619 / 2004-03-22~2026-11-11 / 唯一 | 全对 | ✅ |
| 消费者 | 只登记 `store.py`（锚 `#L582`）与 `build_fund_corporate_actions.py` | 实测读侧 **9 处**：`store.py:1017`、`build.py:1074`、`factors_manifest.py:46`、`build_multi_asset_etf_pool.py:250`、`etf_data_audit.py:221`、`etf_momentum_backtest.py:100`、`fund_split_repair.py:67`、`reconcile_p2_metadata.py:139`、`backup_critical_assets.py:53`；锚 `L582` 已漂移（常量在 620、属性 645、读取 757/772） | ❌ 中 |

### 6.7 派生边对账

| upstream → 本表 | 读的列/层 | 代码位置 | 判定 |
|---|---|---|---|
| `quotes_fund` | `date` + `close`，**不走 `with_factors` 覆盖层** | `fund_factors.py:392-399`、`fund_split_repair.py:104-112` | ✅ 一致 |
| `fund_dividend` | `cash_amount`（> 0），ETF 当日 / 非 ETF 次一交易日 | `prepare_events_for_code:318` | ✅ 一致 |
| `fund_share_change` | `share_ratio`（> 0），次一交易日，159930.SZ 跳过 | 同上 + `QUOTE_SPLIT_ADJUSTED_CODES` | ✅ 一致 |

无 `fund_basic` / `etf_fee` 入边（`is_etf_code` 只看代码前缀 `:102`；fetch 走 akshare 不经 MySQL）→ 血缘图 3 条边**齐全、无漏边**，是本片唯一「派生边全对」的表。

### 6.8 缺陷

| # | 位置 | 现写什么 | 实测什么 | 严重度 |
|---|---|---|---|---|
| A-D1 | `factors/fund_meta.json` | `quote_factor_refresh.targets = 4` | 末次写是 4 只 oneoff；**2,239 只因子止于 11-09、4 只到 11-11，而行情 2,234 只已到 11-11 → 同表混龄** | 高 |
| A-D2 | 缺口登记 L157（G-019） | 「2,619 只 mismatch 0；行情有而因子无 0 行」 | **2,241 只行数不等**（最大差 2 行，如 501001.SH 2,497 vs 2,499）；`561970.SH` 有 1 行行情、**零因子行** | 高 |
| A-D3 | `factor.parquet` 历史段 | 视为现行事件口径产物 | 1,027 只重算：223,186 行差 > 1e-3、218,759 行 > 1e-2；最大 67.66（150304.SZ） | 高 |
| A-D4 | 缺口登记 L157 | 「+8 行未见归因」 | 已归因：2,974,917（`_backup/fund_factor_quote_fix_20261111`）→ 2,974,925，+8 = 159582 / 159941 / 512690 / 516160 各 +11-10、11-11；文档里的 2,974,919 是 150189 阶段旧值，中间还有 `gap019_residual` 两次（2,974,772 / 2,974,835） | 中 |
| A-D5 | 2020 段分级码 | 除权参考价应由事件复现 | 175 个事件残差 > 1e-2、20 个 > 0.1，最大 0.706 | 中 |
| A-D6 | `prepare_events_for_code` 对齐规则 | 事件对齐到首个 ≥ `ex_date` 的交易日 | 200 条分红被行情占位段拉到 > 10 天后（最大 1,427 天）；163804 / 163807 的 2020~2024 分红全部挤到 2025-11-24 | 中 |
| A-D7 | 清单 L75 | 消费者 2 处、锚 `store.py#L582` | 实读 9 处，锚点漂移 | 中 |
| A-D8 | `fund_split_repair.py:357` | 写完即收尾 | 不刷 quotes 镜像、不更新 `fund_meta`（与常规入口 `:487`/`:493` 不等效） | 低 |

---

## 7. 跨表汇总

### 7.1 血缘图（`lineage.json`）系统性缺陷

| # | 缺陷类型 | 命中表 | 证据 |
|---|---|---|---|
| L-1 | `writers_detected` 假阳性（docstring / 常量 / 写别表 / 写 staging 候选） | **5/5** | futures 4 条假 + 漏 1；etf_equity_pool 空数组；membership `:25` 是 bullet；breadth 2 条写别表；fund_adj 6 条 oneoff 属实但常规入口行号偏移 |
| L-2 | 漏边（代码实读但 edges 无） | **4/5** | futures（staging CSV 全链路）；etf_equity_pool（names / fund_basic / fake_bar）；breadth（st_history / code_migration）；fund_adj 0 条（唯一全对） |
| L-3 | 虚边（edges 有、代码不读） | 2/5 | membership（`index_basic`）；futures（`quotes_futures` 方向反了） |
| L-4 | 表级元数据过期 | 3/5 | futures（rows 518 / 「无 manifest」）；etf_equity_pool（`*.csv` 通配 / 「一次性?」）；breadth（stock_basic 反向边） |

建议单列一条治理项：**`build_lineage.py` 的写入口判定必须区分「写盘语句」与「字符串/常量/文档」，并把 `_staging` 与 `tmp` 分离标注**；否则 B3 以后每片都要重做一遍同样的排除工作。

### 7.2 登记类缺陷（只列，本片不改 `docs/`）

| 文档 | 行 | 一句话 |
|---|---|---|
| `数据权威路径清单.md` | L24 | membership 覆盖度指向 `manifest.json` 的 `coverage` 键，与 L99 的 `coverage.json` 自相矛盾 |
| 同上 | L24 / L58 / L261 | 锚点与快照统计过期（20 文件 / 9,250 行 / 5 日期） |
| 同上 | L28 | `refresh` 锚点偏移、消费方漏 3 处 |
| 同上 | L35 | 把产出方 `tmp/.../etf_pool_build.py` 列为消费者；明细 5 只/8 格应为 6 只/9 格 |
| 同上 | L43 / L44 / L46 | futures 读写定位错（`:49` 是常量）、「暂无消费者」不成立、`multiplier` 空 301 未披露 |
| 同上 | L75 | fund_adj_factor 消费者只记 2 处，实为 9 处；锚 `L582` 漂移 |
| 同上 | L89 | 伪 bar 清单「15 行」应为 14 数据行 |
| 同上 | L97 | `coverage.json` 承诺 SHA 但无该键 |
| 同上 | L138 | 声称 `python -m src.data.local_store build` 收口调 breadth refresh，`build.py` 无此步骤 |
| `数据缺口登记_正式表_2026-11-12.md` | L157（G-019） | 「mismatch 0 / 行情有而因子无 0 行」与实测冲突（2,241 只行数不等、561970.SH 零因子）；「+8 行未归因」已可归因 |
| `governance_order.md` | L17、L33-46 | P0 已于 `834d06d` 完成，应改判 |
| `governance_order.md` | L103 | `futures_multi_asset` 被归入「基金链」行，内容纯期货 |

### 7.3 需要下游决策的三件事（不自行改口径）

1. **`breadth_daily` 6,819 行冻结差额**（B-D1）：修法只有一条——重跑 `build_full` 全表重算，但这会改写 6,819 行既有值，属「改变既定口径」之外的数据改写，需要先定「是否接受一次性全表重算并把差额留证」。建议：接受，按 DG-22 先 `copy2` + 新旧 SHA256 + 逐列差额清单。**→ 2026-11-13 00:27 已获批准并执行，结果见 §11**。
2. **`fund_adj_factor` 分级码历史段**（A-D3/A-D5）：150xxx / 502xxx 共 156 只的历史因子不是现行事件口径产物，重算会改 22 万行；建议先在 B3 的 `fund_corp_actions` 体检里确认「分级基金折算」是否本来就该由另一口径覆盖，再决定是否重算。**→ 材料见 `b2_slice2_recommendations.md` §2**。
3. **`membership_intervals` 开闭区间不一致**（M-D2）：生产侧半开、消费侧闭区间，差 279 个 (指数,日期) / 5,935 段、平均 21.27 只（最多 100）。官方快照 20/20 与「`out_date` 当日仍在册 0/5,935」两条证据已把官方口径定死为**半开**，故错在两处消费侧（`run_l1.py:147`、`run_domain.py:97`），全仓无 `state_vector` 侧消费者（消费者仅这 2 处 + `rebuild_index_constituents.py:789` 半开正确）。**→ 三方案与实测影响面见 `b2_slice2_recommendations.md` §1**。

---

## 8. 复现命令

```powershell
cd e:\code\python\quant
.\.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b2s2_futures.py   > 2026-11-12\data-lineage\work\b2s2_futures.json
.\.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b2s2_etfpool.py   > 2026-11-12\data-lineage\work\b2s2_etfpool.json
.\.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b2s2_members.py   > 2026-11-12\data-lineage\work\b2s2_members.json
.\.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b2s2_breadth.py   > 2026-11-12\data-lineage\work\b2s2_breadth.json
.\.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b2s2_fundadj.py   > 2026-11-12\data-lineage\work\b2s2_fundadj.json

# manifest 门禁（只读 --check）
.\.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta/multi_asset
.\.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta/pools
.\.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta/index_members
.\.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta/breadth
.\.venv\Scripts\python.exe tools\manifest_tool.py --check --only factors
```

五个脚本均为只读（`read_parquet` / `read_csv` / `json.load` / `hashlib` / 纯函数复算），不写 `data/`、不改 manifest、不提交。`b2s2_breadth.py` 需全量读 5,830 个行情文件（约 50 秒），其余均在数秒内。

## 9. 成稿复核

本报告全部数字来自上述只读脚本；其中以下 9 项在成稿时用**独立脚本**二次复核，与脚本输出逐项一致：

| 复核项 | 结果 |
|---|---|
| 五表基线（行数 / 字节 / SHA16 / 列数） | 全部一致（见 §2.1、§3.1、§4.1、§5.1、§6.1） |
| `etf_equity_pool` 档位与代码数 | full 1,338 / std3y 677 / pool 138，1,338 只 |
| `futures_contracts.multiplier` 空值 | 301 |
| `membership_intervals` 指数 / 只数 / `out_date` 空值 | 6 指数 / 3,643 只 / 2,000 空 |
| membership 闭区间多纳入 | 5,935 段 / 279 个 (指数,日期) / 平均 21.27 / 最多 100 |
| `breadth_daily` 周末行 / 首日空值 | 63 / 1 |
| `fund_adj_factor` 只数 / 区间 | 2,619 / 2004-03-22~2026-11-11 |
| `fund_adj_factor` vs 行情行数 | 2,241 只不等、1 只在行情无因子（561970.SH） |
| P0 反向边 | `834d06d` 存在，`build_stock_basic.py` 仅 `:212` docstring 残留 |

## 10. 交接与边界

- 本片只做体检与登记，未修数据、未改清单、未提交；`docs/数据权威路径清单.md` 与 `docs/数据缺口登记_正式表_2026-11-12.md` 的修改由其他路径统一处理，本片只在 §7.2 列表。
- 与第一片的分工：第一片 = `suspension_daily` / `implied_suspension` / `universe_price_jumps` / `anomalies`；本片 = 上表 5 张；两片零重叠，`stock_turnover` 两片同判挪后。
- 下一片建议取：`stock_turnover`（待 G-077 收口）、`daily_index` + `ann_date_map`（G-045）、`listing_intervals`、`name_history`（待 G-095）——恰好覆盖 B2 剩余段，且与 B2 解锁条件（`daily_index` 元数据修正 + `listing_intervals` 对拍通过）一致。
- 本片发现的三项需决策事项（§7.3）建议在 B3 开工前定案，否则 B3 的 `stock_basic` / `universe_exclusions` 会在未定的成员口径上重算。

---

## 11. `breadth_daily` 重建执行记录（2026-11-13 00:27）

授权：调用方批准「按 `build_full` 全量重算，属上游变动导致的陈旧，不是口径改变」。脚本 `2026-11-12/data-lineage/work/b2s2_breadth_rebuild.py`（`--prepare` / `--apply` / `--verify` 三段，候选先落 `_staging/breadth_rebuild_20261113/`）。

### 11.1 改前备份（DG-11 / DG-22）

| 项 | 值 |
|---|---|
| 快照目录 | `_backup/meta/breadth_20261113/`（整目录 `copy2`，含 `breadth_daily.parquet`、`coverage.json`、`manifest.json`、`breadth_ma_daily.parquet`） |
| 逐文件校验 | 4/4 拷贝后 SHA256 与源一致；`BACKUP_INFO.json` 记录各文件 bytes/sha256 |
| 旧 `breadth_daily.parquet` | 8,824 行 / 135,009 B / `322571b2c46f7ab0e392f472e5726b4e9525997727a842b4db3dd8d82fcc60e1` |
| 新 `breadth_daily.parquet` | **8,823 行** / 136,200 B / `20869903f724bbf53ee0d5282d813d57eb5a91dcd9b8bb6295d6e5d20aaea42a` |
| 回滚 | `copy2` 备份目录四个文件覆盖回 `data/local_store/meta/breadth/` 即可，SHA256 可逐字节复原 |

### 11.2 四项验收

**① 行数守恒 —— 未达 8,824，实测 8,823（−1），丢失键 1、新增键 0。** 这一项与「8,824 行 / 丢失键 0」的验收口径冲突，原因已定位到具体行：

| 键 | 性质 | 实测 |
|---|---|---|
| `1992-04-18` | 丢失 | **周六**。旧表该行 `up 0 / down 1 / total 1`；当前 `quotes/stock/**` 对该日 **0 个 bar**（已被上游坏 bar 清理移除）→ 重算无行，属「上游不存在、旧行成孤儿」 |
| `1991-08-10` | 上游有、本表不产生 | **周六**。该日仅 1 个 bar（000004.SZ，`volume = 0.0`），被 `_daily_from_quotes` 的 `volume > 0` 剔除 → 按现行代码本就不该有行 |

两点结论：① 两个日期都是**非交易日（周六）**，`quotes` 的周六 bar 合计 63 → 重建后周末行 62，减少的正是 `1992-04-18`；② 若强行保住 `1992-04-18`，则「按现行代码再算一次 0 差异」这条自证必然失败——二者互斥，本片选择保住自证（这也是调用方强调「否则只是把漂移换了个位置」的那条）。**调用方已裁定：接受 8,823、不补行**，口径与依据见 §11.5；若日后推翻，按 §11.1 回滚即可逐字节复原。

**② 改动行数（8,823 个共同日期上统计，与体检时的 6,819 / 5,801 逐项吻合）**

| 列 | 改动行 | 其中旧值非空 | max\|Δ\| |
|---|---|---|---|
| `up_count` | **5,801** | 5,801 | 12 |
| `down_count` | 5,572 | 5,572 | 13 |
| `flat_count` | 1,381 | 1,381 | 4 |
| `total_count` | **6,819** | 6,819 | 13 |
| `limit_up_count` | 2,964 | 2,964 | 45 |
| `limit_down_count` | 2,000 | 2,000 | 40 |
| `new_high_250` | 792 | 792 | 10 |
| `new_low_250` | 1,016 | 1,016 | 8 |
| `adl` | 8,816 | 8,816 | 623 |

**两类重叠单独说明**（8,823 个共同日期上实测）：`total_count` 与 `up_count` 同时变动 = **5,795 行**；仅 `total` 变 = 1,024 行；仅 `up` 变 = 6 行；两者都不变 = 1,998 行（5,795+1,024+6+1,998 = 8,823；5,795+1,024 = 6,819；5,795+6 = 5,801，三式自洽）。`total` 变动面大于 `up`，差额由「`down`/`flat` 变化而 `up` 未变」构成——`total = up + down + flat`，任一分量变则 `total` 变。
`adl` 是 `cumsum(up − down)`，首个改动日之后逐行累积，故 8,816 行全变属结构性必然，不是额外口径变更。

**③ 既有非空值改动数与「其余列必须 0」**

- 允许动的列：9 个数值列全部由**同一次重算**产出，无「未参与重算的列」可做对照；`up`/`total` 及其相关列（`down`/`flat`/`limit_up`/`limit_down`/`new_high_250`/`new_low_250`/`adl`）改动数见上表。
- 其余列（可作对照的只有主键 `trade_date`）：**改动 0**（8,823 个键一一对上，无改名/改格式）。
- `value → null` 改动数：9 列**全为 0**（无任何既有非空值被写成空）。

**④ 空值数前后**

| 列 | 改前 | 改后 |
|---|---|---|
| `limit_up_count` | 1（1990-12-19） | 1（1990-12-19） |
| `limit_down_count` | 1（1990-12-19） | 1（1990-12-19） |
| 其余 8 列 | 0 | 0 |

**⑤ 落盘方式与 manifest**：`bs._write_snapshot`（`write_parquet_atomic` + `write_text_atomic` 写 `coverage.json`）；`manifest.json` 的 `sibling_tables.files[0]` 已同步 `rows = 8,823`、`bytes = 136,200`、`sha256 = 2086…42a`、`date_range = [1990-12-19, 2026-11-11]`、`coverage` 块 → `manifest_matches = true`。
`coverage.json` 由官方代码路径重写，**丢掉了 11-12 那次 staging 补丁写入的 `patch_note` 键**（454 B → 300 B），`built_at` 由硬编码 `2026-11-13T00:00:00` 改为真实写入时刻 `2026-11-13T00:27:21`（顺带消掉 B-D8）。

### 11.3 自证与 DG-23

| 验证 | 结果 |
|---|---|
| 按现行代码**再算一次**（清 `_SNAPSHOT_CACHE` 后重跑） vs 落盘 | **8,823 = 8,823，`frame.equals = true`，schema 相等**，逐列改动 0 → 0 差异成立 |
| 落盘 SHA256 vs manifest | 一致（`20869903f724bbf5…`） |
| DG-23 正式读路径 | `breadth_service.get_breadth('2026-11-01','2026-11-11')` 返回 8 行（2026-11-02~11-11 中的交易日），与 parquet 子集逐值相等 |
| DG-23 下游消费者 | `indicator_service.calculate_indicators_for_period(000001.SH)` 端到端跑通，日志「成功加载市场涨跌家数数据」，产出 `adr10` / `obos10`，末行值与所读快照一致 |

### 11.4 changelog

`logs/data_changelog.csv` 用 `ab` 追加 3 行（parquet / coverage.json / manifest.json），**既有字节前缀逐字节不变**（脚本内 `assert after[:len(before)] == before`），本次追加 **951 B / 3 行**；期间另有他路并发追加，成稿时文件为 10,367 行（写入前 10,350），本片 3 行位于末尾，前缀校验仍成立。

### 11.5 8,824 → 8,823 的成因（调用方已裁定：接受，不补）

| 项 | 内容 |
|---|---|
| 旧行 | `1992-04-18`（**周六**）`up 0 / down 1 / total 1` —— 全行只由 1 只标的的 1 个 bar 贡献 |
| 上游现状 | `quotes/stock/**` 对 `1992-04-18` **0 个 bar** |
| G-107 | `docs/evidence/suspension_quote_fix_20261112/README.md:50`、`:57-76`：`000002.SZ` `1992-04-18`（周六）→ `1992-04-20`（周一）是**日期错位修正**，不是缺行补齐；`factor_backfill_rows.csv:24,26` 对 `stock_adj_factor` / `stock_turnover` 逐行改期 |
| G-112 | 非交易日 bar 裁定 **220 行**（`stock_adj_factor` 220 / `stock_turnover` 219，涉及 16 只 1991–1993 深市老股），白名单 **175 行**；`1992-04-18` 属**白名单之外**那一批，其 bar 已被清理 |
| 传导链 | 上游改期（G-107）＋ 白名单外 bar 清理（G-112）→ 该日无可计入 bar → `_daily_from_quotes` 不产生行 → 派生层少 1 行 |
| 判定 | **不是行数不守恒**，是上游修正传导到派生层。为凑 8,824 补一行，等于把刚被清掉的坏 bar 从派生层长回来，同时让「按现行代码再算 0 差异」这条自证必然失败——两害相权，保自证 |
| 旁证 | 周末行 63 → 62，减少的正是该日；前一版（`docs/evidence/derived_1993b_20261113/` 记录的 8,826 → 8,824）与本版仅差这 1 行 |

### 11.6 建议登记条目（交缺口表统一入册）

> 本节只是**建议文本**，编号由文档那一路分配（建议插在 G-112 附近，便于与「非交易日 bar 裁定」并读）。

| 字段 | 内容 |
|---|---|
| 建议标题 | `breadth_daily` 缺失 `1992-04-18` 行——G-107 改期与 G-112 周末 bar 清理的派生层传导，不补行 |
| 登记对象 | `data/local_store/meta/breadth/breadth_daily.parquet`，主键 `trade_date = 1992-04-18` |
| 现象 | 行数 8,824 → **8,823**（−1），丢失键 1 / 新增键 0；该日旧行 `up 0 / down 1 / total 1` |
| 成因 | 上游 `quotes/stock/**` 该日 0 个 bar：G-107 已把 `000002.SZ 1992-04-18` 改期到 `1992-04-20`，G-112 又把白名单外的非交易日 bar 清理掉，故该日再无可计入 bar |
| 是否缺陷 | **否**（属上游修正的正常传导，非本地计算错误） |
| 与 G-112 的关系 | 该日属 G-112「220 行非交易日 bar 裁定」中**白名单之外**的那批；G-112 收口后，任何由 `quotes/stock` 逐日聚合的派生表（breadth 及其他同源表）在该日都会少一行，不是 breadth 独有 |
| 处置 | **不补行**；文档侧把行数同步为 8,823 |
| 验收 | 按现行代码重算与落盘 **0 差异**（§11.3）；行数 8,823、丢失键仅此 1 个、新增键 0 |
| 关联证据 | §11.1 备份、§11.2 四项验收、`docs/evidence/suspension_quote_fix_20261112/`（G-107）、`docs/evidence/derived_1993b_20261113/`（上一版 8,826→8,824） |
| 后续可查 | 若将来 `quotes` 对 1992 年深市周末 session 重新裁定并恢复该 bar，breadth 应在下一次 `build_full` 自动长回该行——本条不需要人工补数 |

### 11.7 文档侧待改清单（只列不改，交文档那一路）

| # | 位置 | 现写 | 实测 | 建议 |
|---|---|---|---|---|
| 1 | `docs/数据权威路径清单.md:96` | `coverage.json`「唯一……声明（**454 B**）」 | 现 **300 B**，且该文件本就无 `sha256` 键（B-D2） | 字节改 300，并注明 11-13 重建已无 staging 的 `patch_note` 键 |
| 2 | `docs/数据权威路径清单.md:29` | `breadth_ma_daily` 「8,654 行 / 1991-07-15~**2026-11-09**」 | 同目录 `manifest.json`：8,654 行 / `date_range = [1991-07-15, **2026-11-11**]` | 末端日期按 manifest 改 `2026-11-11`（或注明 manifest 更新更晚） |
| 3 | `docs/数据权威路径清单.md:28` | 仅语义与消费者，未记行数/字节/SHA | 8,823 行 / 136,200 B / `2086…42a` | **无需改**：原文不含 8,824，也不含 `1992-04-18` |
| 4 | `data/local_store/meta/breadth/manifest.json` | `sibling_tables.files[0].rows`（L69）与 `sibling_tables.files[0].coverage.rows`（L96） | 均 **8,823**；`bytes` 136,200；`sha256` `20869903f724bbf5…`；`date_range` 1990-12-19~2026-11-11 | **无残留 8,824 / `1992-04-18`**，无需改（本次已同步） |
| 5 | `docs/数据缺口登记_正式表_2026-11-12.md` | 未含本条目 | — | 按 §11.6 入册，由文档那一路处理 |

**历史文档不改，仅供引用时注意时点**（写了 8,824 或旧区间，属当时快照，回改会毁掉可追溯链）：`docs/数据收口状态_2026-11-11.md:182`（8,824 / 至 11-09）、`docs/数据追赶_派生层至20261111_2026-11-12.md:13,114`（8,824→8,826）、`docs/evidence/derived_1993b_20261113/README.md:31` 与 `验收报告.md:22`（8,826→8,824）、`docs/数据可用性审计_2026-11-09.md:39` 与 `_行情_2026-11-09.md:66`（8,823，为 11-06 时点，与本表当前值同数但无关系）。

### 11.8 遗留

- B-D2（`coverage.json` 无 `sha256` 键）**未修**：属文档契约与代码的不一致，本次未扩范围；现文件仍只有 `rows / first / last / built_at / version / rolling_window / note`。
- B-D7（周末行）：重建后仍余 **62** 个周末行（上游 `quotes` 自带周六 bar，不是本表引入），彻底清理需先清上游，不在本片范围。
- 本片只写了 `data/local_store/meta/breadth/`、`_backup/meta/breadth_20261113/`、`_staging/breadth_rebuild_20261113/`、`logs/data_changelog.csv` 四个位置；`src/`、`tools/`、`docs/` 一行未改，未提交。
