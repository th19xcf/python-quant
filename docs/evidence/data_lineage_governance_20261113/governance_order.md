# 施工顺序（按上游优先，可直接照此排任务）

本文件是 `lineage_report.md` 第六节的展开。排法不是按问题严重度，而是按血缘深度：**深度小的先做，同深度的一批并行**。

## 0. 排法与自证

- 深度定义：`depth(t) = 0`（无任何上游派生边）；否则 `1 + max(depth(上游))`。
- 数据源：`outputs/lineage.json` 的 `derivation_edges`（82 条）。
- 自证：82 条边全部满足 `depth(上游) < depth(下游)`，**违反 0 条**，因此同深度表之间必然没有派生边，可以并行。
- 复现：

```powershell
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\compute_depth.py
# 末行输出：参与校验的派生边 = 82，批内同深度边 = 0 即同批可并行
```

**唯一的拓扑例外是 P0**：`breadth_daily → stock_basic` 是一条「下游层号更小」的反向边（L2 → L1）。按深度它不影响合法性（breadth_daily 深度 1、stock_basic 深度 2），但方向本身是错的。P0 不改数据、只改代码，作为 B3 重算 `stock_basic` 之前的硬前置。

## 1. 批次总览

| 批 | 深度 | 表数 | 内容 | 并行形态 | 下一批解锁条件 |
|---|---|---|---|---|---|
| **P0** | — | 0（改代码） | 切断 `stock_basic ← breadth_daily` 反向边 | 单点串行 | `stock_basic` 改造后逐值对拍 0 改动 |
| **B1** | 0 | 21 | 基础落地层（行情四族 / 估值 / 三表 / 名录 / 分红 / 股本 / 宏观 / 外部快照 / 人工补充） | 按表并行，同表内部串行 | 每张表都有可反复跑入口 + manifest 齐全 |
| **B2** | 1 | 15 | 一次派生（日历 / 名录区间 / 复权 / 宽度 / 成分 / 停牌 / 质量检查 / 换手） | 5 条族链并行，链内串行 | `daily_index` 行数元数据修正 + `listing_intervals` 对拍通过 |
| **B3** | 2 | 7 | 二次派生（名录 / 旧码 / 基金除权 / ETF 池 / ST 价格推断 / 可交易剔除 / 月度面板） | 3 条链并行 | 名录与旧码对拍通过；`st_price_inferred` 落盘 |
| **B4** | 3 | 5 | 三次派生（宽度均线 / 行业 PIT / ST 区间 / 陈旧判定 / 新因子） | 宽度+行业并行；ST 链串行 | ST 区间对拍通过（含人工覆盖层留证） |
| **B5** | 4 | 2 | 四次派生（ST 覆盖度 / 状态矢量） | 两张并行 | `state_vector` 杠杆位归因闭环（G-082） |
| **B6** | 5 | 1 | 结论层（universe 分层六表） | 单点 | 三选项定案（建议 C：维持 + 登记） |

---

## P0 · 切断跨层反向依赖（不改数据）

**要改的唯一位置**：`tools/build_stock_basic.py:205-210`，注释写明「交易日历取 breadth_daily」。

**改法**：日历改从 `quotes/stock` 的日期并集（或 `meta/daily_index.parquet`）取。

**为什么必须前置**：只要这条边还在，任何一次 `breadth_daily` 重算都会改变 `stock_basic`，进而触发 `code_migration` / `breadth_ma_daily` / `industry_pit` / `universe_layers` / 研究面板整链返工——这正是「下游已修完、上游又改一遍」的机制来源。

**验收**：
1. 改造前后 `stock_basic` 逐列对拍，除日历来源可能影响的 `list_date`/`delist_date` 外改动数必须为 0；
2. `git grep -n "breadth" tools/build_stock_basic.py` 零命中；
3. `tools/build_stock_basic.py` 单测/`--dry-run` 跑通。

**并行**：单点改动，串行。**它只改代码，动数据必须另开一次任务。**

---

## B1 · 深度 0：基础落地层（21 张）

**包含表**

| 组 | 表（行数） | 关联 G |
|---|---|---|
| 个股行情 | `quotes_stock`（18,129,892 行 / 5,832 分片） | G-061、G-084、G-087、G-104、G-106 |
| 指数行情 | `quotes_index`（4,649,808） | G-001、G-002、G-003、G-105、G-106 |
| 基金行情 | `quotes_fund`（2,979,395） | G-004、G-013、G-020、G-021、G-026、G-062、G-064、G-106 |
| 期货行情 | `quotes_futures`（153,176） | G-049、G-050、G-051、G-058 |
| 估值 | `valuation`（多文件） | G-036、G-037、G-059、G-108、G-109、G-110 |
| 三大报表 | `financials3`（多文件） | G-060、G-068、G-089、G-090 |
| 基金名录 | `fund_basic`（2,618） | G-015、G-016、G-017、G-025、G-044 |
| 指数名录 | `index_basic`（1,335） | G-046、G-047、G-048 |
| 个股分红 | `stock_dividend`（60,071） | G-076 |
| 基金分红 | `fund_dividend`（3,413） | — |
| 基金份额 | `fund_share_change`（2,738） | — |
| 股本 | `stock_shares`（166,284，mixed） | G-077 |
| 宏观五表 | `macro`（多文件） | G-038、G-052、G-053、G-054、G-055、G-056、G-057 |
| 事件 | `events`（185,750） | G-069、G-070、G-071、G-072、G-073、G-074、G-075 |
| 代码表 | `names_json` | — |
| 外部快照 | `st_raw`、`st_status`、`industry_snap`、`index_weight`、`etf_fee`（518） | G-095、G-098、G-030～G-034 |
| 人工补充 | `supplement_intervals` | — |

**为何整批可并行**：21 张表在 `derivation_edges` 中两两之间 0 条边（深度全为 0，机器校验），且写者落在不同文件/不同外部源。

**批内必须串行 / 需要拆开的地方**（同表多写入口，一条任务写一张表）：
- `stock_shares`：4 个写入口（`share_capital.py`、`derive_free_float_fill.py:225`、`backfill_share_div.py`、`derive_total_dividend.py`），必须先定谁是「唯一常规入口」，其余降级为一次性并登记。
- `stock_dividend`：`build.py:979` 写正式列 + `collect_stock_dividend.py` 采集，存在 raw+fix 两层——对拍必须比消费方实际读的 `cash_div_fix.fill_null(cash_div)` 有效层（DG-30），不能只比 `cash_div`。
- `events`：3 个入口（`events.py`、`backfill_events.py`、`update_events.py`），追加/重建边界未写明，先补注释再跑。
- `st_raw` / `st_status` / `industry_snap` / `etf_fee`：**没有常规写入口**，本批第一件事是固化一个 `tools/` 入口或正式登记为「快照型资产」。

**本批要收口的账**
1. 补 manifest：`meta/st/raw/*` 7 张（G-095）、`meta/futures_*` 3 张、`code_migration`、`anomalies/coverage_gaps` —— 当前 13 张无 manifest。
2. 把 `_staging/` 一次性脚本直写过的表补出常规入口（32 张/表族清单见 `lineage_report.md` 5.3）。
3. `quotes_futures` 与三张期货派生表补 `manifest.json`。
4. 已在查、本任务不重查：`quotes_stock` 的非交易日 bar（引用他任务）；`stock_adj_factor` 三只标的 600872/600305/600989 登记与实物不符（在 B2 复核）。

**验收（每张表都按 DG-20 四项）**：行数守恒（manifest vs 磁盘，当前 0 不一致，改动后必须仍为 0）→ 逐值对拍（既有非空值改动数为 0）→ 受影响文件 SHA256 → 抽样断言。另加一条真实读路径端到端（DG-23）：行情类用 `LocalQuoteStore.get` 读，宏观用 `build_state_vector` 的读入口。

**下一批解锁条件**：每张表都有可反复跑的入口（或已登记为快照资产）+ manifest 齐全。

---

## B2 · 深度 1：一次派生（15 张）

**包含表**：`daily_index`(G-045)、`listing_intervals`、`name_history`、`ann_date_map`(G-045)、`stock_adj_factor`（已在查：600872/600305/600989）、`fund_adj_factor`(G-019)、`breadth_daily`、`membership_intervals`(G-039)、`etf_equity_pool`、`futures_multi_asset`、`suspension_daily`、`implied_suspension`、`anomalies`(G-104)、`universe_price_jumps`、`stock_turnover`(G-077/G-078/G-079/G-084)。

**为何整批可并行**：全部只依赖 B1 的表，彼此 0 条边。

**建议的 5 条族链（链内串行，链间并行）**
1. 日历/名录链：`daily_index` → `listing_intervals` → `name_history`
2. 个股复权链：`stock_adj_factor`
3. 基金链：`fund_adj_factor` → `etf_equity_pool`；`futures_multi_asset` 独立
4. 宽度链：`breadth_daily`
5. 状态/质量链：`suspension_daily` → `implied_suspension`；`anomalies`、`universe_price_jumps`、`stock_turnover`

**串行约束**：`fund_adj_factor` 有两个写入口（`fund_factors.py:469` 与 `fund_split_repair.py:357`），必须先确认拆分修正是否与常规口径等价；`implied_suspension` 只有一次性脚本，先固化入口。

**本批优先收口的三笔账**
1. `daily_index` 的 11 行 `rows` 与实际不符（G-045）——它是 L3 多张表的公共输入，先修。
2. `fund_adj_factor` 末行停在 2026-11-06，缺 11-09（G-019）。
3. `stock_turnover` 的 `turn` 依赖 `stock_shares.float_share`，B1 的 G-077 未收口前不重算。

**验收**：除权类跑「除权参考价残差 = 0」；宽度类逐日对拍 `breadth_daily` 行数与上游交易日数；`stock_adj_factor` 三只标的留给在查任务，本批只标记状态不重复判定。

**下一批解锁条件**：`daily_index` 元数据修正完成 + `listing_intervals` 逐值对拍通过。

---

## B3 · 深度 2：二次派生（7 张）

**包含表**：`stock_basic`（mixed，P0 之后）、`code_migration`、`fund_corp_actions`、`etf_pool`、`st_price_inferred`、`universe_exclusions`、`analysis_monthly`(G-096)。

**为何整批可并行**：只依赖深度 0–1 的表，彼此 0 条边。

**串行/前置**
- `stock_basic` 必须在 P0 完成之后重算，重算后**逐列对拍**：除日历来源外改动数 0。
- `code_migration` 依赖 `name_history`，若 B2 的 `name_history` 有变动则必须串在其后（本批内这两张表是跨批关系，不在同一批）。
- `analysis_monthly` 拓扑上可以在这一批重算，但它的 G-096（与 `new_factors` 行键完全相同、列集不同）属于**口径决策**，不在本批自行合并。

**本批要收口的账**：`fund_corp_actions` 未定位常规入口（G-018 的 ETF 登记 0 条）；`etf_pool` / `etf_equity_pool` 的写者是否覆盖全部历史月份。

**验收**：`stock_basic` 行数守恒 + 逐列对拍；`universe_exclusions` 与 `daily_index` 的交易日集合一致；`analysis_monthly` 行键（5,797 只 × 142 月）不变。

**下一批解锁条件**：名录与旧码对拍通过 + `st_price_inferred` 落盘完成。

---

## B4 · 深度 3：三次派生（5 张）

**包含表**：`breadth_ma_daily`、`industry_pit`、`st_intervals`（mixed，含人工覆盖层）、`stale_status`、`new_factors`(G-083/G-085/G-096)。

**为何只有 ST 链串行**：`breadth_ma_daily`、`industry_pit`、`stale_status` 三者互不依赖，可并行；`st_intervals` 依赖 `st_price_inferred` + `st_raw` + `st_status` + `supplement_intervals`，且含**人工裁定覆盖层**——覆盖层不可由代码重建，改动必须 `copy2` + 新旧 SHA256 + 逐条留证。

**本批最需要停下来报用户的一条**：`new_factors` 的 G-085（`points_at` 的 `drop_nulls().last()` + `tail(8)` 无时效上限，把 ≤2009 年财报值前推为当期值）一旦修，`accruals`/`roe_stability`/`sue` 与 26 因子池的 9 个字段都会变，属于「改变既定口径且影响已发布研究结论」——必须先出实测差异再决定，不得自行改。

**验收**：`st_intervals` 与 `st_status` 快照对照 + 人工覆盖逐条留证；`breadth_ma_daily` 与 `breadth_daily` 逐日对拍；`industry_pit` 与 `industry_snap` 对拍。

**下一批解锁条件**：ST 区间对拍通过（含覆盖层留证）。

---

## B5 · 深度 4：四次派生（2 张）

**包含表**：`st_coverage`、`state_vector_daily`（G-081/G-082）。

**并行**：两张无相互依赖，可并行。

**本批要收口的账**：
- `st_coverage` 写回裁定时必须带兜底分支（DG-31），断言必须能对「空值从 0 变 N」失败。
- `state_vector_daily` 杠杆位 2,203 行变化、分母 Σ`amount` +2.24% 归因未闭环（G-082）——先归因再动数；两融缺 11-11 沪深各 1 行（G-081）先补。
- 一次性补丁 `_staging/state_margin_patch_20261112/patch_lev_margin*.py` 改过什么，必须写进登记。

**验收**：
- `state_vector` 的日期域与「个股 bar 日期并集 ∪ 指数日历外单日」对齐（当前 8,825 = 8,824 ∪ {1992-04-18}）。**不得以 `meta/daily_index.parquet` 行数作交易日数基准**——它是指数名录（`ts_code/data_type/market/rows/start/end`），不是日历。日历口径取 `quotes/index/sh/000001.SH.parquet`（当前 8,761 个交易日）；与 8,825 的 64 天差额全部落在 1991-01~1993-08 周末开市段（58 个周六 + 6 个周日），属口径差而非缺失。
- `lev_margin` 列改动前 null 数、改动后 null 数、未命中行数三项同时给出。
- `_q` 列**不是**「原值非空即非空」，必须与表内 `quantile.window_trading_days=1250 / min_valid_in_window=1250` 规则对齐：`lev_margin_buy_ratio` 原值非空 4,035 天，`lev_margin_buy_ratio_q` 非空 2,786 天 = 4,035 − 1,249（前 1,249 个有效观测因窗口未满写 null）。验收时两列非空数分别给出，禁止用「4,035 天全部带 `_q`」这类与窗口规则相矛盾的表述。

**下一批解锁条件**：G-082 归因闭环。

---

## B6 · 深度 5：结论层（1 张）

**包含表**：`universe_layers`（l0..l5，各 18,922,964 行）。

**本批不是重算，是决策**。三选项与建议见 `lineage_report.md` 第三节：**推荐 C（维持现状 + 登记为研究资产，写明与 `universe_filter.py` 的三条差异）**。

- 若选 C：只需改登记语义，数据不动；解锁条件 = 三选项定案。
- 若选 A（接线）：必须先统一三条规则（流动性 2.0e7 vs 1.0e7、面值 1.5、20 日停牌 >5 天），再改生产读路径；成本远高于 C。
- 若选 B（退役）：移 `_backup` + 改权威清单，需用户点名到目录才动手（红线）。

**验收（若重算）**：以 `universe_filter.py` 为基准重跑三日期实测，Jaccard 必须上升且差异可 100% 归因；当前基线 0.9846 / 0.8956 / 0.9012（`outputs/universe_consistency.json`）。

---

## 全局约束（每批都适用）

1. **同一张表不得被两条任务并行写**。同批并行只在不同表之间成立。
2. **`_staging/<任务>_<日期>/` 必须同时有 `progress.*` 与 `failed.csv`**，否则不算收口（DG-7）。
3. 覆盖正式文件只走 `write_parquet_atomic` / `write_text_atomic`（DG-9）；替换派生目录前先整体移 `_backup`（DG-11）。
4. 每批收口按 DG-20 四项给证据：行数守恒 / 逐值对拍（含既有非空值改动数 0）/ 受影响文件 SHA256 / 抽样断言；另加一条真实读路径（DG-23）。
5. 任一验证不通过先停后报，不得放宽断言或下调阈值（DG-26）。
6. 本文件只排顺序，不改任何数据与登记；`docs/数据权威路径清单.md` 的修改不在本任务范围内。
