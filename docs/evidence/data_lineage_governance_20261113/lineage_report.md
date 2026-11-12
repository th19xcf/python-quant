# 表级数据血缘、分层与治理顺序（2026-11-12）

纯只读分析。未改 `data/`、未改代码、未提交、未镜像。

产物目录：`2026-11-12/data-lineage/`

| 文件 | 内容 |
|---|---|
| `outputs/lineage.json` | 机器可读：51 个语义表 × 层/定性/写者/读者/派生边/反向依赖/零消费者证据 |
| `outputs/table_inventory.csv` | 表清单（语义、层、定性、路径、行数、写入口、派生自、登记、重建、第二份、对拍、缺陷、G 编号） |
| `outputs/base_layer_calibration.csv` | 基础层 + 混合类逐表校准清单（24 行） |
| `outputs/zero_consumer_evidence.{md,json}` | 「零消费者」四通道取证原始命中 |
| `outputs/universe_consistency.json` | 分层 vs `universe_filter` 三日期实测集合差 |
| `outputs/universe_divergence_attribution.json` | 差异归因（差异量 100% 落到具体层） |
| `outputs/governance_order.md` | 施工顺序（按批，可直接执行） |
| `work/*.py` | 全部可复现脚本 |

---

## 一、结论摘要

1. **物理表 90 个（86 张非单票 parquet + 4 个行情族共 10,784 个分片），归并为 51 个语义表**：基础层 20、派生层 27、混合类 4（既落地又参与推导）。
2. **`meta/universe/layers/l0..l5` 六张表（各 18,922,964 行）在生产链路零消费者**，四通道取证一致；唯一读它的是它自己的构建器。但它与 `universe_filter.py` **不是死代码关系，而是同一套规则的第二份实现，且已经分叉**（见第三节）。
3. **`state_vector_daily` 同为零生产消费者**：写入口 1 个常规 + 3 个一次性脚本，读者只有 `tools/verify_state_vector.py` 与构建器自身；`src/`、`tests/` 均无读取。
4. **跨层反向依赖 1 处**：`tools/build_stock_basic.py:205-210` 把 `breadth_daily` 当交易日历 —— L1 名录依赖 L2 派生宽度表。这是治理顺序必须前置处理的唯一硬依赖。
5. **32 张正式表/表族被 `_staging/` 一次性脚本直接覆盖写过**（高置信路径匹配）。这是比「多写者」更普遍的结构风险：正式资产的最近一次变更往往无法由 `tools/` 复现。
6. **`implied_suspension`、`st_raw`、`st_status`、`industry_snap`、`etf_fee`、`etf_equity_pool` 未定位到常规写入口** —— 属「只能靠一次性脚本或人工更新」的不可复现资产。
7. 未改任何口径、未下任何「哪边错」的判定；所有差异只做归因，留待对应批次处理。

---

## 二、表清单（按层）

| 层 | 语义表数 | 代表表 |
|---|---|---|
| L0 原始采集 | 20 | `quotes/{stock,index,fund,futures}`、`valuation_daily`、三大报表、`stock_dividend`、`stock_shares`、`stock_turnover`、宏观五表、`events`、`st_raw`、`st_status`、`industry` 快照 |
| L1 基础名录/日历 | 5 | `stock_basic`、`name_history`、`listing_intervals`、`daily_index`、`code_migration` |
| L2 价格派生/因子 | 12 | `stock_adj_factor`、`fund_adj_factor`、`breadth_daily`、`breadth_ma_daily`、`industry_pit*`、`membership_intervals`、`etf_pool`、`futures_*` |
| L3 状态判定 | 11 | `suspension_daily`、`implied_suspension`、`st_*`、`universe_exclusions`、`stale_status`、`state_vector_daily`、`anomalies` |
| L4 universe 分层 | 1（6 文件） | `layers/l0..l5` |
| L5 研究/回测输入 | 2 | `analysis_monthly`、`new_factors` |

完整字段见 `outputs/table_inventory.csv`。行数、字节、manifest 状态来自 `work/inventory_full.json`（86 张表逐一读 parquet footer 元数据）；行情族行数来自 `meta/daily_index.parquet` 逐票聚合。

### 定性判据（可逐条核对）

- **基础层 `base`**：内容由外部源或本地原始行情直接落地，**不读任何其他正式表**。核对方法：`outputs/lineage.json` 中该表 `derived_from == []`。
- **派生层 `derived`**：`derived_from` 非空，且每条边上都写了具体代码位置（构建器路径）。例：`stock_adj_factor` ← `quotes_stock` + `stock_dividend`，代码 `src/data/local_store/build.py:977`、`src/data/local_store/factor_rebuild.py`。
- **混合类 `mixed`（4 张，单列不硬塞）**：
  1. `stock_basic`：整体由行情首末行推导，但把 `breadth_daily` 当日历读入（反向依赖）。
  2. `stock_shares`：外部股本落地 + 本地 `free_float` 推导补全（`tools/derive_free_float_fill.py:225`）。
  3. `stock_turnover`：外部 baostock 换手 + `stock_shares.float_share` 推导。
  4. `st_intervals`：证据推导 + **人工裁定覆盖层**（无法由代码重建）。

---

## 三、`layers/l0..l5` 零消费者：四通道取证

### 通道 1 — 路径常量/路径解析入口

```
git grep -n -E "universe/layers|l[0-5]_(all_history|listed_120d|tradable|risk_excluded|liquid|pool)" -- src tools tests research
```
结果：命中 2 个文件 —— `tools/build_universe_layers.py`（构建器自身）、`src/data/manifest_semantics.py`（仅为 manifest 写文案的字符串表，无读盘）。`src/` 生产链路 0 命中，`tests/` 0 命中。

路径解析入口 `src/data/local_store/store.py` 无 `layers` 键；`src/data/quality/freshness_config.json` 19 个数据集无 `universe_layers`。

### 通道 2 — 文件名片段 + 通配/rglob

对 `l0_all_history / l1_listed_120d / l2_tradable / l3_risk_excluded / l4_liquid / l5_pool / universe/layers / state_vector_daily` 八个片段，全扫 `src tools research tests docs config 2026-11-12 _staging`（跳过 `ci_cov_sim_*` 仓库副本与 `data/local_store` 二进制）：

| 片段 | 代码引用文件数（src/tool/research/test） | 其中 `src/` | 文档文件数 |
|---|---|---|---|
| `l5_pool` | 1 | 0 | 1 |
| `l4_liquid` | 1 | 0 | 1 |
| `l3_risk_excluded` | 1 | 0 | 1 |
| `l2_tradable` | 2 | 0 | 2 |
| `universe/layers` | 2 | 0 | 1 |
| `state_vector_daily` | 2 | 0 | 3 |

命中明细（去除构建器自身与文档）：
- `_staging/derived_1993b_20261113/layer5_apply.py`、`layer4_apply.py`、`layer2_apply.py` 等 —— 2026-11-11 两轮重建用的一次性脚本，属「生成侧」而非「消费侧」。
- `_staging/derived_catchup_20261112/downstream_read.py` —— 一次性下游读验证。
- `docs/股票池分层矩阵与缺失口径_2026-11-11.md` 等 3 份文档 —— 文档提及，非代码读取。

### 通道 3 — manifest 的 `sources` 字段

扫全部 10,821 份 `*manifest*.json`：把 `l0_all_history…l5_pool` 登记为 `sources` 的 manifest **0 份**；`state_vector_daily` 同样 **0 份**。即没有任何表声明以它们为上游。

### 通道 4 — 间接消费（工具/校验脚本 vs 生产链路）

严格分开两件事：

| | `layers/l0..l5` | `state_vector_daily` |
|---|---|---|
| `src/` 生产链路读取 | **0** | **0** |
| 常规 `tools/` 读取 | 0（构建器只写） | 1：`tools/verify_state_vector.py`（校验） |
| `tests/` 读取 | 0 | 0 |
| 一次性脚本读取 | 3（`_staging/derived_*` 的重建/比对脚本） | 2（`_staging/derived_catchup_20261112/downstream_read.py`、`_staging/state_vector_20261111/*`） |
| 结论 | **仅生成侧存在，消费侧为零** | **生产链路为零；仅校验脚本读** |

复现：`.venv\Scripts\python.exe 2026-11-12\data-lineage\work\evidence_zero_consumer.py`，原始命中见 `outputs/zero_consumer_evidence.json`。

### 与 `universe_filter.py` 的逐条对照

两者文档自述同源：`build_universe_layers.py:12-16` 写明 L5 的板块/旧码规则「与 `src/data/universe_filter.py` 的 `UniverseFilterConfig` 默认值一致」。

| 规则 | `build_universe_layers.py`（L0–L5） | `universe_filter.py`（默认配置） | 是否一致 |
|---|---|---|---|
| 次新（上市天数） | `MIN_LISTED_TRADING_DAYS = 120` | `min_listed_days = 120` | ✅ 同 |
| 可交易 | 当日有 bar 且 `volume>0`、`amount>0`，且不在停牌并集 | 当日有 bar（`no_quote`），停牌按 20 日窗口 | ⚠️ 判据口径不同 |
| 停牌 | 只判「当日是否停牌」（`suspension_daily ∪ implied_suspension`） | 额外规则 `suspension_days`：20 日内停牌 >5 天剔除 | ❌ 仅 filter 有 |
| ST | `st_intervals` 区间覆盖当日 | `st_intervals` + 停牌并集 | ✅ 近似 |
| 退市整理 | `DELISTING_WINDOW = 30` 交易日 + 名称含「退」 | 同（30 日 + PIT 名称） | ✅ 同 |
| 流动性 | `ADV_WINDOW=20`，`MIN_AMOUNT = 2.0e7` | `min_amount = 1.0e7` | ❌ **阈值差 2 倍** |
| 面值 | 无 | `min_price = 1.5` | ❌ 仅 filter 有 |
| 板块 | `main / star / chinext` | `include_bse=False`、排除 B 股/老三板 | ✅ 近似 |
| 旧码 | `code_migration.superseded` 剔除 | 同 | ✅ 同 |
| 流通市值 | 无 | 规则存在但默认关闭（0 启用） | ✅ 实际一致 |

### 实测一致性（三日期，输入统一取 `l2_tradable`）

| 日期 | L2 | L4 | L5 | filter 保留 | 交集 | 仅 filter | 仅 L5 | Jaccard |
|---|---|---|---|---|---|---|---|---|
| 2026-11-11 | 5,494 | 5,055 | 4,930 | 4,967 | 4,910 | 57 | 20 | 0.9846 |
| 2024-06-28 | 5,290 | 4,429 | 4,397 | 4,789 | 4,340 | 449 | 57 | 0.8956 |
| 2021-06-30 | 4,140 | 3,441 | 3,438 | 3,798 | 3,430 | 368 | 8 | 0.9012 |

差异**归因（100% 可解释，无残余）**：

- 「仅 filter 保留」= 全部落在 `l3_risk_excluded - l4_liquid`，即**只被 L4 流动性挡掉**：57/57、449/449、368/368（`outputs/universe_divergence_attribution.json`）。对应阈值 2.0e7 vs 1.0e7。
- 「仅 L5 保留」= 全部落在 filter 的两条独有规则：2026-11-11 的 20 只 = `low_price` 17 + `suspension_days` 3。

**判定**：两套实现同源、差异可逐条归因，**不是随机分叉**；但三个参数/规则不同（流动性 2 倍、面值、停牌天数）已足以让同一 (标的, 日期) 在两边得出不同结论。2024-06-28 有 **10.4%** 的标的集合不同。

### 三选项与建议

| 选项 | 代价 | 回滚 |
|---|---|---|
| **A 接线**（生产读分层表） | 需先统一三条规则口径；`universe_filter` 现有 5 条独有规则要并入构建器；生产链路由「现场算」改为「读快照」，每次选股都要等分层重算（全量数十分钟） | 改回调用 `filter_universe`，分层表停更即可；分层表仍在磁盘 |
| **B 退役**（停更 + 登记，磁盘保留） | 放弃 2026-11-11 两轮重建成果；研究侧若有人用分层做样本切分将失去唯一来源 | 保留文件 + `_backup` 快照，随时可恢复更新；只改登记不改数据 |
| **C 维持现状但登记为研究资产**（推荐） | 需在权威清单里把六张表语义改为「研究用分层快照，非生产口径」，并写明与 filter 的三条差异；后续谁要用必须先对齐规则 | 纯文档/登记变更，回滚=删登记行；数据不动 |

**推荐 C**，理由：① 分层矩阵的唯一已知用途（2026-11-11 两轮重建）本身就是研究性口径核对，不是生产选股；② 接线（A）会把「现场算 3 秒」变成「等全量重算」，且要求先统一三条规则，成本远高于收益；③ 退役（B）会丢掉已有的 18.9M×6 行可复用快照。C 的硬要求：**必须把三条差异写进登记**，否则下一个人会再次误以为两边等价。

---

## 四、`state_vector_daily` 同处理

- **写入口**：`tools/build_state_vector.py`（常规，可反复跑）+ 3 个一次性补丁（`_staging/derived_1993b_20261113/layer4_build.py`、`_staging/state_margin_patch_20261112/patch_lev_margin*.py`）。
- **输入（代码内常量）**：`valuation_daily`、`quotes/index`（000300/000852）、`stock_shares`、`cn_market_margin_daily`、`cn_treasury_yield_10y`、`breadth_ma_daily`、`quotes/stock`。
- **读者**：`tools/verify_state_vector.py`（校验）、构建器自身。`src/` 0、`tests/` 0、`freshness_config.json` 未登记。
- **第二套等价口径**：`src/advice/` 下**没有**读取 `state_vector_daily` 或 `breadth_daily` 的模块（`git grep breadth src/advice` 零命中），即**当前不存在第二套等价实现**；宽度口径只存在于 `breadth_service` + `state_vector` 一条链上。

| 选项 | 代价 | 回滚 |
|---|---|---|
| A 接线（回测/建议读取 lev/margin 状态） | 需先在 `src/backtest/engine/input_contract.py` 增加列契约并跑端到端；一次性补丁造成的口径漂移需先确认（`patch_lev_margin*` 改了 lev 计算） | 删契约列即可，表不动 |
| B 退役 | 5 张上游表的联合快照唯一落点消失；`verify_state_vector` 失去对象 | 磁盘保留 + `_backup`，可重建 |
| **C 维持 + 登记为「已生产、待接入」**（推荐） | 需要补 `freshness_config.json` 登记与权威清单语义说明；并登记 3 个一次性补丁改过什么 | 纯登记变更 |

**推荐 C**，理由同上；额外一条：`state_vector_daily` 是**唯一**把估值/两融/国债/宽度拼在一起的表，退役会丢掉这个已建成的交叉视图，而接线的成本主要在回测契约侧，应作为独立议题而不是治理前置。

---

## 五、危险结构清单

### 5.1 跨层反向依赖（1 处）

| 下游 | 上游 | 证据 | 影响 |
|---|---|---|---|
| `stock_basic`（L1） | `breadth_daily`（L2） | `tools/build_stock_basic.py:205-210`，注释「交易日历取 breadth_daily」 | 宽度表重算/延迟 → `stock_basic` 变化 → `listing_intervals`/`universe_layers`/`state_vector`/研究面板全链重算 |

**处置**：批 2 第一件事改为从 `quotes/stock` 的日期并集（或 `daily_index`）取日历，切断反向边；改完对 `stock_basic` 逐值对拍（除日历来源外必须 0 改动）。

### 5.2 多写者（同名表 ≥2 个常规写入口）

| 表 | 写入口 | 风险 |
|---|---|---|
| `fund_adj_factor` | `src/data/local_store/fund_factors.py:469`、`tools/fund_split_repair.py:357` | 拆分修正脚本会整体改写，与常规因子计算口径可能不同 |
| `stock_shares` | `src/data/local_store/share_capital.py`、`tools/derive_free_float_fill.py:225`、`tools/backfill_share_div.py`、`tools/derive_total_dividend.py` | 同一文件被 4 个入口写，`free_float` 与 `total_share` 来源不同 |
| `events` | `src/data/events.py`、`tools/backfill_events.py`、`tools/update_events.py` | 追加 vs 重建混用，易产生重复行 |
| `stock_dividend` | `src/data/local_store/build.py:979`、`tools/collect_stock_dividend.py`、`src/data/factors_manifest.py` | 采集与修正两层（raw+fix）必须区分（DG-30） |
| `universe_exclusions` | `src/data/quality/tradable_universe.py:978`、`tools/fill_internal_gaps.py` | 补数工具会整体重写 |

### 5.3 一次性脚本直写正式表（32 张）

高置信统计：`_staging/` 下脚本以 `write_parquet_atomic` 直接写 `data/local_store/**` 正式路径的表/表族共 **32** 个，例：`fund_adj_factor`（5 个脚本）、`daily_index`（5）、`events`（4）、`cn_market_margin_daily`（4）、`state_vector_daily`（3）、`stock_basic`（3）、`valuation_daily`（3）。

判定：这些表的「当前内容」无法仅由 `tools/` 复现，属 DG-32/DG-7 意义上的**不可复现资产**。治理顺序里必须为每张表补一个常规入口，或把它降级为「快照型资产」并登记。

### 5.4 未定位常规写入口（6 张）

`implied_suspension`、`st_raw`（9 件证据）、`st_status`（7 份快照）、`industry_snap`（7 份快照）、`etf_fee`、`etf_equity_pool`。其中 `st_status`/`industry_snap` 的日期文件名多版本并存，已有 G-098 记录。

---

## 六、施工顺序（按上游优先，按批）

详见 `outputs/governance_order.md`。排法不按严重度，按**血缘深度**：82 条派生边全部满足 `depth(上游) < depth(下游)`（违反 0 条），因此同深度表之间无派生边，可并行。

| 批 | 深度 | 表数 | 内容 | 为何这一批能并行 | 下一批解锁条件 |
|---|---|---|---|---|---|
| P0 | — | 0（改代码） | 切断 `stock_basic ← breadth_daily` 反向边 | 单点，串行 | `stock_basic` 改造后逐值对拍 0 改动 |
| B1 | 0 | 21 | 基础落地：行情四族、估值、三表、名录、分红、股本、宏观、外部快照、人工补充 | 21 张两两之间 0 条边 | 每张表有可反复跑入口 + manifest 齐全 |
| B2 | 1 | 15 | 一次派生：日历/名录区间/复权/宽度/成分/停牌/换手/质量检查 | 只依赖 B1，彼此 0 条边 | `daily_index` 元数据修正 + `listing_intervals` 对拍 |
| B3 | 2 | 7 | 二次派生：`stock_basic`、`code_migration`、基金除权、ETF 池、ST 价格推断、可交易剔除、月度面板 | 只依赖深度 0–1，彼此 0 条边 | 名录与旧码对拍通过 |
| B4 | 3 | 5 | 三次派生：宽度均线、行业 PIT、ST 区间、陈旧判定、新因子 | 宽度/行业/陈旧可并行，ST 链串行 | ST 区间对拍（含人工覆盖层留证） |
| B5 | 4 | 2 | 四次派生：`st_coverage`、`state_vector_daily` | 两张互不依赖 | G-082 杠杆位归因闭环 |
| B6 | 5 | 1 | `universe_layers`（决策项，建议 C 维持+登记） | 单点 | 三选项定案 |

批内串行约束：`stock_shares`（4 写入口）、`stock_dividend`（raw+fix 两层）、`events`（3 写入口）、`fund_adj_factor`（2 写入口）必须一条任务写一张表；`st_intervals` 含人工覆盖层，改动需 `copy2` + 新旧 SHA256 + 逐条留证。

---

## 七、基础层校准清单（摘要，全表见 CSV）

`outputs/base_layer_calibration.csv` 共 24 行（20 基础 + 4 混合），字段：正式路径 / 写入口（常规 vs 一次性）/ 能否低成本重建 / 有无第二份同语义 / 已登记权威路径 / 与权威源对拍 / 当前已知缺陷 / 关联 G / 已在查。

要点：

- **第二份同语义**：24 张全部有（`G:\git\python-quant` 镜像），无「单盘唯一」风险。
- **未登记权威路径**：0 张（全部在 `docs/数据权威路径清单.md` 有登记）。
- **对拍未记录**：`quotes_index`、`quotes_fund`、`fund_basic`、`index_basic`、`etf_fee`、`code_migration` —— 这 6 张只有「与派生侧一致」而没有与外部权威源的独立对拍记录。
- **已标「已在查」**：`quotes_stock`（非交易日 bar）、`stock_adj_factor`（600872/600305/600989 登记与实物不符）—— 引用他任务，本任务不重查。
- **不可低成本重建**：`st_raw`、`st_status`、`industry_snap`、`supplement_intervals`（人工证据）、`fund_basic`/`index_basic`（依赖外部库）。

---

## 八、未知项（不猜）

| 项 | 现状 |
|---|---|
| `implied_suspension` 的生成逻辑 | 仅存在于一次性脚本，脚本未入库；算法无法从正式代码复核 |
| `st_status` / `industry_snap` 的采集来源与写入工具 | 未定位常规入口；快照的日期覆盖为何是这 7 天，无记录 |
| `etf_equity_pool.csv` 写者为 `tools/build_etf_equity_pool.py` 还是仅一次性 | 未逐行核实 |
| `index_weight_monthly` 的拉取脚本 | 存在 `tools/pull_index_weight.py`，未核实其是否覆盖全部历史月份 |
| `events` 三条写入口的分工 | 追加/重建边界未在代码注释中说明 |

---

## 九、复现命令

```powershell
# 1. 表清单（行数/字节/manifest）
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\build_inventory.py

# 2. 代码读写点扫描（src/tools/_staging）
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\scan_code.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\resolve_edges2.py

# 3. 零消费者四通道
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\evidence_zero_consumer.py

# 4. 两套口径实测 + 归因
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\measure_universe_consistency.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\attribute_divergence.py

# 5. 汇总为 lineage.json 与两份 CSV
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\build_lineage.py

# 6. 批次顺序（拓扑深度 + 批内无依赖自证）
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\compute_depth.py
```
