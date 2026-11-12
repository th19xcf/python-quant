# B2 片4 只读体检（daily_index / ann_date_map / listing_intervals / name_history / stock_adj_factor）

- 体检日期：2026-11-13
- 依据：`2026-11-12/data-lineage/outputs/governance_order.md` B2 段（L94-L114）
- 性质：**只读**。未修改 `data/local_store/` 任何文件、未改 manifest、未提交 Git、未调用任何外部采集
- 复算脚本与原始输出：`2026-11-12/data-lineage/work/b2s4_*.py`、`b2s4_*.out.txt`

## 0. 范围、方法与结论概览

### 0.1 取表与不重叠依据

B2 共 15 张（`governance_order.md` L96）。已被覆盖 10 张，本片取剩余 5 张：

| 片 | 报告 | 表 |
|---|---|---|
| 片1 | `b2_slice1_check.md` §0 | suspension_daily、implied_suspension、universe_price_jumps、anomalies |
| 片2 | `b2_slice2_check.md` §0.1 | futures_multi_asset、etf_equity_pool、membership_intervals、breadth_daily、fund_adj_factor |
| 片3 | `b2_slice3_check.md` | stock_turnover |
| **片4（本片）** | 本文件 | **daily_index、ann_date_map、listing_intervals、name_history、stock_adj_factor** |

4+5+1=10，15−10=5，与 B2 清单并集恰为全部 15 张、无越界到 B3。

### 0.2 每张表的「五检」

① 正式路径唯一性 + manifest 一致性；② 写入口与依赖边可核；③ 与上游逐值/逐行对拍；④ 同语义是否另有第二份；⑤ 权威路径清单登记是否与实测一致。

### 0.3 结论概览

| 表 | 行数（实测） | 对拍 | 判定分层 |
|---|---|---|---|
| daily_index | 9,787 | rows/start/end **0 不符**；`file_size` 252 行不符 | 数据无错；**登记陈旧**（file_size 列） |
| ann_date_map | 427,685 | 与权威清单逐字一致（含 SHA256） | 数据无错；登记准确 |
| listing_intervals | 9,761 | 由唯一输入重建应为 **9,787**（差 26）；9,406 行 `end`、9,407 行 `rows`、75 行 `start` 陈旧 | **数据本身错**（派生表落后），B2 验收「对拍通过」不成立 |
| name_history | 14,028 | = 缓存 13,940 + 88 推断段；缓存独有 0 | 数据无错（2 只退市股缺段 → 停手项）；登记准确 |
| stock_adj_factor | 18,129,892 | 与 quotes 逐只一致；步长 58,299 条 **100%** 被事件解释；未应用 49 条逐条可解释 | 数据无错（G-107/G-122 已闭合）；**登记陈旧** 1 处（行数差 8） |

---

## 1. daily_index（G-045）

### 1.1 事实

| 项 | 值 |
|---|---|
| 路径 | `data/local_store/meta/daily_index.parquet`（权威清单 L73） |
| 行数 / 唯一 ts_code | 9,787 / 9,787（(ts_code,data_type) 亦 9,787，无重复） |
| 类型分布 | stock 5,832 / fund 2,620 / index 1,335 |
| 市场分布 | sh 4,822 / sz 4,606 / bj 359 |
| 字节 / SHA256 | 143,687 B（manifest 12 条记录、0 不一致） |
| mtime | 2026-11-13T00:40:41 |

### 1.2 逐行对拍磁盘（G-045 的 11 行问题）

对全部 9,787 行按 `quotes/<data_type>/<market>/<ts_code>.parquet` 重取行数 / 首末 bar / 文件字节：

| 比对项 | 不符行数 |
|---|---|
| `rows` | **0** |
| `start` | **0** |
| `end` | **0** |
| `file_size` | 252（fund 247：sz 212、sh 35；stock 5） |

**G-045 判定：已关闭，既不是 manifest 陈旧也不是文件又变了。** 登记状态「已修（`派生层追赶-20261112`）、实测 9,787 条差异 0」与实际完全一致。决定这一点的是 `logs/data_changelog.csv` 记录：`2026-11-13T00:40:41 / task=fund_factors / update meta/daily_index.parquet` —— 该表在基金因子任务里被整表重写，重写后三列与磁盘逐行一致。

### 1.3 唯一残留：`file_size` 列陈旧（登记陈旧，非数据错）

- 252 行 `file_size` 与磁盘实际字节不等，差额 **−2,685 ~ +611 B**（fund-sz 212 / fund-sh 35 / stock-sh 4 / stock-sz 1）；5 只个股差额仅 1~6 B（`000725.SZ` 175,122→175,125、`600157.SH` 184,775→184,778、`600221.SH` 166,403→166,405、`601288.SH` 99,239→99,240、`601988.SH` 124,359→124,365）
- 因 `rows`/`start`/`end` 全对，说明是文件被**原地重写（列或值变动、行数不变）**，例如基金 `pre_close` 派生列重算；`refresh_meta` 只在这些文件被本任务命中时才刷新 `file_size`
- 消费方：`tools/lineage/build_inventory.py:105` 把 `daily_index.file_size` 当行情族字节规模累加 → 仅影响血缘清单的 `bytes` 汇总，**不影响任何取数、回测、因子、宽度计算**
- 修法建议（留给后续任务，本片不执行）：对 252 只重跑一次 `refresh_meta`（写回后须同步 `meta/daily_index.manifest.json`）。影响面：1 个 parquet 文件的 1 列、252 行；可回滚（改前 `copy2` 到 `_backup/`）

---

## 2. ann_date_map（G-045 同条）

### 2.1 事实与登记一致性

| 项 | 权威清单登记 | 实测 | 一致 |
|---|---|---|---|
| 行数 | 427,685 | 427,685 | ✅ |
| 字节 | 4,179,968 | 4,179,968 | ✅ |
| SHA256 | f686a8d7… | 同 | ✅ |
| symbol 数 | 11,695 | 11,695 | ✅ |
| 期别 | 2010-03-31~2026-09-30，67 期 | 2010-03-31~2026-09-30，67 期 | ✅ |

### 2.2 结构校验

| 校验 | 结果 |
|---|---|
| `(period,symbol)` 重复 | 0 |
| `ann_date < period`（先披露后报告期，不成立） | 0 |
| `update_date < ann_date` | 0 |
| `ann_date` 范围 / `update_date` 范围 | 2010-04-08~2026-11-04 / 2010-04-21~2026-11-04 |
| `parent_net_profit` 空值 | 81 |
| 每期标的数 | min 2,312 / median 5,888 / max 11,589 |

### 2.3 两处需要写明「不是错」的现象

1. **期内标的数呈锯齿**：2026-03-31 = 5,913、2026-06-30 = 11,435、2026-09-30 = 5,769。差额来自新三板代码（83/87/43/40 前缀，全表 83 段 74,989 行、87 段 44,277 行、40 段 14,298 行）——挂牌公司披露半年报与年报、一季报/三季报非强制。**口径事实，不是缺漏。**
2. **81 行 `parent_net_profit` 为空**：属源端未给出归母净利润的个别记录。补它需要重新采集财务接口（外部条件），本片不判定、不补。

### 2.4 判定

**数据本身无错；登记准确。** 无 G-045 遗留问题。G-045 所指的 11 行差异只涉及 `daily_index`，不涉及本表。

---

## 3. listing_intervals（B2 验收项「对拍通过」当前不成立）

### 3.1 事实

| 项 | 值 |
|---|---|
| 路径 | `data/local_store/meta/universe/listing_intervals.parquet`（权威清单 L29） |
| 行数 / 唯一 ts_code | 9,761 / 9,761（登记 9,761，一致） |
| 类型分布 | stock 5,812 / fund 2,616 / index 1,333 |
| listing_status | active 9,019 / delisted_or_stale 742 |
| first_trade_date / last_quote_date | 1990-12-19~2026-11-04 / 2019-07-03~2026-11-06 |
| mtime | 2026-11-12T20:51:32 |
| manifest | `meta/universe/manifest.json` 6 条、0 不一致 |

### 3.2 对拍：落后于唯一输入 daily_index

`src/data/quality/listing_intervals.py` 的输入只有 `daily_index` + `stale_status`。以当前 `daily_index` 重建：

| 比对 | 结果 |
|---|---|
| 重建行数 | **9,787**，磁盘 9,761，**缺 26 行** |
| 共有 9,761 行中 `rows` 不等 | **9,407** |
| 共有 9,761 行中 `start` 不等 | 75 |
| 共有 9,761 行中 `end` 不等 | **9,406**（旧值 2026-11-05 占 9,017 行、2019-09-17 占 226 行；新值 2026-11-11 占 9,085 行） |

缺的 26 行：

- 个股 20 只：`000024.SZ`、`000033.SZ`、`000511.SZ`、`000562.SZ`、`000594.SZ`、`000693.SZ`、`000916.SZ`、`000979.SZ`、`002070.SZ`、`300186.SZ`、`300372.SZ`、`600270.SH`、`600401.SH`、`600432.SH`、`600656.SH`、`600680.SH`、`600806.SH`、`601268.SH`（18 只已退市）+ `688827.SH`、`920085.BJ`（新补行情）
- 基金 4 只：`158076.SZ`、`158081.SZ`、`508081.SH`、`561970.SH`
- 指数 2 只：`000922.SH`、`000985.SH`

**根因链（可核）**：quotes 更新（退市补数 + 11-09/11-10/11-11 增量）→ `fund_factors` 任务于 `2026-11-13T00:40:41` 整表重写 `daily_index`（`logs/data_changelog.csv`）→ `listing_intervals` 仍停在 `11-12 20:51` 的快照，未跟随重建。

### 3.3 影响面（数据本身错）

- `listed_codes(as_of=2026-11-06..2026-11-11)` 缺 26 只：新上市的 `688827.SH`、`920085.BJ` 与 4 只新基金、2 只指数完全不可见
- 9,017 只 active 标的的 `end`/`last_quote_date` 提前 4 个交易日（11-05 而非 11-11）；226 只停在 2019-09-17（新值 2020-12-31）
- 下游实测（已验证，避免夸大）：`tools/build_breadth_ma.py:26/157` 只用本表的 `first_trade_date` 作「上市满 60 交易日」回退，不按 `last_quote_date` 出分母；`breadth_ma_daily` 2026-11-05→11-11 的 `sample_size` 为 5,527→5,533，未见分母塌陷 → **本项陈旧对宽度表无可见影响**
- 真实影响面：`tools/build_universe_layers.py:85` 把本表当上游依赖（含 SHA256 指纹）并用于退市剔除/分层；任何按 `last_quote_date >= as_of` 判在市的消费方在 2019-09-18~2020-12-31 与 2026-11-06~11-11 两段会漏掉在市标的

### 3.4 结构问题：依赖方向与治理分层倒挂

- `listing_intervals` 属 **B2（深度 1）**；其上游 `stale_status` 属 **B4（深度 3）**（`governance_order.md` L141）
- 写入口实际读了更深一层的表：`src/data/quality/listing_intervals.py` 读取 `meta/universe/stale_status.parquet`；而 `stale_status` 自身由 `daily_index` 派生（`src/data/quality/stale_status.py:80`）
- 与 `governance_order.md` L94「B2 · 深度 1：一次派生」的表述冲突：B2 并非只依赖 B1，`listing_intervals → stale_status → daily_index` 构成 B2←B4 的反向边
- `stale_status` 现状：9,761 行、mtime `2026-11-07T17:13:44`、active 9,019 / delisted_or_stale 742 —— 同样落后 `daily_index`（9,787）

### 3.5 修法建议（本片不执行）

顺序不可颠倒：

1. **先重建 `stale_status`（B4）**：补齐 26 只、把 9,787 只重新判陈旧
2. **再重建 `listing_intervals`**（`build_listing_intervals`），整表替换前按 DG-11 把现有文件移入 `_backup/meta/universe/listing_intervals_<日期>/`，走 `write_parquet_atomic`

**为什么不能只做第 2 步**：`listing_intervals.py:50` 以 `index.join(status, on='ts_code', how='left')` 左连 `stale_status`，第 60-62 行对缺失行执行 `is_stale.fill_null(False)`、`action.fill_null('keep')`。缺 26 行时其中 18 只已退市个股（`000024.SZ` 等）会被判成 `active/keep` 进入在市全集——这会把「滞后」升级为「错判」。因此本项属于**跨批依赖**，B2 自身无法闭合，需 B4 先行。

影响面：`meta/universe/listing_intervals.parquet` 全表 9,787 行（新增 26 行、更新 9,406 行 `end`/`rows`、75 行 `start`）+ 同目录 manifest；`tools/build_universe_layers.py:85` 的上游指纹失效需重建分层表，`breadth_ma_daily` 只用 `first_trade_date` 可不必重跑。可回滚（`_backup/` 快照 + SHA256）。

---

## 4. name_history

### 4.1 事实

| 项 | 值 |
|---|---|
| 路径 | `data/local_store/meta/name_history.parquet`（权威清单 L13） |
| 行数 / 代码数 | 14,028 / 5,926 |
| source | `tushare_namechange` 13,940 + `inferred:strip_G` 88 |
| end_date 空（开区间段） | 5,923 行 / 5,923 代码（每代码至多 1 条） |
| start_date 范围 | 1990-12-01 ~ 2026-11-11 |
| SHA256 | `7e29fdaa…`，与 `name_history_coverage.json` 的 `target_sha256` 逐字一致（该 json 记 rows 14,028 / codes 5,926 / open_rows 5,923 / raw_rows 14,262） |
| manifest | `meta/name_history.manifest.json` 2 条、0 不一致 |

### 4.2 可重建性（本次要判的核心问题）

**可由仓库内代码 + 本地缓存完整重建，无需联网。**

| 证据 | 结果 |
|---|---|
| 本地缓存 `tmp/st_namechange_raw/` | 3 个分片（5,000 + 5,000 + 4,262 = 14,262 行），`manifest.json` 标 `complete=true` |
| 缓存去重后 | 13,940 行 |
| 缓存独有（未进表） | **0** |
| 表独有（缓存无） | **88**，全部为 `inferred:strip_G`（由仓库内 `infer_post_g_segments` 生成，非源端数据） |
| 重建命令 | `build_name_history.py --offline`（脚本第 81 行支持只用缓存、不联网） |

结论：13,940 条源端事实 + 88 条仓库内推断 = 14,028，与磁盘逐条一致；**不存在只能用外部源才能复原的段**。

### 4.3 退市段（G-079 口径核对）

先纠正一处口径：`docs/数据缺口登记_正式表_2026-11-12.md` 中的 **G-079 是 `stock_turnover` 的 24 行缺口**（片3 已处理），与 `name_history` 无关。本表在缺口登记表中的唯一出现是 L232（G-045 所属的派生层追赶行，只说明 `name_history` 一并重写）。因此本片按「退市段能否由仓库内代码重建」独立判定。

| 校验 | 结果 |
|---|---|
| delisted_or_stale 个股 | 240 只 |
| 其中 `name_history` 有记录 | 238 |
| **无记录** | **2 只：`000043.SZ`、`300114.SZ`** |
| 二者是否在 tushare 缓存 14,262 行中 | **均不在** |
| 二者在 listing_intervals | `000043.SZ` 有行情段；`300114.SZ` 有行情段（2025-02 止） |

判定：这 2 只的**名称历史段无从重建**——本地缓存与表都没有、源端 `namechange` 未返回（二者属换码/吸收合并类标的）。补齐需要外部采集或代码变更证据，属停手项（见 §7）。

### 4.4 两处「不是错、但需知会」的现象

1. **238 只退市/停更股保留开区间段**（`end_date` 空）：符合本表规则「无后继段即开区间」，不是漏闭合。但消费侧若直接取 as-of 名称，会在退市后仍返回末代简称（如 `000004.SZ` 的 `国华退`）。建议消费侧按 `listing_intervals.last_quote_date` 截断，**不改本表**。
2. **231 处段间留空**，全部落在 `2006-10-09 ~ 2007-06-19`（G 段去前缀后的空档）：如 `000001.SZ` 前段 `end_date=2006-10-08`、后继段 `start_date=2007-06-20`。源端 `end_date` 原样保留（原有修补只覆盖 14 行），因此 as-of 查询在该窗口取不到名称。属**口径后果**而非算错；建议消费侧用「<= 目标日的最近段」兜底，不改本表。

### 4.5 判定

**数据本身无错（除 §4.3 的 2 只缺段，属外部条件）；登记准确；可离线重建。**

---

## 5. stock_adj_factor（G-107 已修 / G-112 待口径）

### 5.1 事实与 G-107 复验

| 项 | 值 |
|---|---|
| 路径 | `data/local_store/factors/stock_adj_factor.parquet`（权威清单 L74） |
| 行数 / 代码数 | **18,129,892** / 5,832 |
| 日期范围 | 1990-12-19 ~ 2026-11-11 |
| 列 | `ts_code, trade_date, qfq_factor, hfq_factor` |
| 重复 `(ts_code,trade_date)` / `<=0` / 空值 | 0 / 0 / 0 |
| qfq 末值 ≠ 1.0 / hfq 首值 ≠ 1.0 | **0 只 / 0 只**（复权锚点正确） |
| mtime | 2026-11-12T22:25:41（`suspension_quote_fix_20261112`） |

与 quotes 逐只对拍：5,832 只**全部**行数与首末日期一致（0 只不符）；抽样 33 只逐日日期集合差 0。

### 5.2 除权类验收：步长 ↔ 事件一一对位

B2 要求「除权类跑『除权参考价残差 = 0』」。按 G-122 的裁定，**不再使用** `docs/adj_factor独立源对拍_2026-11-11.md §14.1` 的「残差归零」判据（`quotes.pre_close` 本身是旧因子的函数，属循环论证），改做结构对位：`prev_bar < ex_date <= step_date`（窗口天然容纳停牌导致的「除权日无 bar、步长落到复牌首日」）。

| 校验 | 结果 |
|---|---|
| 因子步长总数 | 58,299 |
| 被分红事件解释 | **58,299（100%）** |
| **步长无对应事件** | **0** |
| 分红事件总数（去重 ts_code+ex_date） | 59,409 |
| 被步长消化 | 58,314 |
| 早于该股首根 bar（覆盖期外） | 1,032 |
| 未应用 | 49 |

49 条逐条判读，**无一条是漏除权**：

| 类别 | 条数 | 说明 |
|---|---|---|
| `ex_date >= 2026-11-12`（晚于因子表末日 2026-11-11） | 34 | 尚未到除权日，正常 |
| 零参数事件（`cash_div=0`、`share_div=0`、无配股） | 12 | 比率恰为 1.0，不应产生步长（1991-2001 早期记录） |
| `no_ex_rights=True` 标记 | 3 | `600225.SH 2021-12-22`、`603555.SH 2021-06-09`、`603603.SH 2022-12-23`，口径上不除权 |
| 配股价 = 前收（因子恰为 1.0） | 2 | `000822.SZ 2001-11-14`（前收/配股价均 10.0）、`600101.SH 2001-08-09`（均 15.0） |

### 5.3 三只「登记不改」标的全量复核（按已停处理）

| 标的 | 除权日 | 表内步长 | ex 前行数 | 登记（§14.1） | 一致 |
|---|---|---|---|---|---|
| `600872.SH` | 1995-05-10 | **1.674941452**（= 1.6 送转 × 1.046838 现金） | 70 | 70 行 | ✅ |
| `600305.SH` | 2008-05-30 | **1.006240916** | 1,720 | 1,720 行 | ✅ |
| `600989.SH` | 2020-06-04 | **1.036470817** | 257 | 257 行 | ✅ |

TDX `.day` 独立源（`H:\zxzq\vipdoc\{sh,sz}\lday\*.day`，`parse_day_file` 解析）核对：

| 标的 | TDX 除权日前收 = 本地 close | 因子步长隐含每股派现 | 对应口径 |
|---|---|---|---|
| `600872.SH` | 13.41 / 8.30 逐日相等 | 5.40375 | 1990s 送转与现金混算（登记口径差，物理不成立） |
| `600305.SH` | 11.77 相等 | 0.073 | `cash_div` 0.073 自洽；与「流通股 10 派 1.5596」差 0.083（B09 口径差） |
| `600989.SH` | 9.12 相等 | 0.32091 | `cash_div` 0.32091 自洽；与交易所摊薄 0.28 差 0.0409（0.46%，B09 口径差） |

判定：三只标的的**表内状态与登记完全复现**，TDX 原始 OHLC 证实前收基准一致，差异全部落在「分红口径」而非「因子算错」，维持登记「不改」。G-107 的修复未被本片推翻。

### 5.4 G-112 投影（不判定、不改）

因子表中不在指数交易日并集内的 bar：**32 行 / 4 只**（`000002.SZ` 8、`000004.SZ` 8、`000005.SZ` 10、`000012.SZ` 6），全部落在 1991-01~1993（深市早期周六/无休市 bar）。与 G-112「220 行非交易日 bar，待核实、需口径决策」同一口径问题在因子层的投影。按指示**不判定、不改**。

### 5.5 同语义第二份（DG-1 观察）

`quotes/stock/*.parquet` 自带 `qfq_factor`/`hfq_factor` 两列，与因子表同语义。抽样 33 只逐值比对：**差 0**。属设计冗余而非两套并存冲突（因子表的日期集由 quotes 决定）。建议在权威清单本行补一句「quotes 内因子列为本表的逐日投影、不得独立更新」，避免将来出现两份不一致。

### 5.6 登记陈旧（唯一）

权威清单 L74 记 **18,129,900 行**，实测 **18,129,892 行**，差 **8 行**。G-107 修后（18,129,870 → 18,129,892，+22）未回写该行数字，属**登记陈旧**，不涉数据。

---

## 6. 横向问题（跨表）

1. **依赖倒挂**：`listing_intervals`（B2/深度1）→ `stale_status`（B4/深度3）→ `daily_index`。B2 并非「只依赖 B1」，且导致 B2 无法独立闭合（§3.4/§3.5）。
2. **上游刷新无下游重建触发器**：`fund_factors` 于 00:40 整表重写 `daily_index`，`listing_intervals`、`stale_status` 均未跟随。凡写 quotes / daily_index 的任务，都应在收口清单里显式包含这两张派生表。
3. **权威清单行数回写滞后**：`stock_adj_factor` 差 8 行；`listing_intervals` 登记 9,761 虽与磁盘一致，但未标注「已落后唯一输入 26 行」。
4. **`daily_index.file_size` 无自动刷新机制**，却被血缘清单当字节规模消费（`tools/lineage/build_inventory.py:105`）。

---

## 7. 结论分层

### A. 数据本身错（可修，本片不执行）

| # | 对象 | 事实 | 修法方向 | 影响面 |
|---|---|---|---|---|
| A1 | `meta/universe/listing_intervals.parquet` | 落后唯一输入 26 行；9,406 行 `end`、9,407 行 `rows`、75 行 `start` 陈旧 | **先**重建 `stale_status`（B4）**再**重建本表；整表替换走 `_backup/` + `write_parquet_atomic` | 9,787 行全表；`build_universe_layers` 上游指纹需重建（宽度表不受影响，见 §3.3） |
| A2 | 同上（结构） | B2 依赖 B4，深度倒挂 | 治理顺序需修正（要么本表降到 B4 之后，要么把 `stale_status` 上提到 B2 之前） | 治理文档，非数据 |

### B. 登记陈旧 / 口径不同（不改数据，改文档）

| # | 对象 | 事实 | 处理 |
|---|---|---|---|
| B1 | `daily_index.file_size` 列 | 252 行与磁盘不符（−12~+439 B），rows/start/end 全对 | 重跑 `refresh_meta`；或明确该列仅供血缘清单、非权威 |
| B2 | 权威清单 L74 `stock_adj_factor` 行数 | 登记 18,129,900，实测 18,129,892（差 8） | 改文档 |
| B3 | `name_history` 退市股开区间段 238 只 | 表规则「无后继段即开区间」 | 不改表；消费侧按 `last_quote_date` 截断 |
| B4 | `name_history` 段间留空 231 处（均 2006-10-09~2007-06-19） | 源端 `end_date` 原样保留 | 不改表；消费侧用「<= 目标日最近段」兜底 |
| B5 | `ann_date_map` 期别锯齿 / 81 行归母净利空 | 新三板半年报年报披露节奏；源端缺项 | 不改 |
| B6 | `stock_adj_factor` 与 quotes 因子列同语义 | 抽样 33 只逐值差 0 | 权威清单补注「quotes 内因子列为本表投影」 |
| B7 | G-079 口径 | G-079 是 `stock_turnover` 缺口，与 `name_history` 无关 | 纠正引用，不改数据 |

### C. 停手项（需外部采集 / 改口径 / 重跑才能判定，本片未动）

| # | 事项 | 为什么停 |
|---|---|---|
| C1 | `name_history` 缺 `000043.SZ`、`300114.SZ` 名称段 | 本地缓存与表中均无，tushare `namechange` 未返回；补齐需外部采集或换码证据（外部条件 + 证据原则） |
| C2 | `ann_date_map` 81 行 `parent_net_profit` 空 | 需重新采集财务接口；且属「以外部源为准」的口径决策 |
| C3 | G-112 非交易日 bar（因子层 32 行 / quotes 层 220 行） | 「是否保留非交易日 bar」是既定口径变更，需实测证据后决策 |
| C4 | 三只标的口径差（600872 / 600305 / 600989） | 已按「登记不改」处理；若要改为交易所摊薄口径，属改口径，需先有实测证据 |

### D. 未做（明确声明）

- 未修改任何 `data/local_store/` 正式文件、未改 manifest、未写 `_backup/`、未提交 Git
- 未调用任何外部接口（含东财等已在限速的源）
- 未重建 `name_history`（按要求只判「能否重建」）
- 未对片1/片2/片3 已覆盖的其余 10 张 B2 表重复体检

---

## 8. 复算命令

```powershell
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b2s4_facts.py            # 五表基础事实
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b2s4_daily_index.py      # daily_index 逐行对拍磁盘
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b2s4_listing2.py         # listing_intervals vs daily_index
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b2s4_name_history.py     # name_history 连续性/缓存一致性/退市覆盖
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b2s4_adj_factor.py       # 因子规模/日期集/事件对位/三只标的+TDX
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b2s4_event_match.py      # 步长↔事件一一对位（停牌窗口）
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b2s4_report_facts.py     # 报告引用数字汇总
.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta --only factors   # 37 份 manifest / 144 条 / 0 不一致
```

原始输出：同目录 `b2s4_*.out.txt`（`b2s4_adj.out.txt`、`b2s4_event_match.out.txt`、`b2s4_name_history.out.txt`、`b2s4_report_facts.out.txt`、`b2s4_manifest.out.txt`）。

---

## 9. 执行记录（2026-11-13）

本节是片4 报告之后的三件后续动作：`name_history` 离线重建（已执行）、`listing_intervals` 重建执行卡（未执行，等 B4）、`daily_index.file_size` 252 行成因定位（只读）。除 9.1 外未改任何正式数据。

### 9.1 name_history 离线重建（已执行，DG-20 四项 + DG-23 端到端）

**入口与链路**（`tools/build_name_history.py --offline` 只做第一步，单跑会掉 88 条推断段，故按原三步链合成一次执行）：

```text
fetch_raw(tmp/st_namechange_raw, offline=True)          # 14,262 行，零联网
 → normalize()                    13,940 行（去重 322）
 → fix_name_history_end_dates.plan_fixes/apply_fixes     14 段补收尾
 → infer_post_g_segments.plan/apply                      88 条 inferred:strip_G
 → 14,028 行
```

- 改前快照：`_backup/meta/name_history_20261113/`（parquet + coverage + manifest，三件 `copy2` 后 SHA256 与源逐字节相等）
- 候选先落 `_staging/name_history_rebuild_20261113/name_history_candidate.parquet`，断言全过后才 `write_parquet_atomic` 提升
- 复算：`.venv\Scripts\python.exe _staging\name_history_rebuild_20261113\build_candidate.py` → `promote.py`

**DG-20 四项**

| 项 | 实测 |
|---|---|
| 行数守恒 | 13,940 + 88 = 14,028 = 重建前 14,028；`by_source={'tushare_namechange':13,940,'inferred:strip_G':88}`；`end_date` 空 5,923→5,923、`ann_date` 空 89→89 |
| 逐值对拍 | 7 列业务键 `(ts_code,name,start_date,end_date,ann_date,change_reason,source)` 多重集完全相等：候选独有 0 / 遗留 0；逐列「既有非空值改动数」与「非空→空」均为 0（`end_date`/`ann_date`/`change_reason`/`source`） |
| 受影响文件 SHA256 | `name_history.parquet` `7e29fdaa…` → `fd13515e95d0e07380ef726dee6dc8cdebc7e97024e5f65846b2d38dfa25467f`（193,518 B，字节数与改前相同，仅 `fetched_at` 变化）；`name_history_coverage.json` `945ceb4b…`；`name_history.manifest.json` `557b40ee…`；`meta` 全部 manifest `--check` PASS（35 份 / 133 条 / 不一致 0） |
| 抽样断言 | ①「缓存独有 0」成立（见逐值对拍）；② 88 条 `inferred:strip_G` 段代码集完全一致（88→88，差集空）；③ 已退市标的段数削减 0 只、末段开区间 331→331；④ 断言自测：植入 1 个 `end_date` null 后空值门按预期抛 `Fail`（证明断言能对「空值 0→N」失败） |

**DG-23 真实读路径端到端**（`src/data/name_consistency.py`）

```text
load_name_history(store) → 14,028 行 / 5,926 只 / max(fetched_at)=2026-11-13 01:54:27（只有新文件带这个时间戳）
name_at: 000001.SZ@2000-01-04=深发展A(tushare)  000002.SZ@2015-01-05=万科A(inferred:strip_G)
         600872.SH@2020-01-02=中炬高新            000003.SZ@2004-01-05=PT金田A   300750.SZ@2019-01-02=宁德时代
build_tables: names_json_vs_stock_basic 4 / name_history_vs_stock_basic 135 / list_date_vs_history_min_start 81
```

**口径修正（需回写正文数字）**：§4 写的「240 只 delisted 个股、238 只有段」偏低。按 `stock_basic.status='D'` 实测源端 336 只、进表 334 只，缺 `000043.SZ`/`300114.SZ`（源端缓存里也没有，按停手处理）。本表重建后这 334 只段数与末段开闭状态全部未变。

### 9.2 listing_intervals 重建执行卡（未执行，等 B4 交出 stale_status 体检）

| 项 | 内容 |
|---|---|
| 入口 | `src/data/quality/listing_intervals.py:71 write_listing_intervals(store_root)`（内部 `:41 build_listing_intervals`）；无 CLI，用一行 python 调 |
| 参数 | `store_root` 缺省 `DEFAULT_ROOT`；无其它开关 |
| 输入 | `meta/daily_index.parquet`（`:44-45`，主表）+ `meta/universe/stale_status.parquet`（`:46-50`，left join 取 `is_stale`/`action`/`last_trade_date`；文件缺失时 `:51-56` 一律给 `is_stale=False`/`action='keep'`） |
| 改前快照 | `_backup/meta/listing_intervals_<执行日>/`（当前文件 9,761 行，先 `copy2` 再写） |
| 预期行数 | **9,787**（= daily_index 行数）；当前 9,761，应新增 **26** 行 = 20 个股（18 只已退市 + `688827.SH`/`920085.BJ`）+ 4 基金 + 2 指数 |
| 验收断言 | 行数 9,787；26 行新增逐只可列；`end` 9,406 行、`rows` 9,407 行、`start` 75 行陈旧值全部改为磁盘值；18 只已退市 `listing_status` 仍为 `delisted_or_stale`（不得变 `active`）；同目录 `manifest.json` 与 `docs/数据权威路径清单.md` 行数同步 |
| 顺序刚性 | 必须 **先重建 stale_status（B4）→ 再重建本表**。只重建本表会把 18 只已退市股写成 `active`（`is_stale` 全 False 时 `:62-65` 一律给 `active`） |
| 不可做 | 不要「顺手」只补那 26 行：`end`/`rows`/`start` 三列陈旧同时存在，必须整表重建 |

### 9.3 daily_index `file_size` 252 行成因定位（只读，未重跑）

**事实**：252 行 `file_size` ≠ 磁盘；`rows`/`start`/`end` 三列 252 行全部与磁盘相等；总差 **+39,335 B**（登记值偏小）。分布 fund-sz 212 / fund-sh 35 / stock-sh 4 / stock-sz 1。

**分三批，均能落到「写盘之后再被旁路改写」**：

| 批次 | 行数 | `built_at` | 登记 `file_size` 的来源 | 磁盘现状 |
|---|---|---|---|---|
| A | 242（fund） | 2026-11-13 00:40:20 | `logs/data_changelog.csv` 中 `fund_factors` 同一秒写入的 `bytes` 字段，242/252 逐条相等 | 内容已回滚：247 只 fund 中 246 只与 `_backup/quotes_fund_20261113/` 的 size+sha **完全相同**；mtime 停在 11-10（`copy2` 保留），`ctime` 是 00:40:20 |
| B | 5（stock） | 2026-11-11 18:08~18:09 | 与 `_backup/quotes_stock_volume_fix_20261112/` 的 size **完全相同** | 磁盘为修数后的新版本，比登记值大 1~6 B |
| C | 5（fund） | 2026-11-11 18:08~18:09 | 同上（含 `163503` 命中 `_backup/quotes_fund_163503_20261112/`） | 同上 |

**归属判定：既不是 `manifest_refresh` 能力缺口，也不是 `daily_index` 写入口没回写**。

- `src/data/manifest_refresh.py` 从头到尾不处理 `daily_index`；`daily_index` 的 manifest 由 `store.py:555-577 _write_meta_manifest` 单独生成，按设计只登记 `rows/bytes/sha256/columns/coverage`，**没有 `file_size` 字段**，所以「manifest_refresh 不刷 file_size」是真的但不构成这条缺陷。
- `daily_index` 的两个写入口都在写盘后 `stat()`：`store.py:494-534 write_daily` 的 `:529 'file_size': path.stat().st_size`；`store.py:311-370 rescan_meta` 的 `:365 'file_size': path.stat().st_size`。`flush_meta`（`:536-553`）只把内存 `_meta` 落盘，不做任何 stat。A 批 242 行的登记值与 changelog 当次 `bytes` 逐条相等，说明写入口取的是正确的新值。
- 真正的成因是**第三类：旁路改写未回写**——文件在 `daily_index` 落盘之后又被改了一次，而这次改写没有经过 `write_daily`/`flush_meta`：A 批是快照回滚（裸 `copy2`，连台账都不落一行、mtime 还被保留成 11-10，所以任何基于 mtime 的新鲜度检查都看不出来），B/C 批是修数脚本直接 `write_parquet_atomic` 覆盖行情文件、没有同步索引。

**修法建议（不在本任务执行）**：① 给每类旁路改写（回滚 / 修数 / 因子副本对齐）在收尾加一次 `file_size` 单列校准——或新增 `rescan_file_size()`，或给 `rescan_meta` 加「只刷 file_size」模式；② `flush_meta` 落盘前逐条 `path.stat()` 校准，成本一次 5,832 次 stat；③ 回滚通道改走 `write_daily`，禁用裸 `copy2` 覆盖正式行情；④ 加一条巡检断言：`daily_index.file_size == path.stat().st_size` 全表 0 不符。**影响面**：只有 `tools/lineage/build_inventory.py:105` 累加 `file_size` 作行情族 bytes，当前少算 39,335 B（约 0.04 MB），不进入任何取数/回测/展示链路。

### 9.4 深度倒挂缺口条目（文案，待单写者入册）

```text
G-XXX  listing_intervals(B2/深度1) 反向依赖 stale_status(B4/深度3)，治理顺序倒挂
现象：listing_intervals 的 listing_status 由 meta/universe/stale_status.parquet 的 is_stale/action 决定，
      而 stale_status 自身由 daily_index 派生、在治理顺序中被列为 B4（深度 3）。
      治理顺序 L94 声明「B2 只依赖 B1」，与本表的真实上游不符。
代码：src/data/quality/listing_intervals.py:46 读 meta/universe/stale_status.parquet
      :50 frame = index.join(status, on='ts_code', how='left')
      :60-62 is_stale.fill_null(False)、action.fill_null('keep')，:62-65 据此判定 active/delisted_or_stale
      :51-56 stale_status 文件缺失时直接给 is_stale=False / action='keep'，即静默把全部标的判成在市
      src/data/quality/stale_status.py:78-88 build_stale_status 读 meta/daily_index.parquet(:82) 派生停更状态
影响：按 L94 先做 B2 时 stale_status 仍是旧版，is_stale 全 False 会让 18 只已退市个股被判成 active；
      只重建 listing_intervals 而不先重建 stale_status，等于把错判写进正式表，且 B2 验收「对拍通过」不成立。
      当前表 9,761 行、按唯一输入应为 9,787 行（缺 20 个股 + 4 基金 + 2 指数），另 9,406 行 end、9,407 行 rows、
      75 行 start 陈旧（根因：fund_factors 2026-11-13 00:40:41 重写 daily_index 后未重建本表）。
修法：顺序刚性——先重建 stale_status（B4 深度 3）→ 再重建 listing_intervals（B2 深度 1）；
      并把 governance_order L94 的「B2 只依赖 B1」修正为「B2 的 listing_intervals 依赖 B4 的 stale_status」，
      或在顺序表里把 listing_intervals 迁到 stale_status 之后。反向操作不可接受：先 B2 后 B4 会把 18 只退市股写进在市。
状态：未修（等 B4 交出 stale_status 体检结论后按上述顺序执行）。
```

