# B5 + B6 只读体检（深度 4 两张 + 深度 5 一张）

- 体检日期：2026-11-13（数据实测时点 01:40~02:10）
- 依据：`2026-11-12/data-lineage/outputs/governance_order.md` L153-L181（B5 / B6 两段）
- 方式：**全程只读**。未改代码、未写 `data/local_store/` 任何文件、未改 manifest、未提交 Git；登记类缺陷只在本文件逐条列出建议，不动原文
- 不越界：B2 / B3 / B4 三段（另三路在做）本次未碰
- 两条已知陷阱的处理：
  1. `meta/st/st_price_inferred.parquet` 正被另一路重跑 —— 实测该件在 01:42:37 为 64,938 B、01:49:34 已变 64,928 B（7 分钟内两次改写），`meta/st/price_inference.json` 与 `meta/st/manifest.json` 同步在 01:49:35 被重写。**本次未读该件、未基于它下任何结论**，单独列入 §8 待补。
  2. `docs/adj_factor独立源对拍_2026-11-11.md §14.1` 的「残差归零」判据（G-122）**未使用**。本文的独立对拍改用「个股原始 `amount` 重算 Σamount → fin_buy/Σamount×100」这条与 `adj_factor` 无关的链路（见 §4.3）。

## 1. 清单与归属核对（开工前已回报）

| # | 表 | 唯一正式路径 | governance_order 归属 | 处理 |
|---|---|---|---|---|
| 1 | `st_coverage` | `meta/st/st_coverage.parquet`（+ `meta/st/coverage.json`、`meta/st/manifest.json`） | B5 · 深度 4 | 纳入 |
| 2 | `state_vector_daily` | `meta/state/state_vector_daily.parquet`（+ 同目录 `manifest.json`） | B5 · 深度 4（G-081 / G-082） | 纳入 |
| 3 | `universe_layers`（l0..l5） | `meta/universe/layers/l{0..5}_*.parquet`（+ 同目录 `manifest.json`、`counts_by_year.csv`） | B6 · 深度 5 | 纳入 |

3 张全部落在 B5/B6，无一张需要跳段。

## 2. 结论速览

**最重要的一句：这 3 张表没有发现「数据本身错」。** 发现的全部是登记陈旧、口径表述不清、以及代码/验收缺口。

| 表 | 数据本身错 | 登记陈旧 | 口径不同（不是错） | 代码/验收缺口 | 必须停手 |
|---|---|---|---|---|---|
| `st_coverage` | **0** | 0 | 1（36 只无覆盖窗口） | 1（G-01，已于 11-13 补齐断言与反例用例） | 0 |
| `state_vector_daily` | **0** | 2（R-01 `dimensions.lev`、R-02 `dataset` 字符串，均已于 11-13 修正） | 4（`_q` 窗口、日历基准、边缘日期 1 行、breadth 上游覆盖） | 0 | 0（G-082 已按 DG-18 登记 unresolved） |
| `universe_layers` | **0** | 3（R-04 已于 11-13 修正；R-03 已交权威清单写者；R-09 见 §9.5） | 2（「各 18,922,964 行」口径、第 4 条规则差异） | 1（两日缺逐只归因表） | 0 |

缺陷编号索引见附录 B。逐表证据见 §3~§5。

## 3. B5-1 `st_coverage`

### 3.1 文件与登记一致性（通过）

| 项 | 实测 | manifest / coverage.json | 判定 |
|---|---|---|---|
| rows | 5,890 | 5,890 | 一致 |
| bytes | 27,126 | 27,126 | 一致 |
| sha256 | `6869338e9de30d8275a4857f2c3fcb020d7045c0b2ccc00674847e8b57ca3353` | 同 | 一致 |
| `ts_code` 唯一性 | 0 重复 | —— | 通过 |
| 列 | `ts_code, coverage_start, coverage_end, source, fetched_at` | —— | 通过 |
| `coverage.json` | `coverage_rows=5890`、`coverage_sha256` 与 parquet 同值 | 同 | 一致 |
| `coverage_start` 空值 | 0 | —— | 通过 |
| `coverage_end` 空值 | 0 | —— | 通过 |
| `coverage_end < coverage_start` | 0 行 | —— | 通过 |
| `coverage_start` 区间 | 1990-12-19 ~ 2026-11-10 | 同 | 一致 |
| `coverage_end` 区间 | 2015-05-29 ~ 2026-11-12 | 同 | 一致 |
| `coverage_replay_applied` | 64 | 64 | 一致 |

`source` 分布：`merged` 3,446 / `em_name_history_clean` 2,335 / `coverage_ext_segment_quote` 53 / `coverage_ext_segment_listing` 46 / `coverage_ext_segment_evidence` 8 / `cninfo_notice_dated` 2，合计 5,890，无空值。

**判定：数据本身无错，登记无陈旧。** `coverage_replay_applied=64` 正是 2026-11-10 那次 64 行裁定的回放记录，与项目规则中 DG-31 的反例同源；本次实测该批写回后 `coverage_start` 空值为 0（当时曾出现 5,824 行被写成 null 的形态，现数据中不存在）。

### 3.2 「写回兜底」与「断言可失败性」（B5 收口账第一项）

| 收口要求 | 实测 | 判定 |
|---|---|---|
| 写回必须带兜底分支（DG-31 第一句） | `apply_reviewed_coverage` 用 `pl.coalesce([coverage_start_rev, coverage_start])` / `coalesce([coverage_end_rev, coverage_end])`，未命中行回落原列；`how='left'` 保行 | **已满足** |
| 断言必须能对「空值从 0 变 N」失败（DG-31 第二句） | `apply_reviewed_coverage` 与 `_write_coverage_store` 全链路**没有任何空值计数断言**；`tests/test_st_coverage_replay.py` 只断言具体行值、`coverage_replay_applied/appended`、`coverage_rows`、`coverage_sha256`、`sha_match/rows_match`，**没有一条断言 `coverage_start`/`coverage_end` 的空值计数** | **未满足（缺口 G-01）** |

代码位置：[st_history.py](file:///e:/code/python/quant/src/data/st_history.py#L1092-L1124)；用例：[test_st_coverage_replay.py](file:///e:/code/python/quant/tests/test_st_coverage_replay.py#L40-L161)。

后果：2026-11-10 那类「5,824 行被写成 null」的缺陷，用现有用例无法复现失败 —— 因为裁定行本身写对了，被破坏的是**未命中行**，而用例没有校验未命中行的空值总数。

### 3.3 覆盖域的登记表述（C-07，口径差异不是错）

`st_coverage` 有 5,890 行，而 L0 全历史域有 5,915 只，**36 只没有覆盖窗口**：

- 35 只沪市（含 2 只科创板 `688086.SH` / `688555.SH`）+ 1 只深市 `000508.SZ`（1993-04-30 上市、1999-07-12 退市）
- 其中 8 只 `delist_date` 为空（仍处上市状态）：`600606.SH`、`600658.SH`、`600683.SH`、`600775.SH`、`600833.SH`、`600845.SH`、`600855.SH`、`600874.SH`
- 成因：沪市/北交所的窗口起点按 `CNINFO_SAFE_START = 2005-01-01` 截断（[st_history.py:124](file:///e:/code/python/quant/src/data/st_history.py#L122-L124)），且**建行集合只含「深市名称变更记录 ∪ 巨潮公告域 ∪ 本地深市行情」**；沪市标的若从未出现在巨潮公告域里，就整只不建行。这 36 只正属这一类
- 影响：对这 36 只，`load_coverage` 返回无行 → `status_at` 恒为 `unknown`；`unknown_start_count=0` 统计的是「有上市信息但无起点证据」的另一类（[st_history.py:1285-1287](file:///e:/code/python/quant/src/data/st_history.py#L1277-L1287)），**不覆盖「整只没有行」这类**

**判定：属既定口径的必然结果，不是数据错误。** manifest 的 `semantics` 已经写明「无任何记录 → unknown」，所以「缺席 = unknown」本身是登记过的行为；唯一建议是在 `coverage.json` / manifest 里把 36 只的清单与成因（沪市 2005 起点 + 巨潮公告域）显式写出来，避免调用点把「unknown」误当成「当期为非 ST」的硬结论。

## 4. B5-2 `state_vector_daily`

### 4.1 文件与登记一致性

| 项 | 实测 | manifest `files` | 判定 |
|---|---|---|---|
| rows | 8,825 | 8,825 | 一致 |
| 列数 | 25 | 25（`columns` 逐一相符） | 一致 |
| bytes | 738,059 | 738,059 | 一致 |
| sha256 | `35c859acb3cb3e1015cdb22a2a1b8036c1a8fbcb769d60160fdfea20571ab33c` | 同 | 一致 |
| `date` 唯一性 | 0 重复 | —— | 通过 |
| 日期区间 | 1990-12-19 ~ 2026-11-11 | `date_range` 同 | 一致（`dataset` 字符串不一致，见 4.5） |
| `dims_available` / `dims_q_available` 空值 | 0 / 0 | —— | 通过 |

### 4.2 G-081（两融缺 11-11 沪深各 1 行）—— 数据层已闭环

`meta/macro/cn_market_margin_daily.parquet` 2026-11-11 两行齐备：

| market | date | fin_buy | source |
|---|---|---|---|
| sh | 2026-11-11 | 75,642,760,774 | `sse:stock_margin_sse` |
| sz | 2026-11-11 | 75,185,000,000 | `szse:stock_margin_szse` |

`state_vector_daily` 的 2026-11-11 行 `lev_margin_buy_ratio = 8.998`，与补丁记录 `by_market` 完全对应。

**lev 列补丁前后三项（B5 验收要求的三项，用 `_backup/meta/state_margin_20261112_214324/state_vector_daily.parquet`（738,063 B，sha `02fda62b…`）作为补丁前快照逐值比对）**：

| 指标 | 数值 |
|---|---|
| 改动前行数 / `lev` 非空 / 空 | 8,825 / 4,034 / 4,791 |
| 改动后行数 / `lev` 非空 / 空 | 8,825 / 4,035 / 4,790 |
| 命中行数 | **1**（2026-11-11，`null → 8.998`） |
| 未命中行数 | **8,824**（其中「原有值未变」4,034 + 「前后皆空」4,790） |
| 有值 → 空 | **0** |
| 既有非空值被改 | **0** |

行数守恒、既有非空值改动数为 0、空值只减 1，三项全部成立。

### 4.3 独立复算（DG-23 真实读路径，不复用 G-122 判据）

用个股原始 `amount` 重算非北交所 Σamount，再按 `fin_buy / Σamount × 100` 反算 `lev_margin_buy_ratio`，与表内逐值比对：

| 项 | 结果 |
|---|---|
| 可比行数 | 4,035（表内非空且可复算） |
| 最大绝对差 | **5.3e-15**（浮点噪声） |
| 差异 > 1e-9 的行 | **0** |
| 「表内非空但不可复算」 | 0 |
| 「表内空但可复算」 | 0 |

抽样锚点：

- 2014-05-13：重算 Σamount = 150,860,269,533.375，与重建后分母逐位一致
- 2026-11-11：重算 Σamount = 1,676,072,881,100.25，与补丁输入 `amount_shsz` 逐位一致

**判定：当前 `lev` 列与当前输入完全自洽，数据本身无错。**

### 4.4 G-082（2,203 行变化、分母 Σamount +2.24%）—— 归因需重跑，停手

已核实的事实（来自 `_staging/derived_catchup_20261112/state_vector_diff.json`，8,825→8,827 那次重建）：

| 项 | 数值 |
|---|---|
| `lev_margin_buy_ratio.changed` | 2,203 |
| 空值 | 4,792 → 4,792（该快照 8,827 行） |
| Δ 负 / 正 / 零 | 2,166 / 37 / 0 |
| Δ 中位数 | −0.0115972 |
| Δ 最小值 | −0.2353322 |
| Δ 均值（绝对） | 0.0216232 |
| Δ 最大值 | +3.55e-15（37 个「正」全在此量级，实为浮点噪声） |
| 2014-05-13 分母 | 147,553,696,194.875 → 150,860,269,533.375（+2.24%） |
| 分子 `fin_buy` | 不变 |

只读可判定与不可判定的边界：

- **可判定**：当前分母与当前个股 `amount` 逐位吻合（§4.3），所以「现在的值是对的，变化发生在历史序列」
- **已排除的一种归因**：「错位一日」。取 2014-04-25~2014-05-31 逐日 Σamount，**无一等于** 147,553,696,194.875；最接近的 2014-05-30 仍差 2.08e8
- **不可只读判定**：旧口径为什么系统性少 2.24%。旧桶需要旧输入面板或复跑旧口径才能做按日/按标的差分

**结论（2026-11-13 收口，按 DG-18 登记 unresolved，保留当前值）**：

当前值可独立复算为真——分母逐位等于同日全市场个股 `amount` 之和（§4.3，max|Δ| = 5.3e-15），
分子 `fin_buy` 与沪深两融原始值一致，且「错位一日」假设已排除（2014-04-25~05-31 逐日 Σamount
无一等于旧分母 147,553,696,194.875，最接近的 2014-05-30 仍差 2.08e8）。
旧口径为何系统性少 2.24% 无留档、无法只读复原，两个源（旧序列 vs 当前可复算值）之间缺第三证据，
因此按 DG-18 **登记为 unresolved、保留当前值、不改数、不重跑**。将来若要闭环，按 §6 B-01 的二分解
或 staging 复跑方案执行。

### 4.5 登记陈旧（2 条）

| 编号 | 位置 | 登记值 | 实测 | 说明 |
|---|---|---|---|---|
| R-01 | `meta/state/manifest.json` → `dimensions` 中 `key=lev` | `raw_nonnull_rows=4034`、`raw_null_rows=4791` | 4,035 / 4,790 | 这**恰是 2026-11-11 补丁前的值**（用 `_backup` 快照实测 4,034/4,791），说明该段未随补丁刷新；同一文件的 `gaps` 段已是 4,035/4,790，**同一 JSON 两处自相矛盾** |
| R-02 | 同 manifest → `dataset` 字符串 | `… / 1990-12-19~2026-11-09` | 至 2026-11-11 | 同一文件 `date_range` 已是 `["1990-12-19","2026-11-11"]`，字符串未跟 |

### 4.6 口径不同（4 条，不是错）

**C-01「4,035 天全部带 `_q` 列」与表自身分位规则冲突。**

manifest `quantile` 明确：`window_trading_days=1250`、`min_valid_in_window=1250`、不足则写 null 且「禁止插值、禁止向前填充、禁止退回全样本」。实测 `lev_margin_buy_ratio_q` 非空 **2,786**，恰好等于 `4,035 − 1,249` —— 即前 1,249 个有效观测因窗口未满 1,250 必须为空，第 1,250 个起全部非空。

**结论：数据完全符合自身已写明的规则，是 `governance_order.md` 的验收表述写错了。** 建议改为「4,035 天全部有原值；`_q` 自首个满 1,250 有效观测窗口之日起全覆盖（实测 2,786 天）」。

**C-02「逐日行数与 `daily_index` 交易日数一致」未指定基准，且 `daily_index` 本身不是交易日序列。**

`meta/daily_index.parquet` 是指数名录（`ts_code / data_type / market / rows / start / end`），不是逐日日期表，无法直接作「交易日数」基准。三个候选基准实测：

| 基准 | 天数 | 与 SV 差 |
|---|---|---|
| `000001.SH` 指数 bar 日期数 | 8,761 | SV 多 64 天 |
| 个股 bar 日期并集 | 8,824 | SV 多 1 天 |
| `state_vector_daily` | 8,825 | —— |

差的 64 天构成：**58 个周六 + 6 个周日**，全部落在 1991-01-05 ~ 1993-08-21（1990 年代前期周末开市），这些日期个股有 bar 而 `000001.SH` 无 bar。另 1 天是 1992-04-18（见 C-03）。

**判定：SV 的日期域是「个股 bar 日期 ∪ {1992-04-18}」，自洽；验收需先定死基准日历**，否则按 000001.SH 验收会判「多 64 天」。

**C-03 边缘日期 1 行（1992-04-18，周六）。**

该日在个股 bar（8,824 个日期）与 `000001.SH` 中都不存在，但 `valuation_daily` 有 1 行，`breadth_ma_daily` 该日 `sample_size=1`、`pct_above_ma20=100.0`。因此 SV 保留该行，`sent_n_codes=1`、`breadth_ma20=100.0`、`dims_available=2`。

**判定：1 行边缘残值，无扩散影响（下游 `_q` 因窗口不足为 null），不构成系统性错误；但「100% 宽度」由单只标的得出，登记时应注明。**

**C-04 `breadth_ma20` 覆盖 8,654 天、171 天为 null**，恰等于上游 `meta/breadth/breadth_ma_daily.parquet` 的行数 —— 是上游覆盖所限，非本表缺陷。

## 5. B6 `universe_layers`（l0..l5）

### 5.1 文件与登记一致性

| 层 | 文件行数 | 成员行数（`in_layer=true`） | 成员只数 | `in_layer` 空值 | bytes / sha256 |
|---|---|---|---|---|---|
| L0 | 18,922,964 | 18,922,964 | 5,915 | 0 | 5,883,886 / `f1b1a58c…` MATCH |
| L1 | 18,922,964 | 18,223,740 | 5,837 | 0 | 6,034,559 / `999330f1…` MATCH |
| L2 | 18,922,964 | **17,442,546** | 5,753 | 0 | 6,388,765 / `1f09ca97…` MATCH |
| L3 | 18,922,964 | **16,785,784** | 5,753 | 0 | 6,477,436 / `9604b549…` MATCH |
| L4 | 18,922,964 | **13,333,780** | 5,738 | 0 | 6,944,511 / `813f821c…` MATCH |
| L5 | 18,922,964 | **13,198,843** | 5,436 | 0 | 6,927,309 / `8f109b9a…` MATCH |

6 个文件的 bytes 与 sha256 **逐个 MATCH** manifest `files`（`files.true_rows` 亦逐个等于实测成员行数）。

**嵌套性**：把 6 层按 `(date, ts_code)` 全连接后退化为 6 个布尔列，逐对检查 `L(n)=True 且 L(n-1)=False` 的违例行数 —— **5 对全部为 0**，L0 ⊇ L1 ⊇ L2 ⊇ L3 ⊇ L4 ⊇ L5 成立。

**运行期消费者**：全仓 grep `universe/layers|l0_all_history|l5_pool` 仅命中 `src/data/quality/manifest_semantics.py`（语义登记）、`tools/build_universe_layers.py`（产出）、`tools/lineage/*`（血缘），**无任何取数/回测/展示读路径** —— 与 B6「本批是决策不是重算」的定位一致，支持建议 C（维持现状 + 登记，不接线、不退役）。

**判定：数据本身无错。**

### 5.2 登记陈旧（3 条）

| 编号 | 位置 | 登记值 | 实测 | 差额 |
|---|---|---|---|---|
| R-03 | `docs/数据权威路径清单.md` L18 | L2 成员 17,442,522 行 | 17,442,546 | +24 |
| R-03 | 同上 L19 | L3 成员 16,785,760 行 | 16,785,784 | +24 |
| R-03 | 同上 L20 | L4 成员 13,333,769 行 | 13,333,780 | +11 |
| R-03 | 同上 L21 | L5 成员 13,198,832 行 | 13,198,843 | +11 |
| R-04 | `meta/universe/layers/manifest.json` → `transitions.L1→L2/L2→L3/L3→L4/L4→L5.rows_kept` | 17,442,524 / 16,785,762 / 13,333,769 / 13,198,832 | 17,442,546 / 16,785,784 / 13,333,780 / 13,198,843 | +22 / +22 / +11 / +11 |
| R-09 | `2026-11-12/data-lineage/outputs/lineage.json` 的 `st_coverage → universe_layers` 边 | 声明为数据依赖 | `tools/build_universe_layers.py` 中 `st_coverage` **只出现在 `UPSTREAM_FILES`（第 89 行）的 provenance/mtime 清单，无任何读取**；L3 的 ST 剔除只读 `st_intervals`（第 553-557 行） | 声明性上游，非数据依赖 |

说明：

- R-03 与 R-04 的差额可完全由两次已登记补丁解释：`patch_17_quotes_1993b` 使 L2/L3 +2、L4/L5 净 0；`patch_23_nontrading_days` 使 L2/L3 +22、L4/L5 +11。**权威清单停在第 1 次补丁之前，`transitions` 停在第 2 次补丁之前，`files` 段是最新的** —— 同一份 manifest 内 `files` 与 `transitions` 不一致。
- R-03 中「只数」全部正确（5,837 / 5,753 / 5,753 / 5,738 / 5,436 与实测一致），**只有行数陈旧**。
- R-09 的影响：B6 的数值正确性与 B5 的数值正确性**没有实际耦合**，B5 未闭环不构成 B6 的阻塞；拓扑深度 4 < 5 仍成立，治理顺序无需改。建议把该边标注为 `provenance_only`，或从数据依赖边中移除。

### 5.3 口径不同（2 条，不是错）

**C-05「各 18,922,964 行」是文件行数，不是成员行数。**

`governance_order.md` B6 写「各 18,922,964 行」= 每个文件的网格行数（含 `in_layer=false`），实测 6 个文件确实都是 18,922,964；权威清单 L16-L21 写的是成员行数（18,922,964 / 18,223,740 / … / 13,198,843）。**两种表述都对，但指的不是同一个量**。建议统一写成「文件 18,922,964 行 / 成员 N 行」。

**C-06 与 `universe_filter.py` 的差异是 3 条还是 4 条。**

权威清单 L16 登记 3 条：① 流动性 2,000 万（L4 `min_amount`）vs 1,000 万（`illiquid`）；② 面值 1.5 元下限只在 `low_price`；③ 近 20 交易日停牌 > 5 天只在 `suspension_days`。

代码里还有第 4 条：`UniverseFilterConfig.exclude_resumption_day = True`（复牌首日剔除）在 L5 无对应档。实测影响面（`universe_consistency.json` 的 `filter_rule_counts.resumption_day`）：

| 日期 | `resumption_day` 剔除数 |
|---|---|
| 2026-11-11 | 0 |
| 2024-06-28 | 1 |
| 2021-06-30 | 1 |

**判定：影响极小（≤1 只/日），但登记应补为第 4 条**，否则「三处已知差异」的表述会让后续把机读结果判成不一致。

### 5.4 一致性实证（现有证据的强度分层）

三日期 Jaccard 与差异分解（`universe_consistency.json` + `universe_divergence_attribution.json`）：

| 日期 | Jaccard | 交集 | only_in_filter | only_in_l5 | only_in_filter 归因 | only_in_l5 归因 |
|---|---|---|---|---|---|---|
| 2026-11-11 | 0.9846 | 4,910 | 57 | 20 | 57/57 = L4 流动性阈值更严（100%） | 停牌 3 + 低价 17 = **20，不重不漏** |
| 2024-06-28 | 0.8956 | 4,340 | 449 | 57 | 449/449 = L4 流动性（100%） | 低价 62 + 停牌 2 + 复牌首日 1 = 65，**与 57 差 8 只重叠**，无逐只表 |
| 2021-06-30 | 0.9012 | 3,430 | 368 | 8 | 368/368 = L4 流动性（100%） | 停牌 7 + 低价 1 = 8 恰等，另复牌首日 1 只为重叠，无逐只表 |

**判定：`only_in_filter` 三个日期都 100% 归因；`only_in_l5` 只有最新一日给出不重不漏的逐只归因**，另两日只有规则计数（把重叠计入后数字自洽，但没有逐只清单）。权威清单 L16「差异逐只 100% 可归因」这句话**在最新一日成立，在两日属于「按类别可归因」**。缺陷编号 G-02。

## 6. 必须停手 / 待外部条件的项

| 编号 | 项 | 为什么停手 | 谁来解 |
|---|---|---|---|
| B-01 | G-082 归因（旧分母为何系统性少 2.24%） | **已登记 unresolved（DG-18）**：当前值可独立复算为真，「错位一日」已排除，旧口径无留档 | 保留当前值、不改数；将来需闭环时按下方方案派任务 |
| B-02 | 陷阱 1：`meta/st/st_price_inferred.parquet` 相关的全部判定 | 该件 7 分钟内被改写两次（64,938 B → 64,928 B），manifest 同步在变 | 等另一路收口后重做 |

B-01 的执行方案（供主控选，本次未执行）：

1. 二分解：用 `_backup/meta/state_margin_20261112_214324/` 与 `_staging/derived_catchup_20261112/` 的历史快照反解每日 Σamount_old，再按标的求差，定位是「少数大盘股权重变化」还是「系统性口径（如剔除了 `amount<=0`、或换过股池）」
2. 或直接复跑 `build_state_vector` 到 `_staging/state_vector_attrib_<日期>/`，与当前正式表逐日逐标的比差，再决定是否替换
3. 两条都要求 `_staging/<任务>_<日期>/` 同时有 `progress.*` 与 `failed.csv`（DG-7）

## 7. 修法建议与影响面（本次未执行任何一条）

| 编号 | 建议改法 | 影响面 | 需重跑？ |
|---|---|---|---|
| R-01 | 把 `meta/state/manifest.json` 的 `dimensions.lev.raw_nonnull_rows/raw_null_rows` 改为 4,035 / 4,790（与 `gaps` 段对齐） | 仅 manifest 文本；不动 parquet | 否（**已执行**，§9.1） |
| R-02 | 把同 manifest 的 `dataset` 字符串末日改为 2026-11-11 | 仅文本 | 否（**已执行**，§9.1） |
| R-03 | 把权威清单 L18-L21 成员行数改为 17,442,546 / 16,785,784 / 13,333,780 / 13,198,843 | 仅文档 | 否（**已交权威清单写者**，本任务不改该文件） |
| R-04 | 把 layers manifest 的 `transitions.*.rows_kept` 与 `counts.*.avg_codes_per_day` 按 `files.true_rows` 重算 | 仅 manifest 文本 | 否（**已执行**，§9.2） |
| R-09 | 在 `lineage.json` 把 `st_coverage → universe_layers` 标注 `provenance_only`（或从数据依赖边移除） | 血缘图与报告；不影响数据 | 否（入册文字见 §9.5） |
| C-01 | 改写 `governance_order.md` B5 验收句为「4,035 天全部有原值；`_q` 自满 1,250 有效观测窗口起全覆盖（实测 2,786）」 | 仅文档 | 否（**已执行**，§9.3） |
| C-02 | 明确验收基准为「个股 bar 日期并集」并写明 64 个周末开市日 + 1 个边缘日为例外 | 仅文档 | 否（**已执行**，§9.3） |
| C-05 | B6 表述改为「文件 18,922,964 行 / 成员 N 行」 | 仅文档 | 否 |
| C-06 | 权威清单 L16 补第 4 条差异 `exclude_resumption_day` | 仅文档 | 否 |
| C-07 | 在 `meta/st/coverage.json` 与 manifest 里显式列出 36 只无窗口标的及成因（沪市 2005 起点 + 巨潮公告域） | 仅登记文本；不改 parquet | 否 |
| G-01 | 在 `apply_reviewed_coverage` 后加断言：`coverage_start`/`coverage_end` 改动前后空值计数、未命中行数、`null→value` 与 `value→null` 双向计数；并在 `test_st_coverage_replay.py` 增加一条「未命中行不得新增 null」的用例 | 只增断言与用例；不改数据 | 否（**已执行**，§9.4） |
| G-02 | 为 2024-06-28 / 2021-06-30 补去重后的 `only_in_l5` 逐只归因表 | 只补证据文件 | 需按当日截面重算 filter（不碰正式表） |
| B-01 | 见 §6：按 DG-18 登记 unresolved，保留当前值 | 不改数据 | 否（**已登记**，§4.4） |

## 8. 未覆盖与待补

| 项 | 状态 | 原因 |
|---|---|---|
| `meta/st/st_price_inferred.parquet` 及其全部下游判定 | **未读、未下结论** | 陷阱 1：正在被另一路重跑（01:42:37 64,938 B → 01:49:34 64,928 B） |
| `meta/st/manifest.json` 中 `st_price_inferred` 条目 | 未判定 | 同一原因；该 manifest 在 01:49:35 被另一路重写 |
| `meta/universe/layers/counts_by_year.csv` 与六层逐年的对拍 | 未展开 | 优先级低于行数/嵌套/sha 三条硬指标，且不影响结论 |
| `breadth_ma_daily` 自身为何保留 1992-04-18（`sample_size=1`） | 未定位 | 属 B5 上游，不在本批 3 张表范围内；已作为 C-03 记录 |
| G-122（`adj_factor` 残差归零判据） | 未使用 | 已判为循环论证，本次独立对拍改走 `amount` 重算链路 |

## 9. 收口记录（2026-11-13，全部为登记与断言，未动数据）

### 9.1 R-01 / R-02 `meta/state/manifest.json`

| 项 | 改前 | 改后 | 依据 |
|---|---|---|---|
| `dimensions[key=lev].raw_nonnull_rows` | 4034 | **4035** | 当前 parquet 实测 `lev_margin_buy_ratio` 非空 4,035 |
| `dimensions[key=lev].raw_null_rows` | 4791 | **4790** | 8,825 − 4,035 |
| `dataset` 字符串末日 | `1990-12-19~2026-11-09` | `1990-12-19~2026-11-11` | 同文件 `date_range` 与 parquet 末日 |

改前 SHA256 `962fbacb11d20f4d7d6b41f2e452a80a5e64e5c4b8e57e188e38067d99f008a5` → 改后 `4db891b3140385a0744550b35826f4b6e95d3fff67b2bbfd2aafbc8e1d7827e3`；
快照 `_backup/meta/manifest_consistency_fix_20261113/state/manifest.json`。
改后自洽复核：`dimensions.lev` == `gaps.lev` == 4,035/4,790，两者之和 = `rows` 8,825，`dataset` 末日 == `date_range[1]` == 2026-11-11。

### 9.2 R-04 `meta/universe/layers/manifest.json`

| 字段 | 改前 | 改后 |
|---|---|---|
| `transitions.L1->L2.rows_kept` | 17442524 | **17442546** |
| `transitions.L1->L2.rows_dropped` | 781216 | **781194** |
| `transitions.L2->L3.rows_kept` | 16785762 | **16785784** |
| `transitions.L3->L4.rows_kept` | 13333769 | **13333780** |
| `transitions.L3->L4.rows_dropped` | 3451993 | **3452004** |
| `transitions.L4->L5.rows_kept` | 13198832 | **13198843** |
| `counts.L2.avg_codes_per_day` | 2018.34 | **2018.35** |

改前 SHA256 `530ce3bce2dc8ebef7c37fda6ca7c2ac35c006d7088e20bb07d66b31b177fedc` → 改后 `aebb8f9297fb8f805fa6e06ea6e7dd0dfad6894dcfb4a08de5ff38a4588c229b`；
快照 `_backup/meta/manifest_consistency_fix_20261113/universe_layers/manifest.json`。
改后自洽复核：`files.true_rows` == `transitions.rows_kept` == 磁盘成员行（逐层）；`rows_kept + rows_dropped` 守恒；
`counts.<L>.avg_codes_per_day == round(true_rows / member_days, 2)`；`files` / `parameters` / `grid` / `patch` / `retired` / `upstream` 逐值未动。
注意 `counts.<L>.codes` 是**成员只数**（5,915 / 5,837 / 5,753 / 5,753 / 5,738 / 5,436），不是文件只数（六层文件都含 5,915 只）。

### 9.3 C-01 / C-02 `governance_order.md` B5 验收句

改前两句：「`state_vector` 逐日行数与 `daily_index` 交易日数一致；`lev_margin` 列改动前 null 数、改动后 null 数、未命中行数三项同时给出。」

改后（L164-L167 三条）：
1. 日期域按「个股 bar 日期并集 ∪ 指数日历外单日」验收（8,825 = 8,824 ∪ {1992-04-18}）；**明确禁止**以 `daily_index` 行数作交易日基准（它是指数名录），日历取 `000001.SH`（8,761 天），差额 64 天 = 58 周六 + 6 周日，全部落在 1991-01-05~1993-08-21 周末开市段；
2. `lev_margin` 三项 null/未命中计数保留；
3. 新增 `_q` 验收口径：原值非空 4,035，`_q` 非空 **2,786 = 4,035 − 1,249**（前 1,249 个有效观测窗口未满 1,250），禁止「4,035 天全部带 `_q`」。

### 9.4 G-01 `st_coverage` 写回链路空值断言

- 新增 `src/data/st_history.py::assert_replay_preserves_nulls`，在 `apply_reviewed_coverage` 合并完成后立即调用，失败即抛 `AssertionError`，不允许带空值的表落盘。三类判定：① 行数守恒（`after = before + appended`）；② 空值守恒（逐列 `after` null 数 = `before` null 数 + `appended` null 数），且全表「非空 → 空」翻转数必须为 0；③ 未命中行（`patch` 之外的标的）两列必须与改动前逐值相等。
- 写回本身的 `pl.coalesce([rev, orig])` 兜底**未做任何改动**。
- 计数随 `coverage.json` 的 `coverage_replay_null_check` 留档（`before` / `after` / `appended` / `non_null_to_null_rows` / `untouched_rows`）。
- 反例自测（红 → 绿，探针 `_staging/g01_redgreen_20261113/red_green_probe.py`，只跑临时目录）：
  - **红**：把 `pl.coalesce` 换成「只取第一个参数」（= 历史上无兜底的成因）后跑 `build_store` → `AssertionError: 覆盖层写回把 coverage_start 的空值从 0（+appended 0）改成了 1`
  - **绿**：恢复 `pl.coalesce` 后同输入跑通 → `coverage_start null=0`，`null_check = {'before': {'coverage_start': 0, 'coverage_end': 0}, 'after': {...0}, 'appended': {...0}, 'non_null_to_null_rows': {...0}, 'untouched_rows': 1}`
- 单测：`tests/test_st_coverage_replay.py` 由 6 条增至 11 条，新增 5 条（1 条 json 留档 + 4 条反例：未命中行被写 null、null 在两行间平移、未命中行值被改写、行数不守恒），`11 passed`。

### 9.5 R-09 入册文字（`st_coverage → universe_layers` 声明性上游）

> `st_coverage → universe_layers` 在血缘中声明为数据依赖，但 `tools/build_universe_layers.py` 只在 `UPSTREAM_FILES`（第 89 行）把它列入 provenance / mtime 记账清单，全文件对 `st_coverage` 零读取；L3 的 ST 剔除只读 `meta/st/st_intervals.parquet`（第 553-557 行）。因此该边属**声明性上游（provenance-only）**，不是数值依赖：`st_coverage` 的窗口是否闭环、B5 是否收口，都不构成 `universe_layers`（B6）的数值阻塞；拓扑深度 5 > 4 仍成立，治理顺序无需调整。建议在血缘图中把该边标注为 `provenance_only`，或从数据依赖边集中移除。

### 9.6 C-03 入册文字（1992-04-18 那 1 行 SV）

> 1992-04-18（周六）在个股 bar 与 `000001.SH` 指数 bar 中均不存在，但 `valuation_daily` 有 1 行；`meta/breadth/breadth_ma_daily.parquet` 该日 `sample_size=1`、`pct_above_ma20=100.0`，`state_vector_daily` 相应保留 1 行（`sent_n_codes=1`、`breadth_ma20=100.0`、`dims_available=2`）。该日宽度 100% 完全来自 `sample_size=1` 的单只标的，**不具备横截面统计意义**，属边缘残值而非系统性错误：下游 `*_q` 因滚动窗口有效观测不足 1,250 全为 null，不扩散到分位口径。登记时按「1 行 · 样本量 1 · 宽度不可解释为市场宽度」注明，不剔除、不改数。

## 附录 A：可复现命令（全部只读）

```powershell
# A1 目标文件 sha/bytes
.venv\Scripts\python.exe -c "import hashlib,os;[print(r,os.path.getsize(r),hashlib.sha256(open(r,'rb').read()).hexdigest()) for r in [r'data\local_store\meta\st\st_coverage.parquet',r'data\local_store\meta\state\state_vector_daily.parquet']]"

# A2 六层 sha/bytes 对 manifest（逐件 MATCH）
#    见 §5.1；脚本体同 A1，比对 manifest 的 files[*].bytes / sha256

# A3 六层成员行数与只数
#    pl.read_parquet(...).filter(pl.col('in_layer')).height / ['ts_code'].n_unique()

# A4 嵌套性（5 对违例必须为 0）
#    六层按 (date, ts_code) 全连接 → 逐对 filter(pl.col(next) & ~pl.col(prev)).height

# A5 lev 独立复算（不复用 G-122）
#    Σamount = quotes/stock/**/*.parquet 过滤 ~ends_with('.BJ') 后按 date 求和
#    lev_recalc = 边际表 fin_buy 按 date 求和 / Σamount * 100
#    实测 4,035/4,035 可比，max |Δ| = 5.3e-15

# A6 lev 补丁前后三项
#    before = _backup/meta/state_margin_20261112_214324/state_vector_daily.parquet
#    after  = data/local_store/meta/state/state_vector_daily.parquet
#    null 4,791→4,790；命中 1；未命中 8,824；有值→空 0
```

## 附录 B：缺陷编号索引

| 编号 | 类别 | 一句话 |
|---|---|---|
| R-01 | 登记陈旧 | `state/manifest.json` `dimensions.lev` 计数=补丁前 4,034/4,791，与同文件 `gaps` 4,035/4,790 自相矛盾 |
| R-02 | 登记陈旧 | 同 manifest `dataset` 字符串末日仍写 2026-11-09 |
| R-03 | 登记陈旧 | 权威清单 L18-L21 的 L2-L5 成员行数停留在 patch_17 之前（差 +24/+24/+11/+11） |
| R-04 | 登记陈旧 | layers manifest `transitions.rows_kept` 停留在 patch_23 之前（差 +22/+22/+11/+11），与 `files.true_rows` 不一致 |
| R-09 | 登记陈旧 | `lineage.json` 的 `st_coverage → universe_layers` 是声明性上游，代码中无读取 |
| C-01 | 口径不同 | 验收「4,035 天全带 `_q`」与「窗口 1,250」冲突；实测 2,786 天，恰为 4,035−1,249 |
| C-02 | 口径不同 | 验收基准「daily_index 交易日数」不成立；三个候选基准 8,761 / 8,824 / 8,825 |
| C-03 | 口径不同 | 1992-04-18（周六）无任何个股/指数 bar，SV 保留 1 行，宽度 100% 来自单只标的 |
| C-04 | 口径不同 | `breadth_ma20` 171 天为 null，等于上游 `breadth_ma_daily` 行数所限 |
| C-05 | 口径不同 | 「各 18,922,964 行」是文件行数，与权威清单的成员行数不是同一量 |
| C-06 | 口径不同 | 与 `universe_filter.py` 的已知差异应为 4 条，漏了 `exclude_resumption_day` |
| C-07 | 口径不同 | `st_coverage` 非全标的覆盖：36 只（35 沪含 2 只科创板 + 1 深）整只无窗口，`status_at` 恒 unknown |
| G-01 | 代码/验收缺口 | `st_coverage` 写回有兜底但**无空值计数断言**，现有用例无法捕获「空值 0→N」 |
| G-02 | 代码/验收缺口 | `only_in_l5` 仅最新一日有逐只归因表，另两日只有规则计数 |
| B-01 | 停手 | G-082 归因需重跑旧口径或旧输入，复跑会覆盖正式表 |
| B-02 | 停手 | 陷阱 1：`st_price_inferred` 正在被另一路重写，本次相关判定全部未做 |

**数据本身错：0 条。**
