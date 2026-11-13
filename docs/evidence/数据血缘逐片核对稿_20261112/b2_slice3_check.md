# B2 片3 体检：`stock_turnover`（个股换手率派生表）

> 只读体检：未改代码、未写 `data/`、未改 manifest、未提交。无 CSV 产出，辅助脚本在 `_staging/b2_slice3_20261113/`（不入库）。
> 上游：G-077 已解锁（`stock_shares` 于 2026-11-13 收口），本片为 B2 最后一张未检表。

## §0 结论先行

| 口径 | 结论 | 关键证据 |
|---|---|---|
| ① 写入口 | ⚠ 两个写入口 + 1 个自动步骤，**同写一个文件**；"抹列"风险只在 `_assemble`，当前 schema 两侧都是 5 列，未发生 | §2 |
| ② manifest 记录数 vs 实际文件 | ✅ 一致（6 parquet + 4 其他 = 10 条；本表 rows/bytes/sha256/codes/date_range 五项全等） | §3 |
| ③ 独立对拍 | ⚠ 派生部分与行情重算 99.85% 逐值相等；**140 行不符**（19 行为 ~100 倍单位错）；baostock 部分与本地口径 p99 差 8.2%、max 106% | §4 |
| ④ 第二份同语义 | ✅ 无第二份日频换手率；`turnover_20` / `ext_turnover_20` 是派生层 20 日均值，非同语义 | §5 |
| ⑤ 清单登记 vs 实测 | ❌ 行数滞后 33 行（18,055,784 vs 实测 18,055,751）；**口径文字写错**（写成"自由流通股本"，代码读的是 `float_share`） | §6 |
| **重点一：`stock_shares` 改动是否要求重建** | ✅ **不需要重建**，有实测支撑 | §7 |
| **重点二：`_assemble` 静默清表兜底** | ✅ 已删除，anti-join 保留底表是唯一路径 | §8 |
| **重点三：valuation 反算交叉核对 / 缺行** | ⚠ 派生部分与 valuation 反算差 3.3e-16（纯浮点）；缺行 19 只（18 退市 + 1 新上市），分级基金不在覆盖范围 | §9 |

## §1 事实基线

| 项 | 值 |
|---|---|
| 实际路径 | `data/local_store/factors/stock_turnover.parquet`（**不存在** `data/local_store/meta/turnover/`，任务书里的路径口径有误） |
| 字节 / SHA256 | 145,625,057 B / `7f90eda18ee04e31983bef24811f5104f8840f8eac8a5518be69a0e368cba184` |
| 行数 / 标的 / 区间 | 18,055,751 行 / 5,813 只 / 1990-12-19 ~ 2026-11-11 |
| 列 | `ts_code, trade_date, turn, source, fetched_at`（5 列） |
| 键唯一性 | 唯一键 18,055,751 = 行数，**重复键 0** |
| 空值 / 非法值 | `turn` 空值 0；`turn <= 0` **1 行**（`000001.SZ 1991-09-30`，`volume=0` 的真 bar） |
| `source` 分布 | `baostock` 189,419 行（1.05%）、`derived:quotes_volume+float_share` 17,866,332 行（98.95%） |
| mtime | 2026-11-12T22:25:29（早于 `stock_shares` 本次改动 2026-11-13T00:29） |

## §2 ① 写入口：几个、覆盖关系、谁抹谁

| 入口 | 代码位置 | 写入方式 | 能否覆盖既有行 |
|---|---|---|---|
| `backfill_turnover`（CLI `python -m src.data.local_store sync-turnover`） | [turnover.py:512-549](file:///e:/code/python/quant/src/data/local_store/turnover.py#L512-L549) `_assemble` | `write_parquet_atomic` 整文件重写 | **能**，但仅对分块里出现的 `(ts_code, trade_date)` 键；同键以新值为准 |
| `derive_turnover_local`（CLI `derive-turnover`） | [turnover.py:895-991](file:///e:/code/python/quant/src/data/local_store/turnover.py#L895-L991) | anti join 缺口 → `vertical_relaxed` 追加 → `write_parquet_atomic` | **不能**，只追加缺口键，既有行永不重写 |
| 全量构建自动收尾 | [build.py:1475-1499](file:///e:/code/python/quant/src/data/local_store/build.py#L1475-L1499) | 调 `derive_turnover_local` | 同上 |

覆盖关系：三者写的是**同一个文件、同一套 5 列**，无「各自一套」的分叉。`source` 是唯一区分来源的列，两个入口各自写自己的常量（`baostock` / `derived:quotes_volume+float_share`），互不篡改对方的标记。

**谁会把谁的列抹掉**：

- `derive_turnover_local` 的 `vertical_relaxed`（[:970](file:///e:/code/python/quant/src/data/local_store/turnover.py#L970)）在两侧列不一致时**直接抛 SchemaError**，不会静默补 null —— 这是安全形态。
- `_assemble` 的 `diagonal_relaxed`（[:531-537](file:///e:/code/python/quant/src/data/local_store/turnover.py#L531-L537)）在两侧列不一致时**按列并集合并**：目标表独有的列，在分块行上会静默变 null。当前 `_fetch_code` 落盘前 `select(list(TURN_SCHEMA))`，分块固定 5 列，目标表也是 5 列，**实测未发生抹列**；但这是唯一一条可能静默丢列的路径，属潜在风险（若将来给正式表加列而忘记同步 `TURN_SCHEMA`）。
- 已实测分块目录：`_staging/turnover_parts/` 当前为空（分块已合并、目录已清），历史分块在 `_staging/turnover_local_parts`（2026-11-09）与 `_staging/turnover_local_parts_20261112/`；代码路径由 [turnover.py:472](file:///e:/code/python/quant/src/data/local_store/turnover.py#L472) `staging_dir_for_store(root, 'turnover_parts')` 运行时推导，**不带运行日**，符合 DG-5 续跑口径。

## §3 ② manifest 记录数 vs 实际文件

| 项 | 值 |
|---|---|
| `factors/` 实际 parquet | 6 个 |
| `factors/manifest.json` 记录 | 10 条（6 parquet + 4 其他：`covered_codes.txt` / `fund_covered_codes.txt` / `fund_meta.json` / `meta.json`） |
| 差集 | 磁盘多出 0；manifest 多出 0（4 条非 parquet 记录与磁盘 4 个非 parquet 文件一一对应） |
| `totals` | `parquet_files=6` / `other_files=4` / `rows=39,226,790` / `bytes=161,590,536` |

本表记录 vs 实测（5 项全等）：

| 字段 | manifest | 实测 |
|---|---|---|
| rows | 18,055,751 | 18,055,751 |
| bytes | 145,625,057 | 145,625,057 |
| sha256 | `7f90eda1…` | `7f90eda1…`（逐字节一致） |
| codes | 5,813 | 5,813 |
| date_range | `1990-12-19 ~ 2026-11-11` | 同 |

`produced_by` 只写了 `sync-turnover`，**未提及 `derive-turnover`** —— 18,055,751 行里 98.95% 由后者产出，登记不完整（文档类缺陷，见 §10）。

## §4 ③ 独立对拍（A：行情重算）

按现行代码口径（`volume × 100 × 100 ÷ as-of float_share`，`volume<=0` 不派生）对**全表 18,055,751 行**逐行重算，脚本 `_staging/b2_slice3_20261113/recompute_check.py`：

| 市场 | 行数 | 逐值相等 | baostock 行（相等） | derived 行（相等） | derived 最大相对差 |
|---|---|---|---|---|---|
| 沪 | 8,067,286 | 7,973,767 | 75,561（181） | 7,991,725（7,973,586） | 104.01 |
| 深 | 9,705,698 | 9,583,720 | 113,858（507） | 9,591,840（9,583,213） | 102.00 |
| 京 | 282,767 | 282,497 | 0 | 282,767（282,497） | 5.9e-8 |
| **合计** | **18,055,751** | **17,839,984（98.80%）** | 189,419（688） | 17,866,332（17,839,296，99.85%） | 104.01 |

派生行误差分桶（17,866,332 行）：

| 相对误差区间 | 行数 | 判定 |
|---|---|---|
| `= 0` | 17,839,296 | 完全一致 |
| `(0, 1e-9]` | 13,862 | 浮点末位 |
| `(1e-9, 1e-6]` | 13,033 | 浮点末位 |
| `(1e-6, 1e-4]` | 0 | — |
| `(1e-4, 0.1]` | 4 | 待归因 |
| `(0.1, 10]` | 117 | 待归因 |
| `(50, 150]` | 19 | **~100 倍单位错** |
| `(150, +∞)` | 0 | — |
| 0/0（`turn=0` 且 `calc=0`） | 1 | `000001.SZ 1991-09-30`，无成交 |

**140 行不符 / 25 只标的**，年份分布：1993（64）、2019（57）、2026（11）、2015（4）、2024（3）、2025（1）、1991（1，即 0/0 行）。

19 行 ~100 倍错例（`turn` 恰为重算值的 1/100 量级）：

| ts_code | trade_date | turn（现表） | calc（重算） | 比值 |
|---|---|---|---|---|
| 600221.SH | 2024-11-08 | 0.135730 | 14.252435 | 105.01 |
| 600010.SH | 2025-10-14 | 0.164790 | 17.007406 | 103.21 |
| 000725.SZ | 2015-05-28 | 0.219903 | 22.649626 | 103.00 |
| 000725.SZ | 2026-06-08 | 0.126571 | 12.993294 | 102.66 |
| 000725.SZ | 2026-06-26 | 0.127949 | 12.949324 | 101.21 |

归因（已实测，非推测）：这些日期 `amount/(volume×100)` 严格等于 `close`（如 600221.SH 2024-11-08：`10,447,899,648/(46,851,568×100)=2.23=close`），说明**行情 volume 单位是手、重算口径正确**，错在现表 `turn`。现表值等价于「少乘了一个 100」的历史版本产物，且比值并非恰好 100（105.01 / 103.21 / …），说明当时用的 `float_share` 与今日也不同 —— 属**旧版派生逻辑 + 旧股本**双重复合，无法用当前代码复现。

**关键性质**：`derive_turnover_local` 只追加缺口、永不重写既有键（[:923-927](file:///e:/code/python/quant/src/data/local_store/turnover.py#L923-L927) anti join），所以这 140 行**会永久残留**，不会被任何一次日常派生自动修正。

## §5 ④ 第二份同语义数据

| 候选 | 位置 | 是否同语义第二份 |
|---|---|---|
| `turnover_20` | [build_new_factors.py:170](file:///e:/code/python/quant/tools/research/build_new_factors.py#L170)（由日频 `turn` 取 20 日均值） | 否，是派生层滚动均值 |
| `ext_turnover_20` | [fundamental_factors.py:280](file:///e:/code/python/quant/src/advice/fundamental_factors.py#L280) | 否，同上 |
| `valuation_daily.circ_mv` | `meta/financials/valuation_daily.parquet` | 否，是市值；可反算换手（§9 用它做独立对拍） |
| 其它日频 `turn` 表 | — | 无 |

另实测：全仓除 [turnover.py:77/940](file:///e:/code/python/quant/src/data/local_store/turnover.py#L77-L79) 外，**无任何调用点自行重算 `volume×10000/股本`**（`grep 'volume\s*\*\s*10000|VOLUME_LOT_TO_SHARE'` 仅命中本模块 2 行），符合清单 L76「调用点不得各自再算一套」。

## §6 ⑤ 权威路径清单登记 vs 实测

清单 [L76](file:///e:/code/python/quant/docs/数据权威路径清单.md#L76) 现文：

> 唯一换手率派生表（**18,055,784 行** / 5,813 只 / 1990-12-19~2026-11-11），由行情成交额/成交量与 `stock_shares` 的**自由流通股本**计算；调用点不得各自再算一套

| 登记项 | 实测 | 判定 |
|---|---|---|
| 路径 `factors/stock_turnover.parquet` | 同 | ✅ |
| 代码锚点 `turnover.py:100` | `turnover_path` 在 [:101-104](file:///e:/code/python/quant/src/data/local_store/turnover.py#L101-L104) | ✅ |
| 行数 18,055,784 | **18,055,751** | ❌ 滞后 33 行 |
| 只数 5,813 / 区间 | 同 | ✅ |
| 口径「自由流通股本」 | 代码读 `float_share`（[:902-905](file:///e:/code/python/quant/src/data/local_store/turnover.py#L902-L905)） | ❌ 文字写错 |

33 行差额全部有据可查，不是数据回退：

| 变更 | 行数变化 | 依据 |
|---|---|---|
| 1993b 派生层剔除 | 18,055,784 → 18,055,750（−34） | `docs/evidence/derived_1993b_20261113/验收报告.md` |
| 补 `000001.SZ 1991-09-30` 零换手真 bar | 18,055,750 → 18,055,751（+1） | `docs/数据缺口登记_正式表_2026-11-12.md:353`（提交 `29d739d`） |

即清单 L76 停在 1993b 剔除**之前**的数字，此后两次改动都未回写。

## §7 重点一：`stock_shares` 改动**不要求**重建 `stock_turnover`（有证据）

**结论**：`stock_turnover` 无需重建，现有 18,055,751 行与「用当前 `stock_shares` + 当前行情重新派生」等价。

证据三重，逐层收紧：

1. **公式只依赖 `float_share`**：`turn = volume × VOLUME_LOT_TO_SHARE(100) × TURN_PERCENT(100) ÷ float_share`，股本侧唯一输入是 `float_share`（[:939-942](file:///e:/code/python/quant/src/data/local_store/turnover.py#L939-L942)）；`free_float_share(_filled)` 在本模块**零引用**（`grep free_float src/data/local_store/turnover.py` 命中 0 行）。
2. **`float_share` 本次 0 改动**：`_backup/meta/stock_shares_20261113/stock_shares.parquet`（改前）与现表按 `(ts_code, effective_date)` 内连接 166,284 行，null 感知比较（`ne_missing`）逐列结果：`float_share` **0**、`total_share` 0、`source` 0、`change_reason` 0，只有 `free_float_share` 13 行变动。`float_share` 空值数 6,269 → 6,269 不变。
3. **全表重算残差与本次改动无关**：derived 部分 99.85% 逐值相等；残余 26,895 行为浮点末位（≤1e-6），140 行为历史旧版派生遗留（§4），其日期全部落在 1991~2026-07，**没有任何一行是 2026-11-13 之后的**；baostock 部分的偏差来自上游口径，与股本表无关。

因此：`turn` 是 `float_share` 的纯函数，`float_share` 逐值未变 ⇒ 重算结果逐值不变。实测残差只是「历史遗留行 + 浮点末位」，不是本次改动引入，**重建也修不掉它们**（参见 §10-①：只有定点重算才能修）。

## §8 重点二：`_assemble` 静默清表兜底**确认已删除**

| 判据 | 代码位置 | 现状 |
|---|---|---|
| 「整体替换」兜底是否还在 | [turnover.py:529-539](file:///e:/code/python/quant/src/data/local_store/turnover.py#L529-L539) | ❌ 已删除。现在只有两条分支：`if not parts: return None`；`if target.exists(): anti-join 后并集` / `else: 直接写分块` |
| anti-join 是否是唯一保留路径 | 同上 `:533-537` | ✅ 目标表存在就一律 `join(keys, how='anti')`，**不看 `touched` 是否为空** |
| 是否还按 `ts_code` 整段替换 | `:534` keys 为 `['ts_code','trade_date']` 两列 | ✅ 已改为按日键 upsert |
| 文档是否写明 | `:515-527` docstring | ✅ 明确记录「2026-11-06 把 1,780 万行削成 121 万行」的成因与「该兜底已移除，必须保留底表」 |
| 回归用例 | [test_turnover_assemble.py](file:///e:/code/python/quant/tests/test_turnover_assemble.py) | 4 例：增量分块保留历史、同交易日取新值、无目标表首建、空分块 no-op |

**发现一处测试缺口**：旧兜底的触发条件是「目标表存在 **且** `touched` 为空」。现有 4 例里 `test_first_build_without_target` 用了空 `touched` 但目标表不存在，`test_incremental_part_keeps_history` 用了非空 `touched` —— **「目标表存在 + `touched` 为空」这一组合没有用例锁住**。代码现在不看 `touched`，所以行为正确；但若有人把兜底加回来，现有测试**不会失败**。建议补 1 例（§10-⑤）。

## §9 重点三：valuation 反算交叉核对 + 缺行

### 9.1 与 `valuation_daily` 反算的换手率对拍

反算口径（独立于本地 `float_share`）：`turn_rev = volume × 100 × close ÷ circ_mv × 100`，`circ_mv` 取 `meta/financials/valuation_daily.parquet`（东财）。窗口 2024-01-01 ~ 2026-11-11，脚本 `_staging/b2_slice3_20261113/valuation_crosscheck.py`：

| 项 | 值 |
|---|---|
| 可比行数 | 3,736,036（valuation 2024+ 3,743,267 行，`circ_mv` 空值 0、`<=0` 0） |
| 相对差 p50 / p90 / p99 / p99.9 | **0** / **0** / 4.61e-6 / 1.36e-3 |
| 逐值相等 | 2,003,629（53.6%）；`≤1e-6` 3,689,165（98.75%） |
| `>1e-2` / `>1` | 2,140 / 21 |
| 换手有、valuation 无 | 7,094 行 |

分来源：

| source | 行数 | p50 | p99 | max | `>1e-2` |
|---|---|---|---|---|---|
| `derived:quotes_volume+float_share` | 3,686,596 | 0 | **3.28e-16** | 104.01 | 110 |
| `baostock` | 49,440 | 1.24e-5 | **0.0820** | 1.059 | 2,030 |

结论：**派生部分与东财 `circ_mv` 反算在浮点精度内一致**（p99 3.3e-16），说明本地 `float_share × close ≈ circ_mv` 成立、派生口径自洽；反算侧 `max=104` 的 110 行与 §4 的 ~100 倍坏行**完全重合**（同一批 `ts_code/trade_date`），互为佐证。`baostock` 部分系统性偏离本地口径（p99 8.2%、12.2% 的行 >1e-4），属**同一列两套口径混存**，`source` 列可区分但无口径说明。

### 9.2 缺行：退市标的与分级基金

| 类 | 数 | 说明 |
|---|---|---|
| 行情有、换手**完全无行** | **19 只** | 18 只为退市（`000024.SZ 招商局`、`000033.SZ 新都退`、`000511.SZ`、`000562.SZ`、`000594.SZ 国恒退`、`000693.SZ`、`000916.SZ`、`000979.SZ`、`002070.SZ`、`300186.SZ`、`300372.SZ`、`600270.SH`、`600401.SH`、`600432.SH`、`600656.SH`、`600680.SH`、`600806.SH`、`601268.SH`），1 只为新上市 `688827.SH`（`list_date=2026-11-10`） |
| 退市标的总数 / 有换手 / 无换手 | 336 / 234 / **102** | 无换手的 102 只里 18 只有行情（上表），其余 84 只本地行情本身就没有 |
| 换手有、行情无 | 0 | 换手 5,813 只全部是行情的子集 |
| 分级基金 / ETF | **0** 只 | 换手表只覆盖 `quotes/stock`；分级基金子份额（`150xxx`/`502xxx`）与 ETF 不在其中 |

归因：

- 18 只退市标的的行情完整（示例 `000024.SZ` 5,211 行 1993-06-07~2015-12-07，`volume` 空值 0、`<=0` 0），**具备派生条件却一行都没有** —— 与 §4 的 140 行同属「派生覆盖不全」，但性质不同：那 140 行是值错，这 18 只是整只缺。可派生，属可修缺口。
- `688827.SH` 同样具备行情，缺行使 `turnover` 的 `date_max` 停在 2026-11-11 而非最新交易日；下一轮 `derive-turnover` 会自动补上，无需人工。
- 分级基金不在本表属**语义边界**（清单写"个股换手率"），不算缺陷；但清单与 `factors/meta.json` 都未写明「不含基金」，下游若误用会静默缺样本。

## §10 缺陷与建议（**均未执行，等批复**）

| # | 缺陷 | 影响 | 建议 | 需要你批什么 |
|---|---|---|---|---|
| ① | **140 行派生 `turn` 与现行口径不符**（19 行 ~100 倍、117 行 0.1~10 倍、4 行 1e-4~0.1），且 anti-join 只追加 → 永久残留 | 个股换手率在 25 只标的上错值；下游 `turnover_20` 等百分位/秩统计受污染 | 定点重算这 140 行（限定 `ts_code+trade_date` 白名单，其余行 0 改动），改前 `copy2` 到 `_backup/factors_turnover_<日期>/`，落盘后给行数守恒 + 逐值对拍 + SHA256 | 是否批准定点重算；批准后我出对拍方案再执行 |
| ② | 清单 L76 行数滞后 33 行、口径文字写「自由流通股本」 | 读者会误以为换手率用自由流通股本 | 改 L76 数字为 18,055,751、口径改为「流通股本 `float_share`」，并补一句「不含基金」 | 属文档改动，交文档那一路（我不动清单） |
| ③ | `baostock` 189,419 行与本地口径 p99 差 8.2%、max 106%，同列混口径 | 同一列两种定义，按 `source` 才能区分 | 在 `factors/meta.json` 增补 `source_semantics` 段落说明两套口径差异；**不重算**（上游值不可用本地口径替代） | 是否批准补写 meta 说明 |
| ④ | 18 只退市标的整只缺行（行情齐全、可派生） | 退市样本在同表缺失，做长周期研究会被动幸存者偏差 | 用 `derive_turnover_local(codes=[...])` 定点补，`_staging` 断点 + 对拍 | 是否批准补这 18 只 |
| ⑤ | `_assemble` 缺「目标表存在 + `touched` 为空」的回归用例 | 兜底若被加回来，测试不会失败 | 在 `tests/test_turnover_assemble.py` 补 1 例 | 属代码改动，需你批后再动 |
| ⑥ | `factors/manifest.json` 的 `produced_by` 只写 `sync-turnover`，漏 `derive-turnover`（98.95% 行由后者产出） | 重建入口不完整，按 manifest 复现会漏一步 | 改为两个入口并列 | 是否批准（需重写 manifest 记录） |
| ⑦ | 1 行 `turn=0`（`000001.SZ 1991-09-30`）与现行派生 `filter(turn > 0)` 不自洽 | 该行来自历史补行路径，日常派生不会生成也不会删除它 | 无需改，只在 `factors/meta.json` 记一笔来源 | 仅登记，等你确认口径 |

## §11 复现命令

```powershell
# 全表重算对拍（A）
.venv\Scripts\python.exe _staging\b2_slice3_20261113\recompute_check.py
# 派生行误差分桶
.venv\Scripts\python.exe _staging\b2_slice3_20261113\bucket_check.py
# valuation 反算交叉核对
.venv\Scripts\python.exe _staging\b2_slice3_20261113\valuation_crosscheck.py
# manifest 校验（本表）
.venv\Scripts\python.exe tools\manifest_tool.py --check --only factors
```

## §12 文件指纹

| 文件 | 字节 | SHA256 |
|---|---|---|
| `data/local_store/factors/stock_turnover.parquet` | 145,625,057 | `7f90eda18ee04e31983bef24811f5104f8840f8eac8a5518be69a0e368cba184` |
| `data/local_store/factors/manifest.json` | — | 本表记录 5 项与实测全等（§3） |
| `src/data/local_store/turnover.py` | — | 只读，未改 |
| `tests/test_turnover_assemble.py` | — | 只读，未改 |
