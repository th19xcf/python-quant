# B1 第五片 · 宏观五表基础层校准体检

- 体检对象：`data/local_store/meta/macro/` 下 5 张表
- 口径：与前两片一致（① 写入口 ② manifest 一致性 ③ 权威源对拍 ④ 第二份同语义 ⑤ 权威清单登记），外加"起点缺失归因 / 日期连续性 / 内部一致性"三项替代实测
- 性质：**只读**。未改代码、未写 `data/`、未改任何 parquet / manifest / 清单、未提交
- 实测时间：2026-11-12；依赖本地 `000001.SH.parquet` 作为交易日历基准
- 复现脚本：`2026-11-12/data-lineage/work/inspect_macro.py`、`check_macro_cross.py`、`explain_macro_anomalies.py`、`check_macro_extra.py`、`check_macro_consistency.py`、`check_macro_monthly.py`、`check_margin_segments.py`、`compare_macro_copies.py`

---

## 0. 结论摘要

| 项 | 结论 |
|---|---|
| 批次归属 | 五表**全部**在 B1 深度 0 基础层（`table_inventory.csv` L22 标 `L0/base`；`governance_order.md` L68「宏观五表」）。**无一张需要跳过** |
| ① 写入口 | 4/5 有 `tools/` 正式入口且可离线幂等重跑；**信用曲线不合格**——唯一产出方是 `_staging/gap_close_20261111/build_credit_curve.py`，无 `tools/` 入口 |
| ② manifest | 5/5 parquet 的 `rows` 与 `sha256` 实测全对；`manifest_tool --check --only meta/macro` → 4 份 / 8 条记录 / 不一致 0。但 6 处文字字段与实测不符（见 §4） |
| ③ 对拍 | 曲线 10Y 与单期 10Y **5,175 日最大差 0.000000pp**；信用对国债 33,001 行全覆盖；债券指数与 10Y 月度相关 **−0.90**。**10Y 表 2005-01~2006-02 的 288 行无第二源**（两源重叠 0 天）；**信用曲线 7Y 有 197 天源端常量 4.0**，产生 118 行负利差 |
| ④ 第二份同语义 | 无第二份"正式"数据。但 **`backups/critical_assets/` 的两融快照停在 2026-11-10（8,068 行），比正式表少 2 行**，是一次 publish 未刷新 |
| ⑤ 清单登记 | 11 条登记偏差（4 条口径/数字错误：P1~P4；5 条缺漏：P5~P7、P10、P11；2 条编号或指向不一致：P8、P9），逐条见 §6。**未改动该文件** |
| 可补缺口 | 国债曲线 / 信用曲线 / 债券指数 三表停在 **2026-11-10**，而中债 11-11 曲线已发布（10Y 表已有 11-11 行可证）→ 落后 1 个交易日，属"未运行采集"，不属"无数据" |

---

## 1. 批次归属核对（先做，避免凑数）

| 表 | `table_inventory.csv` | `governance_order.md` | 归属判定 |
|---|---|---|---|
| `cn_treasury_yield_10y` | L22 `macro`，`L0` / `base` | L68「宏观五表」 | B1 ✓ |
| `cn_market_margin_daily` | 同上 | 同上 | B1 ✓ |
| `cn_treasury_yield_curve` | 同上 | 同上 | B1 ✓ |
| `cn_credit_yield_curve` | 同上 | 同上 | B1 ✓ |
| `cn_bond_index` | 同上 | 同上 | B1 ✓ |

五表合计关联缺口 G-038 / G-052 / G-053 / G-054 / G-055 / G-056 / G-057（`lineage.json` L1011-1019 与 `table_inventory.csv` L22 一致）。

---

## 2. 五表基础事实（实测）

| 表 | 行数 | 字节 | 覆盖区间 | SHA256（前 16） | 列 |
|---|---|---|---|---|---|
| `cn_treasury_yield_10y` | 5,464 | 34,727 | 2005-01-04 ~ 2026-11-11 | `fa16b05b2a6ebb4f` | `date, y10, source` |
| `cn_market_margin_daily` | 8,070 | 279,550 | 2010-03-31 ~ 2026-11-11 | `ecec8fe5a1b00e32` | `date, market, source, fin_buy, fin_balance, sec_sell_qty, sec_qty, sec_balance, total_balance` |
| `cn_treasury_yield_curve` | 41,400 | 171,839 | 2006-03-01 ~ 2026-11-10 | `03e7cd33f7fc5647` | `date, tenor_years, tenor, ytm_pct, curve, source` |
| `cn_credit_yield_curve` | 33,001 | 150,479 | 2006-12-25 ~ 2026-11-10 | `1d345910a2262b6b` | 同上 |
| `cn_bond_index` | 18,645 | 117,696 | 2002-01-04 ~ 2026-11-10 | `8d8d023de086e8bb` | `date, index_name, value, source` |

结构实测：

| 表 | 主键 | 重复 | 唯一日期 | 期限 / 指数 |
|---|---|---|---|---|
| 10Y | `date` | 0 | 5,464 | 单期 |
| 两融 | `(market, date)` | 0 | 4,035（sh/sz 各 4,035） | — |
| 国债曲线 | `(date, tenor_years)` | 0 | 5,175 | 8 个：3M/6M/1Y/3Y/5Y/7Y/10Y/30Y，**每日期 8 期限齐全**（无缺期限日期） |
| 信用曲线 | `(date, tenor_years)` | 0 | 4,968 | 7 个：3M/6M/1Y/3Y/5Y/7Y/10Y |
| 债券指数 | `(date, index_name)` | 0 | 6,215 | 3 个指数各 6,215 行 |

空值：五表全列 **0 空值**（两融 `sec_qty` / `sec_sell_qty` 已由 2026-11-10 回填，manifest `fill_record_2026_11_10` 记 1,322 → 0，实测一致）。

---

## 3. ① 写入口：代码位置与可重跑性

| 表 | 正式入口 | 命令形态 | 离线可重跑 | 能否自行前进覆盖 | 判定 |
|---|---|---|---|---|---|
| `cn_treasury_yield_10y` | `tools/treasury_yield.py` | `collect` / `build` / `publish` 三段 | ✓（缓存 `_staging/treasury_yield/parts/`，`publish` 走 DG-11 备份 + 原子追加 + 刷新 manifest） | 需先 `collect`（联网） | 合格 |
| `cn_market_margin_daily` | `tools/margin_daily.py` | 同上 | ✓（`_staging/margin_daily/parts/`） | 需先 `collect`（联网） | 合格 |
| `cn_treasury_yield_curve` | `tools/build_cn_bond_tables.py --apply` | 单段 build（无独立 publish） | ✓ 输入 `_staging/multi_asset_tier2_20261110/cbond/` 现存 21 个 `yield_curve_YYYY.parquet` + `progress.json`，同输入 → 同行数（幂等） | ✗ 缓存冻结在 2026-11-10，需 `tools/fetch_cn_bond_data.py`（联网）刷新 | 合格（可重跑，不能自增） |
| `cn_bond_index` | 同上 | 同上 | ✓ 输入 `index_固定利率国债指数/国债总指数/国开行债券总指数.parquet` 3 个 | ✗ 同上 | 合格（可重跑，不能自增） |
| `cn_credit_yield_curve` | **无 `tools/` 入口** | `_staging/gap_close_20261111/build_credit_curve.py`（一次性，G-056 收口） | ✓ 输入 `_staging/gap_close_20261111/cbond/curve_YYYY.parquet` 21 个 + `progress.json`；用 `write_parquet_atomic` / `write_text_atomic` | ✗ | **不合格**：B1 验收"每张表都有可反复跑入口"未满足 |

补充实测：

1. `build_cn_bond_tables.py` 的 parquet 用 `write_parquet_atomic`（L145-146）✓，但 **manifest 用裸 `Path.write_text`（L186-187）**，不是 `write_text_atomic` → 违反 DG-9（正式文件需原子替换）。同目录的信用曲线脚本反而两个都用 atomic，两个写入口口径不一致。
2. `tools/fetch_cn_bond_data.py` 存在 ✓（采集端），`INDEX_CATEGORIES` / `YIELD_YEARS` / `THROTTLE` 均在。
3. `lineage.json` 的 macro 节点（L961-1004）：`writer_primary` 写了三个 tools 脚本，但 `writers_detected` 只有 `build_cn_bond_tables.py:145/146` + 4 个 `_staging` oneoff，**缺 `treasury_yield.py` 与 `margin_daily.py`**（两表的实际 owner）。节点 `rows: null` / `files: null`，未落到具体表。→ 血缘图这一层对"写入口"的记录不完整，建议由改图那一路补。

---

## 4. ② manifest 记录数 vs 实际文件

`manifest_tool --check --only meta/macro` 实测：

```text
manifest 份数 4 / 文件记录 8（无法定位 0）/ 不一致条目 0 / 无 manifest 的 parquet 0
结论: PASS
```

逐项核对（不依赖工具，自行比对）：

| 表 | manifest | 声明 rows | 实测 rows | 声明 sha256 | 实测 sha256 | 判定 |
|---|---|---|---|---|---|---|
| `cn_treasury_yield_10y` | `manifest.json` `files` | 5,464 | 5,464 | `fa16b05b…` | `fa16b05b…` | 一致 |
| `cn_market_margin_daily` | 同上 | 8,070 | 8,070 | `ecec8fe5…` | `ecec8fe5…` | 一致 |
| `cn_treasury_yield_curve` | 单表 manifest | 41,400 | 41,400 | `03e7cd33…` | `03e7cd33…` | 一致（无 `bytes` 字段） |
| `cn_credit_yield_curve` | 单表 manifest | 33,001 | 33,001 | `1d345910…` | `1d345910…` | 一致（有 `bytes`） |
| `cn_bond_index` | 单表 manifest | 18,645 | 18,645 | `8d8d023d…` | `8d8d023d…` | 一致（无 `bytes` 字段） |

**登记一致但文字/字段有偏差的 6 处**（按严重度排序）：

| # | 位置 | 现写 | 实测 | 影响 |
|---|---|---|---|---|
| M1 | `manifest.json` L43 + L77 | 频率"含 **158** 个交易所休市但银行间仍估值的工作日" | **157** 个非 `000001.SH` 交易日，其中 **151 是周六/日**、仅 **6 天** 是工作日（2005-02-07/08、2006-01-26/27、2018-12-31、2024-02-09） | 计数差 1，且把周末说成工作日，会误导消费方"周一补周五"的对齐方式 |
| M2 | `cn_credit_yield_curve.manifest.json` L48 | `cross_check_treasury.overlap_rows = 41400` | 信用表仅 **33,001 行**，不可能与国债曲线重叠 41,400 行；41,400 是"同批国债曲线 vs 已落库国债曲线"的行数。信用对国债的真实同日同期限重叠 = **33,001**（全覆盖） | 字段名 + `against` 指向会让人读成"信用 vs 国债重叠 41,400 行、差 0"，逻辑上不成立 |
| M3 | `cn_credit_yield_curve.manifest.json` L24-46 | `days_by_year` 按 4,968 个日期逐列 | 3Y/5Y/7Y 只有 **4,712** 行、10Y 只有 **3,961** 行；各期限起始日不同：3Y/5Y/7Y = 2008-01-02，10Y = 2011-01-04 | 会把 4,968 误当作每期限行数；缺"逐期限起始日" |
| M4 | `manifest.json` L308 vs L309 | L308"2026-11-11 两融数据源端尚未发布"；L309"最新可得日 2026-11-11（采集于 2026-11-12T21:32:35）" | 实测两融 11-11 沪深各 1 行，`source` = `sse:stock_margin_sse` / `szse:stock_margin_szse` | 同一 `known_limits` 内自相矛盾，后者是 21:32 发布后补写、前者未删 |
| M5 | `cn_treasury_yield_curve.manifest.json` L29 | `known_gaps` 写 **G-040** | 权威清单 L48 同一缺口写 **G-054** | 同一缺口两个编号 |
| M6 | 2 个 CSV 可读副本 | 未进入任何 manifest 的 `files` 段，仅出现在 `manifest.json` 的 `catchup_20261112.before_sha256`（历史哈希）与清单 L101/L102 | 实测 `cn_treasury_yield_10y.csv` 244,423 B / 5,464 数据行、`cn_market_margin_daily.csv` 996,057 B / 8,070 数据行，与 parquet 逐行同日一致 | 工具"未登记的非 parquet 文件 0"是因为按文件名扫到了历史哈希块，不等于有 `rows`/`bytes`/覆盖率核对；清单 L101/L102 的"8,071 行 / 5,465 行"是**含表头行数**，与 parquet 数据行数差 1，字面上易误读 |

---

## 5. ③ 权威源对拍：可对拍部分（实测复现）

| # | 对拍 | 对象 | 比对列 | 重叠 | 容差 / 实测 | 判定 |
|---|---|---|---|---|---|---|
| C1 | 曲线 10Y vs 单期 10Y（同为中债，跨表） | `cn_treasury_yield_curve` ↔ `cn_treasury_yield_10y` | `ytm_pct` vs `y10` | 5,175 日 | 声明 1e-4pp；实测 **max\|Δ\| = 0.000000pp**，>1e-9 的 0 天 | 通过（复现 manifest `cross_check_10y`） |
| C2 | 信用 vs 国债（同日同期限） | 两曲线表 | 利差 = 信用 − 国债 | 33,001 行 | 无阈值（诊断）；中位 **0.8947pp**、区间 **−0.4044 ~ 2.6029pp** | 通过，但见 §5.2-B |
| C3 | 债券指数 vs 10Y（月度方向） | `cn_bond_index` ↔ `cn_treasury_yield_10y` | 指数月收益 vs Δy10 | 263 个月末点 / 有效 262 对 | 无阈值（诊断）；相关系数 **−0.9043**（固定利率国债）/ **−0.9025**（国债总）/ **−0.7910**（国开） | 通过（方向与量级自洽） |
| C4 | 指数两两 | 3 个指数 | 月收益 | 有效 262 对 | 固定利率国债~国债总 **0.9995**、国债~国开 **0.8834**、固定利率国债~国开 **0.8797** | 通过 |
| C5 | 两融勾稽 | 本表 | `fin_balance + sec_balance − total_balance` | 8,070 行 | **7,970 行残差 < 1 元；100 行残差恰为 1e6 元** | 通过，归因见 §5.2-C |
| C6 | 两融沪深对齐 | 本表 | 日期集合 | sh 4,035 / sz 4,035 | 集合差 **0 / 0**，首日同为 2010-03-31 | 通过 |
| C7 | 日历连续性 | 5 表 vs `000001.SH` | 日期集合 | 各表区间内 | 缺失交易日 **全部 0 天** | 通过 |

### 5.1 无法对拍的部分（写清对象、列、容差、归因）

| # | 缺口 | 对拍对象 | 比对列 | 容差 | 归因 |
|---|---|---|---|---|---|
| U1 | 10Y 表 **288 行（5.3%）无第二源**：2005-01-04 ~ 2006-02-28 全部来自 `eastmoney:bond_zh_us_rate` | 本应"东财 vs 中债" | `y10` | 1e-4pp | **实测两源重叠 0 天**（`P6_t10_overlap_between_sources = 0`）。中债段从 2006-03-01 才开始，东财段止于 2006-02-28，**没有任何一天两源同时有值**，因此这 288 行在本地不可对拍。旁证：东财段日变动噪声显著更高——\|Δ\| 均值 **0.0565pp** / p99 **0.2207pp** / 5 天 >0.2pp；中债段均值 **0.0169pp** / p99 **0.0911pp** / **0 天** >0.2pp（最大跳变 2005-11-16 +0.3179pp，也在东财段） |
| U2 | 两融"上交所官方 vs 东财 4,027 日逐日完全相同"、"深市抽样 5 日相对差 ≤0.0041%" | 交易所官网 | 各金额列 | 0 元 / 0.0041% | 本地只有一份合并结果，**离线不可复算**；这些结论由 manifest `crosscheck` 记录、需联网重采才能复现。本次不采信也不否定，标为"未在本轮复核" |
| U3 | 债券指数绝对点位无外部基准（无第二指数源） | — | `value` | — | 只能做内部一致性（C3/C4）+ 起点/值域健全性：3 指数各 6,215 行、0 重复、全部 > 0（值域 97.17~255.61）。逐指数日收益极值：固定利率国债 **−1.772% / +1.987%**、国债总 **−1.585% / +1.734%**、国开 **−1.685% / +1.903%**。\|日收益\|>1% 的天数：固息 **22**（年份 2002/2004/2005/2008/2010/2014/2020）、国债总 **13**（2004/2005/2008/2010/2014/2020）、国开 **45**（2002/2003/2004）。同日例：2004-04-29 三指数 −1.772% / −1.585% / −0.546%（与 2004 年债市大幅调整相符）；2010/2014/2020 的孤立大日与对应年份债市行情方向一致 | 值域与尾部形态合理，未发现单点错位或断点；仍需注意：单日 1.5%~2% 对债券总收益指数偏大，下游做风险平价/波动率目标时应截尾，或从 2009 年起算 |

### 5.2 替代实测：可执行的验证与已得到的结论

**A. 曲线内部一致性（已跑）**
- 每个日期 8 期限齐全：`per_date` 分布只有 `{8: 5,175}` 一个键，无缺期限日期。
- 信用与国债逐日同期限全部可配对（33,001/33,001），说明两表来自同一次响应、日期集合是国债表日期的子集。

**B. 信用曲线存在源端常量段（新发现，需登记）**

| 期限 | 常量段 | 天数 | 常值 |
|---|---|---|---|
| 3Y | 2008-01-02 ~ 2008-04-23 | 77 | 5.2800 |
| 5Y | 2008-01-02 ~ 2008-04-22 | 76 | 5.3000 |
| **7Y** | **2008-01-02 ~ 2008-10-16** | **197** | **4.0000** |
| 10Y | 2011-01-04 ~ 2011-04-06 | 63 | 5.6951 |

- 7Y 的 4.0000 是整数占位特征，且该段与国债曲线重叠后产生 **118 行负利差**（2008-01-02 ~ 2008-09-08，全部为 7Y，最大 **−0.4044pp**，如 2008-07-02 信用 4.0000 / 国债 4.4044）。
- 判定：**源端前推填充/占位，非真实估值**（真实 AAA 中短票不应低于同期限国债，且不应 197 天不动）。10Y 段的 63 天常量同属新增期限时的回填。
- 影响：下游做信用利差因子时，这 197 天（尤其 118 行负利差）必须剔除或置空，不得当作真实信号。
- 建议的可执行验证（不联网也能做）：把 7Y 常量段与同期 5Y/10Y 的差值序列对齐，若 5Y→7Y 的斜率在 2008-10-17 出现一次性跳变，即可坐实"该段是填充"——本次已看到 2008-10-17 首次偏离 4.0。

**C. 两融 100 行勾稽偏差归因（已定位）**
- 全部 100 行为 `market = sz` 且 `source = szse:stock_margin_szse`（即官方 2 位小数口径的补缺行），残差绝对值均为 **1e6 元**（`9.9999999975e5 ~ 1.00000000024e6`，即 0.01 亿 = 官方最小刻度，浮点误差 ≤2.4e-4），其中 **81 行**是 1e6 的整数倍，方向 ±1 格。
- 机制：`total_balance` 与 `fin_balance`/`sec_balance` 在官方页面上各自独立四舍五入，因此三者不严格相加。
- 例：2025-06-03 深市 `fin_balance 880,754,999,999.9999 + sec_balance 3,572,000,000` vs `total_balance 884,328,000,000.0001`，差 1e6。
- 判定：**口径刻度，非逻辑错误**。但 manifest L92 把 `total_balance` 定义为"= 融资余额 + 融券余量金额"这一恒等式在这 100 行不成立，建议在 `known_limits` 明写"szse 补缺行三列各自 2 位小数，恒等式允许 1 格（1e6 元）偏差"。

**D. 起点缺失区间归因（逐表明细）**

| 表 | 实测起点 | 起点前是否有交易日 | 归因 | 判定 |
|---|---|---|---|---|
| 10Y | 2005-01-04 | 是（2005-01-04 之前日历正常） | 东财源起点；中债渠道 2005 全年返回 0 行 | 源端边界，非链路问题 |
| 国债曲线 | 2006-03-01 | 是 | 中债 `historyQuery` 起点 | 源端边界 |
| 信用曲线 | 3M/6M/1Y = 2006-12-25；3Y/5Y/7Y = 2008-01-02；10Y = 2011-01-04 | 是 | 中短期票据 AAA 各期限**分别上线**，旧年份无该期限报价 | 源端边界；但须按期限分别声明（现 manifest 只给全表起点） |
| 债券指数 | 2002-01-04 | 是 | 中债财富指数该接口最早日期（值起步 ~100，与基期口径一致） | 源端边界 |
| 两融 | 2010-03-31 | **是**（2010-03-29 / 03-30 是交易日） | **融资融券试点 2010-03-31 启动**，之前无业务；沪深首日一致 | **"未接线"排除，属"源端确实没有"** |

**E. 交易日期差异（四个债券类表共有，已归因）**

| 表 | 相对 `000001.SH` 缺失交易日 | 多出的非交易日 | 其中周末 | 其中工作日 | 值是否等于前一日 |
|---|---|---|---|---|---|
| 10Y | 0 | 157 | 151 | 6 | 仅 16/157 相等 → 源端真实发布值，非本地前推 |
| 国债曲线 | 0 | 144 | 142 | 2 | — |
| 信用曲线 | 0 | 140 | 138 | 2 | — |
| 债券指数 | 0 | 188 | 167 | 21 | 0/188 相等 |
| 两融 | 0 | 0 | 0 | 0 | — |

工作日型非交易日：10Y 表 6 个 = `2005-02-07/08`、`2006-01-26/27`、`2018-12-31`、`2024-02-09`；曲线 / 信用各 2 个 = `2018-12-31`、`2024-02-09`（起点 2006-03-01 / 2006-12-25，2005-2006 那 4 天不在区间内）；债券指数 21 个 = 上述 6 个 + `2002-02-11`、`2002-02-19~22`、`2002-09-30`、`2003-01-30/31`、`2003-05-06~09`、`2004-01-19~21`——均为**交易所休市、中债仍发布估值/指数**的日期（2002-2006 的为春节 / 五一调休）。

> 结论：四张债券类表用的是**中债发布日日历**，不是 A 股交易日历。消费方必须 `asof` 前向填充，禁止等号 join。**当前只有 10Y 表的 manifest 写了这条（且写成"158 个工作日"），曲线/信用/指数三张表完全没写。**

**F. 新鲜度（实测可证的可补缺口）**

| 表 | 最新日期 | 中债该日是否有数据 | 判定 |
|---|---|---|---|
| `cn_treasury_yield_10y` | 2026-11-11 | ✓（`source = chinabond:bond_china_yield`） | 最新 |
| `cn_market_margin_daily` | 2026-11-11 | ✓ | 最新 |
| `cn_treasury_yield_curve` | 2026-11-10 | ✓（同日中债曲线已被 10Y 表收录，故 11-11 已发布） | **落后 1 个交易日** |
| `cn_credit_yield_curve` | 2026-11-10 | 同上 | **落后 1 个交易日** |
| `cn_bond_index` | 2026-11-10 | 同日中债有发布 | **落后 1 个交易日** |

→ 这是"未运行采集"，不是"无数据"；补齐路径：`tools/fetch_cn_bond_data.py`（刷新 `_staging/.../cbond/` 的 `yield_curve_2026.parquet` 与 3 个 `index_*.parquet`）→ `tools/build_cn_bond_tables.py --apply`；信用曲线还需 `_staging/gap_close_20261111/build_credit_curve.py`（因为无 `tools/` 入口）。

**G. 下游真实读路径（DG-23）**
- 代码级确认：`tools/build_state_vector.py:353` 读两融、`:407-408` 读 10Y（`y10 → y10_pct`），`tools/verify_state_vector.py:258/332` 同样读这两张；`state_vector` 节点确为下游消费者 ✓。
- `cn_treasury_yield_curve` / `cn_credit_yield_curve` / `cn_bond_index`：全仓 grep 无任何读点（仅 `build_cn_bond_tables.py` 自身写出、以及它对 10Y 表的读），与清单"暂无代码消费者"一致 ✓。
- 端到端跑通需重算 `state_vector`（会写 `data/`），超出本次只读范围，未执行；建议在后续收口时补跑一次 `tools/build_state_vector.py`（对比新旧 sha 应完全一致，因为两表最新日期未变）。

---

## 6. ⑤ 权威清单 `docs/数据权威路径清单.md` 登记 vs 实测（逐条，未改文件）

| # | 行 | 现写 | 实测 | 判定 | 建议改法 |
|---|---|---|---|---|---|
| P1 | L31 | "两源在 **5,175 个重叠交易日** 上最大绝对差 0.0001pp，实质一致，故分段拼接成立" | 东财段与中债段**重叠 0 天**；5,175 是"曲线 10Y vs 单期 10Y（同为中债）"的重叠，最大差 **0.000000pp** | **口径错误**（把跨表同源对拍写成了跨源拼接依据） | 改为：中债两表 10Y 重叠 5,175 日、最大差 0.000000pp；东财段与中债段无重叠日（0 天），跨段不可逐日对拍，拼接依据是单段各自可信而非两源一致 |
| P2 | L31（及 manifest L43/L77） | "**158** 个交易所休市但银行间仍估值的**工作日**" | **157** 天，其中 **151 天是周末**、6 天是工作日 | 数字 + 口径双错 | 改为"157 个非 A 股交易日（151 周末 + 6 个调休工作日）" |
| P3 | L32 | "用深交所官网单日值补 **204 天**" | `szse:stock_margin_szse` 实测 **206 行**；落在 2024-11~2025-06 窗口内仅 **152** 天，另 **54** 天在 2010-04(9) / 2017-03(4) / 2018-08(7) / 2023-09(5) / 2024-06~07(16) / 2024-10(9) / 2026-11(4) | 数字错误 | 改为 "206 天（窗口内 152 + 窗口外 54）" 并列出窗口外日期 |
| P4 | L32（及 manifest L106） | "其 2024-11~2025-06 **缺约一半**交易日" | 该窗口共 **160** 个交易日，东财仅覆盖 **8** 天，缺 **152** 天（**95%**） | 程度严重低估 | 改为 "缺 152/160（95%）" |
| P5 | L32（及 manifest L92/L137/L176/L304） | `total_balance` 定义为"融资余额 + 融券余量金额"这一**严格恒等式**；manifest 已说明官方 2 位小数、1 格 = 1e6 元，但那是用于"官方 vs 东财同日偏差"，未说明**恒等式本身**在补缺行不成立 | 100 行 szse 行 `fin_balance + sec_balance − total_balance` = 1e6 元（0.01 亿刻度，81 行为整数倍）；`sec_balance` 另有 8 个零值、`sec_qty` 9 个、`sec_sell_qty` 15 个 | 缺漏（会让按恒等式做的下游校验在这 100 行误报） | 在 L92 定义后补一句"szse 补缺行三列各自 2 位小数独立取舍，恒等式允许 1 格（1e6 元）偏差；`sec_balance` 8 / `sec_qty` 9 / `sec_sell_qty` 15 个零值均为官方真实零值" |
| P6 | L47 | "3 个指数 × 6,215 日 = 18,645 行，2002-01-04~2026-11-10" | 完全一致 ✓；但**未写日历**：188 个非 A 股交易日（167 周末 + 21 调休工作日） | 数字对、缺日历声明 | 补"含 188 个非 A 股交易日，按中债发布日日历，消费方须 asof 前推" |
| P7 | L48 | "8 期限 … 41,400 行，2006-03-01~2026-11-10"；"10Y 重叠 5,175 日、最大差 0.000000pp" | 全部一致 ✓；但**未写日历**（144 个非交易日） | 数字对、缺日历声明 | 补日历声明 |
| P8 | L48 / manifest L29 | 同一缺口：清单写 **G-054**，manifest 写 **G-040** | — | 编号不一致 | 统一为 G-054（清单为准），或同步改清单 |
| P9 | L49 | "同批国债曲线与上表重叠 **41,400 行**、最大差 0.0，证明同批同口径" | 41,400 是**国债表自身的行数**（同批国债 vs 已落库国债），不是信用与国债的重叠；信用表 33,001 行与国债同日同期限**全部可配对**，利差中位 0.8947pp | 表述误导（易被读成"信用比国债差 0"） | 改为"同批响应的国债曲线与已落库国债曲线重叠 41,400 行、差 0（同批同口径证明）；信用与国债同日同期限可配对 33,001 行，利差中位 0.8947pp" |
| P10 | L49 | "7 期限 × 33,001 行，2006-12-25~2026-11-10" | 行数/覆盖一致 ✓；但**未写**：① 逐期限起始日（3Y/5Y/7Y=2008-01-02、10Y=2011-01-04）② **7Y 197 天常量 4.0000 段** ③ 该段造成 118 行负利差 | 重大缺漏（下游会直接把占位段当信号） | 补"7Y 2008-01-02~2008-10-16 共 197 行为源端常量 4.0000（新增期限回填，非真实估值），与国债叠加产生 118 行负利差（最大 −0.404pp），做信用利差须剔除" |
| P11 | `table_inventory.csv` L22 / `lineage.json` L961-1004 | 写入口只列 3 个 tools 脚本；`writers_detected` 缺 `treasury_yield.py` 与 `margin_daily.py`；`rows: null` / `files: null`；`known_defects` 只有"两融起点晚"；`reconciled` 五表统一写"state_vector 派生侧使用" | 信用曲线**无 tools 入口**（只有 `_staging/gap_close_20261111/build_credit_curve.py`）；`writers_detected` 虽有 4 个 staging oneoff 但缺两个正式 owner；`state_vector` **只读两融与 10Y 两表**，另三张零消费者 | 登记类缺漏 + 表述过宽 | 由改图那一路补：信用曲线入口补 `_staging` 脚本并标注"待提为 tools/ 入口"；`writers_detected` 补两脚本写点；节点补 rows/files；`known_defects` 补"7Y 常量段""中债发布日日历"；`reconciled` 按表拆分（仅两融 / 10Y 有 state_vector 消费） |

---

## 7. ④ 第二份同语义数据

全仓 + `G:\git\python-quant` 按文件名递归扫描 5 个数据集，命中结果与判定：

| 路径 | 行数 | 字节 | SHA256（前 16） | 判定 |
|---|---|---|---|---|
| `data/local_store/meta/macro/cn_market_margin_daily.parquet` | 8,070 | 279,550 | `ecec8fe5a1b00e32` | 正式（唯一） |
| **`backups/critical_assets/data/local_store/meta/macro/cn_market_margin_daily.parquet`** | **8,068** | **273,433** | **`3ca2f812ff621d71`** | **缺陷**：CRITICAL 快照停在 2026-11-10，落后正式表 2 行 / 1 个交易日，与 `_backup/meta/macro_margin_20261112_213235/` 逐字节相同 |
| `_backup/meta/macro_margin_20261112_213235/cn_market_margin_daily.parquet` | 8,068 | 273,433 | `3ca2f812ff621d71` | 合规回滚稿（2026-11-12 21:32 发布前快照） |
| `_backup/macro_state_inputs_20261112_003701/meta/macro/cn_market_margin_daily.parquet` | 8,066 | 279,546 | `1c5a8d89fe8f0994` | 合规回滚稿（11-09 状态） |
| `_staging/sz_margin_sec_20261110/backup/*pre_fix0808*` | 8,066 | 279,558 | `042fc6a4e246085c` | 合规（DG-5，修复前快照，manifest `defect_fix_2026_11_10` 有记录） |
| `_staging/sz_margin_sec_20261110/backup/*pre_20261110*` | 8,066 | 270,783 | `6d60fe8ec8fcc222` | 合规（manifest `fill_record_2026_11_10.before_sha256` 指向它） |
| `backups/critical_assets/…/cn_treasury_yield_10y.parquet` | 5,464 | 34,727 | `fa16b05b2a6ebb4f` | 与正式一致 ✓ |
| `_backup/macro_state_inputs_20261112_003701/meta/macro/cn_treasury_yield_10y.parquet` | 5,463 | 34,722 | `8d5b11d8cc763cb9` | 合规回滚稿（11-10 状态） |
| 3 张债券类表 | — | — | — | 除 `G:` 镜像外**无任何副本**（既无 `_backup` 也无 `backups/critical_assets`） |
| `G:\git\python-quant\...` | 与源同 | 与源同 | 与源同 | 备份镜像，非第二语义源 |

判定：

1. **不存在两份并存的"正式"数据**：5 个数据集在 `data/local_store/meta/macro/` 各 1 份，无第二路径、无 fallback 常量（`src/` 内 grep 无 `_backup` 读取）。
2. **缺陷 1 处**：`backups/critical_assets` 的两融快照落后。`tools/backup_critical_assets.py` 的 `CRITICAL_FILES` 含两融与 10Y（L90-91），但 21:32 那次 `publish` 没有回刷 critical 快照。→ 建议下一次收口时以 `copy2` 单向刷新，并把"publish 后刷新 critical_assets"写进 `margin_daily.py publish` 的收尾或巡检清单。
3. 3 张债券类表不在 `CRITICAL_FILES` 内，与"可重建"定位一致；但其唯一输入缓存 `_staging/multi_asset_tier2_20261110/cbond/` 与 `_staging/gap_close_20261111/cbond/` 均未纳入任何备份清单，一旦丢失只能联网重采（信用曲线还没有正式入口）→ 建议把它们登记为 `CRITICAL_DIRS` 或明确"可再生、不需备份"的判断依据。

---

## 8. 待办与去向（供统一改清单那一路与后续收口）

| 优先级 | 事项 | 归口 |
|---|---|---|
| 高 | 信用曲线补 `tools/` 正式入口（现仅 `_staging/gap_close_20261111/build_credit_curve.py`），否则 B1"每张表都有可反复跑入口"无法收口 | 代码侧 |
| 高 | 信用曲线 7Y 197 天常量 4.0 段 + 118 行负利差：登记为缺口并在消费侧剔除 | 清单 + 缺口登记 |
| 高 | 三张债券表补 2026-11-11（源端已发布，10Y 表可证） | 采集侧 |
| 高 | `backups/critical_assets` 两融快照刷新到 `ecec8fe5…`（8,070 行） | 备份侧 |
| 中 | `build_cn_bond_tables.py` manifest 写盘改 `write_text_atomic`（DG-9） | 代码侧 |
| 中 | 清单 L31/L32/L47/L48/L49 按 §6 的 P1~P10 逐条订正 | 清单统一修改那一路 |
| 中 | `table_inventory.csv` / `lineage.json` 补写入口与 `known_defects`（P11） | 改图那一路 |
| 低 | 10Y 东财段 288 行标注"单一来源、日变动噪声为中债段 3.3 倍" | 清单 L31 |
| 低 | 两融 `known_limits` 合并 L308/L309 矛盾句 | manifest |

---

## 9. 复现命令

```powershell
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\inspect_macro.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\check_macro_cross.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\explain_macro_anomalies.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\check_macro_extra.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\check_macro_consistency.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\check_macro_monthly.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\check_margin_segments.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\compare_macro_copies.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\verify_report_numbers.py   # 本报告全部数值的一次性复核
.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta/macro
.venv\Scripts\python.exe tools\treasury_yield.py --help
.venv\Scripts\python.exe tools\margin_daily.py --help
.venv\Scripts\python.exe tools\build_cn_bond_tables.py --help
```

以上脚本全部只读：只 `read_parquet` / `read_csv` / 读文件字节算哈希，不写 `data/`、不写 manifest、不改代码。
