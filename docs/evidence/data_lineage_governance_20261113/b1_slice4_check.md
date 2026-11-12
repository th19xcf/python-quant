# B1 第四片：基础层校准体检（events / stock_dividend / fund_dividend / fund_share_change）

- 体检日期：2026-11-12
- 口径来源：`2026-11-12/data-lineage/outputs/governance_order.md`（B1 深度 0）与已建立的前两片报告 `b1_slice1_check.md`、`b1_slice2_check.md`
- 性质：**只读体检**。未改代码、未写 `data/`、未改任何 parquet 与 manifest、未提交；`docs/数据权威路径清单.md` 也未改动，登记类缺陷只在第 6 节逐条列出，供另一路统一修改
- 复现脚本：`2026-11-12/data-lineage/work/b1s4_*.py`（明细见第 7 节）

## 0. 批次归属核对（先核对再开工）

| 表 | 治理清单登记位置 | 登记行数 | 实测行数 | 结论 |
|---|---|---|---|---|
| `events` | `governance_order.md` B1 表清单（L69） | 185,750 | 185,750 | 在 B1，做 |
| `stock_dividend` | B1 表清单（L64） | 60,071 | 60,071 | 在 B1，做 |
| `fund_dividend` | B1 表清单（L65） | 3,413 | 3,413 | 在 B1，做 |
| `fund_share_change` | B1 表清单（L66） | 2,738 | 2,738 | 在 B1，做 |

四张表全部属于 B1 深度 0，无一张需要跳过。

## 1. 结论总表

✅ 通过　⚠️ 通过但有缺陷　⛔ 不通过　— 不适用

| 表 | ① 写入口 | ② manifest | ③ 权威源对拍 | ④ 第二份同语义 | ⑤ 登记相符 |
|---|---|---|---|---|---|
| `events` | ⚠️ 3 个入口，`import`(replace) 分支无既有行守恒断言 | ✅ 行数/字节/SHA/日期段全对 | ✅ 内容键集合差 0 无法解释行 | ✅ 仅快照副本，代码零消费 | ⚠️ 行数/SHA/覆盖 end 全陈旧，消费方锚点错 |
| `stock_dividend` | ⚠️ 实际 4 个入口（治理只记 2 个） | ✅ 三表 parquet 条目全对；同目录 `fund_meta.json` 条目陈旧致整体 FAIL | ⚠️ 57,113 次对拍 99.98% 通过；10 行现金缺口待核 | ✅ 仅回滚/审计快照 | ⚠️ 「12 起」实为 18 行；`meta.json` 无 `dividend_schema`；消费方锚点错 |
| `fund_dividend` | ✅ 单一生产入口，无第二写者 | ✅ | ⚠️ 3,047 次对拍 99.41%；18 例外中 15 行 LOF 口径不可对拍 | ✅ | ⚠️ 消费方锚点错、`build_fund_corporate_actions.py` 非消费者 |
| `fund_share_change` | ⚠️ 2 个入口，混源 3 类（逐行 `source` 可追溯） | ✅ | ⚠️ 1,354 次对拍 85.82%；192 例外中 186 为分级基金折算 | ✅ | ⚠️ 消费方锚点是常量定义，无真实读取点 |

四表共同的一条系统性问题：**四条生产链路都不刷新同目录 `manifest.json`**——`events.py` 只写 `coverage.json`，`collect_stock_dividend.py` / `fund_factors.py` 只写 `meta.json` / `fund_meta.json`，`manifest.json` 完全依赖 `tools/manifest_tool.py` 另行刷新。第 2 节的 FAIL 就是这条链路的直接后果。

## 2. ② manifest 记录数与实际文件核对

### 2.1 四张目标 parquet（逐文件）

| 表 | manifest 位置 | 登记 rows / bytes | 实测 rows / bytes | 登记 SHA256（前 8） | 实测 SHA256（前 8） | 结论 |
|---|---|---|---|---|---|---|
| `events` | `meta/events/manifest.json` | 185,750 / 3,482,917 | 185,750 / 3,482,917 | `7877d1fc` | `7877d1fc` | ✅ |
| `stock_dividend` | `factors/manifest.json` | 60,071 / 3,385,638 | 60,071 / 3,385,638 | `790981b9` | `790981b9` | ✅ |
| `fund_dividend` | `factors/manifest.json` | 3,413 / 31,981 | 3,413 / 31,981 | `761d7aa3` | `761d7aa3` | ✅ |
| `fund_share_change` | `factors/manifest.json` | 2,738 / 19,631 | 2,738 / 19,631 | `07dc30c1` | `07dc30c1` | ✅ |

行数、字节、SHA256、日期段、代码数五项逐文件一致：`stock_dividend` 5,681 只 / ex_date 1991-02-26~2026-11-19；`fund_dividend` 435 只 / 2004-02-03~2026-11-11；`fund_share_change` 687 只 / 2005-02-04~2026-11-05。

### 2.2 目录级校验（`tools/manifest_tool.py --check --only factors`）

| 项 | 登记 | 磁盘实测 | 结论 |
|---|---|---|---|
| `fund_meta.json` 字节 | 4,112 | 4,234 | ⛔ 不一致 |
| `fund_meta.json` SHA256（前 8） | `fa0a79c8` | `4fe7e823` | ⛔ 不一致 |
| `manifest.json` `generated_at` | 2026-11-12T22:25:31 | — | 早于 `fund_meta.json` mtime 22:34:01 |
| 其余 10 项 | — | — | ✅ 一致 |

归因：`tools/reconcile_p2_metadata.py` 于 22:34:01 以 `--commit` 重写 `fund_meta.json`（其 `reconciled_at` 字段即 22:34:01），但未刷新 `factors/manifest.json`。**本片四张 parquet 不受影响**，但 `--only factors` 当前整体判定 FAIL。修法：重跑 `manifest_tool` 刷新 factors 段，或让 `reconcile_p2_metadata` 写完后自动刷新（后者属改代码，另派）。

### 2.3 manifest 内容缺陷：`produced_by` 错配

`src/data/factors_manifest.py` L46-L52 把 4 个对象的产出方写成 `tools/build_fund_corporate_actions.py`，但该脚本全文对 `fund_dividend` / `fund_share_change` **零引用**（`grep` 0 命中），它实际只产出 `meta/fund_corporate_actions_2019plus.parquet`。

| manifest 条目 | 登记 produced_by | 实测生产方 |
|---|---|---|
| `fund_adj_factor.parquet` | `tools/build_fund_corporate_actions.py` | `src/data/local_store/fund_factors.py`（L475 写因子、L587-L596 写事件表） |
| `fund_dividend.parquet` | 同上 | `src/data/local_store/fund_factors.py::build_fund_store` |
| `fund_share_change.parquet` | 同上 | 同上 + `tools/fund_split_repair.py`（追加） |
| `fund_meta.json` | 同上 | `fund_factors.py` L512 主写 + `tools/reconcile_p2_metadata.py` L128-L140 修正 |

## 3. ① 写入口核查（逐入口，不只核一个）

### 3.1 `events`：3 个入口

| 入口 | 写入模式 | 覆盖声明处理 | 存量行保护 | 结论 |
|---|---|---|---|---|
| `src/data/events.py` CLI `import` / `append`（L503-L613、L784、L800-L811） | `import` → `mode='replace'` 整表替换；`append` → 增量合并 | `--coverage-start/--coverage-end` 显式传入 | append 分支有「既有行 `event_id` 不得丢失」断言 + `_drop_near_existing`；**replace 分支无任何存量守恒断言** | ⚠️ 唯一无保护的写入口；治理清单 L79 只列了另外两个，未登记这个 CLI |
| `tools/backfill_events.py` | append | **推进** `coverage.end` 到 `args.end` / `declared_end` | 走同一 `import_file`，保护同上 | ✅ 与 update 的分工已在代码与清单写明 |
| `tools/update_events.py` | append | **不推进**：显式回传原 `coverage['start']/['end']`，另有 `copy2` 备份 + SHA 校验 | 同上 | ✅ |

**口径差异结论**：三入口共用 `import_file` 的同一套行级规则（`event_id = art_code-stock_code`、同 `(ts_code,event_type)` 30 日内近重复剔除、字段与类型一致），行级口径**没有分歧**；差异只在「覆盖声明是否推进」——backfill 推进、update 不推进、CLI 由参数决定。真正的缺口是 `replace`：它把整表换成输入文件内容，且不校验既有行是否丢失，一旦误用即静默丢历史（治理清单 L79 写的「追加/重建边界未写明」本质就是这一条）。

补充事实：三入口都不写 `meta/events/manifest.json`（`events.py` 只写 `coverage.json`，L131），manifest 靠 `tools/manifest_tool.py` 刷。

### 3.2 `stock_dividend`：4 个入口（治理清单只记 2 个）

| 入口 | 写哪些列 | 数据来源口径 | 保护 / 落盘 | 结论 |
|---|---|---|---|---|
| `tools/collect_stock_dividend.py` | 全列，保留本地扩展列（`*_fix`、`cash_div_diluted`、`source`、`total_div_est_*`） | `akshare:em_fhps_bulk` + 交易所除权参考价仲裁 | `merge_frames` 保留扩展列；`write_parquet_atomic` | ✅ 主入口 |
| `src/data/local_store/build.py:979`（`refresh-factors`） | 整表覆盖 | **MySQL 远端**（与主入口不同源） | 三重门禁：确认 token、`validation`（最新日回退 / 因子偏差）、`_attach_local_only_columns` 回接扩展列且丢值即报错；跨文件非事务（L974-L976 已注明） | ⚠️ 一旦执行等于整表换源；当前表未走此路（`source` 以 `mysql_remote` 52,536 行存在，说明历史上走过） |
| `tools/backfill_share_div.py:424` | 仅 `share_div` / `share_div_source` | 本地行情反算 + 逐条核 | 默认 `--dry-run`；`write_parquet_atomic`；`copy2` 备份 | ✅ 一次性补列，列级不冲突 |
| `tools/derive_total_dividend.py:491` | 仅 `total_div_est_*` 派生列 | 本地分红表派生 | 默认 dry-run；`write_parquet_atomic`；备份落仓库根 `_backup/`（docstring 仍写 `data/local_store/_backup/`，为陈旧注释，代码 `DEFAULT_BACKUP = PROJECT_ROOT/'_backup'`） | ✅ 一次性派生 |

**口径差异结论**：三路增量入口列级互不重叠、均保留其余列、均原子写，不产生互相覆盖。唯一真实口径冲突是 `refresh-factors`（MySQL）与主入口（akshare/交易所公告）：两者若交替执行就是整表换源，必须保持「谁执行谁负责对拍」的纪律。**治理清单 L77 把 `backfill_share_div.py`、`derive_total_dividend.py` 记在 `stock_shares` 的多入口清单里，但实测这两个脚本同时写 `stock_dividend`**（分别写 `share_div*` 与 `total_div_est_*`），因此本表实际是 4 入口而非 2 入口。

### 3.3 基金两表

| 表 | 入口 | 写入模式 | 口径 / 保护 | 结论 |
|---|---|---|---|---|
| `fund_dividend` | `src/data/local_store/fund_factors.py::build_fund_store`（L587-L596） | 全量重建 | `akshare:fund_fh_em`；写前 CSV 快照到 `logs/fund_factor_*/`；写 `fund_meta.json`（L512） | ✅ 单入口，无第二写者（`fund_split_repair.py` 不写分红） |
| `fund_share_change` | 同上 | 全量重建 | `akshare:fund_open_fund_info_em/拆分详情` | ✅ 主入口 |
| `fund_share_change` | `tools/fund_split_repair.py`（`_append_missing_events`） | 只追加缺失事件 | 本地行情 `pre_close` 反算比例（1 行）+ cache 标记（4 行） | ⚠️ 第二入口；不改 `fund_meta.json` 计数，靠 `reconcile_p2_metadata` 兜底 |

**混源事实（逐行可追溯，符合 DG-19）**：`fund_share_change` 的 `source` 三值——`akshare:fund_open_fund_info_em/拆分详情` 2,733 行、同源 `(cache splits.json 2026-11-11)` 4 行、`local:quote_pre_close_ratio(0.65/1.03)|akshare:...(1:0.6712 为NAV口径)` 1 行（150189.SZ 2019-12-16，比例由 NAV 口径 0.6712 改为价格口径 0.631068，见 `fund_meta.gap150189_split_ratio`）。两类比例口径不同但逐行可辨，不属混源掩盖。

## 4. ③ 与权威源对拍

### 4.1 `events`

- **对拍对象**：`research/events/data/backfill/202306.parquet`（巨潮原始公告分片，2,608 行，列 `keyword/art_code/stock_code/title/column_name/display_time/notice_date/url`）
- **比对键**：`(ts_code, event_type, title, publish_time::date)`，容差 0（集合差）
- **复现方式**：用 `src.data.events.normalize_announcements` 离线重推，再用 `_drop_near_existing` 复算剔除

| 指标 | 结果 |
|---|---|
| 分片归一化行数 | 1,750 |
| 表内当月 `source='cninfo'` 行数 | 1,516 |
| 分片有 / 表内无 | 346，其中 **292 行被同函数 30 日近重复剔除复现**、**54 行为同一公告 `art_code` 差 1**（表内无同内容重复行，sibling 检查 0 命中） |
| 表内有 / 分片无 | 112，标题前 20 字**全部能在分片命中**（同公告、`event_id` 差 1） |
| 无法解释行 | **0** |

- **全表自洽**：`coverage.json` 声明 `rows=185,750` = 实测行数；`source` 聚合字符串 `cninfo=181110|em_yjyg=4543|em_ann=97` = 表内分组计数（精确相等）；`event_id` 重复 0；6 列空值 0。
- **真实读路径**：`EventStore`（`risk_filters.py#L88-L96`）实测 `available=True`、`rows=185,750`、`risk_events=47,454`、`coverage=2015-01-01~2026-11-10 (complete=True)`、`blocked_map` 8 个交易日 / 31,612 个代码日条目。
- **归因与遗留**：覆盖声明 `end=2026-11-10` 比数据 `max=2026-11-11` 晚一天，属 `update_events` 不推进声明、由 `coverage.json.declare_advance` 单独推进的设计，非缺陷；但权威清单 L50 写的 `end=2026-11-06` 已过期。全表 833 对「同标的同类型间隔 ≤30 天」记录抽样标题均为不同公告（进展/预中标等），是 append 不回溯删除的正常结果，不是重复行。

### 4.2 `stock_dividend`（raw+fix 两层分别给结论）

口径：交易所除权参考价 `pre_close(ex_date) = (prev_close - cash_eff + rights_price×rights_ratio) / (1 + share_eff + rights_ratio)`；`cash_eff = cash_div_fix.fill_null(cash_div)`、`share_eff = share_div_fix.fill_null(share_div)`（DG-30 有效层）。容差：1 个价位 0.011 为通过线。

**两层覆盖统计**

| 层 | 非空行数 | 说明 |
|---|---|---|
| 原始列 `cash_div` / `share_div` | 60,071 / 60,071 | — |
| 覆盖层 `cash_div_fix` | 1 | 601258.SH 2019-12-30：0.0 → 0.04 |
| 覆盖层 `share_div_fix` | 2 | 600306.SH 2023-12-26：0.85 → 0.471744；601258.SH 2019-12-30：0.56416 → 0.0 |
| 只读列 `cash_div_diluted` | 5 | 不接因子与派生链路 |
| `no_ex_rights=True` | 3 | 剔除后不参与对拍 |

**3 行覆盖层的双层结论**（同一行分别用原始层与有效层复算）

| 标的 / 除权日 | prev_close | pre_close | 原始层复算 | 原始层残差 | 有效层复算 | 有效层残差 | 结论 |
|---|---|---|---|---|---|---|---|
| 601258.SH 2019-12-30 | 1.46 | 1.42 | (1.46−0.0)/(1+0.56416)=0.9335 | −0.4865 | (1.46−0.04)/(1+0)=1.42 | **0.0000** | 有效层对、原始层错 |
| 600306.SH 2023-12-26 | 11.98 | 8.140003 | 11.98/(1+0.85)=6.4757 | −1.6643 | 11.98/(1+0.471744)=8.140003 | **0.000000** | 有效层对、原始层错 |

只比原始列会得出「这两只数据错」的相反结论（DG-30 反例在本表的复现）；消费方走 `dividend_effective.apply_effective_columns`（`adj_factor_calculator.py#L363`）读的正是有效层，已实测 `601258.SH` 经该函数后 `cash_div=0.04`、`share_div=0.0`。

**全量对拍结果**

| 指标 | 值 |
|---|---|
| 可对拍事件（`ex_date ≤ 2026-11-11` 且行情有该日 bar，按 `(code, ex_date)` 聚合后） | 57,113 |
| 有效层残差中位数 / p95 | 0.0 / 0.0 |
| \|残差\| ≤ 0.011 | 57,102（99.98%） |
| 0.011 < \|残差\| ≤ 0.02 | 1 行（601966.SH 2025-07-10，−0.014） |
| \|残差\| > 0.02 | 10 行 |
| 对照：若按「pre_close = 原始昨收」口径（H2） | 仅 1,300/57,113 通过、中位残差 0.18 → H1 成立，行情 `pre_close` 是除权参考价口径 |

**10 行超差清单（全部为负残差，即交易所参考价低于表内参数复算值）**

| 标的 | 除权日 | prev_close | pre_close | 表内 cash | 表内 share | 残差 | 隐含参数与表内差额 |
|---|---|---|---|---|---|---|---|
| 300760.SZ | 2025-05-29 | 230.51 | 228.54 | 1.41 | 0 | −0.560 | 隐含现金 1.97，差 0.56 |
| 601966.SH | 2024-06-14 | 18.68 | 18.303 | 0.091 | 0 | −0.286 | 隐含现金 0.377，差 0.286 |
| 000667.SZ | 2006-02-20 | 4.83 | 3.215573 | 0 | 0.387 | −0.267 | 隐含送转 0.5021，差 0.115 |
| 600015.SH | 2003-11-04 | 7.22 | 6.867 | 0.13 | 0 | −0.223 | 隐含现金 0.353，差 0.223 |
| 601318.SH | 2018-06-07 | 63.81 | 62.61 | 1.0 | 0 | −0.200 | 隐含现金 1.20，差 0.20 |
| 601898.SH | 2024-08-20 | 13.52 | 12.965 | 0.442 | 0 | −0.113 | 隐含现金 0.555，差 0.113 |
| 600317.SH | 2014-06-16 | 7.94 | 2.470333 | 0.2 | 2.0 | −0.110 | 隐含送转 2.214，差 0.214 |
| 601828.SH | 2023-07-24 | 4.96 | 4.88 | 0.034 | 0 | −0.046 | 隐含现金 0.08，差 0.046 |
| 300163.SZ | 2015-09-25 | 29.92 | 9.94 | 0 | 2.0 | −0.033 | 隐含送转 2.0101，差 0.0101 |
| 000738.SZ | 2013-06-14 | 12.11 | 12.048 | 0.041 | 0 | −0.021 | 隐含现金 0.062，差 0.021 |

归因（证据方向明确）：10 行全部是「实际参考价比表内参数算出的更低」，即**表内少登记了一笔同一除权日实施的现金/送转**（典型为特别分红、中期分红与年度分红同日实施），与 L37 记载的「tushare 把特别股息重复计入」正好相反——本表是漏计/合并。`601318.SH` 2018-06-07 就是「年度 1.0 + 30 周年特别 0.2」被并成 1.0 的形态。属**待核差异**，不是已证实的错值：需按交易所分派实施公告逐条确认，替代实测命令见第 7 节（用 `pre_close` 反算隐含现金，与本表 `cash_div` 比对）。

**未参与对拍的 2,233 行**（不能据此判对错）

| 类别 | 行数 | 归因 |
|---|---|---|
| `ex_date` 早于该股行情首日 | 1,046 | 早期上市/行情起始前，行情侧无可比 bar |
| `ex_date` 在行情区间内但该日无 bar | 1,187 | 其中 1,158 行该日全市场（000001.SZ）有 bar → **个股停牌日**；29 行全市场同日无 bar → 早期行情缺日 |
| `ex_date` 晚于 2026-11-11 | 75 | 未来除权日（如 2026-11-19），行情尚无该日 |

### 4.3 `fund_dividend`

口径依据 `fund_meta.json.event_alignment`：`etf_dividend=ex_date`（除息日当天反映）、`non_etf_dividend=next_trade_date`（次日反映）。容差：基金价位 0.001 → 通过线 0.0015。

| 指标 | 值 |
|---|---|
| 可对拍事件 | 3,047（ETF 968 + 非 ETF 2,079） |
| 通过 | 3,029（**99.41%**） |
| 失败 | 18 |
| `ex_date` 无对应行情 bar | 366（未上市 / 停牌 / 晚于最新行情） |

18 行失败分解：

| 类别 | 行数 | 代表 | 归因 |
|---|---|---|---|
| 最新两个交易日行情侧未按除息调整 | 2 | 159118.SZ 2026-11-10、159545.SZ 2026-11-11 | `pre_close` 等于前收，行情日更未写入除权调整；待 T+1 复核，非事件表问题 |
| 同除权日有大比例份额折算但事件表无记录 | 1 | 159922.SZ 2024-12-02（隐含比例 2.554） | **`fund_share_change` 缺该折算事件的直接证据** |
| LOF 场内价格无强制除权 | 15 | 160603/160605/163804/501059×3/501064/160105… | 场内价格由供求决定，`pre_close` 不按分红调整 → 该子集**口径上不可对拍**，不是错值 |

### 4.4 `fund_share_change`

口径：折算/分拆在公告折算日的**下一交易日**生效，`pre_close(next bar) = close(ex_date) / share_ratio`。容差 0.0015。

| 指标 | 值 |
|---|---|
| 可对拍事件 | 1,354 |
| 通过 | 1,162（**85.82%**） |
| 失败 | 192 |

失败按代码前缀与年度分布：

| 维度 | 分布 |
|---|---|
| 代码前缀 | `150xxx`（分级基金）167、`502xxx`（分级基金）23、`159xxx` 1、`161xxx` 1 → **186/192 = 96.9% 为分级基金** |
| 年度 | 2020 年 141/165 失败（通过率 14.5%）、2019 年 45/110（59.1%）；2014–2018 各 1 行、2024 年 1 行；其余年份 100% |
| 事件类型 | `份额折算` 1,362 行、`份额分拆` 1,376 行 |

归因：分级基金在折算日普遍停牌，复牌日 ≠ 折算生效日，行情「次一 bar」落在停牌前或复牌后错位；另一类是折算比例本身取的是 **NAV 口径**而非价格比例——150189.SZ 2019-12-16 已实测并修正（`fund_meta.gap150189_split_ratio`：0.6712 → 0.631068），说明这类风险真实存在且曾实际发生。因此 186 行应判「口径待核」，不能直接判数值错。替代实测：用 `close(ex_date)/pre_close(next bar)` 反算隐含比例，与表内 `share_ratio` 逐行比对，差额 >0.5% 的按公告原文核（清单可由第 7 节脚本直接输出）。

## 5. ④ 第二份同语义数据扫描

扫描范围：仓库根（排除 `.git`/`.venv`/缓存）与 `G:\git\python-quant` 镜像，匹配四张表同名文件，共命中 169 个同义词路径（含 `fund_dividend_*.csv` 类审计证据文件）。

| 表 | 副本位置类型 | 是否被代码当数据源读取 | 与正式表 SHA 一致性 |
|---|---|---|---|
| `events` | `_backup/`（8 个按日期快照）、`_staging/`（4 个 candidate_store）、`reports/`、`backups/critical_assets/`、`tmp/pytest_*` | 否（`_backup` 仅作回滚，`critical_assets` 仅作恢复快照） | `backups/critical_assets/.../events.parquet` **3,479,009 B ≠ 正式 3,482,917 B**（= `_backup/meta/events_2026-11-12_g074` 版本，落后 97 行 g074 追加） |
| `stock_dividend` | `_backup/`（13 个快照）、`logs/`（4 个改数前快照）、`_staging/`、`backups/critical_assets/` | 否 | critical 快照一致；其余为历史版本 |
| `fund_dividend` | `_backup/`（3 个）、`logs/fund_factor_20261104/`、`backups/critical_assets/` | 否 | critical 快照一致 |
| `fund_share_change` | `_backup/`（4 个）、`_staging/quote_fix_20261111/`、`backups/critical_assets/` | 否 | critical 快照一致 |

结论：**四表均无第二份「在用」同语义数据**，不存在 DG-1/DG-14 一类的双正式路径。唯一需要动作的是 `backups/critical_assets/.../events.parquet` 落后一次日增量（差 3,908 B），应在下次 critical 快照刷新时对齐（本次只读，未动）。

## 6. ⑤ 权威清单登记缺陷清单（供 `docs/数据权威路径清单.md` 维护方统一修改）

说明：本节只列事实，未改文档。行号为 `docs/数据权威路径清单.md` 当前实测行号。

| # | 位置 | 现写什么 | 实测什么 | 建议改法 |
|---|---|---|---|---|
| 1 | L50 公告事件 | 185,651 行；SHA256 `c9c35d86…`；覆盖声明 `end=2026-11-06` | 185,750 行；SHA256 `7877d1fc…`；`coverage.end=2026-11-10`（差 99 行，`coverage.json` 已记录 `g074_eastmoney_fill_2026_11_12 +97` 与 `declare_advance`） | 三个数字改为实测值，并注明「以 `coverage.json` 为准」 |
| 2 | L50 消费者 | 第三个锚点 `events.py#L455` | `events.py#L455` 在 `_write_coverage` 内部；`EventStore` 类在 **L638**，`blocked_map` L689、`status` L713 | 锚点改 `#L638-L700` |
| 3 | L50 产出方 | 只列 `backfill_events.py`、`update_events.py` | 还有第三个写入口 `src/data/events.py` CLI（`import`=`replace`、`append`），且 `replace` 无存量守恒断言 | 补第三个入口并标注 replace 风险 |
| 4 | L37 个股分红 · 消费方 1 | `adj_factor_calculator.py#L355-L360` | L355-L360 是注释块，`apply_effective_columns` 实际在 **L363** | 锚点改 `#L363` |
| 5 | L37 个股分红 · 消费方 2 | `factor_rebuild.py#L82 _compute_code_factors` | `_compute_code_factors` 在 **L99**；L82 属相邻的 sidecar 逻辑 | 锚点改 `#L99` |
| 6 | L37 个股分红 · 消费方缺失 | 只列 2 个 | 还有 `src/data/dividend_effective.py`（有效层唯一解析点）、`src/backtest/engine/corporate_actions.py#L97`（撮合）、`src/data/services/dividend_service.py#L12`、`tools/derive_total_dividend.py#L61` | 补这 4 处；有效层解析点应单列 |
| 7 | L37 个股分红 · 修正计数 | 「已按交易所分派实施公告修正 **12 起**现金参数」 | `source` 含 `cash=交易所公告:` 的实测 **18 行 / 16 只**，URL 年份跨 2009–2026（非单一批次） | 改为 18 行 / 16 只，或写明「12 起」的统计口径 |
| 8 | L37 个股分红 · 字段定义 | 「字段定义同步写入 `factors/meta.json` 的 `dividend_schema`」 | `factors/meta.json` 仅 15 个聚合字段，**不存在 `dividend_schema` 键**（该键全仓仅出现在 2 个 md 文档里） | 要么补写 `dividend_schema`，要么把这句话改为实际保存位置 |
| 9 | L78 基金分红 · 消费者 | `store.py#L583`、`tools/build_fund_corporate_actions.py` | `_fund_dividend_path` 定义在 **L621**、读取在 **L678-L683**、使用在 L652/L817-L829；`build_fund_corporate_actions.py` 全文 **0 处**引用该表 | 锚点改 `#L621`、`#L678-L683`；删除误列的 `build_fund_corporate_actions.py` |
| 10 | L78 基金分红 · 生产方缺失 | 未写生产方 | `src/data/local_store/fund_factors.py::build_fund_store`（L587-L596），单入口 | 补生产方 |
| 11 | L78 / L79 · 对拍口径缺失 | 未写事件生效口径 | `fund_meta.json.event_alignment` 明确 `etf_dividend=ex_date`、`non_etf_dividend=next_trade_date`、`share_change=next_trade_date` | 补一行口径，否则外部按「同日」对拍会把 2,079 行非 ETF 分红误判为错 |
| 12 | L79 基金份额 · 消费者 | `fund_factors.py#L70` | L70 是常量区，不是读取点；真实读取在 **L556-L557**（事件接入因子构建）与 **L626-L627**（重建后复核）；另有 `tools/build_fund_corporate_actions.py` 间接依赖 | 锚点改 `#L556-L557`、`#L626-L627` |
| 13 | `factors/manifest.json` `authority` | 基金分红事件 L75、基金份额 L76（个股复权 L72、基金复权 L73、换手 L74） | 实际为 **L78 / L79**（该整块行号相对文档已漂移 +3） | 整块重算行号 |
| 14 | `factors/manifest.json` `produced_by` | `fund_adj_factor` / `fund_dividend` / `fund_share_change` / `fund_meta.json` 全写 `tools/build_fund_corporate_actions.py` | 该脚本只产出 `meta/fund_corporate_actions_2019plus.parquet`；实际生产方是 `fund_factors.py`（+ `reconcile_p2_metadata.py` 修 `fund_meta.json`） | 按第 2.3 节逐条改 |
| 15 | `factors/manifest.json` `fund_meta.json` 条目 | 4,112 B / `fa0a79c8…` | 4,234 B / `4fe7e823…` | 重跑 `manifest_tool` 刷新 |

另需知悉（非文档缺陷，属事实补充）：`stock_dividend` 的 `source` 列是逐行混源（`mysql_remote` 52,536、`akshare_fhps` 4,408、`sina_verified` 1,755、`akshare_pg` 874、`em_fhps_bulk` 457、`audit_fix_*` 20、`cash=交易所公告` 18），且**同一行不同字段可能有不同来源**（`share_div_source` 单独一列）；按 DG-19 这条是合规的，但权威清单 L37 未提，建议补一句「字段级来源见 `source` / `share_div_source`」。

## 7. 复现命令与产物

| 命令 | 用途 | 产物 |
|---|---|---|
| `.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s4_verify.py` | 四表行数/字节/SHA/空值/日期段 | `work/b1s4_stats.json` |
| `.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta/events --only factors` | manifest 逐项核对 | 控制台（FAIL 项即第 2.2 节） |
| `.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s4_checks.py` | events 自洽 + EventStore 读路径 + 个股除权参考价对拍 + 基金联合对拍 | `work/b1s4_checks.json` |
| `.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s4_events_replay.py` / `b1s4_events_key.py` | 202306 分片重推、30 日去重复算、内容键比对 | 控制台 |
| `.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s4_stock_attr.py` | 个股超差行清单与未对拍归因 | `work/b1s4_stock_attr.out` |
| `.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s4_fund_aligned.py` / `b1s4_fund_attr.py` | 基金两表按 `event_alignment` 对拍与按年归因 | `work/b1s4_fund_attr.out` |
| `.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s4_copies.py` | 第二份同语义扫描（含 `G:` 镜像） | `work/b1s4_copies.json` |

第 4.2 节 10 行待核差异的可执行替代实测（只读）：

```text
对每个超差 (code, ex_date)：取 quotes/stock/<market>/<code>.parquet 中 ex_date 前一行的 close 与当日 pre_close，
隐含现金 = prev_close - pre_close（无送转时）；
与 stock_dividend 中该 (code, ex_date) 的 cash_div_fix.fill_null(cash_div) 比较，
差额 > 0.02 即列入待核，再到 research/events/data/backfill/<yyyymm>.parquet 检索该标的「权益分派实施公告」原文核对。
```

## 8. 未覆盖与限制

1. **未回源外网**：巨潮/交易所公告原文未重新抓取，10 行现金缺口只能证明「表内参数小于交易所参考价隐含值」，不能证明应补的具体金额；补数需公告原文。
2. **events 只抽了 202306 一个月做分片重推**（其余月份未逐月重推），但补做了全表自洽校验、`event_id` 唯一性、同内容重复扫描与 30 日近重复抽样，结论仍成立。
3. **基金 2019–2020 分级基金折算 186 行未逐条定性**：需业务按「NAV 口径 vs 价格口径」「复牌日 vs 折算生效日」裁定，本片只给证据与可执行核法，未改数（改数属数据治理替换流程，应另开任务并按 DG-5/DG-11 走 staging 与备份）。
4. **下游未跑全量重算**：只验证了有效层读取路径与两行 fix 的除权参考价残差，未重跑 `stock_adj_factor` / `fund_adj_factor` 全量比对（属 B2 范围，且本片只读）。
5. **`backups/critical_assets` 的 `events.parquet` 落后一次日增量**：已在第 5 节记录，需在下次快照刷新时对齐，本片未动。
