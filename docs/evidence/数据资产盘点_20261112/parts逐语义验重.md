# `parts` 逐语义验重（2026-11-12，只读）

> 对象：`data/local_store/meta/financials/parts/` 下除 `valuation` 外的全部子集，外加 `/ann_date`（实测不在 `parts/` 下，是 `parts/` 的同级目录）。
> 方法：全量读入分片 → 去重 → 与正式表按同一唯一键集合求双向差集 → 再对共有键逐数据列比对值。
> 全部只读：未移动、未删除、未写入 `data/` 任何文件。复算脚本 `work/parts_probe.py`、`work/parts_dup.py`、`work/parts_dup_values.py`，结果 JSON 同名。

## 0. 实测的目录结构与唯一键

| 语义 | 实际目录 | 分片命名 | 唯一键 | 正式表 |
|---|---|---|---|---|
| income | `parts/income/` | `{end_date}.parquet`（67 期） | `(ts_code, end_date)` | `meta/financials/income.parquet` |
| balance | `parts/balance/` | 同上 | `(ts_code, end_date)` | `meta/financials/balance.parquet` |
| cashflow | `parts/cashflow/` | 同上 | `(ts_code, end_date)` | `meta/financials/cashflow.parquet` |
| income_bj | `parts/income_bj/` | `{ts_code}.parquet`（354 只） | `(ts_code, end_date)` | 同上 `income.parquet` |
| balance_bj | `parts/balance_bj/` | `{ts_code}.parquet`（43 期×单票） | `(ts_code, end_date)` | 同上 `balance.parquet` |
| cashflow_bj | `parts/cashflow_bj/` | `{ts_code}.parquet`（354 只） | `(ts_code, end_date)` | 同上 `cashflow.parquet` |
| ann_date | **`meta/financials/ann_date/`（不在 parts 下）** | `{end_date}.parquet`（67 期） | `(period, symbol)`（`symbol` 为 6 位无后缀） | **无正式表** |

键定义逐一实测确认，未套用 valuation 的 `(ts_code, trade_date)`。`ann_date` 列集为 `symbol / ann_date / update_date / parent_net_profit`，报告期只存在于文件名，故键为 `(period, symbol)`。

## 1. 逐块验重表

| 语义 | 分片数 | 行数 | 唯一键 | 唯一键行数 | 重复行数 | 正式表名 | 正式表行数 | 唯一键是否完全相同 | 单边独有日期 / 标的 |
|---|---:|---|---|---:|---:|---|---:|---|---|
| income | 67 | 266,327 | `(ts_code, end_date)` | 266,327 | 0 | `income.parquet` | 344,703 | **否**（分片是正式表真子集） | 分片独有 **0 键 / 0 期 / 0 只**；正式表独有 78,376 键 / 114 期 / 3,163 只（全部早于 2010-03-31） |
| balance | 67 | 258,303 | `(ts_code, end_date)` | 258,303 | 0 | `balance.parquet` | 330,299 | **否**（真子集） | 分片独有 **0 / 0 / 0**；正式表独有 71,996 键 / 111 期 / 2,120 只 |
| cashflow | 67 | 265,900 | `(ts_code, end_date)` | 265,900 | 0 | `cashflow.parquet` | 328,708 | **否**（真子集） | 分片独有 **0 / 0 / 0**；正式表独有 62,808 键 / 92 期 / 2,108 只 |
| income_bj | 354 | 10,695 | `(ts_code, end_date)` | 10,695 | 0 | `income.parquet` | 344,703 | **否**（分片是正式表子集） | 分片独有 **0 / 0 / 0**；正式表独有 334,008 键（含非北交所标的，天然不同） |
| balance_bj | 43 | 9,821 | `(ts_code, end_date)` | 9,821 | 0 | `balance.parquet` | 330,299 | **否**（子集） | 分片独有 **0 / 0 / 0**；正式表独有 320,478 键 |
| cashflow_bj | 354 | 10,725 | `(ts_code, end_date)` | 10,725 | 0 | `cashflow.parquet` | 328,708 | **否**（子集） | 分片独有 **0 / 0 / 0**；正式表独有 317,983 键 |
| ann_date | 67 | 427,685 | `(period, symbol)` | 427,685 | 0 | **无** | — | 无正式表可对 | 不适用（无对照侧） |
| （参考）valuation | 266 | 37,257,842 | `(ts_code, trade_date)` | 17,969,137 | **19,288,705** | `valuation_daily.parquet` | 17,969,137 | **是** | 两侧均 0（已于前一轮单独出结论） |

分片内部一律零重复，`valuation` 是唯一例外（重复 19,288,705 行）；财报与公告日分片都是「一文件一语义切片、每键只出现一次」。

## 2. 逐值比对（共有键内，排除 `ts_code/end_date/ann_date/ann_date_source/source/fetched_at` 六个元数据列）

| 语义 | 内连接行数 | 逐值不一致行数 | 不一致列 | 结论 |
|---|---:|---:|---|---|
| income | 266,327 | **201,838（75.8%）** | 仅 `net_profit` 一列 | 其余 11 列逐值全等；`parent_net_profit` 266,327/266,327 全等；`ann_date` 0 行不同 |
| balance | 258,303 | 0 | — | 11 列逐值全等 |
| cashflow | 265,900 | 0 | — | 5 列逐值全等 |
| income_bj | 10,695 | **413** | 仅 `net_profit` 一列 | 其余列全等 |
| balance_bj | 9,821 | 0 | — | 全等 |
| cashflow_bj | 10,725 | 0 | — | 全等 |

`net_profit` 差异的口径来源（实测）：分片的 `net_profit` 有 265,069/266,327（99.5%）等于自身 `parent_net_profit`，而正式表 `net_profit` 只有 64,096/266,327 等于 `parent_net_profit` —— 即**分片该列取的是归母净利润，正式表取的是合并净利润**。北交所分片里这个比例只有 57.4%，说明该列在分片侧并非严格单一口径。这是列口径缺陷，不改变「分片无独有键」的事实，但决定了**不能用 `parts/income` 反向重建正式表**（会把归母值写进 `net_profit`）。

## 3. 每块一行判定

| 语义 | 判定 |
|---|---|
| income | 键是正式表真子集（分片独有 0 键），无重复；值上仅 `net_profit` 列口径不符。→ **纯冗余、可退役；禁止反向重建**。 |
| balance | 键是正式表真子集（分片独有 0 键），无重复，逐值全等。→ **纯重复，可退役**。 |
| cashflow | 同上。→ **纯重复，可退役**。 |
| income_bj | 键全部落在正式表内，无重复；值上仅 `net_profit` 413 行口径不符。→ **可退役**（同样禁止反向重建）。 |
| balance_bj | 键全部落在正式表内，无重复，逐值全等。→ **纯重复，可退役**。 |
| cashflow_bj | 同上。→ **纯重复，可退役**。 |
| ann_date | 不在 `parts/` 下，且全仓无对应正式表；键 `(period, symbol)` 严格唯一，427,685 行 / 11,695 只 / 67 期。有真实读侧（`src/data/financials/collector.py` 的 `fetch_ann_date_map` 与断点续跑），不是「重复残留」。→ **不可按重复退役**：要么先合并成正式表，要么随 collector 落点一起迁 `_staging/`。 |

## 4. 可退役清单

以下 7 块唯一键均无「正式表没有的键」，退役不丢数据：

| # | 语义 | 文件数 | 字节 |
|---|---|---:|---:|
| 1 | `parts/valuation` | 266 | 1,384,844,879 |
| 2 | `parts/income` | 67 | 22,897,493 |
| 3 | `parts/balance` | 67 | 20,890,957 |
| 4 | `parts/cashflow` | 67 | 10,617,828 |
| 5 | `parts/income_bj` | 354 | 3,392,136 |
| 6 | `parts/cashflow_bj` | 354 | 2,014,362 |
| 7 | `parts/balance_bj` | 43 | 970,619 |
| — | **合计** | **1,218** | **1,445,628,274** |

**可退役总体积：1,445,628,274 B ≈ 1.35 GB（1.35 GiB / 1.45 GB 十进制）**，占盘点时正式层 12,526 文件 / 3.69 GB 的 9.7% 文件数与 36.6% 体积。整块 `parts/` 退役后正式层约剩 **11,308 文件 / 2.34 GB**（体积为同口径推算：3.69 − 1.35）。

## 5. 不可退役清单

| 语义 | 路径 | 文件数 | 行数 | 字节 | 为什么不能按重复退役 |
|---|---|---:|---:|---:|---|
| 公告日索引缓存 | `meta/financials/ann_date/` | 67 | 427,685 | 4,388,854 | 无任何正式表承载这份语义；`collector.py` 的 `fetch_ann_date_map` 在断点续跑时读它。按 DG-32 属采集中间态，但必须先有承接方（合并成正式表或随 collector 落点迁 `_staging/`）才能搬 |

`ann_date` 与 `parts/` 合计 1,285 文件 / 1,450,017,128 B，即此前 DG-32 记录的「财务采集分片 1,285 个」口径来源。

## 6. 为什么只能退役，不能拿分片反建正式表

「分片唯一键是正式表真子集」只说明**不丢键**，不说明**值可互换**。本轮的逐值比对给出了反证：

1. **`income` 分片的 `net_profit` 是归母口径，正式表是合并净利润口径。** 分片该列 265,069/266,327 行（99.5%）等于自身 `parent_net_profit`；正式表该列只有 64,096/266,327 行等于 `parent_net_profit`。两边在 201,838 行（75.8%）数值不同，例如 `000002.SZ` 2020-09-30：分片 19,863,034,997.94，正式表 30,071,668,295.60。
2. **其余列没有这个问题。** 12 个数据列中另外 11 列（含 `parent_net_profit`，266,327/266,327）与正式表逐值全等；`ann_date` 0 行不同；`balance`/`cashflow`/`balance_bj`/`cashflow_bj` 四块与正式表逐值 0 行差异。所以缺陷只在 `income` 的 `net_profit` 一列（`income_bj` 同源，413/10,695 行）。
3. **正式表的口径是后来修出来的，分片是修之前的样子。** `tools/backfill_financials_history_sina.py --fix-net-profit` 于 2026-11-11 把 1990–2026 存量 `net_profit` 全列改为新浪 `NETPROFIT` 的真合并口径（缺失时回退归母并逐行标 `|net_profit=parent_fallback`），`docs/数据权威路径清单.md` L36 已记。分片停留在该修正之前。
4. **反建还会丢历史。** 分片只覆盖 67 个报告期（2010-03-31 起），正式表多出 78,376 个 `(ts_code,end_date)` 键 / 114 期 / 3,163 只，全部是 2010 年前的历史；反建会把这段历史整段抹掉。

结论：`parts` 六块（income / balance / cashflow / income_bj / balance_bj / cashflow_bj）**只能按重复残留退役，不能作为任何重建源**。若将来需要重建正式表，唯一合法输入是正式表自身 + `tools/backfill_financials_history_sina.py` 这类已留证的修正工具。该口径差异已登记为 `docs/数据缺口登记_正式表_2026-11-12.md` 的 **G-089**（类 E，判定「不修，不可反向重建」）。
