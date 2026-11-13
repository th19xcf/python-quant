# B1 第三片（深度 0 基础层）只读体检

- 体检日期：2026-11-12
- 依据：`2026-11-12/data-lineage/outputs/governance_order.md` B1 批次；五项口径沿用 `b1_slice1_check.md` / `b1_slice2_check.md`
- 方式：全程只读。未改代码、未写 `data/`、未改任何 parquet / manifest、未提交；`docs/数据权威路径清单.md` 未改动，登记类缺陷只在 §4 逐条列出
- 实测脚本：`2026-11-12/data-lineage/work/b1_slice3_check.py`、`b1_slice3_recalc.py`（输出 JSON 同目录，供复核）；`tools/manifest_tool.py --check --only meta`

## 1. 批次归属核对（开工前）

| # | 请求表 | 唯一正式路径 | governance_order 归属 | 处理 |
|---|---|---|---|---|
| 1 | 三大报表 | `meta/financials/{income,balance,cashflow}.parquet` | B1（「财报到 PIT」行） | 纳入 |
| 2 | 基金名录 | `meta/fund_basic.parquet` | B1（「基金/ETF 基础信息」行） | 纳入 |
| 3 | 指数名录 | `meta/index_basic.parquet` | B1（「指数基础信息」行） | 纳入 |
| 4 | 通达信代码表 | `meta/names.json` | B1（「当前简称」行） | 纳入 |
| 5 | ETF 费率表 | `meta/multi_asset/etf_fee.parquet` | B1（「多资产 ETF 费率」行） | 纳入 |

5 张全部在 B1，无一张需要跳过。

## 2. 总览

| # | 表 | ① 写入口 | ② manifest 记录数 | ③ 对拍 | ④ 第二份同语义 | ⑤ 清单登记 | 缺陷 |
|---|---|---|---|---|---|---|---|
| 1 | 三大报表 | 通过（两条入口均可断点重跑） | 通过（10/10 一致） | 部分闭环（公告日 100% 对拍通过；两条恒等式未闭环） | 无代码第二来源；5 处磁盘副本均在 `_backup/_staging/backups` | 主体一致，1 处路径已失效 + 1 处行号失效 | 3 |
| 2 | fund_basic | 条件通过（依赖 MySQL；仅补空合并，非独立可重建） | 计数一致、**记录层 5 处不符** | 集合缺 2 只、2 只状态不符、名录上市日按项目口径完全复现 | 无代码第二来源（17 个回滚/manifest 快照目录 + 1 份关键资产快照） | 主体一致，漏 1 个消费者 | 9（含 1 条边界观察） |
| 3 | index_basic | 条件通过（同 fund_basic，无独立重建脚本） | 计数一致、无 `files` 键、**空值未披露** | 集合全等（1,335/1,335），描述字段大面积为空 | 无代码第二来源（5 处快照/差异台账） | 行数一致，漏 1 个真实读路径 | 3 |
| 4 | names.json | 通过（tmp + `os.replace` + 旧件入 `_backup`，可 `--dry-run`） | **无 manifest**，从未被记录核对 | 在市股票 0 缺失；4 处文本差全为当日标记 | 无代码第二来源（仅关键资产快照，sha 相同） | 消费者漏 4 处；「全量键」表述与实测不符 | 3 |
| 5 | etf_fee | 通过（staged 断点 + `--apply` 原子写 + 旧目录移 `_backup`） | 计数一致（4/4），`cross_check` 欠记录 | 集合全等、费率口径可内部重算 | 成品副本留在 `_staging`（逐字节相同） | 主体一致 | 2 |

合计 21 条缺陷（高 2 / 中 9 / 低 10），清单见 §4；其中 20 条落在 5 张表本身，B1S3-21 为同目录 `etf_pool` 的 manifest 缺陷（只照明、不并入本表判定）。

## 3. 逐表体检

### 3.1 三大报表 `meta/financials/{income,balance,cashflow}.parquet`

**① 写入口**

| 项 | 事实 |
|---|---|
| 2010+ 采集 | `tools/collect_financials.py`（默认 2016Q1 起；`--since/--until` 增量、`--merge-only` 只合并）；分片与断点落仓库根 `_staging/financials_parts_<YYYYMMDD>/`（[store.py](file:///e:/code/python/quant/src/data/financials/store.py#L158-L182)），正式层只留合并表 |
| pre-2010 补齐 | `tools/backfill_financials_history_sina.py`（`--verify/--collect/--build`），只写 `end_date < 2010-01-01`，重叠年份仅对拍不覆盖 |
| 断点证据 | `_staging/financials_history_20261111/`：`progress.csv` 319,400 B、`failed.csv` 1,653 B、`parts/` 53 件 44.9 MB、`raw/` 12 件 1.1 MB |
| 原子写 | [store.py](file:///e:/code/python/quant/src/data/financials/store.py) 统一走 `write_parquet_atomic` |
| 可重跑判定 | 可重跑且可续跑；但重跑依赖东财/新浪外部接口，离线环境只能重放 staging `raw/` |

**② manifest 记录数 vs 实际**

`meta/financials/manifest.json` 的 `tables.{income,balance,cashflow}` 与实测逐项相等：

| 字段 | income 记录/实测 | balance 记录/实测 | cashflow 记录/实测 |
|---|---|---|---|
| rows | 344,703 / 344,703 | 330,299 / 330,299 | 328,708 / 328,708 |
| codes | 5,899 / 5,899 | 5,840 / 5,840 | 5,836 / 5,836 |
| bytes | 26,711,299 / 同 | 24,647,262 / 同 | 11,395,687 / 同 |
| sha256 | `d50d515a…` / 同 | `81d8393a…` / 同 | `cfecda58…` / 同 |
| pre-2010 rows / codes | 67,681 / 2,821 同 | 62,175 / 1,763 同 | 52,083 / 1,754 同 |
| sources 分布 | 6 段全等 | 3 段全等 | 3 段全等 |

`manifest_tool --check --only meta/financials`：10 条记录 10 条一致，0 不一致。

全库横向（`--only meta`）：33 份 manifest / 113 条文件记录 / 不一致 3 条，且**没有一条落在本片 5 张表自身的 manifest 上**——2 条为 `meta/anomalies/manifest.json` 的 `file_missing`（属其他片），1 条为 `meta/multi_asset/manifest.json` 的 `summary.json` 缺失（B1S3-21，etf_pool 那份，非 `etf_fee.manifest.json`）。

**③ 对拍**

| 对拍对象 | 比对列 | 容差 | 实测 | 结论 |
|---|---|---|---|---|
| `meta/financials/ann_date_map.parquet`（单表，键 `period+symbol`） | `income.ann_date` ↔ `map.ann_date`（2010+，277,022 行） | 0（日期精确） | 275,802 行逐值相等、0 行不等；1,220 行无 map，恰等于 `ann_date_source=statutory_deadline` 的 1,220 行 | **通过**（99.56% 有 map 覆盖且全等） |
| 东财原始分片（`_staging/financials_parts_*`） | 6 个金额列 + `ann_date` | 0 | 当前无 `financials_parts_20261112`，仅有 pre-2010 的 `financials_history_20261111/{raw,parts}`；本片未重放 | 未做，归因见下 |
| 新浪 raw（pre-2010，12 件） | 金额列 | 0 | 本片未重放（`--verify` 可离线复现） | 未做 |
| 内部恒等式：`total_assets = total_liab + total_equity` | 三列 | 相对差 1% | 可比 328,531 行，323 行（0.098%）>1%，最大 14.68；抽查 600700.SH 2008 为 `total_liab=0`（源端缺分量） | **未闭环**，见 B1S3-18 |
| 内部恒等式：`debt_ratio = total_liab / total_assets × 100` | 两列 | 1e-4 | 仅 1 行不符（同一条 `total_liab=0` 行） | 基本通过 |
| 内部恒等式：`net_cash_flow = op+inv+fin` | 四列 | 毛额相对差 1% | 可比 319,110 行；毛额相对差 ≤1e-9 的 105,250 行（33.0%）、>1% 的 40,223 行（12.6%）；比值中位数 1.0000、p01–p99 为 0.5497–1.4355 | **未闭环**，见 B1S3-18 |
| 三表主键覆盖 | `(ts_code,end_date,ann_date)` 两两相交 | — | income↔balance 交 297,283（income 独有 47,420、balance 独有 33,016）；income↔cashflow 交 310,500（独有 34,203 / 18,208）；balance↔cashflow 交 295,273（独有 35,026 / 33,435） | 记录为事实，不判缺陷（三表披露粒度不同） |
| 真实读路径（DG-23） | `latest_announced_as_of('000001.SZ','2026-11-01','income')` | — | 返回 2026-09-30 期、ann_date 2026-10-24、revenue 1.031e11；`latest_announced_panel` 3 行、`ann_date > as_of` 0 行；`as_of=1990-01-01` 返回 None | **通过** |
| `net_profit` 真合并口径 | `source` 标记 | — | `parent_fallback` 14 行、`keep_local` 2,135 行，与清单 L38 完全一致 | 通过 |

对拍不了的归因：金额列需重放外部原始响应（东财分片当前不在盘上，pre-2010 需重跑新浪接口）。本片按规则给出替代实测（上表 4 条恒等式 + 公告日映射 + PIT 读路径），**不把未重放判成「已错」**。

**④ 第二份同语义数据**

| 位置 | 性质 | 是否被代码读取 |
|---|---|---|
| `backups/critical_assets/data/local_store/meta/financials/*.parquet` | 关键资产快照，三表 sha 与正式表完全相同 | 否 |
| `_backup/financials_{income,balance,cashflow}_20261111_*` | DG-11 回滚快照 | 否（仅 `tools/fill_internal_gaps.py` 作前后行差对照） |
| `_backup/meta/financials_pit_20261112/` | 已退役的 `meta/financials_pit/` 四表（income 55 MB / balancesheet 92 MB / cashflow 73 MB / fina_indicator 148 MB） | 否 |
| `_staging/new_factors_audit_20261112/store_old/meta/financials/*` | 审计用旧版副本（sha 与正式表不同：`41750adb…` / `138e9e76…` / `40fac69d…`） | 否 |
| `_staging/restore_drill_20261112/` 6,874.6 MB / 127 件 | 恢复演练副本 | 否 |

`grep -rn "store_old|restore_drill|financials_pit" --include=*.py` 仅命中 2 条注释/清单引用（`universe_filter.py:31`、`manifest_semantics.py:25`），无读取代码。判定：**不存在可被代码读到的第二正式路径**；`_staging` 两份大副本见 B1S3-19。

**⑤ 清单登记（不改，只列）**

| 项 | 清单现写 | 实测 |
|---|---|---|
| 路径/语义 | L38 `meta/financials/{income,balance,cashflow}.parquet` | 一致 |
| 数字 | 14 行 `parent_fallback` / 2,135 行 `keep_local` / pre-2010 分段 | 14 / 2,135 / 分段一致 |
| 代码锚点 | L38 写 `store.py#L217-L260` | `latest_announced_as_of` 实为 L316、`latest_announced_panel` L340（锚点已失效）→ B1S3-17 |
| `meta/financials_pit/` | L38 称「是原始采集副本（含 `f_ann_date`），无任何代码消费者」 | 该目录实测**已不存在**，已移入 `_backup/meta/financials_pit_20261112/`，且未进「已废弃路径」表 → B1S3-17 |

### 3.2 基金名录 `meta/fund_basic.parquet`

**① 写入口**

| 项 | 事实 |
|---|---|
| 主入口 | [local_first_basic.py](file:///e:/code/python/quant/src/data/services/local_first_basic.py#L364-L377) `export_fund_basic`，由 `sync_fund` 落库后自动调用；`FUND_PATH`/`FUND_MANIFEST` 见 L33-L34 |
| 交易属性列 | `tools/mark_fund_tradability.py --apply`（写回 `meta/fund_basic.parquet`，覆盖前备份到 `_backup/fund_basic_tradability_<时间戳>/`，`write_parquet_atomic`） |
| 合并策略 | 以本地为基：只补本地空值；冲突保留本地并登记 `_staging/local_first_<日期>/`；未命中分支写回原值并校验空值数不增（DG-31） |
| 可重跑判定 | 可重跑，但**强依赖 MySQL `fund_basic` 内容**，且本地取证列（fund_type / list_date / delist_date）只靠「冲突保留本地」活下来——本地文件丢失后无法由代码独立重建（manifest `last_export`：库 2,618 行、`local_only_keep` 1,090、`conflict_keep_local` 1,222） |

**② manifest 记录数 vs 实际**

`meta/fund_basic.manifest.json`：`rows 2,618` ✓ / `codes 2,618` ✓ / `sha256 1bee84f7…` ✓ / `delist_date_nonnull 361` ✓ / `fund_type_dist` ETF 1,734 + LOF 627 + 分级 257 ✓ / `status_dist_after` L 2,254 + D 361 + null 3 ✓ / `etf_codes_count 1,734` ✓。

记录层缺陷 5 条（B1S3-01~05，共 6 行事实）：

| 项 | manifest 现写 | 实测 |
|---|---|---|
| 空名计数 | `name_null_after: 17` | `name` 空值 0、空串 0（`source` 列带 `name=` 标记 363 行，说明后续已回填）→ B1S3-01 |
| 空名说明 | `column_basis.name`：「仅 null 用东财当前名录补 11 只；**仍空 352 只**」 | 同上，已全部有名称 |
| 随附文件 | 4 个 sidecar（`fund_basic_pending_d_20261110.json`、`fund_basic_unresolved_20261110.csv`、`fund_basic_changes_20261110.csv`、`fund_basic_fill_summary_20261110.json`） | **4 个全部不存在**；仅 `reports/fund_basic_delist17_evidence_20261110.csv` 在 → B1S3-02 |
| 逐文件记录 | 无 `files` 键 | 无逐文件 rows/sha；`manifest_tool` 只能按 `path`+`sha256` 判 2 条（2/2 一致）→ B1S3-03 |
| 备份/暂存路径 | `backup_path: data/local_store/_backup/…`、`staging_removed: data/local_store/_staging/…` | 两目录均不存在，且路径口径违反 DG-11/DG-5（备份应在仓库根 `_backup/`、候选应在 `_staging/`）→ B1S3-04 |
| 生命周期 | `status: "replaced"` | 与「当前正式 manifest」矛盾 → B1S3-05 |

**③ 对拍**

| 对拍对象 | 比对列 | 容差 | 实测 | 结论 |
|---|---|---|---|---|
| MySQL `fund_basic` | 全表逐单元格 | — | manifest `last_export`：补空 0、冲突保留 1,222、库有本地无 0、追加 0 | DG-15：不作「哪个值对」的标尺，只作来源归因 |
| 本地 `quotes/fund` 文件集合 | `ts_code` 集合 | 0 | 表 2,618 只全部有行情文件；行情多 2 只不在表内：`505888.SH`（1,051 根 bar，2015-03-16~2019-07-03）、`561970.SH`（2026-11-11 单根 bar） | **缺口** → B1S3-06 |
| `status=L` 规则重算（末 bar ≥ 2026-09-20，manifest 自述口径） | `status` | 0 | L 2,254 行中 2,252 一致；不符 2 只：`166007.SZ` 末 bar 2026-05-25、`501023.SH` 末 bar 2021-10-11 | **不符** → B1S3-07 |
| `list_date` vs 首根**非零成交量** bar（项目口径） | `list_date` | 0 | 相等 2,427（与 manifest「2,427/2,618」完全复现）；`list_date` 晚于首真实成交 0；早于 106（行情起始晚于上市日）；空 0。按含 0 量占位 bar 的首 bar 统计会得 2,560 / 58，属口径错误，已弃用 | **通过**；106 只属行情覆盖观察 → B1S3-08 |
| 项目内 `_classify_local_fund_symbol(name, code)` 重算 | `fund_type` | 0 | 2,617 一致，1 只不符：`160718.SZ` 记「分级」、名称规则判「LOF」 | 轻微 → B1S3-09 |
| `is_tradable` 与 `tradable_reason` | 两列 | — | 2,532 True / 86 False（84 `placeholder_only` + 2 `nav_only_no_trade`），与标记表一致 | 通过 |
| `meta/names.json` | `ts_code` 键 | 0 | 367 只无键（150xxx 分级份额与已退市份额为主），属预期 | 通过 |
| 守恒：行数 / 主键 | — | 0 | 2,618 行 / 2,618 `ts_code`，重复 0 | 通过 |

**④ 第二份同语义数据**

| 位置 | 性质 | 是否被代码读取 |
|---|---|---|
| `backups/critical_assets/data/local_store/meta/fund_basic.parquet` | 关键资产快照，sha 与正式表相同 | 否 |
| `_backup/fund_basic_*`（17 个目录） | 2026-11-10 逐轮回滚快照 + manifest 快照（`fund_basic_manifest_*`） | 否 |
| `_staging/local_first_20261110/` | 合并式导出的差异台账（csv/json，非全表） | 否 |

判定：无代码第二来源。

**⑤ 清单登记（不改，只列）**

| 项 | 清单现写 | 实测 |
|---|---|---|
| 路径 | L22 `meta/fund_basic.parquet` + 说明 `meta/fund_basic.manifest.json` | 一致 |
| 消费者 | L22：`export_fund_basic`、`clean_set.py`、`build_multi_asset_etf_pool.py`、`build_etf_equity_pool.py`、`mark_fund_tradability.py` | 全部命中；漏 `src/data/quality/freshness_check.py:81`（freshness 检查） |
| 行数 | 未记录 | 2,618 行 / 2,618 只（建议补记） |
| 合并口径 | L22「只补本地空值，冲突保留本地既有取证值」 | 一致 |

### 3.3 指数名录 `meta/index_basic.parquet`

**① 写入口**

| 项 | 事实 |
|---|---|
| 主入口 | [local_first_basic.py](file:///e:/code/python/quant/src/data/services/local_first_basic.py#L379-L388) `export_index_basic`，`--update-index` 落库后自动调用；`INDEX_PATH`/`INDEX_MANIFEST` 见 L35-L36 |
| 独立重建 | 无。只能由 MySQL `index_basic` 合并式导出；本地独有 64 行靠「冲突/独有保留」维持 |
| 可重跑判定 | 可重跑但依赖 MySQL；无第二套本地重建路径 |

**② manifest 记录数 vs 实际**

| 字段 | 记录 | 实测 |
|---|---|---|
| rows | 1,335 | 1,335 |
| codes | 1,335 | 1,335 |
| sha256 | `650aa996…` | `650aa996…` |
| `last_export` | 库 1,333 行 / 补空 0 / 冲突保留 0 / 本地独有 64 / 追加 0 / 库缺 2 | 与 1,335 = 1,333 + 2 自洽 |
| `files` | 无该键（`null`） | 无逐文件记录 → B1S3-11 |
| 空值披露 | 未记录 | `name` 空 33 / `list_date` 空 1,152（SH 1,222 行中 1,152 空）/ `base_point` 空 1,335 → B1S3-10 |

`manifest_tool --check --only meta`：`meta/index_basic.manifest.json` 2 条记录 2 条一致。

**③ 对拍**

| 对拍对象 | 比对列 | 容差 | 实测 | 结论 |
|---|---|---|---|---|
| 本地 `quotes/index` 文件集合 | `ts_code` 集合 | 0 | 1,335 ↔ 1,335，双向差 0 | **通过** |
| MySQL `index_basic` | 逐单元格 | — | 库 1,333 行、库缺 2、本地独有 64（manifest `last_export`） | DG-15：只作归因 |
| 交易所官方名录 | 名称/发布日/base_point | — | 需联网或官方文件，本片不可得 | 未做，替代见下 |
| 内部一致性：`base_point` | 单列 | — | 1,335 行全空，与库同空（清单 L23 已声明，不得填 0） | 通过（已知事实） |
| 内部一致性：描述字段 | `name/publisher/category/list_date` | — | 33 行（全部 `880xxx.SH`）四列全空，等于「只有代码的占位行」；SH 段 1,152/1,222 行无 `list_date` | **披露缺口** → B1S3-10 |
| 单列证据 | `000985.SH` / `000922.SH` | — | 两只均有独立日线 manifest（3,707 行 / 4,485 行），与 L23「2026-11-10 追加」一致 | 通过 |

**④ 第二份同语义数据**

`_backup/index_basic_20261110_local_export/`、`_backup/index_fill_20261110/`、`_backup/names_backfill_20261109_235916/index_basic.parquet`、`backups/critical_assets/…`（sha 与正式表相同）、`_staging/local_first_20261110/index_basic_diff.csv`（差异台账）。均无代码读取。

**⑤ 清单登记（不改，只列）**

| 项 | 清单现写 | 实测 |
|---|---|---|
| 行数 | L23「唯一本地正式表（1,335 行，2026-11-10 追加 000985.SH / 000922.SH）」 | 1,335 行，两只追加属实 |
| `base_point` | 全空、库中同空、不得填 0 | 一致 |
| 消费者 | L23：`export_index_basic`、`meta/daily_index.parquet` 标的域推导、`src/main.py` 指数日线补库 | 漏 `src/data/services/index_service.py:39-47`（真实读路径）→ B1S3-12 |
| 空行披露 | 未提 | 33 行全空 + 1,152 行空 `list_date` 未登记 → B1S3-10 |

### 3.4 通达信代码表 `meta/names.json`

**① 写入口**

| 项 | 事实 |
|---|---|
| 唯一写入方 | `tools/build_names_json.py`（同目录 tmp 写 → `os.replace`，旧文件移仓库根 `_backup/`，支持 `--dry-run`） |
| 可重跑判定 | 可重跑；输入为通达信本地码表，需本机 TDX 客户端目录 |

**② manifest 记录数 vs 实际**

| 项 | 事实 |
|---|---|
| manifest | **不存在**（`meta/` 下无 `names.manifest.json`，也无目录级 `manifest.json`） |
| 覆盖盲区 | `manifest_tool` 只枚举 `*.parquet`，JSON 不在覆盖范围，故该表从未被记录核对 |
| 实测基线 | 53,015 键（SH 27,933 / SZ 24,688 / BJ 394）、1,352,524 B、SHA256 `788066b1fc0921cf423939caa096d95a9aebbed57d7095cd7173a3678830bcbf`、空值 0 → B1S3-13 |

**③ 对拍**

| 对拍对象 | 比对列 | 容差 | 实测 | 结论 |
|---|---|---|---|---|
| 通达信当日码表（外部 TDX 目录） | 全键值 | 0 | 本机不可离线取得（仓库内无该目录） | 未做 |
| `meta/stock_basic.parquet` 在市股票 | `ts_code` 键 + `name` | 0 | 在市 5,580 只，**0 只缺失**；4 处文本差（`ts_code`：names 侧 → stock_basic 侧）：`603279.SH` XD景津装 → 景津装备；`605338.SH` XD巴比食 → 巴比食品；`688053.SH` 思科瑞 → ST思科瑞；`688827.SH` C钶锐 → 钶锐。全部是 N/C/XD/ST 当日标记与名称截断 | **通过**（与 [name_consistency.py](file:///e:/code/python/quant/src/data/name_consistency.py#L1-L19) 判定规则一致） |
| 退市股票 | 键覆盖 | — | `stock_basic` 退市 252 只（`quotes/stock` 有文件）在 `names.json` 中 **0 覆盖** | 符合「当日快照」语义，但「全量键」表述不准确 → B1S3-15 |
| `meta/fund_basic.parquet` | 键覆盖 | — | 367 只无键（150xxx 分级/退市份额为主） | 符合「在市」语义 |
| `meta/index_basic.parquet` | 键覆盖 | — | 35 只无键 = 31 只 `880xxx` + `000904.SH`/`000922.SH`/`000985.SH`/`399904.SZ`（33 只空名行里有 2 只在 names 有键） | 记录为事实 |
| 债券 | 键覆盖 | — | `quotes/` 下无 `bond` 目录，本地无可比对集合 | 无法验证 |

**④ 第二份同语义数据**

`backups/critical_assets/data/local_store/meta/names.json`（sha 与正式表相同，`788066b1…`）。`_staging` 下无 `names*.json` 副本。无代码读取。

**⑤ 清单登记（不改，只列）**

| 项 | 清单现写 | 实测 |
|---|---|---|
| 路径 | L14 `meta/names.json` | 一致 |
| 消费者 | L14 仅列 `src/advice/pipeline.py#L160`、`src/data/name_consistency.py` | 漏 4 处真实读路径：`src/data/risk_flags.py:108`、`src/data/quality/clean_set.py:1071`、`src/data/quality/code_migration.py:431`、`src/data/services/finance_service.py:853`（另 `src/data/quality/freshness_config.json:274/286` 作新鲜度检查）→ B1S3-14 |
| 覆盖范围 | L14「覆盖股票/基金/债券全量键」 | 「在市股票」0 缺失，但退市股 0 覆盖、367 只基金无键、债券本地无法验证 → B1S3-15 |

### 3.5 ETF 费率表 `meta/multi_asset/etf_fee.parquet`

**① 写入口**

| 项 | 事实 |
|---|---|
| 唯一写入方 | `tools/fetch_etf_fee.py`（`--stage current/history/reparse/build/apply`） |
| 断点证据 | `_staging/etf_fee_20261110/raw/` 6,683 件 1,939.9 MB、`progress_*.csv`、`failed.csv` |
| 落盘 | `--apply` 才原子写 `meta/multi_asset/`，并先按 DG-11 把旧目录移入 `_backup` |
| 可重跑判定 | 可重跑、可续跑；费率原始响应齐全，可离线重解析 |

**② manifest 记录数 vs 实际**

`meta/multi_asset/etf_fee.manifest.json`：`rows 518` ✓；`tier_counts` core 153 / extend 330 / delisted 35 ✓；`files` 4 条（parquet / csv / gaps / summary）bytes + sha256 全部一致（`manifest_tool` 4/4）✓；`history_coverage` core 153/153、extend 330/330、delisted 6/35 ✓。

记录层 2 处不足：

| 项 | manifest 现写 | 实测 |
|---|---|---|
| 交叉核对分布 | `cross_check` 只记 core 153 的第三方核对 | 表中 `cross_check_status` 全池为 consistent 502 + single_source 16；这 2 个数在 manifest 里无法复核 → B1S3-16 |
| `files` 明细 | 4 条只记 bytes + sha256 | 未记 rows（同目录另两份 manifest 都记 rows），口径不统一 → B1S3-16 |

**③ 对拍**

| 对拍对象 | 比对列 | 容差 | 实测 | 结论 |
|---|---|---|---|---|
| `meta/multi_asset/etf_pool.parquet` 的 `in_scope` | `ts_code` 集合 | 0 | 518 ↔ 518，双向差 0 | **通过** |
| `etf_pool.tier` | `tier` | 0 | 0 行不符 | 通过 |
| 天天基金 F10 两页（原始响应已落 `_staging/.../raw`） | `mgmt_fee_pct`/`custody_fee_pct` | 0 | 可离线重解析，本片未重放 | 未做 |
| 内部一致性：`burden_current = mgmt + custody (+ 销售服务费)` | 三/四列 | 1e-6 | 517 行差 0；唯一例外 `159003.SZ` 0.53 = 0.20 + 0.08 + 0.25（销售服务费已计入） | **通过** |
| 内部一致性：分段覆盖 | `burden_basis` | — | `segments` 489 / `current_fallback` 29；29 只全为 delisted，与 `history_coverage.delisted` 35 中 6 只已覆盖互补（35−6=29） | **通过** |
| 内部一致性：取值域 | 两列 | — | `mgmt_fee_pct ∈ {0.15,0.2,0.25,0.3,0.35,0.4,0.45,0.5,0.6,0.7,1.0}`；`custody_fee_pct ∈ {0.05,0.06,0.07,0.08,0.1,0.15}`；无负值、无 0 | 通过 |
| 守恒 | 行数 / 主键 | 0 | 518 行 / 518 只，重复 0 | 通过 |

**④ 第二份同语义数据**

| 位置 | 性质 | 是否被代码读取 |
|---|---|---|
| `_staging/etf_fee_20261110/etf_fee.parquet` / `etf_fee.csv` | 与正式表**逐字节相同**（parquet `6d3fcdc5…`、csv `aec543b1…` 两侧相等） | 否 |
| `_backup/meta/multi_asset_fee_20261111_070013/` | 11-11 前快照（parquet 22,361 B，不同） | 否 |
| `backups/critical_assets/` | **未收录** `etf_fee` | —（可由 staging raw 重建，不判缺陷） |

成品副本留在 staging 与 L36「原始响应与进度在 `_staging/etf_fee_20261110/`」的描述不符 → B1S3-20。

**⑤ 清单登记（不改，只列）**

| 项 | 清单现写 | 实测 |
|---|---|---|
| 行数 | L36「518 行 = `etf_pool.parquet` 的 in_scope 全量」 | 一致 |
| 交叉核对 | L36「502 一致 + 16 单源（全为退市标的）」 | 表中 consistent 502 + single_source 16 一致 |
| 历史覆盖 | L36「core 153 + extend 330 + 退市 6（合计 489）；extend 330/330」 | 489 / 330 一致 |
| 销售服务费 | L36「仅 159003.SZ 有值 0.25%，其余留空不填 0」 | 精确一致（非空仅 1 行） |
| 产出脚本 | L36 `tools/fetch_etf_fee.py` + `_staging/etf_fee_20261110/` | 一致（但 staging 现多留成品副本，见 B1S3-20） |

## 4. 缺陷清单（供另一路修清单 / 后续任务；本片未动任何文件）

| 编号 | 表 | 检查项 | 严重度 | 事实（现写 → 实测） | 建议 |
|---|---|---|---|---|---|
| B1S3-06 | fund_basic | ③ 同语义集合 | **高** | 行情有、名录无 2 只：`505888.SH`（1,051 根 bar，2015-03-16~2019-07-03，已终止；`names.json` 亦无键）、`561970.SH`（2026-11-11 单 bar 新上市）。任何按 `fund_basic` 过滤的下游都会静默丢掉这 2 只 | 分两条登记：`505888.SH` 属名录历史缺口（应补 `status=D` 行并取证退市日）；`561970.SH` 属新上市同步滞后（下一轮 `sync_fund` 自动补齐） |
| B1S3-07 | fund_basic | ③ 状态重算 | **高** | manifest 自述 `status_to_L` 由「末行 ≥ 2026-09-20 的真实成交」判定 → 实测 2,254 只 L 中 2 只不满足：`166007.SZ` 末 bar 2026-05-25、`501023.SH` 末 bar **2021-10-11**（近 5 年无成交仍记 L） | 逐只取证：若已终止应转 D 并补 `delist_date`；若长期停牌应补停牌证据字段，不得维持「L 即近期有成交」的口径 |
| B1S3-08 | fund_basic / quotes 交界（观察） | ③ 口径校正 | 低 | 按项目口径「首次非零成交量 bar」统计，`list_date` 相等 **2,427**，manifest 的「2,427/2,618」**完全复现**（此前按含 0 量占位 bar 的首 bar 得 2,560/58，属口径错误，已弃用）；剩余 106 只 `list_date` 早于首根真实成交 bar（行情起始晚于上市日），不是名录错误 | 不属本表缺陷；交 `quotes/fund` 片核对这 106 只是否需补历史 bar |
| B1S3-01 | fund_basic | ② 记录数 | 中 | `name_null_after: 17`、`column_basis.name`「仍空 352 只」→ 实测空名 0（`source` 带 `name=` 363 行）；同一 sha 文件上的记录已过期 | manifest 以当前文件重算空值计数，删除失效的「352/17」表述 |
| B1S3-02 | fund_basic | ② 记录数 | 中 | manifest 指向 4 个随附文件全部不存在（`fund_basic_pending_d_20261110.json`、`fund_basic_unresolved_20261110.csv`、`fund_basic_changes_20261110.csv`、`fund_basic_fill_summary_20261110.json`） | 要么补建（若属应长期留证），要么从 manifest 删除并在「已废弃路径」注明去向；不得让 manifest 指空 |
| B1S3-04 | fund_basic | ② 路径口径 | 中 | `backup_path`/`staging_removed` 指向 `data/local_store/_backup`、`data/local_store/_staging`，两目录均不存在，且违反 DG-11「`data/` 下不得出现 `_backup`」与 DG-5「staging 在仓库根」 | 改为仓库根 `_backup/…` 与 `_staging/…`；不存在的历史路径标「已迁移」 |
| B1S3-03 | fund_basic | ② 记录数 | 低 | 无 `files` 键，无逐文件 rows/sha256；`manifest_tool` 只能按 `path`+`sha256` 判 2 条（2/2 一致） | 补 `files`，与 `etf_fee` 口径统一 |
| B1S3-05 | fund_basic | ② 生命周期 | 低 | `status: "replaced"` 与「当前正式 manifest」矛盾 | 改为 `current`/去掉该字段 |
| B1S3-09 | fund_basic | ③ 类型重算 | 低 | `160718.SZ` 记「分级」，按项目内 `_classify_local_fund_symbol(160718, '嘉实多利')` 判 LOF | 逐只核对名称与类型来源后改一处，或在 manifest 写「名称规则例外的清单」 |
| B1S3-10 | index_basic | ②/⑤ 空值披露 | 中 | manifest 与清单都只写「1,335 行」→ 实测 33 行（全 `880xxx.SH`）`name/publisher/category/list_date` 四列全空，SH 段 1,152/1,222 行无 `list_date` | 在 manifest 增空值统计并把「33 行仅代码」写进清单 L23 说明；否则下游无法区分「无此指数」与「指数存在但无描述」 |
| B1S3-11 | index_basic | ② 记录数 | 低 | 无 `files` 键（`null`） | 补 `files` |
| B1S3-12 | index_basic | ⑤ 登记 | 低 | L23 消费者未含 `src/data/services/index_service.py:39-47`（真实读路径，缺文件返回 None） | 清单补该消费者 |
| B1S3-13 | names.json | ② 记录数 | 中 | 无 manifest；`manifest_tool` 只覆盖 parquet，故从未被核对。实测 53,015 键 / 1,352,524 B / SHA256 `788066b1fc0921cf423939caa096d95a9aebbed57d7095cd7173a3678830bcbf` | 增 `meta/names.manifest.json`（keys / bytes / sha256 / 生成时间 / 来源目录），或把 manifest_tool 扩到 JSON |
| B1S3-14 | names.json | ⑤ 登记 | 中 | L14 只列 2 个消费者 → 漏 `risk_flags.py:108`、`clean_set.py:1071`、`code_migration.py:431`、`finance_service.py:853`（+`freshness_config.json:274/286`） | 清单补全消费者；`risk_flags` 是风控口径依赖，漏登记会让「改名」类变更评估不足 |
| B1S3-15 | names.json | ⑤ 登记 | 中 | L14「覆盖股票/基金/债券全量键」→ 在市股票 0 缺失，但退市股 252 只 0 覆盖、`fund_basic` 367 只无键、债券本地无集合可验 | 表述改为「在市股票全量 + 在市基金/债券部分」，并写明退市股不在本表 |
| B1S3-16 | etf_fee | ② 记录数 | 低 | `cross_check` 只记 core 153；表中全池分布为 consistent 502 + single_source 16，manifest 无法复核；`files` 4 条无 rows | `cross_check` 增全池分布；`files` 补 rows |
| B1S3-17 | 三大报表 | ⑤ 登记 | 中 | L38 仍称 `meta/financials_pit/`「是原始采集副本（含 `f_ann_date`），无任何代码消费者」→ 该目录**已不存在**，已移入 `_backup/meta/financials_pit_20261112/`（4 表），且未进「已废弃路径」表；同段代码锚点 `store.py#L217-L260` 实测应为 L316/L340 | 清单把 `financials_pit` 移入「已废弃路径」并给出 SHA256 与退役原因；修正行号锚点；同步修 `src/data/manifest_semantics.py:25` 与 `src/data/universe_filter.py:31` 的旧引用（这两处是注释，不改代码逻辑） |
| B1S3-18 | 三大报表 | ③ 对拍 | 中 | `balance` 恒等式 328,531 行中 323 行（0.098%）相对差 >1%（max 14.68，抽查为 `total_liab=0` 源端缺分量）；`cashflow` `net = op+inv+fin` 毛额相对差 >1% 达 40,223 行（12.6%），比值中位 1.0000 但 p01–p99 为 0.5497–1.4355；表内无汇率变动列，无法逐行闭环 | 对拍对象=东财/新浪原始响应，比对列=6 金额列，容差=0；把差异拆成「源端分量缺失」与「汇率影响」两类分别留证，再决定是否新增 `fx_effect` 列。在此之前不得把该恒等式当校验门槛 |
| B1S3-19 | 三大报表 | ④ 第二份 | 低 | `_staging/restore_drill_20261112/` 6,874.6 MB、`_staging/new_factors_audit_20261112/store_old/meta/financials/*`（sha 与正式表不同）各含一份三大报表副本 | 属 staging 中间产物，代码零引用；收口时按任务归属评估是否清理（本片只读，不动） |
| B1S3-20 | etf_fee | ④ 第二份 | 低 | `_staging/etf_fee_20261110/etf_fee.parquet` 与 `etf_fee.csv` 与正式表逐字节相同（sha 两侧相等），成品副本留在 staging | 与 L36「原始响应与进度」的描述不符；交付后可只保留 `raw/` + `progress_*.csv` + `failed.csv` |
| B1S3-21 | 目录级（非本表） | ② 记录数 | 低 | 同目录 `meta/multi_asset/manifest.json`（`etf_pool` 那份）：`file_missing meta/multi_asset/summary.json`；`etf_pool.csv`/`etf_gap_detail.csv`/`etf_event_gaps.csv` 登记了 rows 但不是 parquet，无法核对 | 交 etf_pool 那一片处理；此处只照明，不并入 etf_fee 判定 |

## 5. 复现命令

```powershell
# ② manifest 核对（只读；退出码 1 表示有不一致）
.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta --max-details 20
.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta/financials --only meta/multi_asset

# 主检与替代实测（输出 JSON 到 work/，不写 data/）
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1_slice3_check.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1_slice3_recalc.py

# PIT 真实读路径
.venv\Scripts\python.exe -c "import sys; sys.path.insert(0,'.'); from src.data.financials.store import latest_announced_as_of; print(latest_announced_as_of('000001.SZ','2026-11-01','income'))"
```

## 6. 边界与未做

| 项 | 说明 |
|---|---|
| 未重放外部原始响应 | 三大报表的金额列、`etf_fee` 的 F10 原始页均只做内部一致性与集合对拍，未逐值重放（`_staging/financials_history_20261111/raw` 12 件、`_staging/etf_fee_20261110/raw` 6,683 件具备离线重放条件，属后续任务） |
| 未联网 | 通达信码表原始目录、交易所指数名录、MySQL 均未读取 |
| 未改清单 | `docs/数据权威路径清单.md` 由另一路统一修改；§4 的 B1S3-10/12/14/15/17 是登记类缺陷，逐条给出「哪一行、现写什么、实测什么」 |
| 与其他片重叠 | `meta/anomalies/manifest.json` 的 2 条 `file_missing` 属其他片范围，本片只记录不判定；`etf_pool` 的 manifest 缺陷（B1S3-21）同理 |
