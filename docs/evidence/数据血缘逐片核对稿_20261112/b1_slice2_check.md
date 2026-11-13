# B1 第二片：基础层 5 张表体检（只读，2026-11-12）

取片依据：`2026-11-12/data-lineage/outputs/base_layer_calibration.csv`，排除前 4 行「行情四族」（个股日线 / 指数日线 / ETF-LOF 日线 / 期货合约日线），按行序取接下来 5 张。

范围约束：不查不改任何派生层表（复权因子 / 换手 / 宽度 / 六层 / 状态向量等）；未写 `data/`、未改任何 parquet 与 manifest、未改代码、未提交。

## 一、结论总表

| # | 表名（语义） | 正式路径 | ① 写入口可反复跑 | ② manifest 齐全与磁盘一致 | ③ 与最权威源对拍 | ④ 第二份同语义数据 | ⑤ 权威清单登记与实测一致 | 总评 |
|---|---|---|---|---|---|---|---|---|
| 1 | ST 判定原始证据（9 件） | `data/local_store/meta/st/raw/*.parquet` | **通过（6/9）**：6 件外采取证由 `src/data/st_history.py` 的 `sync`（`--start-year/--end-year/--force`，断点续跑）与 `tools/build_st_history_from_tushare.py --offline` 重跑；3 件人工裁定（reviewed_conflicts / reviewed_coverage / reviewed_pending）**不可重建**，重建不出来时只能从 `G:\git\python-quant\data\local_store\meta\st\raw` 与 `backups/` 的 `CRITICAL_DIRS` 快照恢复 | **通过**：`reviewed_conflicts.manifest.json` 覆盖全部 9 个 parquet，`manifest_tool --check` 11 条记录 0 不一致（行数 / 字节 / SHA256 与磁盘逐一相符） | **对不了 + 原因**：6 件外采件须重抓交易所公告与 tushare 才有对拍对象，本任务只读不联网；3 件人工裁定本身即源头，无可对拍对象。替代做了下游落地验证：reviewed_conflicts 200 段在 `st_intervals.parquet` 中 192 段精确命中 + 8 段以 `end_date=None` 同 `(ts_code, start_date)` 命中，**200/200 全部生效** | **通过**：无「同语义正式第二份」。副本均非正式——`G:` 镜像（正式备份）、`_backup/tushare_namechange_intervals_20261112_002943.parquet`（28,947 B 旧快照）、`_backup/st_history_20261112/meta/st/raw/closure_evidence.parquet`（2,208 B 旧快照）、`_backup/meta/st_raw_manifest_20261112/reviewed_conflicts.manifest.json`（旧版 manifest）、`_staging/st_pending_review_20261112/pending_review.parquet`、`_staging/st_em_pages_cache_20261112/`、`_staging/st_candidate_notices_cache_20261112/` | **有缺陷**：L68 只登记 `reviewed_{conflicts,coverage,pending}` 3 件，实际目录 9 件，另 6 件外采取证未登记（DG-3 未全覆盖）；本表行的写入口列写「未定位到常规写入口」，实测 6 件有代码写入口 | **有缺陷**（登记不全） |
| 2 | ST 状态日快照（7 份带日期文件） | `data/local_store/meta/st_status/st_status_<YYYYMMDD>.parquet` | **通过**：`python -m src.data.risk_flags refresh --as-of <日期>`（`src/data/risk_flags.py:460-468`）为常规入口；口径限定「按 as_of 生成当日」，**历史日期不可补生成**（manifest 已声明） | **通过**：7/7 文件行数 / 字节 / SHA256 / 列 / `date_range` 全部相符，`total_rows` 40,401 与逐文件求和恒等 | **通过**：以 `meta/names.json` 逐只重跑 `status_from_name` 与 `st_status_20261112.parquet` 对拍——5,580 只有本地名称的标的 `status` **0 不一致**；252 只无本地名称（`unknown`，与行数 5,832−5,580 吻合）。分布 st 196 / non_st 5,384 / unknown 252 | **通过**：仅 `G:` 镜像（8 文件 = 7 parquet + manifest）；`_staging/` 内无同语义副本（`delisted_coverage_audit_20261111/missing_st_status.csv` 是审计清单，不是数据副本） | **有缺陷**：L64 登记「6 份 / 34,569 行 / 5,818 只 / 2026-11-03~11-10」，实测 **7 份 / 40,401 行 / 5,832 只 / 2026-11-03~11-12**；且「消费方硬编码 `st_status_20261103.parquet`」的行号写 L1030、manifest 写 1036，实测为 `src/data/quality/clean_set.py:1085`（缺陷本身仍在，行号漂移） | **有缺陷**（数字陈旧 + 行号漂移） |
| 3 | 行业归属日快照（7 份带日期文件） | `data/local_store/meta/industry/industry_<YYYYMMDD>.parquet` | **通过**：`python -m src.data.industry refresh`（`src/data/industry.py:308-317`）；依赖通达信本地 cfg `h:\zxzq\T0002\hq_cache`（`tdxhy.cfg` / `tdxzs.cfg` / `tdxzs3.cfg` 实测均存在） | **通过**：7/7 文件行数 / 字节 / SHA256 / 列 / `date_range` 全部相符，`total_rows` 40,401 恒等 | **通过**：用 `parse_tdxhy` + `parse_industry_names` + `_levels` 重新解析本地 cfg 与 `industry_20261112.parquet` 逐值对拍——映射命中 5,589 只，5,832 行 `industry_l1/industry_l2/industry_code` **0 不一致**，252 只无映射且两侧同为 `UNKNOWN` | **通过**：`_backup/meta/legacy_tables_20261110/meta/industry/industry_20261105_v2.parquet`（26,678 B，已按 DG-13 退役的 `_v2` 旧命名，非正式）；`G:` 镜像 | **有缺陷**：路径登记正确，但消费者列只写 `industry.py::load_snapshot` 与 `build_analysis_monthly.py`，实测另有 `src/advice/risk_filters.py:26`（经 `load_snapshot` 日频数千次）、`src/data/services/finance_service.py:839`、`src/data/quality/extended_builders.py:266-281`、`tools/single_window_diag.py:764`（默认硬编码 `industry_20261104.parquet`）4 处未登记 | **有缺陷**（消费者列不全） |
| 4 | 指数成分权重月表（外部） | `data/local_store/meta/index_members/index_weight_monthly.parquet` | **通过**：`tools/pull_index_weight.py --phase fetch/build/validate`（fetch 可断点续跑，原始分片落 `tmp/index_weight_raw/<index>/<yyyymm>.parquet`）；重抓需 `.secrets/tushare_token.txt` + 联网 | **通过**：2/2 记录（本表 + `membership_intervals.parquet`）行数 / 字节 / SHA256 / 列 / `date_range` 全部相符 | **对不了 + 原因**：原始响应缓存 `tmp/index_weight_raw/` 实测 **0 个文件**，无法本地重推；重抓 tushare 需联网与 token。替代做了内部一致性：6 个指数逐期成分数、期数与总行数与 manifest 登记**完全相等**（375,727 行 / 3,643 只 / 每期权重合计 99.83~100.21），仅 3 个期次不足额——`000300.SH` 2009-12-31 为 298、`399006.SZ` 2010-06-30/2010-07-30 为 86/93；因各指数总行数与登记逐指数完全相等，判定为**源端该期返回不足**，非本地写入丢失 | **通过**：无同语义第二份；`tmp/index_weight_raw` 为空（原始分片缓存，非正式件）、旧 `meta/index_membership/` 目录实测已不存在、`G:` 镜像在 | **有缺陷（轻微）**：L57 的 375,727 行 / 3,643 只 / 2005-04-29~2026-10-30 与实测一致；消费者只写 `extended_builders.py#L80`，实测常量在 `:85`，且漏登记 `tools/rebuild_index_constituents.py`（区间表产出方）与 `tools/pull_index_weight.py`（本表产出方）；manifest `total_periods: 259` 实为单指数最大期数（6 指数期数合计 1,126），易被误读为总期数 | **有缺陷**（消费者列 + 一个易误读字段） |
| 5 | 估值日表（PE/PB/DV/总市值） | `data/local_store/meta/financials/valuation_daily.parquet` | **通过**：`src/data/financials/collector.py` 常规采集入库，分片落 `_staging/financials_parts_<YYYYMMDD>/`（`src/data/financials/store.py:158-159`），非正式目录 | **通过**：`meta/financials/manifest.json` 的 `tables.valuation` 登记 17,969,103 行 / 642,217,157 B / SHA256 / 12 列，`--check` 判定一致；`unique_source_per_semantics.valuation` 亦指向本表 | **对不了 + 原因**：权威源为外部（tushare / baostock），只读任务不联网。替代做了与「收盘价 × 股本」的独立一致性：2026-11-11 当日 5,579 只全部可比，`total_mv/(close×total_share)` **98.57% 逐值恒等（1e-9）**，`circ_mv/(close×float_share)` **97.96% 恒等**；`|r−1|>1%` 为 12 只（total_mv）/ 27 只（circ_mv），逐只核对均为 `meta/factors/stock_shares.parquet` 当日 PIT 股本与 tushare 同期股本不同步，差额方向与股本变更一致，**非估值列自身错**；当日 `pe/pb/dv_ratio/total_mv/circ_mv/close` 空值 0 | **通过**：`_backup/` 7 份回滚快照（`derived_catchup_20261112`、`meta/financials/valuation_daily.2026110*.parquet` 6 份、`valuation_daily_20261111`、`valuation_daily_1993b_20261112_220105`、`history_backfill_20261108`、`p1p2_final_20261108`）与 `_staging/{g080_rewrite_20261112,valuation_history_20261111}/valuation_daily_candidate.parquet` 2 份候选，**全部非正式**（符合 DG-5 / DG-11 / DG-12）；`G:` 镜像在 | **有缺陷（轻微）**：L26 登记在、语义与实测一致；消费者 `store.py#L117-L125` 实测为 `:150-155`（`table_path`），行号漂移；`fundamental_factors.py`、`freshness_config.json` 命中属实 | **有缺陷**（行号漂移，其余通过） |

## 二、逐项实测证据

### 2.1 manifest 核对（统一用现成工具，未自创判据）

```
.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta/st --only meta/st_status \
    --only meta/industry --only meta/index_members --only meta/financials
```

实测输出：`结论: PASS`，manifest 10 份 / 文件记录 51 条 / 不一致 0 / sha256 格式告警 0 / 无法定位 0 / 语义未登记 0。
分组：`meta/financials/manifest.json` 10/10、`meta/index_members/manifest.json` 2/2、`meta/industry/manifest.json` 7/7、`meta/st/manifest.json` 3/3、`meta/st/raw/reviewed_conflicts.manifest.json` 11/11、`meta/st_status/manifest.json` 7/7 全部一致。

注：`base_layer_calibration.csv` 第 6 行「7 张无 manifest（G-095）」已不成立——`reviewed_conflicts.manifest.json` 于 2026-11-12 19:25 覆盖了 `st/raw` 全部 9 个 parquet。

### 2.2 对拍实测

| 表 | 对拍对象 | 比对列 | 容差 | 差异 | 归因 |
|---|---|---|---|---|---|
| ST 状态快照 | `meta/names.json` + `risk_flags.status_from_name` | `status` / `is_st` | 0 差异 | 5,580 有名称标的中 0 条 | 252 只 `names.json` 无键 → `unknown`，与 `reason=name_missing` 一致 |
| 行业快照 | `h:\zxzq\T0002\hq_cache\{tdxhy,tdxzs,tdxzs3}.cfg` | `industry_l1` / `industry_l2` / `industry_code` | 0 差异 | 5,832 行 0 条 | 252 只 cfg 无映射，两侧同为 `UNKNOWN` |
| ST 原始证据 | `meta/st/st_intervals.parquet`（下游落地） | `(ts_code, start_date, end_date)` | 精确 | 200 段中 192 精确 + 8 段开区间 | 8 段源端 `end_date` 为空，`st_intervals` 同 `(code,start)` 同为空，属开区间保留，非丢失 |
| 指数成分权重 | manifest `coverage` 逐指数登记 | 期数 / 每期成分数 / 总行数 | 精确 | 6 指数期数与行数全等；3 期成分数不足额 | 源端该期返回不足（总行数守恒，非本地丢失） |
| 估值日表 | `quotes/stock` 收盘价 × `meta/factors/stock_shares.parquet` PIT 总/流通股本 | `total_mv` / `circ_mv` | 1e-9 恒等；>1% 记差异 | total_mv 98.57% 恒等、12 只 >1%；circ_mv 97.96% 恒等、27 只 >1% | 股本 PIT 生效日与 tushare 同期股本不同步，非估值列自身错 |

### 2.3 消费者 grep 实测（未沿用旧结论）

| 表 | 实测消费者 | 与清单的差异 |
|---|---|---|
| ST 原始证据 | `src/data/st_history.py:274/279/284`（`reviewed_*_path`）、`:430/:465/:484/:578/:615`（外采件写回）、`:1170-1228/:1770-1771`（读取）、`src/data/st_name_history.py:139/:667/:682`、`src/data/quality/code_migration.py:434-436`、`tools/build_st_history_from_tushare.py:68` | 清单只写 `st_history.py#L275-L285`，实际读点更广；6 件外采件完全未登记 |
| ST 状态快照 | `src/data/risk_flags.py:76-78`（`snapshot_path`）、`:221-225`、`src/data/st_history.py:754/:773-777`、`src/advice/risk_filters.py:34`、`src/data/quality/clean_set.py:1085`（硬编码 `20261103`）、`src/data/quality/freshness_config.json`（`latest_glob_distinct`）、`tools/build_universe_layers.py:339-340` | 行号漂移：清单 L1030 / manifest 1036 → 实测 1085 |
| 行业快照 | `src/data/industry.py:105-108`（`snapshot_path`）、`:220`、`src/advice/risk_filters.py:26/:128-140`、`src/data/services/finance_service.py:839`、`src/data/quality/extended_builders.py:266-281`、`tools/research/build_analysis_monthly.py:91`、`tools/single_window_diag.py:764` | 清单只列 2 处，漏 4 处（含 1 处硬编码 `industry_20261104`） |
| 指数成分权重 | `src/data/quality/extended_builders.py:85`（`_WEIGHT_REL`）、`:448`、`tools/rebuild_index_constituents.py`、`tools/pull_index_weight.py`（产出方） | 行号 L80 → 实测 L85；漏产出方与区间表构建方 |
| 估值日表 | `src/data/financials/store.py:150-155`（`table_path`）、`src/advice/fundamental_factors.py:10/:58/:171`、`src/data/quality/freshness_config.json` | 行号 `#L117-L125` → 实测 `:150-155` |

### 2.4 第二份同语义文件清单（含 `_backup` / `_staging` / 镜像）

- 全部 5 张表均存在 `G:\git\python-quant\data\local_store\...` 镜像（正式备份，路径实测均存在；`meta/st_status` 下 8 个文件）。
- `reports/` 下实测 **0 个** 这 5 张表的副本。
- `_backup/` 与 `_staging/` 下的副本见结论表第 ④ 列，**无一为正式件**（旧快照 / 候选 / 已退役命名）。

## 三、本次未做与原因

1. 未与外部源（交易所公告、tushare、baostock、东财）重抓对拍：任务约束为只读且不联网，两处记为「对不了」并给出替代实测。
2. 未触碰任何派生层表（复权因子、换手、宽度、六层、状态向量），也未因对拍需要去读它们；本次对拍全部落在基础层内部或本地 cfg / 外部缓存清单。
3. 未修改 `docs/数据权威路径清单.md`：本次为只读体检，登记类缺陷只在本文报告，未动清单文件。

## 四、建议后续动作（未执行）

1. 补登记 `meta/st/raw` 的 6 件外采取证，或把它们从「ST 判定原始证据」语义中拆出单独登记。
2. 更新 L64 的份数 / 行数 / 截止日，并修正 clean_set 行号。
3. 补 `industry` / `index_weight_monthly` / `valuation_daily` 的消费者列并统一行号。
4. 修正 `meta/index_members/manifest.json` 的 `total_periods` 语义（单指数 vs 全指数合计）。
