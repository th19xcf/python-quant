# B2 第一片体检报告（状态/质量派族）

- 体检日期：2026-11-13
- 批次来源：`2026-11-12/data-lineage/outputs/governance_order.md` B2 = depth 1（15 张）
- §0~§10 为**只读体检**（未改代码、未写 `data/`、未改 parquet 与 manifest、未提交）
- §11 / §12 为体检后获批的两件落地动作，**不属只读**：① `meta/st/raw/manifest.json` 补齐（另见回报）；② `universe_price_jumps` 正式表替换；改动路径与新 SHA256 见 §11
- 本片产出：本文件（`2026-11-12/data-lineage/outputs/b2_slice1_check.md`）；实测脚本在 `2026-11-12/data-lineage/work/b2s1_*.py`

## 0. 本片取表清单（已对外声明，供另一路划边界）

| # | lineage 表 id | 族别 | 正式路径 | 本片处理 |
|---|---|---|---|---|
| 1 | `suspension_daily` | A 停牌派生族 | `data/local_store/meta/suspension/suspension_daily.parquet` | 全量五检 |
| 2 | `implied_suspension` | B 隐含停牌族 | `data/local_store/meta/suspension/implied_suspension.parquet` | 全量五检 |
| 3 | `universe_price_jumps` | C 跳变族 | `data/local_store/meta/universe/universe_price_jumps.parquet` | 全量五检 |
| 4 | `anomalies` | D 异常台账族 | `data/local_store/meta/anomalies/`（`quarantine_rows.parquet` + 目录内 13 个文件） | 全量五检；含 `placeholder_codes.parquet` 磁盘缺失、`coverage_gaps` 退役核账 |

**为什么这 4 张同片（同一表族）**

1. 语义同族：都是「逐票逐日（或逐票）的异常/状态标记」，服务于同一层消费——读侧剔除或标记可疑行（停牌并集、隔离行清单、跳变排除、占位代码排除），不产出价格或因子。
2. 上游同源：四张表只依赖 `quotes_stock` / `quotes_fund` / `quotes_index` 与交易日历，不依赖 `stock_shares`、`st_intervals`、`stock_basic`、`stock_dividend` 等仍在体检/修复中的上游。本片全片可独立验收，不会带着未定上游口径开工。
3. 落点相邻：两张在 `meta/suspension/`、一张在 `meta/universe/`、一张在 `meta/anomalies/`，均属 `meta/` 质量层，manifest 与读侧常量集中在同几处代码。
4. 交叉可互证：`implied_suspension` 与 `quarantine_rows` 的判据只差一个 `ohlc_flat` 条件，`suspension_daily` 又是它们的并集上游，可在片内做三向行集对拍，比拆开单独检更有证据力。

**本片不取的表与原因（B2 内）**

| 表 | 归属 | 不取原因 |
|---|---|---|
| `stock_turnover` | B2 / depth 1 | 上游 `stock_shares.float_share` 的缺口（G-077）仍在修复窗口，`governance_order.md` 明确「B1 的 G-077 未收口前不重算」，按闸 2 挪到下一片 |
| `name_history` | B2 / depth 1 | 上游 `st_raw`（`meta/st/raw/`）已由本任务补齐 `manifest.json`（G-095 的"无 manifest"一半已解除），但该表自身缺退市段（G-079）仍需在同一口径下重判，且 `st_raw` 实测**不可重建**（见 §9），不宜与未定上游同片 |
| `membership_intervals` | B2 / depth 1 | 上游 `index_weight` / `index_basic` 属「外部快照·无常规写入口」组（G-030~G-034），尚未定位写入口与口径 |
| `daily_index` / `ann_date_map` | B2 / depth 1 | 上游 `quotes_*` 与 `announcement` 相关口径仍在其他路径处理（G-045 跟踪中） |
| `breadth_daily` | B2 / depth 1 | 族别不同（L2 宽度指标，产出数值序列而非状态标记），留待后续片 |
| `stock_adj_factor` / `fund_adj_factor` / `etf_equity_pool` / `futures_multi_asset` / `listing_intervals` | B2 / depth 1 | 族别不同（复权/池/合约/名录），留待后续片 |

> 越界核对：以上取表全部在 `governance_order.md` 的 B2 清单内（第 96 行 depth 1 段），未取任何 B3 及以后的表。

- 实测时间：2026-11-13；行情基准为本地 `quotes_stock`（末 bar 2026-11-10）+ `quotes_fund`；交易日历 `authoritative_end = 2026-11-11`
- 复现脚本：`2026-11-12/data-lineage/work/b2s1_facts.py`、`b2s1_manifest_check.py`、`b2s1_recompute.py`、`b2s1_jump_gap.py`、`b2s1_mtime_log.py`、`b2s1_dupes.py`、`b2s1_candidate.py`

---

## 1. 结论摘要

| 项 | 结论 |
|---|---|
| 批次归属 | 4 张全部在 B2（`governance_order.md` L96 depth 1），未越界到 B3 及以后 |
| ② manifest 一致性 | 4 张表所在三个目录的 manifest 记录**全部与磁盘一致**（`rows`/`bytes`/`sha256` 逐条相符，声明但缺失 = 0）；缺陷在覆盖范围：`meta/anomalies` 有 7 个磁盘文件未登记、`meta/universe` 有 4 个、`meta/suspension` 有 `coverage.json` 1 个未登记 |
| ③ 对拍 | `suspension_daily` 独立重算 987,171 行与表内 `quote_gap` 行**逐行相等（0 差异）**；`implied_suspension` / `quarantine_rows` 独立重算 3,137 行**完全相等**；`universe_price_jumps` 体检时为表内 9,910 行 vs 重算 9,927 行（**6 行陈旧多报 + 23 行漏报**，根因是 2026-11-10 13:37 之后上游行情被改了 17 个文件而本表未重算），**已于 §11 替换为 9,927 行、对拍 0/0** |
| ④ 第二份同语义 | `implied_suspension` 与 `quarantine_rows` **行集完全相同**（3,137 行，双向差集 0），同一批物理行挂在两条正式路径上（定性见 §12）；`universe_price_jumps` 的计数另存一份在 `meta/universe/universe_exclusions.json`（`by_label` 合计体检时 9,910、已随替换同步到 9,927），且 `fill_internal_gaps.py:656-667` 明确"未重算跳变明细" |
| ⑤ 清单登记 | 13 条偏差：4 条数字过期、1 条引用错误、7 条结构性缺漏、1 条下游未同步。逐条见 §8，**未改动清单与缺口登记文件** |
| 已落地一条 | `universe_price_jumps` 已按批准替换为 9,927 行（§11）：四项验收全过，manifest 与 `universe_exclusions.json` 同步；替换前 6 条陈旧假报的成因是 17 个行情文件在建表后被动过 |
| 新暴露一条 | 替换后下游 `universe_exclusions.parquet` 的 `bad_price_jump` 未同步：仍 67 行 / 64 只，与新表 `data_error` 68 行 / 65 只差 3 移除 + 4 补入（§11 末）；重建需 DB+TDX，本轮未授权、未动 |
| 另一条硬缺陷 | `suspension_daily` 的 23 行 `is_suspended=false` 留档行**在 `src/` 与 `tools/` 里没有任何写入者**（`git grep bar_present_prev_quote_gap` 命中全在 `reports/`、`docs/`）——重跑 `build_suspension` 会把它们覆盖掉，代码文档与覆盖声明也仍写"只登记停牌行、is_suspended 恒为 True" |
| 定性一条 | `implied_suspension` 与 `quarantine_rows` 行集相同属**数据巧合而非构造保证**（判据不同：一个 AND `ohlc_flat`，一个走违规清单的 `structural` 规则集），推荐**维持两条独立正式路径 + 加键集相等联动校验**，不合并、不退役（§12） |

---

## 2. 四张表的派生边核对（与 `lineage.json` 逐条对）

| 表 | 血缘图 `derived_from` | 代码实测读的上游（列 / 层） | 结论 |
|---|---|---|---|
| `suspension_daily` | `quotes_stock` | `quotes_stock[trade_date]` + `quotes_fund[trade_date]`（`extended_builders.py:156` 默认 `DEFAULT_ASSET_CLASSES`），交易日历 `get_calendar` 给网格 | **血缘图漏边**：缺 `quotes_fund`。数据里 fund 405,234 行 / stock 581,960 行，基金占 41% |
| `implied_suspension` | `quotes_stock` | `quotes_stock[volume, amount, open, high, low, close]`，只取 stock | 边一致；但**写入口漏登**（见 §4） |
| `universe_price_jumps` | `quotes_stock` | `quotes_index/stock/fund[close, pre_close]` + 分红事件（`stock_dividend` / `fund_dividend`，用于判 `corporate_action_missed`）+ 基准日历（判 `market_wide_event`） | **血缘图漏 4 条边**：`quotes_index`、`quotes_fund`、`stock_dividend`、`fund_dividend`。数据里 index 9,481 行占 95.7%，最主的分支完全没画 |
| `anomalies` | `quotes_stock` | `quotes_stock[volume, amount]`（quarantine）；`placeholder_codes` 读同一批行情判单位数价格 | 边方向一致；但 `anomalies` 的 `path` 仍写三个文件（含已退役 `coverage_gaps`、已移出正式目录的 `placeholder_codes`），`known_defects` 也仍写"coverage_gaps 无 manifest" |

补充：`lineage.json` 没有独立的 `edges` 数组，派生边只存在 `tables[].derived_from` 里，且是表级粒度——这解释了上面为什么"漏边"能同时漏掉基金、指数、分红三类上游而不被任何校验拦住。

`writers_detected` 与代码实测同样不一致（不是漏边，是静态扫描漏语法）：

| 表 | 血缘图 `writers_detected` | 代码实测写点 | 判定 |
|---|---|---|---|
| `suspension_daily` | 只列 `_staging/derived_1993b_20261113/layer2_apply.py:47`（oneoff） | `src/data/quality/extended_builders.py:156`（`build_suspension`）→ `_write_frame`（L120-121）→ `write_parquet_atomic`（L242）；CLI `python -m src.data.quality.extended_builders suspension --commit` | **血缘图漏常规写入口**：写不在 `build_suspension` 里直调 `write_parquet_atomic`，中间隔了一层 `_write_frame`，静态扫描只认直调 |
| `universe_price_jumps` | `tradable_universe.py:979`（routine）、`:61`（routine）、`fill_internal_gaps.py:46`（routine） | `:979` 是写点；`:61` 是路径常量；`fill_internal_gaps.py:46` 是**读**（`verify_jumps` 用它做交叉校验） | **两条误标为写入口**（常量与读侧被算成写） |
| `implied_suspension` | 空 | `reports/audit_quotes_20261109/fix2_write_table.py`（未跟踪一次性脚本，遇已存在文件直接拒绝覆盖） | 血缘图"未定位到常规入口"**判定正确**，可补上这个实际写点（G-113 同一条） |
| `anomalies` | `tools/reconcile_p2_metadata.py:182`（routine） | `quarantine_rows` 真实写点 `tools/repair_quote_anomalies.py:266-270`（一次性修复工具，需外部违规清单输入）；`src/data/quality/quarantine.py` **只有读函数，零写函数** | 血缘图写入口指错文件 |

---

## 3. 四张表基础事实（实测）

| 文件（目录 + 文件名） | 行数 | 字节 | 列数 | SHA256（前 16） | 时间跨度 |
|---|---|---|---|---|---|
| `data/local_store/meta/suspension/suspension_daily.parquet` | 987,194 | 1,909,096 | 11 | `73418a62a7cea13a` | 1990-12-21 ~ 2026-11-10 |
| `data/local_store/meta/suspension/implied_suspension.parquet` | 3,137 | 12,228 | 9 | `e5ded0f4197ad089` | 2019-12-12 ~ 2025-12-04 |
| `data/local_store/meta/universe/universe_price_jumps.parquet` | 9,927 | 155,773 | 15 | `000faf82646fb01d` | 1991-02-26 ~ 2026-11-11 |
| `data/local_store/meta/anomalies/quarantine_rows.parquet` | 3,137 | 5,918 | 3 | `b5cc90fb47513cd3` | 2019-12-12 ~ 2025-12-04 |
| `data/local_store/meta/anomalies/placeholder_codes.parquet` | — | — | — | **文件不存在** | — |

> `universe_price_jumps` 一行是 §11 替换后的现状；体检时点（替换前）为 9,910 行 / 154,965 B / `adcfb57f245e13d2` / 至 2026-11-09，旧表已整体备份到 `_backup/meta/universe_price_jumps_20261113/`。

分层构成（实测）：

| 表 | 关键分布 |
|---|---|
| `suspension_daily` | `asset_class`：stock 581,960 / fund 405,234；`source`：`quote_gap` 987,171 + `bar_present_prev_quote_gap` 23；`is_suspended`：True 987,171 / **False 23**；`confidence`：high 666,126 / low 321,068；`detected_at` 全为 2026-11-11；11 列全部 0 空值 |
| `implied_suspension` | `asset_class` 全 stock；`reason` 全 `volume_null+amount_null+ohlc_flat`；`ohlc_flat` 全 True；`in_existing_suspension` 全 False；`conflicts_with_existing_suspension` 全 True；`source` 是常量串 `volume_isnull\|amount_isnull\|ohlc_flat`（逐行原因在 `reason` 列） |
| `universe_price_jumps` | `asset_class`：index 9,481 / stock 293 / fund 136；15 列全部 0 空值；`pct_chg` 与 `close/pre_close-1` 的自洽检查：**0 行偏差 > 1e-6** |
| `quarantine_rows` | `rule_ids` 全为 `required_columns,volume_invalid` |

---

## 4. ① 写入口

| 表 | 常规写入口 | 原子写 | 结论 |
|---|---|---|---|
| `suspension_daily` | `src/data/quality/extended_builders.py:156` `build_suspension()`，CLI `python -m src.data.quality.extended_builders suspension --commit`；同时写 `meta/suspension/coverage.json` | ✅ `write_parquet_atomic` / `write_text_atomic` | 有常规入口。**缺陷：23 行留档行无写入者**——`bar_present_prev_quote_gap` 在 `src/`、`tools/` 零命中，只在 `reports/` 一次性脚本里；重跑 builder 只会写出 987,171 行，23 行会被静默丢弃 |
| `implied_suspension` | **无**。`src/`、`tools/`、`config/` 全无写点 | 写点脚本内部自行写 parquet | 与 **G-113** 完全一致；实际写点 `reports/audit_quotes_20261109/fix2_write_table.py`（未跟踪、拒绝覆盖已有文件），规则是 `volume IS NULL OR amount IS NULL`（**OR**，不是 AND），`ohlc_flat` 单独一列 |
| `universe_price_jumps` | `src/data/quality/tradable_universe.py:963` `build_exclusions()` → `:975` 写；入口 `tools/build_universe_exclusions.py:76` | ✅ `_write_atomic` | 有常规入口；`tests/test_tradable_universe.py` 有幂等用例 |
| `anomalies` | `quarantine_rows` 仅 `tools/repair_quote_anomalies.py:266-270`（一次性修复，需外部违规清单）；`placeholder_codes` 仅 `tools/reconcile_p2_metadata.py:182` | ✅ `_write_atomic` | **无常规写入口**。`src/data/quality/quarantine.py` 只有 `read_quarantine_rows` 一类读函数，与 manifest 里"由质量闸门按规则集实时写出"的说法矛盾 |

---

## 5. ② manifest 记录数与实际文件是否一致

`suspension_daily` / `implied_suspension` / `universe_price_jumps` / `quarantine_rows`：`rows`、`bytes`、`sha256` **四项逐条一致**（实测见 §3）。

同目录的 manifest 完整性问题（不影响上面 4 条记录，但属同一登记单元）：

| 目录 | manifest 条数 | 磁盘文件数 | manifest 有、磁盘无 | 磁盘有、manifest 无 |
|---|---|---|---|---|
| `meta/suspension` | 2 | 3 | 0 | `coverage.json`（1 个） |
| `meta/universe` | 4（`listing_intervals` / `stale_status` / `universe_exclusions` / `universe_price_jumps`） | 8 | 0 | `universe_exclusions.json`、`internal_gap_inventory.csv`、`internal_gap_fix_failed.csv`、`internal_gap_fix_progress.json`（4 个） |
| `meta/anomalies` | 5（`fund_fake_bar_bars.csv` / `fund_pseudo_registry.json` / `placeholder_quote_codes.csv` / `quarantine_rows.parquet` / `weekend_calendar_registry.json`） | 13 | 0 | `factor_tail_lag_codes.csv`、`no_local_quote_codes.csv`、`quality_exemptions.csv`、`rights_issue_codes.csv`、`rights_issue_verified.csv`、`rights_verify_progress.json`、`source_stale_codes.csv`（7 个） |

已退役件的登记核对（**通过，可作对照样板**）：

- `meta/anomalies/placeholder_codes.parquet`：正式目录已无文件，内容在 `_backup/meta/anomalies_placeholder_codes_20261112/placeholder_codes.parquet`，**3,331 B / SHA256 `4f2fdf7ddab4a118…` 与清单 L173 逐字节一致**；manifest 已不再收录，清单 L173 已进「已废弃路径」表、并记明写盘入口由 `393937c` 摘除。DG-13 三要素（移动而非删除、SHA 证明、废弃登记）齐备。
- `meta/anomalies/coverage_gaps.parquet`：同样已在清单 L171 登记退役至 `_backup/meta/anomalies_coverage_gaps_20261112/`（5,336 B / 6 行），正式目录无残留。

另：manifest 的引用错误——`meta/anomalies/manifest.json` 的 `authority` 字段把「占位行情代码清单」引成 **L92**，实际该行是「质量门禁人工豁免基线（`quality_exemptions.csv`）」，占位行情代码清单是 **L91**。

文件级卫生问题（不属登记缺陷）：`meta/anomalies/no_ex_rights_events.csv` 曾在目录内（旧 manifest 快照有 7 行 / 1,945 B 记录），现正式目录无此文件、**`_backup` 无快照**，只剩 `backups/critical_assets/data/local_store/meta/anomalies/`（1,945 B）与 `logs/` 两处归档副本；当前 manifest 已不收录、清单也从未登记，故按"缺回滚快照的删除"记录，不计入清单偏差。

---

## 6. ③ 能否与权威源对拍（逐表给出对象、列、容差、结果）

全部为"按权威源独立重算 → 与正式表逐行比对"，键为 `(ts_code, trade_date)`（跳变表加 `asset_class`），**容差 0 行**（集合精确相等）。

| 表 | 对拍对象 | 比对列 / 判据 | 重算结果 | 与正式表差异 |
|---|---|---|---|---|
| `suspension_daily` | 交易日历（`calendar.source`，8,452 只 stock+fund 的 `date` 列） | 日历 − 实际 bar 日，窗口 `[首 bar, min(末 bar, horizon)]`；键 `(ts_code, trade_date)` | 987,171 行 | 对 `source='quote_gap'` 的 987,171 行 **0 / 0**（双向差集均为 0）；23 行 `bar_present_prev_quote_gap` 按定义不进重算，属留档行 |
| `implied_suspension` | `quotes_stock` 全量（8,452 只中的 stock 部分） | `volume IS NULL AND amount IS NULL AND open=high=low=close` | 3,137 行 | **0 / 0** |
| `quarantine_rows` | `quotes_stock` 全量 | `volume IS NULL AND amount IS NULL`（`required_columns,volume_invalid`） | 3,137 行 | **0 / 0** |
| `universe_price_jumps` | `quotes_index/stock/fund` 全量（`close`,`pre_close`） | `pre_close` 有限且 >0 时 `abs(close/pre_close-1) > 0.5` | 9,927 行 | 交集 9,904；**正式表多 6 行、少 23 行** |

`universe_price_jumps` 差异归因（逐条查证，不是估计）：

- **多 6 行（陈旧假报）**：`159582.SZ 2026-07-13`、`163503.SZ 2007-10-16` 等 6 条，当前行情下 `close/pre_close-1` 已是 -6.93% / +0% 这类正常值；6 条的行情文件 mtime **全部晚于本表构建时刻 2026-11-10 13:37:16**（2026-11-11 / 11-12），其中 `163503.SZ` 在 `logs/data_changelog.csv` 有 2026-11-12 19:18:51 的修正记录。
- **少 23 行（漏报）**：9 行是 2026-11-10 / 11-11 的 bar（8 只指数 `880005/880006/880015/880016/880035/880036/880045/880046` 在 11-10、`880076` 在 11-11，表建完才入库）；其余 14 行落在 1992~2015，代码为 `000033/000511/000594/000693/000979/300372/600401/600432/600656` 等退市或改名标的，行情文件 mtime 均为 2026-11-12 16:26~16:27（退市回补那一批）。
- **归因结论**：算法没错、表内 `pct_chg` 自洽（0 行偏差），纯属**上游改完未重算**。可执行替代实测：`_staging/derived_1993b_20261113/excl_candidate/universe_price_jumps.parquet`（2026-11-12 20:52:41，155,773 B，9,927 行）与本次独立重算 **0 / 0 完全相等**；2026-11-13 00:25 已按此替换正式表（§11）。

> 本节 ③ 的差异数是**替换前**的体检结论（9,910 vs 9,927）。替换后正式表与独立重算、与生产构建器重算三者均为 9,927 行、双向差集 0，证据见 §11。

`suspension_daily` 的 23 行留档行单独说明（对拍可解释、可复现性不可）：

- 23 行 `is_suspended=false` / `source='bar_present_prev_quote_gap'`，含义是"缺口候选被当天真实 bar 推翻"，保留是为可追溯。
- 但 `src/data/quality/extended_builders.py:163-164` 的文档、`coverage.json` 的 `caveats` 第 6 条（L264）都仍写"本表只登记停牌行、`is_suspended` 恒为 True"，与数据矛盾。
- 重跑 `build_suspension` 只产出 987,171 行，23 行不会被重建；`git grep 'bar_present_prev_quote_gap'` 在 `src/`、`tools/`、`config/` 零命中，只在 `reports/` 与 `docs/` 出现。

---

## 7. ④ 是否存在第二份同语义数据

| 表 | 正式层内是否第二份 | 正式层外同内容副本 | 判定 |
|---|---|---|---|
| `suspension_daily` | 否 | `backups/critical_assets/…`（逐字节同）、`_backup/derived_1993b_20261113/meta/suspension/`（1,908,438 B，11-12 16:54 那版）、`_backup/meta/suspension_daily_20261112/`（2,055,685 B，更早那版） | 合规（均属快照，`src/` 无读取） |
| `implied_suspension` | 否 | `backups/critical_assets/…`、`_backup/derived_1993b_20261113/…`（三份逐字节相同） | 合规；**但行集与 `quarantine_rows` 完全相同**，见下 |
| `universe_price_jumps` | 否 | 31 份同名文件全在 `backups/`、`_backup/`、`_staging/`、`tmp/`、`logs/`，`data/local_store` 内只有正式一处 | 合规；**计数结论另存一份**在 `meta/universe/universe_exclusions.json` 的 `price_jumps.by_label`，体检时为 9,910、已随替换同步到 9,927（§11）；`fill_internal_gaps.py:656-667` 只刷新 `pre_close_zero_rows`，注释明写"未重算跳变明细" |
| `anomalies` | 否（`quarantine_rows` 唯一） | `placeholder_codes` 3 份、`placeholder_quote_codes.csv` 19 份，均在 `backups/`、`_backup/`、`_staging/`、`tmp/` | 合规 |

**`implied_suspension` ≡ `quarantine_rows`（同一批物理行）**：两张表 3,137 行的 `(ts_code, trade_date)` 双向差集都是 0，`implied` 的 `in_existing_suspension` 全 False、`conflicts_with_existing_suspension` 全 True（即与 `suspension_daily` 无交集，这一点与我的集合实测一致）。语义上二者不同（一个供停牌并集、一个供隔离剔除），但**同源、同批行、无联动写入口**：任一表单独刷新都会立刻分叉。定性结论见 §12——不是 DG-1 违规，但必须补一条键集相等联动校验。

---

## 8. ⑤ 权威路径清单登记是否与实测相符

| 清单行 | 现写 | 实测 | 判定 |
|---|---|---|---|
| L62 `suspension_daily` | 987,196 行 / 4,329 只 / 1990-12-21~2026-11-10 | 987,194 行 / 4,329 只 / 1990-12-21~2026-11-10 | **行数差 2**（其余相符） |
| L63 `implied_suspension` | 3,137 行 / 117 只 / 2019-12-12~2025-12-04 | 3,137 行 / 117 只（与 `quarantine_rows` 同集）/ 同区间 | 相符 |
| L70 `quarantine_rows` | 3,137 行 / 117 只；manifest 已收录 rows/bytes/sha256 | 3,137 行 / 117 只；manifest 一致 | 相符 |
| L83 `universe_price_jumps` | 9,910 行 / 432 只 / 1991-02-26~2026-11-09 / 15 列；生产读侧未接线 | 替换后 9,927 行 / 438 只 / 1991-02-26~2026-11-11 / 15 列（§11）；读侧仍只有 `fill_internal_gaps.py:46`（校验用途） | **数字过期**：需按实测改写（见缺陷 10） |
| L88 `fund_fake_bar_bars` | 15 行 / 5,770 B | 磁盘 14 行 / 5,770 B（manifest 也是 14 行） | **行数差 1** |
| L91 `placeholder_quote_codes` | 476 行 / 309 只 / 21,424 B | 476 行 / 21,424 B（manifest 一致） | 相符 |
| L171 / L173 已废弃路径 | `coverage_gaps`、`placeholder_codes` 的移入位置与 SHA256 | 两份 `_backup` 副本 SHA256 均与清单一致 | 相符 |

登记类缺陷汇总（供统一修改清单与缺口登记的两条路径，本报告不改这两份文件）：

1. `docs/数据权威路径清单.md` **L62**：987,196 → 987,194。
2. `docs/数据权威路径清单.md` **L88**：15 行 → 14 行。
3. `docs/数据缺口登记_正式表_2026-11-12.md` **G-104 ②** 正文引 `suspension_daily` manifest "987,194 行 / 2,055,685 B"：现盘是 987,194 行 / **1,909,096 B**，2,055,685 B 是 `_backup/meta/suspension_daily_20261112/` 里那一版；该行文字需按现盘改写。
4. `meta/anomalies/manifest.json` 的 `authority` 字段：「占位行情代码清单」引 **L92**，应为 **L91**（L92 是质量门禁人工豁免基线）。
5. 缺登记（DG-3）：`meta/anomalies/` 下 7 个磁盘文件未进同目录 `manifest.json`；其中 `rights_verify_progress.json` 是核验断点文件，按 DG-32 应落 `_staging/` 而非正式目录。
6. 缺登记（DG-3）：`meta/suspension/coverage.json` 存在但未进 `manifest.json` 的 files 段，覆盖声明与 manifest 双轨。
7. 缺登记（DG-3）：`meta/universe/manifest.json` 无 `generated_at` / `as_of` / 上游 mtime，且 `universe_exclusions.json`、`internal_gap_inventory.csv`、`internal_gap_fix_failed.csv`、`internal_gap_fix_progress.json` 4 个文件未收录。
8. 结构性缺漏（血缘图）：`2026-11-12/data-lineage/outputs/lineage.json` 中 `anomalies.path` 仍含已退役的 `coverage_gaps.parquet` 与已移出的 `placeholder_codes.parquet`；`suspension_daily` 漏 `quotes_fund` 边、`universe_price_jumps` 漏 `quotes_index`/`quotes_fund`/`stock_dividend`/`fund_dividend` 四条边；`universe_price_jumps.writers_detected` 把路径常量 `:61` 与读侧 `fill_internal_gaps.py:46` 误标为写入口；`anomalies` 的写入口指向 `tools/reconcile_p2_metadata.py:182`（实为已退役的 `placeholder_codes` 产出方，其写盘入口已由 `393937c` 摘除），`quarantine_rows` 的真实写点 `tools/repair_quote_anomalies.py:266-270` 未登记。
9. 缺登记（缺口登记）：未记录 `universe_price_jumps` 的 6+23 陈旧差。**该条已于 2026-11-13 00:25 落地修复（§11）**，登记口径应改为"已替换 + 四项验收通过"，并追加下面第 10、11 条。
10. `docs/数据权威路径清单.md` **L83**：现写 9,910 行 / 432 只 / 2026-11-09，实测已替换为 **9,927 行 / 438 只 / 1991-02-26~2026-11-11**（同 15 列、"生产读侧未接线"仍成立，不用改）。
11. 下游未同步（新缺陷）：`meta/universe/universe_exclusions.parquet` 的 `reason_code='bad_price_jump'` 为 **67 行 / 64 只**，与新 `universe_price_jumps` 的 `label='data_error'` **68 行 / 65 只**不一致——应移除 `159582.SZ` / `159941.SZ` / `516160.SH`，应补入 `000511.SZ` / `300372.SZ` / `600401.SH` / `600432.SH`。重建走 `tools/build_universe_exclusions.py`（默认连 DB + TDX），本轮未获授权、未动。
12. 缺登记（缺口登记，用户第三条指定）：`suspension_daily` 的 23 行 `bar_present_prev_quote_gap` 留档行**零写入口**——`src/`、`tools/`、`config/` 全域零命中，重跑 `build_suspension` 会静默丢弃；代码 docstring（`extended_builders.py:163-164`）与 `meta/suspension/coverage.json` 的 `caveats` 第 6 条仍写"只登记停牌行、`is_suspended` 恒为 True"。
13. 写点位置缺陷：`implied_suspension` 的唯一写点 `reports/audit_quotes_20261109/fix2_write_table.py` 位于 `reports/`（`.gitignore` 已锚定 `/reports/`，不入库），脚本直接 `to_parquet`（非 `write_parquet_atomic`）、遇目标已存在即拒绝执行，因此该表**没有可重放的常规入口**——G-113 需补的正是这条：不仅要登记写点在哪，还要登记"写点在 gitignored 目录内，仓库一旦换机即丢失"。

---

## 9. 附带发现（不属本片四表，但阻塞其他片，仅记录）

| 项 | 事实 |
|---|---|
| `_staging/derived_1993b_20261113/` | 11-12 21:xx~22:xx 正在做派生层重建：有 `failed.csv`（37 B），**无 `progress.csv` / `progress.json`**，按 2026-11-12 起的 `_staging` 断点硬条件属未收口状态；该目录同时是 `universe_price_jumps` 修正候选的来源 |
| `_staging/derived_1993b_20261113/layer2_apply.py:47` | 是 11-12 22:20 那次 `suspension_daily` 重写的实际写点（`lineage.json` 已登记为 oneoff），与常规入口 `build_suspension` 并存于同一张表 |
| `meta/universe/internal_gap_fix_progress.json` | 修复断点文件落在正式目录（DG-32），与 `rights_verify_progress.json` 同类 |
| `meta/st/raw/`（G-095 堵点） | **已由本任务补齐 `manifest.json`**（9 条 parquet 记录，`manifest_tool --check --only meta/st/raw` PASS）。实测结论：`cninfo_notices` ← `st_history.py:465`、`sz_change_name` ← `:430`、`org_ids` ← `:493`、`closure_evidence` ← `:615`、`tushare_namechange_intervals` ← `tools/build_st_history_from_tushare.py:68`；`reviewed_conflicts/coverage/pending` 与 `em_org_basic` **零写点**（纯读侧 `st_history.py:274-286`、`code_migration.py:434`、`st_name_history.py:157`）。故该目录登记为 `rebuildable=false`：3 件人工裁定不可再生、1 件外部导出无写点、5 件写点需外网。`name_history` 因此**只解开了"无 manifest"这一半**（G-079 退市段仍缺） |

---

## 10. 验收证据清单

| 检查 | 证据 | 结果 |
|---|---|---|
| 行数守恒（重算 vs 正式） | `b2s1_recompute.json` 的 `suspension/official/intersection` | 987,171 / 987,171 / 987,171 |
| 逐行对拍 | 同上 `only_official` / `only_recomputed` | implied 0 / 0，quarantine 0 / 0，suspension 0 / 0 |
| 受影响文件 SHA256 | §3 表（5 个路径，4 个存在） | 与 manifest 声明一致（存在者） |
| 抽样断言 | `implied_suspension.conflicts_with_existing_suspension` 全 True 且与 `suspension_daily` 交集为 0 | 一致 |
| 下游一致性 | `universe_price_jumps` 候选 vs 独立重算 | 9,927 / 9,927 / 0 / 0 |
| 替换后四项验收 | 见 §11 表（行数守恒 / 既有非零改动 / 空值 / 正式读路径） | 全部通过 |
| `manifest --check` | `--only meta/universe`、`--only meta/st/raw` | 均 PASS |

---

## 11. `universe_price_jumps` 替换记录（获批的落地动作，不属只读）

授权：上级会话 2026-11-13 指令，批准用 `_staging/derived_1993b_20261113/excl_candidate/universe_price_jumps.parquet` 替换正式表；成因认定为 **17 个行情文件在建表（2026-11-10 13:37:16）之后被改**（退市回补 16:26、`163503` 修正 19:18 那一批），属上游变动导致的陈旧，**不是口径改变**——因此没有触 DG-29 的"改变既定口径"停手条件。

**新旧对照**

| 项 | 替换前 | 替换后 |
|---|---|---|
| 正式路径 | `data/local_store/meta/universe/universe_price_jumps.parquet` | 同（路径不带版本号，DG-4） |
| 行数 | 9,910 | **9,927**（+23 补入 −6 移除 = +17） |
| 字节 | 154,965 | **155,773** |
| SHA256 | `adcfb57f245e13d2e76bc932edefae7ed08878ff7ec2169583c9de099a568963` | `000faf82646fb01d8da18e1ec5375f61e72e338dc1a260223e5c5a3400aa6acd` |
| 标的数 | 432 | 438 |
| 日期上限 | 2026-11-09 | 2026-11-11 |
| 列数 | 15 | 15（列名与顺序不变） |

**落盘与回滚**

- 回滚快照：`_backup/meta/universe_price_jumps_20261113/universe_price_jumps.parquet`（`copy2` 后复算 SHA256 与替换前逐字节一致：154,965 B / `adcfb57f…`）。`meta/` 层按 DG-13 口径落 `_backup/meta/<语义>_<日期>/`。
- 落盘方式：`src/data/atomic_write.py` 的 `write_parquet_atomic(df, LIVE)`（tmp → fsync → `os.replace`），未直接 `write_parquet` 覆盖。
- 顺序与失败残留（DG-10）：① parquet → ② `manifest.json` → ③ `universe_exclusions.json`。三件**不是一个事务**：若 ① 成功而 ② 失败，`manifest_tool --check --only meta/universe` 会立刻报 rows/bytes/sha256 不一致（可检出，不会静默）；回滚按 ③→②→① 反序，恢复 parquet 后 `--check` 复归 PASS。

**四项验收（落盘前后实测）**

| 验收项 | 实测 | 结论 |
|---|---|---|
| 行数守恒 | 9,910 + 23 − 6 = 9,927；新表与独立重算、与生产构建器 `build_universe_exclusions.py --no-db --no-tdx` 重算三者均 9,927 行、双向差集 0/0（且后两者产物**逐字节相同**：`000faf82…` / 155,773 B） | 通过 |
| 既有非空值改动数 | 9,904 个共有关键里：`row_pos` 5 行各 −2（`000012.SZ 1994-08-01`、`000020.SZ 2007-05-18`、`000502.SZ 2007-01-10`、`000502.SZ 2022-06-06`、`000506.SZ 2009-06-05`，因 5 只标的被删行导致文件内位置左移）；`150189.SZ 2015-07-10` 的 `factor_evidence` / `evidence` 2 格文本由 `qfq_factor 16.4733→1.17619` 改为 `26.1038→1.86381`（基金复权因子被上游修订）。其余 11 列（`label`/`pct_chg`/`close`/`pre_close`/`prev_date`/`gap_trading_days`/`factor_changed`/`has_dividend`/`split_evidence` 等）**0 改动** | 通过（7 格差异逐格归因，均上游变动连带） |
| 空值数 | 替换前 15 列合计 0 空值；替换后 0 空值（逐列 null 计数均为 0） | 通过 |
| 正式读路径端到端（DG-23） | `import tools.fill_internal_gaps as fig` 后读 `fig.JUMPS`：9,927 行、日期上限 2026-11-11（旧表为 9,910 / 2026-11-09） | 通过 |

**同步登记的另两处**

- `meta/universe/manifest.json`：该文件记录的 `rows` 9,927 / `bytes` 155,773 / `sha256` `000faf82…` / `date_range` `["1991-02-26","2026-11-11"]`，并加 `refreshed_at=2026-11-13T00:25:26` 与 `refreshed_reason`；`python tools/manifest_tool.py --check --only meta/universe` → **PASS（2 份 manifest / 10 条记录 / 不一致 0 / 无法定位 0）**。
- `meta/universe/universe_exclusions.json`：`price_jumps` 段整体换为同源块，`total_rows` 9,927 / `total_codes` 438，`by_label` 7 类合计 `132+68+9,490+36+17+29+155 = 9,927`（两处不一致即为新缺陷，已对齐）。

**23 行补入逐行归属**（新增行 = 新表 − 旧表，键 `(asset_class, ts_code, date)`）

| 归属 | 行数 | 逐行 |
|---|---|---|
| `index_rebalance`（11-10 / 11-11 新 bar） | 9 | `880005.SH`/`880006.SH`/`880015.SH`/`880016.SH`/`880035.SH`/`880036.SH`/`880045.SH`/`880046.SH` @2026-11-10；`880076.SH` @2026-11-11 |
| `pre_price_limit_era`（1996-12-16 前无涨跌幅） | 4 | `600656.SH` 1992-06-09 / 1993-06-21 / 1995-05-22；`000594.SZ` 1996-06-10 |
| `resumption_gap`（长期停牌复牌） | 3 | `000979.SZ` 2006-08-02；`600401.SH` 2012-02-17；`000693.SZ` 2014-01-10 |
| `data_error`（孤立跳变） | 4 | `000511.SZ` 2008-04-28；`600432.SH` 2008-05-06；`600401.SH` 2015-05-28；`300372.SZ` 2015-06-02 |
| `market_wide_event` | 2 | `600656.SH` 1992-12-10；`000033.SZ` 1995-05-19 |
| `corporate_action_missed` | 1 | `502006.SH` 2015-08-28 |

**6 行移除逐行归属**（旧表有、新表无，全部为 fund，均因 `pre_close` 被上游修正后不再构成 >50% 跳变）：`163503.SZ 2007-10-16`、`512690.SH 2021-08-23`、`159941.SZ 2022-07-05`、`516160.SH 2024-09-18`、`516160.SH 2026-06-10`、`159582.SZ 2026-07-13`。

**替换后新暴露的下游不一致（未修，待授权）**：`meta/universe/universe_exclusions.parquet` 的 `reason_code='bad_price_jump'` 仍是 67 行 / 64 只，与新表 `label='data_error'` 68 行 / 65 只不符——多 3 只（`159582.SZ`、`159941.SZ`、`516160.SH`）、少 4 只（`000511.SZ`、`300372.SZ`、`600401.SH`、`600432.SH`）。重建该表需 DB + TDX（`tools/build_universe_exclusions.py` 默认链路），本轮未获授权、未动。

---

## 12. `implied_suspension` 与 `quarantine_rows` 行集相同的定性（只出结论，不动数据）

**两条路径的基本面**

| 项 | `meta/suspension/implied_suspension.parquet` | `meta/anomalies/quarantine_rows.parquet` |
|---|---|---|
| 产者代码 | 无常规入口；唯一写点 `reports/audit_quotes_20261109/fix2_write_table.py`（`to_parquet`，目标已存在即退出） | 无常规入口；`tools/repair_quote_anomalies.py:255-270` 的 `_build_quarantine`，入参是**外部违规清单 CSV**（`--violations`） |
| 判据 | `volume IS NULL AND amount IS NULL AND open=high=low=close`（`reason` 列逐行写死三元组） | 违规清单里 `rule_id ∈ {volume_invalid, required_columns, price_non_positive}` 的行按 `(ts_code, trade_date)` 去重 |
| 语义 | 源端在停牌期仍写出的**占位 bar**：价格无意义、当日不可交易 | 源端**结构坏行**：`volume/amount` 为 null，不可编造也不该整行删除，改由读侧剔除 |
| 消费方 | `load_suspension_union`（`src/data/quality/suspension_union.py:83`）→ `clean_set.py:79/981`、`src/data/universe_filter.py:64/547`、`tools/build_universe_layers.py:50/528` | `drop_quarantined`（`src/data/quality/quarantine.py:122`）→ `src/data/managers/data_fetcher.py:314`（单票本地读）、`:548`（批量读）；另有 `quarantine_stats` 供诊断 |
| 字段 | 9 列：`ts_code`/`trade_date`(Date)/`asset_class`/`reason`/`ohlc_flat`/`in_existing_suspension`/`conflicts_with_existing_suspension`/`source`/`detected_at` | 3 列：`ts_code`/`trade_date`(**String**)/`rule_ids` |
| 实测规模 | 3,137 行 / 117 只 / 2019-12-12~2025-12-04 / mtime 2026-11-09 12:32:47 | 3,137 行 / 117 只 / 同区间 / mtime 2026-11-07 23:35:42 / `rule_ids` 全为 `required_columns,volume_invalid` |

**行集相同的成因**：不是构造保证，是两条判据在当前数据上**恰好等价**。`implied` 比 `quarantine` 多一个 `ohlc_flat` 必要条件；实测全库 3,137 个 `volume/amount` 双 null 的行**全部**同时满足四价相等，因此两集合重合、双向差集 0。二者产出于不同日期、不同输入（11-07 的违规清单 vs 11-09 的独立扫描），能对齐纯属数据形态所致——**只要将来出现一行"成交量缺失但四价不等"，两张表就会立刻分叉**。

**若是同一批物理行的两个视图 / 同语义两套路径** —— 判定为**前者**，理由三条：
1. 语义不同：一行是"不可交易（但行情真实存在）"，另一行是"数据不可用（应被剔除）"。前者进停牌并集做可交易性判定，后者进读侧做坏行剔除，作用轴不同，不能互替。
2. 消费方无重叠：`load_suspension_union` 的三个调用点都不读 `quarantine_rows`；`drop_quarantined` 的两个调用点都不读 `implied_suspension`。
3. 字段与类型不同（Date vs String 的 `trade_date`、3 列 vs 9 列），合并必然要做类型与语义折算。

因此**不构成 DG-1 违规**（DG-1 管的是同一语义两条路径），但**满足 DG-1 的精神要求：一处语义只能有一个可解释来源**——现状的风险在"两张表各自静默过期、没人校验它们还相等"。

**合并会不会丢信息**：会。`implied` 独有的 `reason`/`ohlc_flat`/`in_existing_suspension`/`conflicts_with_existing_suspension` 与 `source` 是停牌归因证据；`quarantine` 独有的 `rule_ids` 是坏行规则链。任一方向合并都会丢掉另一侧的判定依据，且会让"剔除"与"标记停牌"两种相反动作共用一个文件，读侧必须再按列分流——反而更难维护。

**保留两条路径是否需要写入口联动**：需要，且这是本轮唯一该补的动作。当前两条都是"一次性产物 + 无常规入口"，任一被重放都会单独改变行集：
- `implied` 重放需先删目标（脚本遇存在即退出），`quarantine` 重放需外部违规清单；
- 两侧都无"刷新后通知另一侧"的机制，也无任何断言守护相等关系。

**DG-1 结论：维持两条独立正式路径，不退役、不合并。** 理由：语义、消费方、字段三者都不同，退役任一条都会丢掉真实下游（停牌判定或坏行剔除），合并会把相反动作塞进同一文件。**但必须补一条联动校验**（建议落为单测/门禁，不动数据）：

```python
# 断言：两张表的键集必须相等（当前 3,137 行）；不相等即报错，禁止静默分叉
import polars as pl
im = pl.read_parquet('data/local_store/meta/suspension/implied_suspension.parquet')
q  = pl.read_parquet('data/local_store/meta/anomalies/quarantine_rows.parquet')
k = ['ts_code', 'trade_date']
ki = im.select(k).with_columns(pl.col('trade_date').cast(pl.String))
kq = q.select(k)
assert ki.unique().height == kq.unique().height
assert ki.join(kq, on=k, how='anti').height == 0 and kq.join(ki, on=k, how='anti').height == 0
```

**不做的后果**：任一侧重放后面临两套结论；若 `quarantine` 先更新而 `implied` 未更新，一批"已剔除的坏行"会继续以停牌身份进入 `universe_filter` / `build_universe_layers` 的可交易性判定，反之则会让本该剔除的坏行重新进入行情读侧——两种都不会报错，只会让回测样本静默偏移。

**可执行替代实测（本轮已做）**：
- `quarantine_rows` 可由留存违规清单复现：`reports/data_audit_2026-11-07/data_quality_local_20261107_235720_052091_violations.csv` 等 6 份清单经同一 `_build_quarantine` 逻辑得到 3,137 行，与正式表 `equals()` **完全相等**（22:43 起的 6 份均为 3,137 行；22:41 及更早的 6 份为 3,138 行，多 1 行属当日修复前状态）。
- `implied_suspension` 可由 `reports/audit_quotes_20261109/implied_suspension_candidate.parquet`（3,137 行）核对：键集与正式表完全相同；但该写点脚本拒绝覆盖已存在文件，**不具备无人工介入的重放能力**。

---

## 13. 收口声明

- §0~§10：只读体检，未改代码、未写 `data/`、未改 parquet 与 manifest。
- §11：经上级会话批准的一次正式表替换，改前有 `_backup` 快照、改走 `write_parquet_atomic`、四项验收通过；同目录 `manifest.json` 与 `universe_exclusions.json` 同步。
- `meta/st/raw/manifest.json`：本任务新建（该目录原无 manifest），已 PASS。
- 未改动 `docs/数据权威路径清单.md` 与 `docs/数据缺口登记_正式表_2026-11-12.md`，登记类缺陷逐条列在 §8（共 13 条）。
- 未改动 `implied_suspension` / `quarantine_rows` 任何数据（§12 仅结论）。
- 未提交 Git：本次改动路径全部落在 `.gitignore` 覆盖范围内（`data/local_store/`、`_backup/`、`2026-11-12/`），源仓无可提交的跟踪文件；镜像另行执行。
- §14：`universe_exclusions` 重建**未落盘**——触发"`bad_price_jump` 之外任何段变化即停手"的条件，正式表与 manifest 未改，待上级在 A/B/C 中选定。
- 代码侧一件已落地：`suspension_union.verify_quarantine_alignment()` + 3 个用例（键集不等必须失败），`pytest tests/test_suspension_union.py` 8 passed、`ruff` 通过。

---

## 14. `universe_exclusions` 重建：链路可用但**未落盘**（触发停手条件）

上级会话批准"按 3 移除 / 4 补入重建同步"，并附停手条件：**若 `bad_price_jump` 之外任何段行集变化，先停手回报**。实测触发该条件，已停手，未写正式表。

**链路可用性（先测后跑）**

| 项 | 实测 | 结论 |
|---|---|---|
| DB（MySQL） | `stock_basic` 5,813 / `index_basic` 1,333 / `fund_basic` 2,618 均查通 | 可用 |
| TDX 本地目录 | `sh/lday` 5,131 个 `.day`、`sz/lday` 4,896 个、`bj/lday` 361 个 | 可用 |
| 全链重跑 | `build_universe_exclusions.py --output _staging/universe_exclusions_rebuild_20261113` 正常完成 | 通过 |

**重跑结果与现表的差异（`(asset_class, ts_code)` 去重后按 `reason_code` 计）**

| reason_code | 现表 | 重建 | 仅现表有 | 仅重建有 |
|---|---|---|---|---|
| `bad_price_jump` | 64 | **65** | **3** | **4** |
| `internal_gap` | 149 | 167 | 0 | 18 |
| `no_basic_info` | 6 | 23 | 0 | 17 |
| `placeholder_price` | 95 | 96 | 6 | 7 |
| `stale_feed` | 665 | 696 | 8 | 39 |
| `tdx_truncated` | 424 | 719 | 0 | **295** |

- 合计：现表 1,474 行 → 重建 1,837 行（+363）；快照日 `generated_as_of` 由 **2026-11-09** 前进到 **2026-11-11**，覆盖代码数 9,770 → 9,787。
- 只有 `bad_price_jump` 的差异恰好是批准过的 3 / 4；其余 5 段全部变动（`tdx_truncated` 一项就 +295，与 11-12 TDX 目录刷新、退市回补一致；`stale_feed` 随快照日前移重判；`no_basic_info` 反映 DB 基础表现状）。
- **因此这不是"构建器多改一段"，而是整表按今天的上游整体前进了一版**——照此全量落盘会连带改变 `strict` 可交易池（现表 1,474 → 1,837 只被排除），远超"只同步 `bad_price_jump`"的授权范围。

**当前状态**：正式表与 manifest **一字未改**；重建产物只留在 `_staging/universe_exclusions_rebuild_20261113/`（按 DG-5）。旧表已按授权备份到 `_backup/meta/universe_exclusions_20261113/universe_exclusions.parquet`（1,474 行 / 与现表逐字节相同），该快照无论走哪条路线都成立。

**三条可选路线（需上级选定）**

| 方案 | 做法 | 结果 | 风险 |
|---|---|---|---|
| A（推荐） | 段级局部替换：保留现表其余 5 段原样，只把 `bad_price_jump` 的 67 行换成重建的 68 行（差异恰为 3 移除 / 4 补入） | 1,474 → 1,475 行，仅该段变动，完全满足原验收口径 | 同表内两版快照共存（其余段仍是 11-09 口径），需在 manifest 记明分段口径与 `detected_at` |
| B | 全表刷新到 2026-11-11（1,837 行） | 六段全部为当前口径，与跳变表同时点 | +363 行连带改变可交易池，需重新验收 `strict` 名单与回测样本，等于一次口径前进 |
| C | 只登记不改表 | 保留现表，把 5 段陈旧与 3/4 差异记为缺口 | `bad_price_jump` 与新跳变表继续不一致 |



---

## 15. B 路线归因门结论：`internal_gap` 段 ② 占 100%，该段不得落盘

上级选定 B（全表刷新到 2026-11-11）并加归因门：五段新增标的逐个判定 ① 上游变 / ② 本地 TDX 缺数误判（不得落盘）/ ③ 越域，② 超过 5% 的段维持旧值。以下为实测结论，**正式表未动**。

**逐段归因（标的级，`(asset_class, ts_code)` 段内新增口径）**

| reason_code | 新增 | ① 上游 | ② TDX 侧误判 | ③ 越域 | ② 占比 | 移除 | 判定 |
|---|---|---|---|---|---|---|---|
| `bad_price_jump` | 4 | 4 | 0 | 0 | 0.00% | 3 | 可落盘 |
| `internal_gap` | 18 | 0 | **18** | 0 | **100.00%** | 0 | **不落盘** |
| `no_basic_info` | 17 | 17 | 0 | 0 | 0.00% | 0 | 可落盘 |
| `placeholder_price` | 7 | 7 | 0 | 0 | 0.00% | 6 | 可落盘 |
| `stale_feed` | 39 | 39 | 0 | 0 | 0.00% | 8 | 可落盘 |
| `tdx_truncated` | 295 | 295 | 0 | 0 | 0.00% | 0 | 可落盘 |

证据脚本：`work/b2s1_excl_attribution.py`（产出 `_staging/universe_exclusions_rebuild_20261113/attribution.json`，含每标的 `.day` 是否存在、行数、首末日、mtime、与行情末日差几个交易日、`list_date` / `list_status`、实际缺口日）；补充核验 `work/b2s1_attr_evidence{2,3}.py`、`work/b2s1_attr_weekend.py`、`work/b2s1_attr_hold_check.py`。

**`tdx_truncated` 295 行逐类交代**

| 子类 | 行数 | 证据 |
|---|---|---|
| fund（TDX 对已退市品种停更） | 294 | `.day` 全部存在；末根集中 2019-08-20~2019-09-17（2019-09-17 占 226 只、09-16 21 只、09-12 16 只、09-11 7 只、09-10 8 只、09-09 5 只，其余 11 只更早）；本地行情回补到 2020—2022 的退市日；本地行情文件 mtime 集中 2026-11-13（236 只）/ 2026-11-10（57 只）→ **上游行情回补传导，①** |
| stock `000001.SZ` | 1 | 本地 8,511 行 / TDX 8,489 行，首根同为 1991-04-03；TDX 缺 22 根中间 bar（其中 2 根是 1993-05-01/02 幻影 bar，本地另多 24 根真实 bar）→ **TDX 源历史确实短，①** |

- **纠错（前一版归因作废）**：曾把 294 只 fund 判为 ③「TDX 对账只覆盖 stock/index」。实测旧表 `tdx_truncated` 本就含 **181 只 fund**（`asset_class` 构成：fund 181 / index 14 / stock 229），fund 在该段域内，判 ③ 与旧表口径自相矛盾。已按标的级证据改判 ①。
- 本段新增与旧表 fund 共存后共 475 只 fund，属同一成因（TDX 侧停更 + 本地退市回补），非规则边界问题。

**`internal_gap` 18 只：全部 ②，逐只可复现**

| 项 | 实测 |
|---|---|
| 缺口日 | 17 只缺 `1993-05-01`（周六）/`1993-05-02`（周日）；`000002.SZ` 缺 `1992-04-18`（周六） |
| 权威日历 | `get_calendar(STORE).authoritative_dates()` 自 1990-12-19 起，**不含**这三个日期 |
| 本地侧 | 全库无任何标的在这三天有 bar（本地 1993-04-20~05-12 仅工作日 bar） |
| TDX 侧 | 17 只 stock 的 `.day` 写了 1993-05-01/02 两根 bar，同时漏掉 1993-05-03（周一）、05-06（周四）。以 `000001.SZ` 为例：本地 `04-30, 05-03, 05-04, 05-05, 05-06, 05-07`；TDX `04-30, 05-01, 05-02, 05-04, 05-05, 05-07` → TDX 早期日期错位 |
| 代码根因 | `tradable_universe.py` L475 `missing = np.setdiff1d(tdx[tdx <= local_end], local)` 未按交易日历过滤，非交易日 bar 被计入「本地缺口」 |
| 下游影响 | `internal_gap` ∈ `_STRICT_WARNINGS`，`is_tradable(strict=True)` 会剔除。落盘将把 **000001.SZ（平安银行）、000002.SZ（万科A）** 等 18 只主板大票移出 strict 可交易池 |

→ ② 占比 **100% ≫ 5%**，按上级设定 `internal_gap` 段**维持旧值不落盘**。

**其余四段 ① 明细**

| 段 | 新增标的与判定依据 |
|---|---|
| `bad_price_jump` 4 | `universe_price_jumps` 重建后新增的 4 个 `data_error` 键：000511.SZ −51.57%、300372.SZ −50.39%、600401.SH −65.03%、600432.SH −51.81%（行情文件修正传导） |
| `no_basic_info` 17 | 688827.SH（11-11 有行情的次新，`stock_basic` 未入）/ 000922.SH、000985.SH（`index_basic` 无）/ 14 只已退市股（DB 基础表已清理） |
| `placeholder_price` 7 | 159089.SZ、159100.SZ、166105.SZ（11-11 仍在更新但收盘恒 1.0）+ 150190.SZ、150282.SZ、502004.SH、502007.SH（2020 年退市 fund 尾部占位） |
| `stale_feed` 39 | 本地末根 11-09/11-10 < 新快照日 11-11（时点前进 2 个交易日），判定成立；含 fund 13 / index 12 / stock 14 |

**落盘口径（若按上级设定走 ②>5% 分支）**

- `internal_gap` 段保留旧表 217 行（全 index，`detected_at=2026-11-09`）；其余五段用重建值。
- 行数：1,837 − 235（新 `internal_gap` 含 18 只 stock）+ 217 = **1,819 行**。
- 已知代价：旧 217 行与新 217 行有 **165 行** `expected_rows`/`actual_rows` 各 +2（日历窗口前进 2 个交易日），`missing_rows` 全不变；保留旧值会让这 165 行停在 11-09 窗口，manifest 须记明。

**两条落地路线（待上级选定）**

| 路线 | 做法 | 结果 | 代价 |
|---|---|---|---|
| B1（严格按分支） | 分段落盘 1,819 行，`internal_gap` 保持 11-09 旧值 | 与上级设定一致，无新代码改动 | 表内混两个 `detected_at`；165 行 `expected/actual` 停在旧窗口；`_tdx_rows` 缺陷继续存在，下次重建复发 |
| B2（根因优先，推荐） | 先修 `_tdx_rows`：`tdx = tdx[np.isin(tdx, calendar_days)]` 后再算 `missing`，加单测；重跑后 `internal_gap` 自然回到 217 行 index，全表按 11-11 落盘 | 1,819 行，`detected_at` 统一 11-11，无「段未刷新」残留，缺陷从根上消除 | 改共享模块 `tradable_universe.py`，`internal_gap` 判定收紧（非交易日 bar 不计缺口），属口径收紧需上级确认 |

**上游仍在变动（落盘时点须记账）**：`quotes/fund/**` 有 242 个文件 mtime = 2026-11-13（今日）、57 个 = 2026-11-10，另有任务在回补 fund 历史；`tdx_truncated` 的 294 只 fund 数字是 2026-11-13 00:43 的快照值，后续回补会继续变。`universe_price_jumps` 本次未动。

---

## §16 B2 落盘：根因修复 + 全表刷新到 2026-11-11

上级选定 **B2（根因优先）**：先修 `_tdx_rows` 的非交易日 bar 误判，再整表按 11-11 落盘。本节按「代码改动 → 单测与变异验证 → 落盘 → DG-20 四项 → 端到端 → 口径变更」顺序留证。

### 16.1 代码改动（唯一一处）

`src/data/quality/tradable_universe.py` L464-L483，`_tdx_rows` 内新增交易日历过滤：

```python
calendar = _benchmark_calendar(store_root)
...
tdx_upto = tdx[tdx <= local_end]
if calendar is not None and calendar.size:
    tdx_upto = tdx_upto[np.isin(tdx_upto, calendar)]
missing = np.setdiff1d(tdx_upto, local)
```

- 只影响 `internal_gap` 的 `missing`；`tdx.size < local.size`（`tdx_truncated` 判据）与 `local_end` 均未改，故 `tdx_truncated` 计数不受本次改动影响。
- 日历来源仍是既有的 `_benchmark_calendar(store_root)`（`meta/daily_index.parquet` 的 `date` 列，权威段 1990-12-19 ~ 2026-11-11），未引入新数据源、未新增路径常量。

### 16.2 单测与变异验证

`tests/test_tradable_universe.py` 追加 2 个用例 + 3 个 helper（`_write_tdx_day` / `_patch_calendar` / `_tdx_store`）：

| 用例 | 断言 | 作用 |
|---|---|---|
| `test_tdx_bars_on_non_trading_days_are_not_internal_gap` | 周末 bar 插在**本地区间内部**（1/5~1/9 + 1/10 六、1/11 日 + 1/12 一）时，`internal_gap` 行数 = 0，且 `is_tradable(strict=True)` 为 True | 证明过滤生效 |
| `test_tdx_missing_real_trading_day_still_flagged` | 本地缺 1/7、TDX 有全部 6 个交易日时，`internal_gap` = 1 行、`missing_rows=1`、`expected_rows=6`、`last_trade_date=2026-01-12` | 证明过滤没把真缺口一起吞掉 |

**变异验证（已实测）**：把 L482 的 `tdx_upto = tdx_upto[np.isin(tdx_upto, calendar)]` 换成 `pass`（等价去掉过滤），`test_tdx_bars_on_non_trading_days_are_not_internal_gap` **变红**——`assert gaps.height == 0` 实际得到 1 行（两天的周末 bar 被算成 1 条聚合缺口行）；恢复该行后 `pytest tests/test_tradable_universe.py tests/test_suspension_union.py -q` = **27 passed**，`ruff check` = `All checks passed!`。

> 首版用例曾把周末 bar 放在 `local_end` 之后，被 `tdx <= local_end` 天然排除，去过滤也仍绿——等于没测到；已改为插在区间内部。

### 16.3 重跑结果（staging）

```
.venv\Scripts\python.exe tools\build_universe_exclusions.py --output _staging\universe_exclusions_b2_20261113
→ 排除清单已生成: 1819 行 / 996 只标的，strict 可交易 9012/9787 只
```

| 段 | 旧表（11-09） | 新表（11-11） | 净增 |
|---|---|---|---|
| `bad_price_jump` | 67 | 68 | +1 |
| `internal_gap` | 217 | **217（全 index）** | 0 |
| `no_basic_info` | 6 | 23 | +17 |
| `placeholder_price` | 95 | 96 | +1 |
| `stale_feed` | 665 | 696 | +31 |
| `tdx_truncated` | 424 | 719 | +295 |
| 合计 | 1,474 | **1,819** | +345 |

`detected_at` 全表唯一 = `2026-11-11`。

### 16.4 落盘（DG-11 / DG-13 / DG-9）

脚本：`2026-11-12/data-lineage/work/b2s1_excl_land.py`（`QUANT_CHANGELOG_TASK=b2s1_universe_exclusions`）。

| 步骤 | 实测 |
|---|---|
| 快照目录 | `_backup/meta/universe/universe_exclusions_20261113/`（先断言目标不存在，拒绝覆盖） |
| `.parquet` 移动 | `f152841c777c735c…` / 23,246 B → 移动后 `f152841c777c735c…` / 23,246 B，**SHA256 一致 = True** |
| `.json` 移动 | `16dfc83281a0081b…` / 16,487 B → 移动后 `16dfc83281a0081b…` / 16,487 B，**SHA256 一致 = True** |
| 台账 | 两个文件各记一次 `record_move`（拆 delete + create 两行，`src_sha256`/`src_size_bytes` 显式传移动前实测值） |
| 替换 | `write_parquet_atomic(1819 行)` + `write_text_atomic(json)`，均为临时文件 → fsync → `os.replace` |
| manifest | `rows=1819 / bytes=26891 / sha256=e9c8c9dd… / date_range=['2005-02-24','2026-11-11'] / refreshed_at=2026-11-13T00:54:16`，并记 `refreshed_reason` 与 `snapshot_backup` |

`tools/manifest_tool.py --check --only meta/universe` → **PASS**：manifest 2 份 / 文件记录 10 条 / 不一致 0 / sha256 格式告警 0 / 无法定位 0 / 无 manifest 0。

### 16.5 DG-20 四项验收（旧表 = 快照，新表 = 正式路径）

**① 行数守恒（逐段账，多重集口径）**

| reason | 旧行 | 新行 | 净增 | 完全相同 | 旧独有 | 新独有 |
|---|---|---|---|---|---|---|
| bad_price_jump | 67 | 68 | +1 | 64 | 3 | 4 |
| internal_gap | 217 | 217 | 0 | 52 | 165 | 165 |
| no_basic_info | 6 | 23 | +17 | 6 | 0 | 17 |
| placeholder_price | 95 | 96 | +1 | 29 | 66 | 67 |
| stale_feed | 665 | 696 | +31 | 0 | 665 | 696 |
| tdx_truncated | 424 | 719 | +295 | 279 | 145 | 440 |
| **合计** | **1,474** | **1,819** | **+345** | **430** | **1,044** | **1,389** |

守恒自证：`430 + 1,044 = 1,474`（旧），`430 + 1,389 = 1,819`（新），两侧均精确闭合。

**② 逐值对拍（严格配对：该键两侧各仅 1 行的才进入比对）**

| 段 | 配对行 | `severity` 改动 | **非空 → 空（全列）** |
|---|---|---|---|
| bad_price_jump | 58 | 0 | 0 / 0 / 0 / 0 / 0 / 0 |
| internal_gap | 128 | 0 | 0 / 0 / 0 / 0 / 0 / 0 |
| no_basic_info | 6 | 0 | 0 / 0 / 0 / 0 / 0 / 0 |
| placeholder_price | 89 | 0 | 0 / 0 / 0 / 0 / 0 / 0 |
| stale_feed | 657 | 0 | 0 / 0 / 0 / 0 / 0 / 0 |
| tdx_truncated | 424 | 0 | 0 / 0 / 0 / 0 / 0 / 0 |

- **既有非空值改成空 = 0**（六段 × 六列全 0），`severity` 改动 = 0。
- 发生改动的值全部可归因到时点前进 2 个交易日 / 上游行情更新，逐条对应：
  - `internal_gap` 125 行：`expected_rows`/`actual_rows` 各 +2（日历窗口 11-09 → 11-11），`missing_rows` **0 改动**；
  - `placeholder_price` 60 行 `expected_rows`/`missing_rows` 与 5 行 `reason_text`：文案内嵌尾部行数（实例 160603.SZ「尾部 1589 行」→「1591 行」）；
  - `stale_feed` 297 行 `last_trade_date` + 631/301/653 行计数类：上游 `quotes/**` 在 11-10/11-13 有更新；
  - `tdx_truncated` 143 行 `last_trade_date`、145 行 `expected_rows`：同步随本地 end 前移。
- **空值数**（DG-31 要求的空值计数）：`no_basic_info.last_trade_date` 6 → 23（新增 17 行本就无日期）；`placeholder_price.last_trade_date` 84 → 83；其余段无空值变化，没有「非空被写成 null」的行。

**③ 受影响文件 SHA256**

| 文件 | 字节 | SHA256 |
|---|---|---|
| `data/local_store/meta/universe/universe_exclusions.parquet` | 26,891 | `e9c8c9dd4fa1a62e05e8408bcecfa550501b1d7304ce507708b794894e4b0c70` |
| `data/local_store/meta/universe/universe_exclusions.json` | 19,359 | `c7d9773ca892b1e6d55df5d62c858ab080253a82f8c94aad04b82759135333f2` |
| `data/local_store/meta/universe/manifest.json` | 5,016 | `a1a92e982ef548d07a7582dd24cb7f9ae53e9794fe923be340ae1591bfa33ae5` |
| `_backup/meta/universe/universe_exclusions_20261113/universe_exclusions.parquet` | 23,246 | `f152841c777c735c8f23c5dfa01ef793363d6fd7a217b5d9b8366f25489d8291`（= 改前正式路径值） |
| `_backup/meta/universe/universe_exclusions_20261113/universe_exclusions.json` | 16,487 | `16dfc83281a0081b39bef98ec6b03311d407912e6af050c3b5713a2f4241d340`（= 改前正式路径值） |

**④ 抽样断言**（脚本内 `assert`，非人工目测）

- `detected_at` 唯一且 = `2026-11-11`；
- `internal_gap` 217 行、`asset_class` 唯一值 = `['index']`；
- `bad_price_jump` 68 行（与 `universe_price_jumps` 的 `data_error` 逐键 0 差异，见 §15 与本轮 verify）；
- 新表列集合与旧表完全一致（10 列，顺序一致）；
- `000001.SZ` / `000002.SZ` 的 `internal_gap` 行数 = **0**（旧代码同口径下各 1 行）。

复现：`.venv\Scripts\python.exe "2026-11-12\data-lineage\work\b2s1_excl_verify3.py"`。

### 16.6 端到端（DG-23）：真实读路径确实读到新文件

用消费方真实入口（不带 `root` 参数，走 `DEFAULT_ROOT`）：

```
load_exclusions() -> 1819 行 / 996 只，detected_at=[2026-11-11]
load_exclusions(_backup/.../universe_exclusions_20261113) -> 1474 行 / 974 只（改前基线）
is_tradable('000001.SZ', 2026-11-11, strict=True) = True
is_tradable('000002.SZ', 2026-11-11, strict=True) = True
is_tradable('881101.SH', 2026-11-11, strict=True) = True   # 881 自编指数 internal_gap 豁免
internal_gap 149 只全部为 880/881 自编指数，strict 全豁免
```

严格口径剔除集（以 `meta/daily_index.parquet` 的 9,787 只全集逐只调用 `is_tradable(strict=True, as_of=2026-11-11)`）：**729 只 → 761 只**，新进 44 / 移出 12。

- 新进 44 只的命中原因（逐只实测，合计 44）：`stale_feed` 39 只（其中 8 只同时带 `tdx_truncated`、15 只同时带 `no_basic_info`：000033.SZ / 000511.SZ / 000594.SZ / 000693.SZ / 000922.SH / 000979.SZ / 000985.SH / 002070.SZ / 300372.SZ / 600401.SH / 600432.SH / 600656.SH / 600680.SH / 600806.SH / 601268.SH）、`placeholder_price` 3 只（159089.SZ / 159100.SZ / 166105.SZ，末位兼有 `tdx_truncated`）、`no_basic_info` 单独命中 2 只（561970.SH / 688827.SH）；
- 移出 12 只：`stale_feed` 不再成立 8 只（002623.SZ、302132.SZ、501028.SH、501047.SH、501202.SH、501218.SH、880641.SH、880665.SH）、`placeholder_price` 不再成立 4 只（158010.SZ、159662.SZ、515920.SH、516530.SH）；
- 这些进出全部来自 §15 归因门判为 ① 的四个段，`internal_gap` 段本身进出 0（880 系列因 `internal_gap` 豁免，其进出均由 `stale_feed` 决定）。

### 16.7 口径变更专段：`internal_gap` 收紧的改前 / 改后

**变更点**：TDX `.day` 写在非交易日的 bar 不再计入「本地缺口」。改动只作用于 `internal_gap` 的 `missing` 集合。

| 维度 | 改前（B 路线，未修代码，1,837 行版本） | 改后（B2，1,819 行） |
|---|---|---|
| `internal_gap` 行数 | 235 | 217 |
| `internal_gap` 标的数 | 167 | 149 |
| `asset_class` 构成 | index 217 + **stock 18** | index 217（全为 880/881） |
| 该段是否影响 strict 可交易池 | **影响**：18 只 stock 会被 `_STRICT_WARNINGS` 剔除 | 不影响：880/881 全部按 `_is_tdx_index_exempt` 豁免 |
| 整表行数 | 1,837 | 1,819 |

**进出的 18 只标的（全部为深市主板 stock）**：`000001.SZ`、`000002.SZ`、`000006.SZ`、`000007.SZ`、`000008.SZ`、`000011.SZ`、`000012.SZ`、`000014.SZ`、`000016.SZ`、`000018.SZ`、`000019.SZ`、`000020.SZ`、`000501.SZ`、`000502.SZ`、`000503.SZ`、`000504.SZ`、`000506.SZ`、`000507.SZ`。

**依据的日历证据**（逐条可复现）：

1. 缺口日：17 只缺 `1993-05-01`（**周六**）/ `1993-05-02`（**周日**），`000002.SZ` 缺 `1992-04-18`（**周六**）；
2. `get_calendar(STORE).authoritative_dates()` 权威段自 1990-12-19 起，**不含**这三天；
3. 本地侧：9,787 只标的在这三天**零 bar**（全库无任何周末 bar）；
4. TDX 侧：`000001.SZ.day` 在 1993-05-01/02 写了 bar，同时漏掉 `1993-05-03`（周一）、`1993-05-06`（周四）——本地 `04-30,05-03,05-04,05-05,05-06,05-07` vs TDX `04-30,05-01,05-02,05-04,05-05,05-07`，属源端早期日期错位；
5. 因此这 18 行的「缺口」是 TDX 侧错误 bar 造成的假缺口，本地没有丢数据。

**影响面结论**：修复避免了一次真实的口径回退——若直接按 B 落盘，`000001.SZ`（平安银行）、`000002.SZ`（万科A）等 18 只主板大票会进入 `internal_gap` 并被 `strict=True` 剔除。落盘后实测这 18 只在严格口径下均可用（§16.6）。相对旧 11-09 表，`internal_gap` 段键集**零进出**（149 只 → 149 只），故本次落盘没有改变任何标的的 `internal_gap` 归属，改动只是阻止了新增误判。

### 16.8 更正记录保留（294 只 fund）

§15 的纠错段保留：前一版把 `tdx_truncated` 的 294 只 fund 判为 ③「TDX 对账只覆盖 stock/index」是错的——旧表 `tdx_truncated` 本就含 181 只 fund，fund 在该段域内；已按标的级证据改判 ①。本轮落盘实测该段新进 295 只（fund 294 + stock 1），构成与 ① 判定一致。

### 16.9 台账与遗留

- 自动记账：`logs/data_changelog.csv` 本任务共 7 行（00:54:15~00:54:16）——`record_move` 两个文件各拆 delete + create 共 4 行（`universe_exclusions.parquet` / `.json`，sha256 与快照一致）；`write_parquet_atomic` / `write_text_atomic` 自动记 create + create + update 共 3 行（新 parquet 26,891 B `e9c8c9dd…`、新 json 19,359 B `c7d9773c…`、manifest `f268c06a…` → `a1a92e98…`）。
- `universe_price_jumps` 本次未动（文件 mtime 仍为 2026-11-13 00:25:26，`rows=9927`）。
- **既有的、与本任务无关的台账缺陷**：`logs/data_changelog.csv` 第 10365 行（`b2s2_breadth_rebuild`，2026-11-13T00:27:21）的 `reason` 含未加引号的逗号（`6,819 天`），CSV 解析为 11 列，导致 `tools/check_changelog.py` 本轮 FAIL（`1 行列数不符`）。该行不由本任务产生，未修改（DG-24），留给 b2s2 侧修正。
