# B4-c · 排除「未股改 S 股」制度来源（落地记录）

- 依据裁定：[b4b_st_price_inferred_verdict_20261113.md](b4b_st_price_inferred_verdict_20261113.md) §8 第 1 条（K / 可补）
- 改动单元：`src/data/st_price_infer.py`、`tests/test_st_price_infer.py`、`data/local_store/meta/st/{st_price_inferred.parquet, price_inference.json, manifest.json}`
- 未改动：窗口算法参数（`WINDOW=120 / CAP_TOL=0.056 / TOUCH_MIN=0.045 / TOUCH_NEED=10 / AMP_MIN=0.02 / VALID_MIN=0.80 / MIN_RUN=40` 全部原值）、`meta/name_history.parquet`、`st_intervals.parquet`、缺口登记表、`docs/数据权威路径清单.md`
- 复现脚本（均只读）：`work/b4c_s_scope_probe.py`、`work/b4c_s_share_verify.py`（含 `--self-test`）、`work/b4c_s_share_e2e.py`、`work/b4c_s_share_source.py`、`work/b4c_s_share_sz_crosscheck.py`

---

## 1. 改了什么

| 项 | 内容 |
|---|---|
| 新增判定 | `is_s_share_name()`：`S+` 判为未股改 S 股；`ST` / `*ST` / `SST` / `S*ST` 一律归风险警示，**不作** S 证据（`SST` 同时含两个标记，优先归 ST） |
| 新增区间表 | `load_s_share_intervals()`：主源 `meta/name_history.parquet` 的 S 行给精确区间；补源东财 `em_org_basic.former_names` 在 `name_history` 漏记时取「前后同名的唯一空档」。**现算不落独立文件**——新建正式数据路径会触发 DG-3 登记，本轮不扩路径 |
| 新增字段 | 产物加 `excluded_s_share: bool`；`flag` 仍是 `suspected_st`，**不覆盖**原标记 |
| 判定口径 | 只做「完全落袋」：段首尾都在同一 S 区间内才排除。跨边界段价格证据混源、无法归因，保留疑似并计 `s_partial_overlap_intervals` |
| 兜底 | `_apply_s_exclusion` 结束时校验 `excluded_s_share` 空值数，非 0 直接 `RuntimeError`；未命中任何分支不允许落成 null（DG-31） |
| 消费方 | `_annotate` / `_early_fill` / `suspected_status_at` / `suspected_intervals_for_backtest` / `samples` 全部按 `excluded_s_share=False` 过滤；`confidence` 仍对**全部**行计算，保证既有列逐值不变 |
| 摘要 | `price_inference.json` 新增 `s_share` 块（区间表行数/代码数/各源命中/无法定日期清单/29 段明细），`metrics` 新增 `excluded_s_share_intervals` |

**S 简称区间表规模**：170 段 / 166 只（`name_history` 134 段 + 东财空洞补出 36 段）；7 只标的有 S 名但 `name_history` 既无 S 行也无唯一空洞，**不猜**，记入 `s_share.unresolved_codes`（`000009.SZ`、`000430.SZ`、`000776.SZ`、`000783.SZ`、`000799.SZ`、`000906.SZ`、`600290.SH`）。

## 2. 验收结果

**被排除 29 段，不是 11 段**。全表「完全落在 S 区间内」的疑似段共 29 段，其中 B4-b 裁定的验收样本 11 段全部命中，另外 18 段同源：

| 组 | 段数 | 明细 |
|---|---|---|
| 验收样本 | 11 | 600133 / 600135 / 600211 / 600379 / 600643 / 600664 / 600682 / 600688×3 / 600865 |
| 同源追加（深市 9） | 9 | `000026` `000068` `000521` `000637` `000838` `000908` `000923` `000975` `000999`，段首简称经深交所原始件 `sz_change_name.parquet` 逐一独立确认是 `S飞亚达A` / `S 三  星` / `S 美  菱` / `S 茂实华` / `S 蓝石化` / `S 天一科` / `S 宣  工` / `S 科学城` / `S 三  九`（9/9） |
| 同源追加（沪市 4） | 8 | `600182` ×5（`S佳通`，2006-10-09~2021-04-25 与 2023-05-17~今两段）、`600733`（`S前锋`）、`600871` ×2（`S仪化`）、`600898`（`S商社`），均来自 `name_history` 的 S 行 |

29 段**全部** `confidence=low`；与确证 `st_intervals` 的重叠数为 **0**（不存在「S 与 ST 同时成立而误排除」）。

汇总变化（改造前 → 改造后）：

| 指标 | 前 | 后 |
|---|---|---|
| 产物行数 | 1,443 | **1,443**（守恒） |
| `excluded_s_share` True | — | **29** |
| `metrics.suspected_intervals` | 1,443 | **1,414** |
| `metrics.suspected_only_intervals` | 67 | **38**（-29，与排除段数一致） |
| `metrics.both_intervals` | 1,329 | 1,329 |
| `metrics.suspected_day_precision` | 0.8865 | **0.9012** |
| `metrics.validated_day_precision` | 0.9215 | **0.9374** |
| `metrics.confirmed_recall` | 0.7674 | 0.7674（未降） |

## 3. DG-20 四项举证

**① 行数守恒**：`1,443`（改造前 `_backup/meta/st_price_inferred_20261113_r2/st_price_inferred.parquet`，1,443）→ 改造后 `1,443`；标的数 831 → 831；`start_date` 1998-04-10~2026-04-08、`end_date` 1999-03-24~2026-09-14 均未变。

**② 逐值对拍**：按 `(ts_code, start_date)` 内连接 1,443 行，逐列比对

| 列 | 值改动数 | 空值数（前 → 后） |
|---|---|---|
| `end_date` | **0** | 0 → 0 |
| `flag` | **0** | 0 → 0 |
| `source` | **0** | 0 → 0 |
| `confidence` | **0** | 0 → 0 |
| `evidence` | **0** | 0 → 0 |
| `excluded_s_share`（新列） | — | 新增，空值 **0** |

**既有非空值改动数为 0**。`fetched_at` 是每次重建必然刷新的运行时字段，按方法论备忘（含运行时字段的表 SHA 不能证内容等价）单独排除在逐值对拍之外，其变化不表示内容改动。

**③ SHA256**：

| 文件 | 字节 | SHA256 |
|---|---|---|
| `st_price_inferred.parquet`（前，已移入 `_backup/meta/st_price_inferred_20261113_r2/`） | 64,928 | `61495F74735DF2C53DA1B345021EE3CAC25F2FD57555B53CA23DE9275AF54237` |
| `st_price_inferred.parquet`（后） | 65,309 | `E3BC4CD497239C6CA2014B11381112F9DB7916DBAF778D1DF44B4071D80BCCBA` |
| `price_inference.json`（后） | 19,974 | `1C5439A26F4BC6A85C0C53DA5BEEB580304CD559DF5800C56725139A674322BD` |
| `manifest.json`（后） | 4,157 | `B40B5D8698EB8C2603733F95D20F050BAB034BF30086148289ACB8D2250894D5` |

迁移前后 SHA256 一致（`_backup` 侧与改造前的正式文件同为 `61495F74…`），属移动而非删除（DG-13）。

**④ 抽样断言**（4 条，正反各 2 条）：

| 断言 | 结果 |
|---|---|
| `600688.SH` 2006-10-25 → `excluded_s_share=True` | 通过 |
| `600063.SH` 2011-02-10 → `excluded_s_share=False`（B 组保留疑似） | 通过 |
| `600283.SH` 2004-11-04 → `excluded_s_share=False`（段外有 S 名但区间不覆盖） | 通过 |
| `601567.SH` 2011-06-27 → `excluded_s_share=False` | 通过 |

另加一条更强的检查：用**独立重算**（在验证脚本里另写一遍 `name_history` + 东财空洞逻辑，不复用被测代码）得出「应为 excluded」的键集合，与产物逐段比对，**多排 0 / 漏排 0**。

**断言自测**（`--self-test`）：向 `excluded_s_share` 注入 1 个空值后，「空值 0 个」断言确实失败——证明该断言能对「空值 0→N」失败，不是恒真。

**端到端读**（DG-23）：`spi.load_price_inferred()` 返回 9 列含 `excluded_s_share`，空值 0；`spi.suspected_status_at(600688.SH, 2007-06-01)` 由原命中变为 `None`；`st_history.status_at(600688.SH, 2007-06-01, include_suspected=True)` 返回 `non_st`（`source=em_name_history_clean`），不再有疑似标记；对照组 `600063.SH 2011-05-23` 仍返回 `suspected_st`（B 组保留）。

**反例自测**（要求的第 ④ 条）：单测 `test_empty_s_table_restores_suspected_flag` 把 S 区间表置空后，同一段 `excluded_s_share` 变回 `False`、`metrics.suspected_intervals` 由 0 变回 1——证明「不再判疑似」确实由新逻辑产生，而不是原本就不判。

## 4. 余下 6 段：接受偏差 + 重开条件

B 组 6 段（`600063` / `600283` / `600517` / `601567` / `601908` / `603085`）本轮**不修**，保留 `confidence=low`，成因是窗口法假阳性（`cap_tol=5.6%` 过宽 + 只用 `close` 不看 `high`/`low`），不是 S 股。

**重开条件（满足任一才回头改算法）**：

1. **参数方向**：`CAP_TOL` 从 `0.056` 收到 `0.052`。依据——真 ±5% 股的收盘上限只有 5.0%~5.2%（一个价位单位的取整余量），5.2% 以上必非 5% 制度。但单独收紧会误伤「恰在取整边界」的真 ST 段，所以必须**同时**把 `breach` 判据从「只看 `close`」扩为「`close` 或 `high`/`low` 越界」。
2. **验收门槛（三者必须同时满足才允许改）**：改动后在全表 1,443 段上 ① `suspected_only_intervals` 下降；② `confirmed_recall` 不低于当前 **0.7674**；③ `validated_day_precision` 不低于当前 **0.9374**。任一项退步即回滚。
3. **值得回头的证据**：
   - 沪市简称独立原始件（交易所简称变更表或公告全文）到位，能对 6 段给出简称级的「非 ST」正证据 → 此时**不需要改算法**，只需把该源接成第二个 ST 名称源；
   - 出现人工裁定样本证明 6 段中某段**确实**处于 ST（即真 ST 被漏判），且漏判可归因到 `cap_tol` 过宽 → 才动参数；
   - 只要证据仍是「价格形态」这一类，一律不动——§15 硬规则：价格只能回答「有无权益变动」，不能回答「比例是多少」。

**本轮明确未改**：`WINDOW` / `CAP_TOL` / `TOUCH_MIN` / `TOUCH_NEED` / `AMP_MIN` / `VALID_MIN` / `MIN_RUN` 全部原值。

## 5. 保留的旁证问题（本轮不动）

`meta/name_history.parquet` 漏记 S 段是**另一个问题**，本轮只把它当证据用，不改它：

- `600211.SH` 缺 2006-10-09~2007-09-10 整段（实为 `S藏药业`）；
- `600283.SH` 缺 2006-10-09~2006-12-26 整段（实为 `S钱江水`）。

本轮的补源（东财 `former_names` + 同名空洞）正是为绕开这个缺口而设；空洞不唯一或不存在时不猜，7 只留 unresolved（见 §1）。

## 6. 遗留待办（不在本轮范围）

1. **`docs/数据权威路径清单.md` L65 需其写者更新**：行数与标的数仍是 1,443 / 831，但字节 64,928 → **65,309**、SHA256 `61495f74…` → `E3BC4CD4…`，且说明列未提新增的 `excluded_s_share` 列；L104 的 `price_inference.json` 字节 18,584 → **19,974**。本轮按既有约定不碰该文件。
2. **缺口表 G-136 起**：按本报告 §2 口径登记 3 条（可补已完成本条 / 接受偏差 6 段 / 沪市简称缺独立源），由另一路排期。
3. `s_partial_overlap_intervals=14`——这 14 段跨 S 区间边界，价格证据一部分来自 S 制度、一部分来自普通波动，无法归因，本轮保留为疑似、不做排除。保留疑似中 `confidence=low` 由 114 段降为 **85 段**（114 − 29），`high` 段数不变（1,329）。
