# B1 第七片：`meta/factors/stock_shares.parquet` 只读校准体检

- 体检对象：`data/local_store/meta/factors/stock_shares.parquet`（说明 `data/local_store/meta/factors/stock_shares.meta.json`，目录说明 `data/local_store/meta/factors/manifest.json`）
- 体检日期：2026-11-12；性质：**只读**（未改代码、未写 `data/`、未动任何 parquet / manifest、未提交）
- 批次归属：`governance_order.md` L77 与 `base_layer_calibration.csv` G-077 均列 **B1 / depth 0**，本片按 B1 口径体检
- 不在本片范围：`factors/stock_turnover.parquet`（依赖本表 `free_float_share_filled` 口径，属后一批次，本次未碰）

## 0. 结论先行

| # | 结论 | 证据位置 |
|---|---|---|
| 1 | **表内容无法由仓库内代码复现**：`free_float_share` 的 148,587 个非空值在 `src/` + `tools/` 中没有任何产出方，`share_capital.py` 两处一律写 `None`；唯一产出方是未跟踪的一次性脚本 `tmp/from_www_tmp_20261109/fill_shares_20261105.py` | §2、§3 |
| 2 | 权威清单 L39「前 8 列由 `share_capital` 产出」与实测不符；manifest 的 `source` / `semantics` 把该口径指向 `share_capital.py:12-18` 的**文档串行号**，不是代码 | §6 D1/D4 |
| 3 | `filled > float` 违例 549 行 / 479 只（历史最大超出 857%）；**14 只在 2026-11-11 最新可见记录仍违例**，最大 600387.SH +14.61% | §5 D2 |
| 4 | DG-19 混源未声明：143 行 `source=cninfo` 的 `free_float_share` 实测 143/143 等于东财同日 `FREELIQCI_SHARES`，行级 `source` 无法区分 | §4 D3 |
| 5 | 写入口实测 **仓库内 2 个 + 仓库外 2 个（一次性）**；`governance_order.md` 记的 4 个里，`backfill_share_div.py` / `derive_total_dividend.py` **只读本表**、写的是 `stock_dividend` | §2 D5 |
| 6 | manifest 与磁盘逐项一致（`--check` PASS），派生 2 列可由当前代码逐值复算（diff 0/0）；无第二份同语义面板 | §3、§5、§7 |

事实基线：166,284 行 / 5,813 只 / 2,792,975 B / `sha256 2e721c87e57e1da8aa47fbd3ee490f2044a87d3624c2c996cea9bd4a95ba9b35` / mtime 2026-11-10 21:25:51；`meta.json` 2,908 B / `sha256 c09f419e…` / mtime 21:26:07。

---

## 1. 检查①：写入口（代码实测逐个列出）

判据：写入 `SHARES_FILENAME`（`share_capital.py:106`）指向的同一文件，且非 `dry-run` / 非 `--no-write`。

| # | 入口 | 性质 | 写入列 | 写入方式 | 在仓库内 | 实测证据 |
|---|---|---|---|---|---|---|
| 1 | `tools/fetch_stock_shares.py` → `share_capital.assemble_shares` → `write_shares` | 常规采集 / 重建主入口 | `SHARE_COLUMNS` **8 列** | `write_parquet_atomic` 整文件替换 + `_write_meta` 覆盖 `meta.json` | 是 | `fetch_stock_shares.py:486`、`share_capital.py:256-265`、`:919-938` |
| 2 | `tools/derive_free_float_fill.py`（`apply`） | 派生 2 列入库 | **全 10 列**（读 8 列 + 追加 2 列后整表重写） | `write_parquet_atomic` 整文件替换 + `_meta_payload` 合并写 `meta.json` | 是 | `derive_free_float_fill.py:186-205`、`:127-142` |
| 3 | `tmp/from_www_tmp_20261109/fill_shares_20261105.py` | 一次性补数（`free_float_share` 与 `float_share` 空值） | 8 列 | 调 `write_shares` 整表覆盖 | **否**（`tmp/` 未跟踪） | `apply()` 内 `write_shares(...)`；`BACKUP_DIR` 指 `data/local_store/_backup/` |
| 4 | `_staging/freefloat_fill_20261110/build.py` | 一次性派生（#2 的前身） | 10 列 | 整表重写 | 否（`_staging/`） | 已被 #2 取代，回执在 `build_result.json` |
| 5 | `tools/backfill_share_div.py` | **只读**（`--shares` 输入） | — | 输出写 `stock_dividend.parquet` | 是 | `:63` `DEFAULT_SHARES`、`:371` `--shares`、`:62` `DEFAULT_DIV` |
| 6 | `tools/derive_total_dividend.py` | **只读**（`shares_as_of` 输入） | — | 输出写 `stock_dividend.parquet` | 是 | `:15` 文档、`:68` `DEFAULT_SHARES` |

### 1.1 各入口口径是否一致、是否互相覆盖

| 关系 | 实测结论 |
|---|---|
| #1 ↔ #2 | **列级不重叠但强串行依赖**：#1 只写 8 列并整表替换 → 派生 2 列被丢弃；#2 必须紧跟重跑。此点在权威清单 L39 已登记（"否则退回 8 列"），实测属实 |
| #1 ↔ #3 | **互相覆盖**：#3 也调 `write_shares`（8 列整表覆盖）。若在 #2 之后运行 #3，2 列派生被静默抹掉；#1 重建后 #3 的 `free_float_share` 全部回到 `None` |
| #2 ↔ #4 | #4 已被 #2 取代，两者同时存在属历史残留，当前只有 #2 在仓库内 |
| `free_float_share` 口径 | #1 恒写 `None`（`share_capital.py:380`、`:528` 两处 `'free_float_share': None`）；**只有 #3 会填**，规则为 `FREELIQCI_SHARES > 0 且 ≤ float_share 且 float_share 非空`，0 值转空 |
| 历史代码证据 | `git log --all -p -S "FREELIQCI_SHARES" -- src tools` 的新增行**只有 2 类文本**（`share_capital.py:16` 文档串、`:956` meta 文本），**0 行可执行代码**。即该列从来没有过仓库内产出方，不是"曾经有、后来被删"的回归 |

### 1.2 写入口与 `governance_order.md` L77 的差异

| `governance_order.md` L77 记录 | 实测 | 判定 |
|---|---|---|
| `src/data/local_store/share_capital.py` | 是（经 `write_shares`，由 `fetch_stock_shares.py` 调用） | ✅ |
| `tools/derive_free_float_fill.py:225` | 是（写入语句在 `:186-205`，`:225` 属 `check`/`main` 区段，行号有偏移） | ⚠ 行号偏移 |
| `tools/backfill_share_div.py` | **只读**，写 `stock_dividend.parquet` | ❌ 错记 |
| `tools/derive_total_dividend.py` | **只读**，写 `stock_dividend.parquet` | ❌ 错记 |
| （未记）`tmp/from_www_tmp_20261109/fill_shares_20261105.py` | 是 `free_float_share` 唯一产出方 | ❌ 漏记 |

与上一片对 `stock_dividend` 的结论互为印证：L37 已写"另 2 个被错记在 `stock_shares` 名下"，本次从本表侧复核成立——两边是同一处错记的两面。

---

## 2. 检查②：同目录 manifest 记录数 vs 实际文件

| 项 | manifest / meta.json 记录 | 磁盘实测 | 结论 |
|---|---|---|---|
| `files[0].rows` | 166,284 | 166,284 | ✅ |
| `files[0].bytes` | 2,792,975 | 2,792,975 | ✅ |
| `files[0].sha256` | `2e721c87…5ba9b35` | `2e721c87…5ba9b35` | ✅ |
| `files[0].columns` | 10 列（8 事实 + 2 派生） | 同 | ✅ |
| `files[0].date_range` | 1979-02-23 ~ 2026-11-06 | 同（未来日期 0 行） | ✅ |
| `manifest_tool.py --check --only meta/factors` | — | PASS，未登记的非 parquet 文件 0 | ✅ |
| `meta.json.rows` / `.codes` | 166,284 / 5,813 | 同 | ✅ |
| `meta.json.sources` | 东财 166,087 + 巨潮 197 = 166,284 | 同 | ✅ |
| `meta.json.free_float_fill.counts` | raw 148,587 / ffill 1,145 / excluded 16,552 / null 17,697 | 同 | ✅ |
| `meta.json.free_float_fill.backup_file` | `data\local_store\_backup\stock_shares_20261110\stock_shares.parquet` | 该路径**不存在**；实际在仓库根 `_backup\stock_shares_20261110\stock_shares.parquet`（2,159,770 B / `sha256 3e9dc46e…` / 8 列） | ❌ D6 |
| `meta.json.free_float_fill.backup_sha256` | `3e9dc46e…` | 与仓库根 `_backup` 副本一致 | ✅（哈希对得上，只有路径过期） |

回滚副本有效性（DG-11）：`_backup` 副本与现表在 6 个基础列上逐值比对——行数 166,284 / 主键差 0 / `total_share` 差 0 / `float_share` 差 0 / 空值形态差 0 / `change_reason` 差 0，即派生步骤只追加了 2 列、未改动任何既有值，回滚可用。

---

## 3. 检查③：能否与权威源对拍

可对拍，给出两条独立链路：

### 3.1 与东财 F10 原始缓存对拍（同源逐值，证"值就是它"）

输入：`E:\www\tmp\em_equity_all_20261105.parquet`（324,806 行 / 24,754 只，`SECUCODE` + `END_DATE` + 三列股本）

| 口径 | 结果 |
|---|---|
| 按 `(ts_code, effective_date)` 配对 | 165,838 / 166,284；未配对 446（东财行 399 + 巨潮行 47） |
| `total_share` 逐值相等 | **165,838 / 165,838 = 100%** |
| `float_share` | 相等 159,441 + 两侧同为空 3,613 = 163,054；余 2,784 全部是「东财 `LISTED_A_SHARES=0`（上市前股本）而本地写 NULL」，属正确转空 |
| `free_float_share` | **相等 148,587 / 148,587 = 100%** + 两侧同为空 16,321 + 表空源有值 930（分解见 §4） |
| `float_share` 真实值冲突（两侧非空且不等） | **0** |

### 3.2 与 `valuation_daily` 对拍（独立接口，反算股本）

口径：`total_share ≈ total_mv / close`、`float_share ≈ circ_mv / close`，取 2026-11-06 快照。

| 指标 | 结果 |
|---|---|
| 联接只数 | 5,579 |
| `total_share` 完全相等 | 5,544（99.37%） |
| `total_share` 相对差 ≤1% / ≤2% | 5,576（99.95%）/ 5,577 |
| `total_share` 相对差 >5% | 2 |
| `float_share` 完全相等 | 5,527 |
| `float_share` 相对差 ≤1% | 5,570（99.84%） |
| `float_share` 相对差 >5% | 4 |

差额归因（6 只，均无第三源，按 DG-18 不判谁错）：

| 代码 | 本表值 | 估值隐含 | 相对差 | 本表最后生效日 | 归因 |
|---|---|---|---|---|---|
| 300018.SZ | 488,710,536 | 541,509,110 | 9.75% | 2026-06-30 | 估值快照已反映 7 月后新增股本，股本表未收录 |
| 300587.SZ | 1,298,268,797 | 1,372,268,797 | 5.39% | 2026-04-23 | 同上 |
| 603376.SH（流通） | 32,000,800 | 99,401,000 | 67.81% | 2026-05-06 | 同上 |
| 603334.SH（流通） | 31,816,703 | 51,611,989 | 38.35% | 2026-05-06 | 同上 |
| 300464.SZ（流通） | 457,389,163 | 380,702,047 | 20.14% | 2026-10-12 | 同上（方向相反，需第三源才能定责） |
| 603215.SH（流通） | 187,228,380 | 214,595,078 | 12.75% | 2026-06-26 | 同上 |

共性：6 只的差额对标的本表记录都止于 2026-04~10，而估值侧是 11-06 当日的隐含值 → 指向「东财 `gbjg` 事件行滞后于 `spot` 估值接口」，属源端两接口不同步，不是本地错值。

### 3.3 派生层可复算

`derive()` 用现表 8 个基础列 + 交易日历重跑：`free_float_share_filled` 逐值差 **0 行**、`free_float_source` 逐值差 **0 行**，三态 148,587 / 1,145 / 16,552 与 `meta.json` 一致 → 派生层**完全可复现**（与 #1 的不可复现形成对照）。

---

## 4. 逐字段来源分级（DG-19）

| 列 | 来源层级 | 产出方（实测） | 覆盖 | 说明 |
|---|---|---|---|---|
| `ts_code` / `effective_date` | 外部：东财 `stock_zh_a_gbjg_em` 166,087 / 巨潮 `stock_share_change_cninfo` 197 | #1 `share_capital` | 100% 非空 | 行级 `source` 可追溯 |
| `total_share` | 同上（外部） | #1 | 100% 非空、0 个 ≤0 | 与源缓存 165,838/165,838 一致 |
| `float_share` | 外部 + **#3 补空** | #1 + #3 | 159,928 / 166,284 非空（空 6,269） | 空值全部可归因（见下） |
| `free_float_share` | **外部：东财 F10 `FREELIQCI_SHARES`** | **仅 #3（仓库外）** | 148,587 非空 / 17,697 空 | 与源缓存 148,587/148,587 逐值一致 |
| `change_reason` | 外部 | #1 | 166,269 非空（空 15） | 源端原文，审计用 |
| `source` | 行级单一标记 | #1 | 100% | **掩盖 `free_float_share` 的跨源事实 → 本表唯一 DG-19 违规点** |
| `fetched_at` | 采集时间 | #1 | 100% | 2026-11-04 23:43 ~ 2026-11-08 22:29 |
| `free_float_share_filled` | **本地推导**（`free_float_share` 的 250 交易日 ffill） | #2 | raw 148,587 / ffill 1,145 / excluded 16,552 | 可复算，diff 0 |
| `free_float_source` | 本地推导标记 | #2 | 100% | 三态与取值互斥（实测 6 项互斥校验全 0） |

空值闭合（每一列都能对到具体原因，无"未知空值"）：

| 列 | 空值合计 | 分解 |
|---|---|---|
| `float_share` | 6,269 | 未配对 22 + 源端为空 3,613 + 源端为 0（上市前股本）2,634 |
| `free_float_share` | 17,697 | 未配对 446 + 源端为空 16,321 + 源端为 0 145 + `>float` 被规则跳过 772 + **规则内漏填 13** |
| `change_reason` | 15 | 源端原文为空 |
| `free_float_share_filled` | 16,552 | = `excluded` 三态计数，口径定义即如此 |

**跨源行明细**：197 行 `source=cninfo:stock_share_change_cninfo` 中 143 行 `free_float_share` 非空，逐行与东财同日 `FREELIQCI_SHARES` 比对 **143/143 相等** → 这些行的 `total_share`/`float_share` 来自巨潮、`free_float_share` 来自东财，而 `source` 只写了巨潮。

---

## 5. 数据一致性缺陷（非登记类）

### 5.1 `free_float_share_filled > float_share`（549 行 / 479 只）

| 项 | 实测 |
|---|---|
| 违例行 | 549（全部 `free_float_source=ffill_250d`，`free_float_share` 原值层违例 0 行） |
| 涉及标的 | 479 |
| 历史最大超出 | **857.14%**（002563.SZ 2014-04-01：fill 670,000,000 / float 70,000,000） |
| `> total_share` | 0 行 |
| **2026-11-11 最新可见记录仍违例** | **14 只**，最大 600387.SH +14.61%（fill 314,114,405 / float 274,075,882），其余 13 只 ≤0.74% |
| 代码守护 | `derive_free_float_fill.check()` 只校验行数守恒、原 8 列不变、三态互斥，**不含 `filled <= float` 断言**（`:127-142`） |
| 影响面 | `build_state_vector.py:236` 情绪位分母 `Σ(free_float_share_filled × close)` 直接受影响；`turnover.py` 本地派生读的是 `float_share`，**不受影响** |

### 5.2 13 行"规则内漏填"

13 行满足 `em_free > 0 且 em_free ≤ float_share` 却仍为空。成因：`#3` 在同一个 `with_columns` 里同时补 `float_share` 与 `free_float_share`，后者的 `pl.col('float_share').is_not_null()` 判定的是**填充前的原值**（Polars 表达式基于原 frame），故这 13 行在填充时 `float_share` 尚为空 → 被跳过。现表这 13 行 `float_share` 已由同一脚本填上，`free_float_share` 仍空。属一次性脚本的求值顺序缺陷，量小、只影响历史记录。

### 5.3 覆盖缺口

| 项 | 实测 |
|---|---|
| 行情池标的 | 5,832（`quotes/stock/**/*.parquet`） |
| 本表标的 | 5,813（差 19） |
| 19 只明细 | 18 只已退市（行情末日 ≤ 2019-07-08，如 000024.SZ 2015-12-07、600680.SH 2018-04-27）；1 只 `688827.SH` 2026-11-10 才上市，仅有 2 个 bar，晚于本表采集窗口 |
| 未来生效行 | 0（`effective_date > 2026-11-12`） |
| 主键 `(ts_code, effective_date)` 重复 | 0；全表按 `(ts_code, effective_date)` 有序 |
| 最新可见记录分布 | 5,753 / 5,813 只的末次生效日 < 2026-11-01（PIT 表常态，靠 ffill 使用）；仅 1 只末次生效日 = 2026-11-06 |
| as-of 2026-11-11 可用性 | 5,813 只可见：raw 5,697 / ffill 51 / excluded 65 → 可用 5,748（98.9%），65 只下游需按剔除处理 |

---

## 6. 检查①⑤与登记类缺陷（只列，不改 `docs/数据权威路径清单.md`）

| 编号 | 位置 | 现写什么 | 实测什么 | 判定 |
|---|---|---|---|---|
| **D1** | 权威清单 `docs/数据权威路径清单.md` L39 | "前 8 列由 `share_capital` 产出" | `free_float_share` 在 `share_capital.py:380`、`:528` 恒写 `None`，仓库内 0 处 `FREELIQCI` 代码；148,587 个非空值只来自 `tmp/from_www_tmp_20261109/fill_shares_20261105.py`（未跟踪） | ❌ 高 |
| **D2** | 同上 L39 | "自由流通换手类计算须同时读 `free_float_share_filled` 与 `free_float_source`" | 属实（`build_state_vector.py:99,215,236`、`verify_state_vector.py:421-443`），但未提 `filled > float` 的 14 只现行违例 | ⚠ 中 |
| **D3** | 同上 L39 | 消费方只列 `read_shares` / `shares_as_of` | 实测 9 个读取点：`share_capital.py:178,229`、`fetch_stock_shares.py:112,171,214,317,391`、`src/advice/fundamental_factors.py:313`、`turnover.py:861,902-905`、`factors_manifest.py:133`、`build_state_vector.py:215`、`verify_state_vector.py:421`、`anomalies/build_no_ex_rights_events.py:57`、`derive_total_dividend.py:15`、`backfill_share_div.py:63` | ⚠ 中（不全） |
| **D4** | 同上 L39 | 无写入口清单 | 仓库内 2 个（`fetch_stock_shares.py` 主入口、`derive_free_float_fill.py` 派入口）+ 仓库外 2 个一次性（`tmp/…/fill_shares_20261105.py`、`_staging/freefloat_fill_20261110/build.py`） | ❌ 高 |
| **D5** | `2026-11-12/data-lineage/outputs/governance_order.md` L77 | 写入口 4 个，含 `backfill_share_div.py`、`derive_total_dividend.py` | 两者只读本表、写 `stock_dividend.parquet` | ❌ 错记 |
| **D6** | `meta/factors/stock_shares.meta.json` → `free_float_fill.backup_file` | `data\local_store\_backup\stock_shares_20261110\stock_shares.parquet` | 该路径不存在；实际 `_backup\stock_shares_20261110\stock_shares.parquet`（2,159,770 B，`sha256 3e9dc46e…`） | ❌ 低（DG-11 迁出 `data/` 后未回写） |
| **D7** | `meta/factors/manifest.json` → `source` / `semantics` | 称 `free_float_share` 取自东财 F10 `FREELIQCI_SHARES`、"强制 free ≤ float"，并把出处指向 `src/data/local_store/share_capital.py:12-18` | 口径描述与实际值相符，但 `:12-18` 是**文档串行号**，代码中无该实现 | ❌ 中（指针失效） |
| **D8** | `src/data/local_store/share_capital.py:15-18`（模块文档串） | 同 D7，声称本模块实现 `FREELIQCI` 填充与 `free ≤ float` 校验 | 模块从未实现；`git log --all -p -S FREELIQCI_SHARES -- src tools` 新增行仅 2 类文本 | ❌ 中（文档与代码漂移） |
| **D9** | 权威清单 L39 | "原 `free_float_share` 保持源端原值不变，禁止就地覆盖" | 无代码守护；#3 脚本对整列有写权限，`write_shares` 会整表覆盖 | ⚠ 低（仅文档约束） |

### 6.1 检查④：是否存在第二份同语义数据（无）

全库 schema 扫描（`data/local_store/**/*.parquet` 含 `total_share` / `float_share` / `*free_float*` 列）：

| 候选 | 规模 | 判定 |
|---|---|---|
| `meta/events/proxy/equity_issue_proxy.parquet` | 7,810 × 13（`total_share` / `total_share_prev` / `issued_shares`） | 事件代理表，`basis` / `source_data` 明示"事件日取 `stock_shares.effective_date`"+"eastmoney:stock_zh_a_gbjg_em"，是**本表的派生事件行**，不是股本面板 → 不构成 DG-1 第二份 |
| `meta/events/proxy/unlock_proxy.parquet` | 24,323 × 15（`float_share` / `float_share_prev` / `total_share` 及 `_prev`） | 同上，解禁事件代理 → 不构成 |
| `meta/financials/valuation_daily.parquet` | 只有 `total_mv` / `circ_mv`，无股数列 | 本次即用其反算股本做对拍 → 不构成 |

结论：**无第二份同语义股本面板** ✅。`stock_turnover` 的本地派生直接读本表 `float_share`（`turnover.py:902-905`），不落第二份股本。

---

## 7. 缺陷汇总（按严重度）

| 级别 | 编号 | 一句话 |
|---|---|---|
| 高 | D1 / D4 | `free_float_share` 无仓库内产出方，重建即丢 148,587 个非空值；权威清单把该列记在 `share_capital` 名下、且无写入口清单 |
| 中 | D2 | `free_float_share_filled > float_share` 549 行 / 479 只，14 只现行仍违例；`check()` 无该断言 |
| 中 | D3 | 143 行 `source=cninfo` 的 `free_float_share` 实测来自东财，行级 `source` 掩盖混源（DG-19） |
| 中 | D5 | `governance_order.md` 把 2 个只读脚本记为写入口 |
| 中 | D7 / D8 | manifest 与模块文档串把口径出处指向文档行号，代码无此实现 |
| 低 | D6 | `meta.json` 的 `backup_file` 指向已不存在的 `data/local_store/_backup/…` |
| 低 | §5.2 | 13 行 `free_float_share` 规则内漏填（一次性脚本求值顺序） |
| 低 | §5.3 | 19 只行情池标的无股本记录（18 退市 + 1 只 2026-11-10 新上市） |
| 提示 | §3.2 | 6 只与估值反算差额 >5%，归因为源端两接口不同步，不是本地错值 |

---

## 8. 复现命令

```powershell
# 结构 / 空值 / 三态 / 覆盖 / 与估值对拍 / manifest 内容
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s7_shares.py

# 派生层复算、filled>float 违例、与东财缓存对拍、19 只缺口
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s7_verify.py

# 回滚副本一致性、923/13 行归因、14 只现行违例、巨潮行来源
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s7_verify2.py

# manifest 一致性门禁
.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta/factors
```

三个脚本均只读（`read_parquet` / `derive()` 纯函数 / `json.load`），不写 `data/`、不改 manifest。

## 9. 交接说明

- 本片只做体检与登记，未修数据、未改清单、未提交。
- 与后续批次的接口：`free_float_share_filled` 是 `stock_turnover`（后一批次）的自由流通口径输入；D2 的 14 只现行违例落在该口径上，做 `stock_turnover` 前需先决定是"接受 ffill 继承"还是"在派生层截断到 `float_share`"。
- 与另一路的分工：`docs/数据权威路径清单.md` 由另一路统一修改，本报告只逐条列出 D1~D9 的"哪一行 / 现写什么 / 实测什么"，不直接改该文件。
