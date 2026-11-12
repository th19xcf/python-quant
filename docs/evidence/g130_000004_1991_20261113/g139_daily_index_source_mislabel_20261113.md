# G-139 `meta/daily_index.parquet` 把混合来源文件标成单一 TDX 来源（全库取证）

- 取证日期：2026-11-13
- 范围：`data/local_store/quotes/stock/{sh,sz}` 全库逐只（5,832 只）
- 性质：只读取证。未改 `daily_index`、未改任何行情文件、未改缺口登记表、未提交。
- 关联条目：G-130（`000004.SZ` 1991 段 112 行无 TDX 旁证，A 类 / unresolved）。本条登记的**不是数据集本身**，而是覆盖度索引对它的**来源标注错误**（K 类）。两条互补，不得互相替代。

---

## 0. 结论摘要

1. 全库命中判据的只有 **1 只 / 1 个文件 / 112 行**：`quotes/stock/sz/000004.SZ.parquet`。不存在第二只。判据放宽到「不看 mtime」仍是同一只，说明没有「start 早于 `.day` 首行但 mtime 不相等」的漏网。
2. 112 行全部落在 **1991 年**：1991-01（8）、02（16）、03（19）、04（9）、05（18）、06（14）、07（27）、08-01（1）。不是「散在 1991–1993」，而是**一整段紧贴 TDX 起点之前的连续前段**，起点 1991-01-14，止点 1991-08-01，`.day` 首行为 1991-08-02。
3. 成因是 **2026-11-05T00:59:18 的一次性单文件外部导入**，不是第二条构建路径。该小时全库 `quotes/stock` 只有这 1 条写入记录，`action=create`（写前目标文件不存在），同一小时其余 233 条是 ST / 财务采集。当晚 11:52–11:54 的全市场重建（5,564 只）**没有写入它**。
4. 它能活到现在，靠的是「**从未被重建**」而不是「被 upsert 保护」：`built_at` 仍停在 `2026-11-11 16:58:56.384235`（`rescan_meta` 重推时刻），11-11 18:08–18:10 那批重建没有写它。upsert 保护（`cc228e1`）是 11-06 才落地的；在 11-05 11:52 那批运行时，代码还是「整帧覆盖」，若覆盖到本只，112 行会被静默删除。
5. `source_mtime_ns` 全库只剩 **1 个读点**（`store.py:304-307`，`is_stale` 的 mtime 兜底），保鲜判定主路径已改为内容级末行日期。因此误标的**直接功能危害很小**，真正的问题是：`full=True` + `source_mtime_ns == .day mtime` 这两项组合对外宣称「整只文件由 TDX 构建」，与 G-130 的「无 TDX 旁证」结论**直接矛盾**，且 `rescan_meta` 每次运行都会把这个标注重算回来。
6. 建议登记 **G-139（K 类）**；最小改法是给 `daily_index` 增列 `source_start`（对应 `.day` 首行日期），**不改 `full` 语义**、不动任何行情行。

---

## 1. 判据与方法

主判据 A（题目给定）：

```
daily_index.source_mtime_ns == 对应 .day 文件 st_mtime_ns
且 daily_index.start < 该 .day 文件首行交易日
```

辅助判据 B：只要求 `start < .day 首行`，不看 mtime —— 用来证明「A 不是 B 的子集漏网」。

第三层交叉验证：不只看首行，直接逐只求 `parquet 日期集合 − .day 日期集合` 的差集，把「本地有、源端没有」的行按位置切成 `pre_first`（早于源首行）与 `within`（源首行之后）。这一层用来回答「是否还有 inside-range 的混合来源行被一并标成 TDX」。

实现：
- [scan_mislabel.py](../../_staging/daily_index_source_mislabel_20261113/scan_mislabel.py)：判据 A / B，产出 `scan_all_stock.csv`、`hits_A.csv`、`hits_A_detail.csv`、`scan_summary.json`
- [step2_datediff.py](../../_staging/daily_index_source_mislabel_20261113/step2_datediff.py)：日期集合差，产出 `datediff_all_stock.csv`、`datediff_summary.json`
- [step3_withindiff.py](../../_staging/daily_index_source_mislabel_20261113/step3_withindiff.py)：差集形态分类，产出 `withindiff_profile.csv`、`withindiff_summary.json`
- [step4_lineage.py](../../_staging/daily_index_source_mislabel_20261113/step4_lineage.py)：快照与台账追溯，产出 `changelog_000004.csv`、`changelog_20261105_night.csv`、`rows_drop_*.csv`
- [step5_fieldcheck.py](../../_staging/daily_index_source_mislabel_20261113/step5_fieldcheck.py)：字段级核对与 `built_at` 分布

`.day` 记录长度 32 字节，首行日期取头 4 字节小端整数；parquet 侧只读 `date` 列。

---

## 2. 规模（①）

| 项 | 值 |
|---|---|
| `daily_index.parquet` 总行数 | 9,787 |
| `data_type='stock'` | 5,832 |
| 有对应 `.day` 的 stock | 5,814 |
| 无 `.day` 的 stock（退市/改码，清单见 `scan_summary.json`） | 18 |
| **判据 A 命中** | **1 只** |
| **判据 B 命中（放宽 mtime）** | **1 只**（与 A 完全相同） |
| 命中涉及行数 | 112 |
| 命中行时间范围 | 1991-01-14 ~ 1991-08-01 |
| `full=True` 的命中数 | 1 / 1 |
| `daily_index.rows` 与 parquet 实际行数一致 | 1 / 1 |

命中行月份分布：

| 月份 | 行数 |
|---|---|
| 1991-01 | 8 |
| 1991-02 | 16 |
| 1991-03 | 19 |
| 1991-04 | 9 |
| 1991-05 | 18 |
| 1991-06 | 14 |
| 1991-07 | 27 |
| 1991-08（仅 08-01） | 1 |
| 1992 / 1993 | 0 |

结论：**不是 1991–1993 的面上问题，而是单点集中在 1991 这一段的「TDX 起点之前」**。全库 5,814 只有源文件的股票里，只有这一只的本地起点早于其 TDX 起点。

`000004.SZ` 当前实况（本文重新实测，与 G-130 一致）：

| 项 | 值 |
|---|---|
| 本地 parquet | 8,251 行，1991-01-14 ~ 2026-07-13 |
| `sz000004.day` | 8,124 行，1991-08-02 ~ 2026-04-27（mtime `2026-04-27 15:46:28.642069`） |
| 本地独有行合计 | 127 = 112（早于源首行）+ 15（`2026-06-23`~`2026-07-13`，晚于源末行） |
| 源独有行 | 0 |
| 1991 年行数 | 本地 240 / 源 128，差恰为 112 |

### 2.1 同源但不同形态的次要发现

不要把它和 G-139 混为一谈，但同属「单来源标注无法表达混合来源」：

- 231 只股票存在「本地有、当前 `.day` 没有」的行，合计 159,582 行；其中 `pre_first` 仅 112（全部来自 000004），其余 159,470 行都在源首行之后。
- 这 159,470 行的形态是「**本地尾巴长于源文件**」：`local_last > day_last` 的有 229 只，其中 197 只的 `.day` 末行停在 `2019-09-17`。这与 `stale_status` 的 `tdx_2019_09_17` 停更批次完全对得上，属于「TDX 停更 + 别源续尾」，不是日内缺口回补。
- 反向差集（源有本地无）合计 35 行，是 `filter_nontrading_rows` 挡下的非交易日占位行，与本题无关。

因此：**「本地起点早于 TDX 起点」这一形态在全库是孤例**，不是在某个年代区间成片出现。

---

## 3. 成因区分（②）

### 3.1 `daily_index` 当前行的字段证据

```
ts_code          = 000004.SZ
rows             = 8251
start            = 1991-01-14
end              = 2026-07-13
full             = True
source_mtime_ns  = 1777275988642069000   # = 2026-04-27 15:46:28.642069，与当前 .day mtime 完全相等
built_at         = 2026-11-11 16:58:56.384235   # 全库唯一一只（同刻仅 1 只）
```

`built_at` 落在 `2026-11-11 16:58:56` 而不是 18:08–18:10，说明 **11-11 那次重建没有写它**，这一行是 `rescan_meta` 在 16:58:56 从磁盘重推出来的。全库 stock 的 `built_at` 高峰在 `2026-11-11 18:08:42`~`18:10:37`（每个时点 54~58 只），000004 明显在批次之外。

`rescan_meta` 是当前标注的**直接来源**，其推导规则见 [store.py](../../src/data/local_store/store.py#L348-L356)：

```python
full = True
source_mtime_ns = None
if tdx_path is not None:
    source = tdx_path / market / 'lday' / f'{market}{ts_code.split(".")[0]}.day'
    if source.exists():
        source_mtime_ns = source.stat().st_mtime_ns      # 无条件抄 .day 的 mtime
        source_first, _ = read_first_last_trade_date(source)
        if source_first is not None:
            full = start <= source_first                # 1991-01-14 <= 1991-08-02 → True
```

两处设计在这里同时失效：
- `source_mtime_ns` 被赋予「本文件由这个 `.day` 构建」的语义，但实际只是「该 `.day` 现在的 mtime」，与行从哪来无关；
- `full = start <= source_first` 把「本地起点更早」判成「完整覆盖 TDX 历史」，于是**覆盖得比 TDX 还多**的文件被标成 `full=True`。`_META_SCHEMA` 对该字段的定义是「True 表示覆盖该标的在通达信里的全部历史」（[store.py:101-102](../../src/data/local_store/store.py#L101-L102)），而 000004 的事实是「TDX 全部历史 + 112 行来路不明」。

### 3.2 台账证据：只有一次单文件写入

`logs/data_changelog.csv`，2026-11-05 00:00 这一整小时共 234 条：

| 时间 | 路径 | action | bytes | prev_sha256 |
|---|---|---|---|---|
| 2026-11-05T00:59:18 | `quotes/stock/sz/000004.SZ.parquet` | **create** | 246,892 | **null** |

- 该小时 `meta/st/raw/em_pages` 50 条、`factors/rebuild` 49 条、`meta/financials/*` 86 条、`quotes/fund` 少量 —— 也就是当时在跑 ST 与财务采集，**行情侧只写了这一只**。
- `action=create` 且 `prev_sha256=null` 是硬证据：`atomic_write._prepare_record` 在写盘前取 `existed = target.exists()`（[atomic_write.py:171-173](../../src/data/atomic_write.py#L171-L173)），只有写前文件**不存在**才会记 create。一次正常的 TDX 重建（目标已存在 8,139 行）只会记 `update`。
- 000004 在 `2026-11-05T11:*` 的全市场批次里**零记录**；同一批次写了 5,564 只 stock（全类共 8,454 条）。

### 3.3 快照证据：112 行在 11-05 00:59 之后才出现

| 快照 | 时点 | 000004 rows | start | 备注 |
|---|---|---|---|---|
| `logs/factor_copy_backup_20261104/...` | 11-04 | 8,139 | 1991-08-02 | G-130 已记 |
| `_backup/internal_gap_fix/20261105/...` | 文件 mtime 11-03 14:04 | 8,139 | 1991-08-02 | 原文件 copy2 副本 |
| `_backup/snap_20261105/daily_index.parquet` | 11-05 12:08 | 8,251 | 1991-01-14 | 已含 112 行 |
| `_backup/meta/daily_index_20261111/...` | 11-07 | 8,251 | 1991-01-14 | 未回退 |
| 当前 | 11-11 16:58 重推 | 8,251 | 1991-01-14 | `full=True` |

行数守恒可以闭合：`8,124（.day）+ 15（尾段，别源）+ 112（前段，本次导入）= 8,251`。112 与 `.day` 无任何交集（`only_src = 0`），也不可能是 `filter_nontrading_rows` 漏放（那只挡源端新行，不会新增源端没有的日期）。

### 3.4 代码路径：仓库内没有任何一处能「凭空」产出早于源首行的行

全部 5 个写入口，每一个都只能「带 TDX 的 mtime 盖章」，没有一个能把外来行写进正式文件：

| 写入口 | 位置 | 行为 |
|---|---|---|
| 全量/增量构建 | [build.py:401-403](../../src/data/local_store/build.py#L401-L403) | 写入帧来自 `build_frame(path)`，即 `.day`；`source_mtime_ns=path.stat().st_mtime_ns` |
| 元数据重推 | [store.py:353](../../src/data/local_store/store.py#L353) | 不改行情行，只重算覆盖度 |
| 缺口回补 | [fill_internal_gaps.py:387-388](../../tools/fill_internal_gaps.py#L387-L388) | 只补「TDX 有、本地无」的日期，`full=True` + 抄 `.day` mtime |
| 腾讯续尾 | [daily_fill.py:491-494](../../src/data/tencent/daily_fill.py#L491-L494) | 保留旧 `full` / `source_mtime_ns`，只追加尾段 |
| 复权列刷新 | [store.py:976-979](../../src/data/local_store/store.py#L976-L979) | 继承原 `full` / `source_mtime_ns` |

结论：写入者只可能是**仓库外的临时脚本 / 交互式命令**调用了 `LocalQuoteStore.write_daily(..., full=True, source_mtime_ns=<.day mtime>)`。因为 `QUANT_CHANGELOG_TASK` 未设，台账记为 `task=unattributed`（[atomic_write.py:155-160](../../src/data/atomic_write.py#L155-L160)）。脚本本身未入库，与 G-130 §6.4 的判断一致。

### 3.5 残留不确定性与为何判定它很小

11-05 00:59 记录下来的 `source_mtime_ns = 1782824438456979700`（`2026-06-30 21:00:38`）与当前 `.day` 的 `1777275988642069000`（`2026-04-27 15:46:28`）不等，说明**当时那个 `.day` 已经被换掉**，其内容无法本地取证。理论上存在「当时那份 06-30 的 `.day` 本身就含 112 行 1~8 月 1991 数据」的可能，但有四条反向证据：

1. `.day` 的首行边界是稳定的：04-27 版首行 1991-08-02，G-130 在 11-13 观测的也是 1991-08-02；TDX 日线只增不减，不会在后续同步里补出 1 月的历史。
2. 行数守恒：`8,124 + 15 + 112 = 8,251`。若 06-30 版含这 112 行，它就应有 8,236 行，那么它比 04-27 版多出的 112 行恰好全是 1991-01~08-01 —— 与「TDX 历史边界不变」冲突。
3. 写入形态：一次 TDX 重建不会只写单只，也不会在目标文件已存在时报 `create`。
4. 数值口径：G-130 实测 112 行与新浪逐值一致（成交量恰为新浪的 1/100、成交额在 95 行上完全相同），与 baostock 在 19 行上不同。TDX 作为第三家口径，95/95 完全命中成交额的概率极低。

因此判为「外部导入」的证据强度足够，但**「旧 `.day` 内容不可取证」这一事实必须留在条目里**，不得表述为已完全排除。

---

## 4. 影响面（③）

### 4.1 `source_mtime_ns` 的读点

全库只有一个读点：

| 位置 | 用途 |
|---|---|
| [store.py:304-307](../../src/data/local_store/store.py#L304-L307) | `is_stale` 的 **mtime 兜底**：仅当 `.day` 无法解析出末行日期时才生效 |

`is_stale` 的调用点两个：
- [data_fetcher.py:280](../../src/data/managers/data_fetcher.py#L280)：单只取数前判 stale，**不阻断**，只返回本地数据并登记告警；
- [data_fetcher.py:538](../../src/data/managers/data_fetcher.py#L538)：批量取数时把 stale 的代码放进 `pending` 待刷新。

保鲜判定主路径已在 [store.py:288-303](../../src/data/local_store/store.py#L288-L303) 改为**内容级末行日期比对**，mtime 只是兜底。**所以误标 `source_mtime_ns` 不会造成保鲜误判，也不会造成「因 mtime 相等而跳过重建」。**

`_META_SCHEMA` 里该字段的注释仍写「用于判断源端是否已更新」（[store.py:103-104](../../src/data/local_store/store.py#L103-L104)），与实际用途已经漂移 —— 这本身是文档级缺陷，建议随 G-139 一并修注释。

### 4.2 `full` 的读点（真正承载「来源声明」的字段）

| 位置 | 判定的后果 |
|---|---|
| [store.py:285](../../src/data/local_store/store.py#L285) `covers()` | `full=True` 时允许请求区间早于本地起点而不回退 TDX；对 000004 意味着 `end>=1991-01-14` 的请求都判「已覆盖」。本次数据确实存在，所以结果仍正确，但**判据与事实脱钩** |
| [build.py:280](../../src/data/local_store/build.py#L280) `_target_is_stale()` | `full=False` 会强制重建；`full=True` 则进入日期比价。000004 因此走的是「日期相等 → 比末行 → 相同 → 跳过」 |
| [store.py:978](../../src/data/local_store/store.py#L978) | 复权列刷新时原样继承，误标会一路传下去 |

`_target_is_stale` 的完整判据（[build.py:274-301](../../src/data/local_store/build.py#L274-L301)）**不看 `source_mtime_ns`、不看 `start`**，只看 `cov.end` 与 `.day` 末行。这正是题目问的「因为 mtime 相等而跳过重建」的真实机制：**不是 mtime，而是末行日期相等**。

### 4.3 会不会「重建时按 TDX 覆盖、把外部导入行静默删掉」

现在不会，历史上差一点。

- `cc228e1`（2026-11-06）引入 [`_merge_source_with_existing`](../../src/data/local_store/build.py#L318-L339)，按 `date` 做 upsert，docstring 明确写着「源端缺失的历史行只可能来自此前的缺口回补，整只覆盖会把它静默删掉」。这是当前唯一的保护。
- 在 `cc228e1^` 里，`_rebuild_target` 是（已导出为 `_staging/daily_index_source_mislabel_20261113/build_before_cc228e1.py`）：

```python
full = True
if since is not None:
    fresh, full = _apply_since(quote_store, kind, ts_code, fresh, since)
...
return quote_store.write_daily(kind, ts_code, fresh, full=full,
                               source_mtime_ns=path.stat().st_mtime_ns)
```

  `since is None` 时 `fresh` 就是 `.day` 单源帧，**整只覆盖**。11-05 11:52–11:54 的全市场重建（5,564 只）运行在那个版本上：只要它写了 000004，112 行就会无声消失。
- 实际没消失，是因为那一批**没有写它**（`built_at` 保持 `00:59:18`，11 点整小时 `quotes/stock` 无 000004 记录）。也就是说：**存活是「未被重写」的侥幸，不是「被保护」的结果。**

### 4.4 会不会「因为标注正确而永远不再被校验」

会，而且这是当前状态的实质。

- 112 行既不在 `.day` 里，也没有任何字段说明它来自外部；`full=True` + `source_mtime_ns == .day mtime` 的组合恰好对外宣称「整只由 TDX 构建」。
- [rescan_meta](../../src/data/local_store/store.py#L348-L356) 每次运行都会重新算出 `full=True`，**任何手工修正都会被下一次重推翻掉**。
- 没有任何代码检查「`start` 是否早于 `source_first`」。这个方向现在完全无人看守。

### 4.5 会被误用的下游

按 `daily_index` 的 `start/end/rows` 取数的消费方（都会把 000004 的起点读成 1991-01-14，且无法与 TDX 来源区分）：

| 消费方 | 位置 | 用途 |
|---|---|---|
| `universe_filter` | [universe_filter.py:323-324](../../src/data/universe_filter.py#L323-L324) | 个股在市区间（首末日行情） |
| `listing_intervals` | [listing_intervals.py:44](../../src/data/quality/listing_intervals.py#L44) | 上市区间 |
| `anomaly_lists` | [anomaly_lists.py:354](../../src/data/quality/anomaly_lists.py#L354) | 异常清单的覆盖基线 |
| `daily_advice` | [daily_advice.py:512](../../tools/daily_advice.py#L512) | 推荐池可用性 |
| `build_inventory` | [build_inventory.py:97](../../tools/lineage/build_inventory.py#L97) | 血缘盘点 |
| `etf_data_audit` | [etf_data_audit.py:84](../../tools/etf_data_audit.py#L84) | 数据审计 |
| `stale_status` | [stale_status.py:85-88](../../src/data/quality/stale_status.py#L85-L88) | 只取 `rows/end`，**不受影响** |

其中 `stale_status` 显式只读 5 列，是唯一免疫的消费方；其余按 `start` 推理的都会把「外部导入的 1991-01-14」当成「TDX 认定的上市起点」。

---

## 5. 结论与建议（④）

### 5.1 是否需要登记

**需要，登记为 G-139（K 类：工具链与产物缺陷 / 流程合规）**，与 G-130 并列而不合并：

- G-130 说的是**数据**：112 行无 TDX 旁证，unresolved，保留存量（DG-18）。
- G-139 说的是**元数据**：`daily_index` 用一个无法表达混合来源的字段组合，把这只文件标成纯 TDX。它是「即使 G-130 保留存量也依然存在」的独立缺陷，且会在 `rescan_meta` 每次运行时自我复现。

现登记表最大编号 G-138（`docs/数据缺口登记_正式表_2026-11-12.md`，共 138 个有效条目），本条顺延为 G-139。

### 5.2 最小改法（推荐 A）

给 `daily_index` 增列 `source_start`（对应 `.day` 文件首行交易日），**不改 `full` 的语义、不改任何行情行**：

- `_META_SCHEMA` 加一列（[store.py:94-105](../../src/data/local_store/store.py#L94-L105)）；
- `rescan_meta` 里 `read_first_last_trade_date(source)` 已经在取 `source_first`（[store.py:354-356](../../src/data/local_store/store.py#L354-L356)），只是当前只拿来算 `full`，顺手落列即可，**零额外 IO**；
- `_rebuild_target` 已有 `path`，可用同一个 `read_first_last_trade_date` 取首行（[build.py:401-403](../../src/data/local_store/build.py#L401-L403)）；
- 判定段：`start < source_start` 即「混合来源（含源端起点之前的外部行）」，`start > source_start` 即「前缀缺失」，两者都不该被读成 `full=True` 的纯 TDX。

兼容性：所有消费方都是显式 `select` 列名，加列不破坏任何读者；旧索引缺列时按 null 处理，`source_start` 为 null 时不参与判定。

### 5.3 备选与不推荐的理由

| 方案 | 改动量 | 问题 |
|---|---|---|
| B：`full = (start == source_first)` | 1 行 | 同时改变 `covers()` 与重建决策；把「停牌/停更导致的起点后移」与「有外部前缀」混为一谈；会让 000004 每次都进重建（虽然 upsert 使其无害），语义更糟 |
| C：`write_daily` 增 `source_kind` 参数（默认 `'tdx'`） | 5 个调用点 | 更彻底，但改不动存量文件，且**无法回溯**已写好的 2.3 万只文件；只能防未来 |
| D：只在报告里写清楚、不改代码 | 0 | `rescan_meta` 每次重推都会把结论重写成「纯 TDX」，等于缺陷会自我复现 |

推荐 A，理由：**纯派生、零 IO 增量、不动数据、不与 `full` 现有语义冲突**。C 可以作为后续增强叠加，但不应替代 A。

### 5.4 不修的风险

1. **审计结论会被反向推翻**：任何按 `daily_index` 判来源的盘点（`build_inventory`、`etf_data_audit`）都会把 112 行读成「有 TDX 旁证」，与 G-130 的 `unresolved` 直接矛盾，存在把 G-130 误判为「已收口」的实际路径。
2. **删行无人看守**：upsert 是当前唯一保护，但它保护的是「未经 `--code` 直写」的路径。任何新的整帧覆盖写入（例如再写一个不走 `_merge_source_with_existing` 的补数工具）都会让 112 行消失，而台账里**不会有任何删除记录**（`data_changelog` 只记 create/update，不记行级变化）。
3. **第二份拷贝只有一份**：112 行只存在于正式文件（8,251 行）与 `G:\git\python-quant\data\local_store\quotes\stock\sz\000004.SZ.parquet`（已核实，同为 8,251 行、1991-01-14~2026-07-13）。`_backup/internal_gap_fix/20261105/` 那份是 **8,139 行，不含这 112 行**，不能当回滚基线。
4. **同类问题会复现**：`source_mtime_ns` 的写入方式是「写盘时把 `.day` 的 mtime 抄过来」，任何非 TDX 来源只要复用 `write_daily`，都会被标成 TDX。000004 是第一个被撞见的，不是唯一可能发生的。

---

## 6. 复现命令

```powershell
# ① 全库判据 A / B + 命中行年份分布
.venv\Scripts\python.exe _staging\daily_index_source_mislabel_20261113\scan_mislabel.py

# ② 逐只日期集合差（本地有 / 源端有）
.venv\Scripts\python.exe _staging\daily_index_source_mislabel_20261113\step2_datediff.py

# ③ 差集形态分类（截断 / 内部块 / 零散）
.venv\Scripts\python.exe _staging\daily_index_source_mislabel_20261113\step3_withindiff.py

# ④ 快照与台账追溯（000004 各快照 rows / start、rows 下降清单、changelog 记录）
.venv\Scripts\python.exe _staging\daily_index_source_mislabel_20261113\step4_lineage.py

# ⑤ 字段级核对与 built_at 分布
.venv\Scripts\python.exe _staging\daily_index_source_mislabel_20261113\step5_fieldcheck.py
```

单条快速复核（一行确认 000004 命中）：

```powershell
.venv\Scripts\python.exe -c "import polars as pl,datetime;from src.data.tdx_day_parser import read_first_last_trade_date as r;from pathlib import Path;i=pl.read_parquet(r'data/local_store/meta/daily_index.parquet').filter((pl.col('data_type')=='stock')&(pl.col('ts_code')=='000004.SZ')).to_dicts()[0];d=Path(r'H:\zxzq\vipdoc\sz\lday\sz000004.day');f,l=r(d);print(i['start'],f,i['full'],datetime.datetime.fromtimestamp(d.stat().st_mtime_ns/1e9))"
```

---

## 7. 产物清单（全部在 `_staging/daily_index_source_mislabel_20261113/`）

| 文件 | 内容 |
|---|---|
| `scan_mislabel.py` / `scan_all_stock.csv` / `hits_A.csv` / `hits_A_detail.csv` / `scan_summary.json` | 判据 A/B 全库结果与规模 |
| `step2_datediff.py` / `datediff_all_stock.csv` / `datediff_summary.json` | 逐只日期集合差 |
| `step3_withindiff.py` / `withindiff_profile.csv` / `withindiff_summary.json` | 差集形态分类 |
| `step4_lineage.py` / `changelog_000004.csv` / `changelog_20261105_night.csv` / `changelog_unattributed.csv` / `rows_drop_*.csv` | 台账与快照追溯 |
| `step5_fieldcheck.py` | 字段与 `built_at` 核对 |
| `build_before_cc228e1.py` | `cc228e1^` 版 `build.py`，用于证明「整帧覆盖」旧语义 |
