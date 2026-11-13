# B4 段只读体检（深度 3 · 5 张表）

- 体检时点：2026-11-13 01:40–02:20（本地时间，Asia/Shanghai）
- 范围：`governance_order.md` B4 段的 5 张表 —— `breadth_ma_daily` / `industry_pit` / `st_intervals` / `stale_status` / `new_factors`
- 边界：不碰 B2（另两路）、不碰 B3（第三路）；`industry_pit_sw` 只作 industry 血亲旁证，不评价其覆盖（B2 范围）
- 只读声明：未修改 `data/local_store/**` 任何文件、未改任何 manifest、未提交 Git；全部结论可用第 8 节命令复算
- 陷阱规避：
  - `meta/st/st_price_inferred.parquet` 已在 2026-11-13 01:49 由另一路重跑收口，第 6 节按**新值**补做，未沿用旧值结论
  - `docs/adj_factor独立源对拍_2026-11-11.md §14.1` 的「残差归零」判据（G-122，循环论证）**未作任何基准**；本段无 adj_factor 对拍需求，因此未触发；需独立对拍时一律用通达信 `.day` 原始 OHLC（本次 1992-04-18 判定即用此法）

---

## 1 结论总览

| 表 | 指纹（行 / 字节 / sha 前 8 / mtime） | manifest 对账 | 数据本身错 | 登记陈旧·口径不同 |
|---|---|---|---|---|
| `breadth_ma_daily` | 8,654 / 330,498 / `48ae8ac5` / 11-12 21:02:19 | rows·bytes·sha **全一致** | A1 A2 | B4 |
| `industry_pit`（含 sw / change） | 11,162 / 46,620 / `f279668b` / 11-12 16:54:38 | 全一致 | A8 | B3 |
| `st_intervals` | 2,304 / 33,908 / `28042755` / 11-12 23:59:06 | 全一致 | A3 A4 A5 A6 | B7 B8 |
| `stale_status` | 9,761 / 47,010 / `c783676d` / 11-07 17:13:44 | 全一致 | A9 A10 | B5 |
| `new_factors` | 567,541 / 8,000,137 / `9da7de40` / 11-12 15:14:14 | rows·bytes·sha 一致，mtime 不一致 | 无 | B1 B2 B6 |

一句话：**5 张表都没有「结构损坏 / 键重复 / 算术恒等式破」这一层的错**；问题集中在 ① `breadth_ma_daily` 正式表内容与当前行情不同步（A1/A2）、② `st_intervals` 的 `*ST` 被降级为 `ST` 与证据未回写（A3/A4）、③ `stale_status` 时点停在 11-05（A9）。`new_factors` 三条登记项（G-083/G-085/G-096）已全部闭环，本段无需再改。

---

## 2 A 类：数据本身错（可定位、有实测数字）

### A1 `breadth_ma_daily` 正式表内容与当前行情不符 —— 正式表陈旧

用当前 `quotes/stock` 按 `tools/build_breadth_ma.py` 的文档口径（`volume>0 & adj>0`、上市满 60 个交易日、剔除 superseded、delist 后剔除）全历史复算，与表内 8,653 个共同日期逐列对拍：

| 列 | 正式表与复算不符天数 | 占 8,653 天 |
|---|---|---|
| `above_ma20` | 546 | 6.31% |
| `above_ma60` | 1,131 | 13.07% |
| `above_ma120` | 1,893 | 21.88% |
| `sample_size` | 44 | 0.51% |
| `sample_size_ma60` | 26 | 0.30% |
| `sample_size_ma120` / `sample_size_250` | 25 / 25 | 0.29% |
| `new_high_250` | 275 | 3.18% |
| `new_low_250` | 336 | 3.88% |

差分不是单点：1992–2020 每年都有 9–48 天，2026 年 2 天。

**同批产物的对照**：`_staging/breadth_ma_20261112/breadth_ma_daily.parquet`（11-12 20:59、8,653 行）与同一份复算在**上述 9 列、8,653 天全部 0 差异（100%）**。即：同一套行情 + 同一构建规则下，20:59 的产物与复算完全一致，而正式表不一致。

**可佐证来源关系**（只陈述事实）：`_backup/meta/breadth_20261113/breadth_ma_daily.parquet` 与正式表逐格 0 差异（8,654 行，mtime 同为 11-12 21:02）——说明 21:02 那份内容已被整体快照过一次，但正式表仍停在它上面，20:59 的新产物没有落到正式路径。推断为「替换/合并未落地」，具体代码路径未取证，不写死。

内部一致性（这一层是干净的）：`pct = above/sample` 三项恒等式 0 不符、`above<=sample` 0 例外、`new_high/low <= sample_size_250` 0 例外、13 列 0 空值、8,654 个日期唯一且严格升序。

### A2 `breadth_ma_daily` 单日数值：2026-11-10 `new_low_250` = 22，应为 21

| 口径 | 2026-11-10 `new_low_250` |
|---|---|
| 正式表 | **22** |
| 当前行情按构建口径独立复算 | **21** |
| `breadth_daily`（B2 产物） | 21 |
| `_staging/breadth_ma_20261112`（20:59） | 21 |

近 10 个交易日里只有这一日不一致；其余 9 日四方一致。归入 A1（正式表陈旧的具体表现），不是 `breadth_daily` 侧的问题。

### A3 `st_intervals`：25 行负长度区间，`*ST` 被降级成 `ST`

- 25 行满足 `start_date > end_date`（负长度），涉 22 只标的；全部 `flag='*st'`、`source='cninfo_notice'`、`precision='announcement'`。
- 25 行**全部**在同一 `start_date` 上存在 `flag='st'` 的孪生行（`twin_flag_set = ['st']`），所以布尔「是否 ST」不受影响，受影响的是 `*ST`（退市风险）这一层标记。
- 机制：同一公告标题同时解析出 `*st` 与 `st` 两个 impl 事件，`src/data/st_history.py` 的 `_segments_to_intervals` 用 `next_start - 1` 闭合上一段 → 同日起点必然产出 `start > end` 的空区间；落盘前未像 `_close_snapshot_rows` 那样剔除（同一文件里两条路径口径不一致）。
- 另有 8 处同码区间重叠，全部由这些负长度行与其孪生行造成（同一根因，不另计数）。

### A4 `st_intervals`：`evidence` 2,104/2,304 行为空（91.3%）

| `source` | 行数 | `evidence` 为空 |
|---|---|---|
| `szse_change_name` | 1,307 | 1,307 |
| `cninfo_notice` | 790 | 790 |
| `snapshot_anchor` | 5 | 5 |
| `cninfo_notice_dated` | 2 | 2 |
| `tushare_namechange_reviewed` | 200 | 0 |
| 合计 | 2,304 | **2,104** |

`raw_fetched_at` 同为 2,104 行为空（与 evidence 空集完全相同）。`meta/st/manifest.json` 的语义段明写「带 source/precision/evidence」，属登记承诺与实现不符；`_segments_to_intervals` 产出的 dict 里根本没有 `evidence` 字段，只有人工复核层写入。与另一路刚修好的 `st_price_inferred` 「evidence 未回写」是同一类缺陷，但**不在**该路修复范围（它只负责推断表）。不影响任何 `flag` / 区间 / 下游判定，只影响逐条追溯。

### A5 `st_intervals`：推断层输入的推断表已换版，3/19 条在新表中已无对应

`st_intervals` 于 11-12 23:59 构建，读的是旧 `st_price_inferred`（1,351 行）；新表 11-13 01:49 收口后为 1,443 行 / 831 只。逐条核对 `precision='inferred'` 的 19 行：

- 16 行在同码新推断表中仍有重叠区间，可继续支撑；
- 3 行在新表中**已完全没有对应行**：`600710.SH`、`600738.SH`、`688053.SH`（`st_price_inferred` 该码 0 行）。

结论：推断层需要基于新表重跑；且按新表口径这 3 条的推断起点已不再成立，重跑后应被重新裁定，而不是照旧保留。行数 1,351→1,443 属旧快照停在 11-04 的正常补齐（非修复副作用），已按提示采信。

### A6 `st_intervals`：375 只开口区间 + 179 只在 11-12 仍被判 ST

- `end_date` 为 null 的 375 行 / 375 只；`st_coverage` 对这 375 只的 `coverage_end` **全部 = 2026-11-12**（5,890 行无空值）。
- 以 11-12 为时点，区间命中 375 只，而 `st_status_20261112` 快照只有 196 只 ST → **多 179 只**，逐份快照核对 extra 为 179–181、missing 恒为 0。
- 这 179 只的行情末行最晚 2026-09-03，130 只早于 2026-11-01，1 只早于 2020（例：`000003.SZ`）。即：**已停止交易的码，其 ST 区间与覆盖窗口都没有在退市/末次交易处收口**，`status_at()` 又不按 coverage 门控（只做区间命中），所以今天仍返回 ST。
- 影响面：按「当日有成交」的域取码（L3 universe 主路径）不受影响；任何直接拿 `st_intervals` 当「当日 ST 名单」的用法会多 179 只。

### A7 `industry_pit`：688328.SH 边界日重叠 1 天

`industry_pit.parquet` 中该码 4 行，L1/L2 各一对，旧段 `end_date = 2026-11-12`、新段 `effective_date = 2026-11-12`：

| level | 旧段 | 新段 |
|---|---|---|
| L1 | `T07` 制造 2026-11-03 → 2026-11-12 | `T12` 信息 2026-11-12 → null |
| L2 | `T0705` 工业机械 2026-11-03 → 2026-11-12 | `T1203` 半导体 2026-11-12 → null |

as-of 2026-11-12 同时命中两段，`group_by(last)` 结果取决于行序（本次实测取到的是新段，属侥幸）。影响 1 只 / 1 天；全表仅此 1 例（`pit_overlaps = 2`，即 L1、L2 各一对）。

### A8 `stale_status`：时点停在 2026-11-05，已过期 4 个交易日

- `detected_at` 全表只有 2026-11-05 一个值；`last_trade_date` 最大 2026-11-06；而 `daily_index` 的 8 个 `data_type|market` 最新 `end` **全部是 2026-11-11**。
- 9,405 只的 `daily_index` 末行比表内 `last_trade_date` 新，日历差中位 6 天。
- 表在自身时点是自洽的：以 11-05 为锚，期望 stale 742 只 = 实际 742 只，假阳 0 / 假阴 0。
- 因此判定是「结论过期」而非「判定算错」，修法为按常规入口重跑，不需外部采集、不改变口径。

### A9 `stale_status`：1 行负滞后被记成 0

`300237.SZ`：`last_trade_date = 2026-11-06` 晚于 `detected_at = 2026-11-05`，`lag_calendar_days` 记 **0**，实际应为 **-1**。全表 9,761 行中仅此 1 行 `lag_calendar_days != detected_at - last_trade_date`。说明构建时已有 11-06 数据落进快照，但 `detected_at` 与滞后计算未同步（负值被截断或未校验）。

---

## 3 B 类：登记陈旧 / 口径不同（不是数据错）

### B1 `new_factors`：G-085「无时效上限」已修，治理文档的 B4 措辞已过期（本条最重要）

`governance_order.md` B4 把 G-085 列为「最需要停下来报」的未修项；但登记表和实测都表明它已闭环：

| 复算方式 | `accruals` | `roe_stability` | `sue` | 与表内差异 |
|---|---|---|---|---|
| 带 24 个月守卫（现行代码 `MAX_FIN_STALENESS_MONTHS=24`） | 0 | 0 | 0 | **逐值 0 差异**（567,541 行） |
| 去掉守卫（即 G-085 描述的旧行为） | +8,905 | +8,197 | +8,146 | 共 **+25,248** 个非空值 |

25,248 与登记 G-085 记录的数字**完全相等**，即：守卫确实落在数据里，"把 ≤2009 年财报值前推为当期值" 的那批格已被置空。带守卫复算与盘上数据逐值 0 差异，直接证明当前数据是 24 个月口径。

→ 结论：数据无错；`governance_order.md` B4 对 G-085 的描述是**登记滞后**，不应再按「待修、且改口径」处理。本段无需停手。

### B2 `new_factors`：G-083 符号、G-096 双表一致均成立

- `turnover_20`：`null 2,449`、`0` 值 16 个、负值 0 个、最大 0.577 → 符号正确，16 个 0 值待确认为长期停牌月（非错）。
- 与 `analysis_monthly` 键集合：两侧均 567,541 行，`key_only_nf = 0`、`key_only_am = 0` → G-096 的双表并存（列集不同）不是 DG-1 冲突。
- 空值数与登记逐项相等：`turnover_20 2,449 / accruals 15,354 / roe_stability 19,120 / sue 72,039`。

### B3 `industry_pit`：「只覆盖 10 天」是既定口径，层级差异不是错

- 与 `industry_20261112` 快照对拍 5,580 只非 unknown 标的：按前缀比较 **5,580/5,580 全部匹配，0 失配**。快照存 L3 码（`T110201`），`industry_pit` 存 L2 码（`T1102`）——粒度不同，不是错。
- `industry_pit.parquet` 的 `effective_date` 仅 2026-11-03…11-12、`end_date` 11,160 行为 null，manifest 已明写「通达信近 6 日快照视图，非正式 PIT，禁止 as-of 早于 2026-11-03 回溯」→ 覆盖短是登记口径，不是缺陷。
- `industry_change_pit` 是巨潮标准（Z 码，54,763 行，`80f8b0cd`），与 TDX（T 码）不同源、不同粒度；688328 的 TDX 变更没有巨潮侧的 change 记录属跨标准不互通，不是缺数。
- `industry_pit_sw` 12,931 行 / `in_date` 至 2026-11-02，属 B2 覆盖范围，本文不评价。

### B4 `breadth_ma_daily` 与 `breadth_daily` 的口径差（不是错）

- 日期：共同 8,653 天；仅 MA 有 1 天（1992-04-18，见 C1）；仅 `breadth_daily` 有 170 天（1990-12-19…1991-07-14，MA120 尚不可算）。
- 样本量：`total_count - sample_size` 中位 27 只、最大 193 只、667 天为 0。差在口径——MA 表要求上市满 60 个交易日且 `adj>0`，`breadth_daily` 只要求 `volume>0 & pre_close` 非空。
- `new_high_250` / `new_low_250` 两表差异：286 / 409 天非零，`|差|` 的 p95 均为 0、最大 2 / 4（占样本 0.04%/0.08%），且逐日复算证明属口径与浮点边界，不是错。
- 早期 651 天 `sample_size < 100`（末次 1993-12-07），1991–1992 年 `sample_size_250` 无样本 → `new_high_250/new_low_250` 记 **0 而非 null**。消费方若把 0 当「零只创新高」，会与「当日无 250 日样本」混淆，建议登记口径（不属数据错）。

### B5 `stale_status`：`stale_batch` 空串 9,476 行

仅 285 行为 `tdx_2019_09_17`，其余 9,476 行是空串而非 null。属元数据口径瑕疵，不影响 `is_stale`。

### B6 `new_factors`：manifest 时间戳记账不准

manifest 记 `mtime = 2026-11-12T15:30:00`，磁盘实际 `15:14:14`；`rows / bytes / sha256` 三项一致。与 G-104 同类的记账瑕疵，不是数据问题。

### B7 `st_intervals` 人工覆盖层：全部在表内，仅 14 只"有覆盖无区间"待裁定

| 覆盖层 | 行数 | 码数 | 在 `st_intervals` 内 | 备注 |
|---|---|---|---|---|
| `reviewed_conflicts` | 200 | 138 | 138 / 138 | 区间精确匹配 192 行；8 行被合并/截断，需人工确认 |
| `reviewed_pending` | 3 | 3 | 3 / 3 | — |
| `reviewed_coverage` | 64 | 64 | **50 / 64** | 14 只（`000022.SZ` 等）有覆盖记录但无 ST 区间 |
| `supplement_intervals` | 2 | 2 | 2 / 2 | — |

14 只属「已裁定覆盖但表中无 ST 段」，无法从数据本身判定是"确无 ST"还是"漏补"，单列待人工裁定，不判错。

### B8 `new_factors`：154,642 个面板外键（既定口径）

按构建函数复算会产出 716,795 个 `(code, month)` 键，其中 154,642 个是「有财报但不在 12 个月度面板」的码/月，未入表是既定口径（面板以 `analysis_monthly` 为准），不是漏数。

---

## 4 C 类：上游缺数（跨段，需记给行情/B2 侧）

### C1 1992-04-18：表是对的，缺的是 `quotes/stock` 里的那一条 bar

这一条是 `verify_breadth_ma.py` 报「8,653 vs 8,654、1 个日期缺口」的真实成因，三方分开写：

| 侧 | 事实 |
|---|---|
| **`breadth_ma_daily`（表侧）** | 有 1992-04-18 一行，`sample_size = 1` —— **与通达信原始一致，不是幽灵行** |
| **通达信 `.day` 原始（独立源）** | `h:\zxzq\vipdoc\sz\lday\sz000002.day` 含 `19920418` 记录：open 15.50 / high 15.60 / low 14.55 / close 15.00 / **volume 152,100**（非零），前后 04-17、04-20 也齐全 |
| **`quotes/stock`（行情侧）** | 该日 **0 行**（全表扫描）；同期 `1992-05-03`（周日）却有 12 行 → 早期存在非常规交易日，1992-04-18 是周六特殊交易日，行情侧漏了这一条 |
| **`breadth_daily`（B2 侧）** | 同样没有该日 → 与行情侧同源，同样缺 |
| **校验脚本** | 以 `quotes` 有效交易日为准算 8,653，本身口径没错；它只能看到行情侧，所以把「表多一行」报成缺口 |

判定：**不是表算错，是 `quotes/stock` 少 1 条 `sz000002.SZ` 1992-04-18 的 bar**（且 `breadth_daily` 同受影响）。归口在行情层（B1/B2 范围），B4 只记账不改表。修法建议：行情侧补该 bar（TDX `.day` 即证据，不需外部采集），补齐后 `breadth_daily` 与校验脚本的期望会自动对齐；在补齐前不要为了让校验脚本变绿而删表内该行——删了才是丢数据。

---

## 5 `verify_breadth_ma.py` 三项不通过的判因（三侧分开）

| # | 脚本报的现象 | 判定 | 支撑数字 |
|---|---|---|---|
| ① | 2026-11-11 `sample_size` 5,528 / 期望 5,533 | **校验脚本口径过期**（表是对的） | 表 5,533；独立复算 5,533；20:59 staging 5,533。脚本 L322–323 把期望值**硬编码**为 `5528`，而 `last_day` 随表推进到 11-11；脚本自己第 2 段复算 `counters[last_day]['ma20_ok']` 也算出 5,533，只是没被用于这条断言 |
| ② | 近 10 日 `new_low_250` 与 `breadth_daily` 不一致 | **表侧错（正式表陈旧）**，`breadth_daily` 侧无问题 | 仅 2026-11-10 一天：正式表 22 / 复算 21 / `breadth_daily` 21 / staging 21。其余 9 日四方一致；`breadth_daily` 的近 10 日与新复算 10/10 一致 |
| ③ | 日期连续 8,653 vs 8,654，1 个缺口 | **表侧无错；缺口在 `quotes/stock`**（见 C1）；校验脚本口径也没旧 | 区别仅在 1992-04-18：TDX `sz000002.day` 有该日且 volume=152,100；`quotes` 0 行；`breadth_daily` 亦无该日 |

脚本退出码 1 是上述三项共同所致；①②③ 修完后应归零，其中 ① 只需去掉硬编码（改用 `counters[last_day]`），②③ 依赖正式表与行情的收口，**本次均未改动**。

补充：脚本按样本年份（1997 / 2004 / 2012 / 2018 / 2023）做的独立复算在正式表上全部通过，与本次 A1 的结论不矛盾——A1 的差异分布在**非样本日**，脚本的抽样点恰好都落在一致日上，说明现有抽样不足以发现 A1，建议把抽样改为「全量对拍 + 差异日抽样复核」。

---

## 6 `st_price_inferred` 收口后补做的条目（原单列项已补）

新表实测与来件一致，列为已验证：

| 项 | 来件口径 | 实测 |
|---|---|---|
| 行数 / 码数 | 1,443 / 831 | 1,443 / 831 ✓ |
| 字节 / sha256 | 64,928 / `61495f74…4237` | 64,928 / `61495f74735df2c53da1b345021ee3cac25f2fd57555b53ca23de9275af54237` ✓ |
| `evidence.end == end_date` | 全量 1,443、不符 0 | 逐行解析 1,443 行，不符 **0** ✓（旧表 13 行「证据短于区间」现象在新表为 0） |
| `end_date` 为 null | — | 13 行（`end_max = 2026-09-14`），即尚未闭合段 |
| `source` / `confidence` | — | 全部 `price_inference`；`high` |

由此产生的 B4 侧影响只有一条（A5）：`st_intervals` 的推断层是照旧表建的，3/19 条在新表已无对应，需随新表重跑。除此之外，本报告其他结论**不依赖** `st_price_inferred`，无旧值残留。

---

## 7 停手项与修法建议（本次一律未执行）

### 7.1 需改正式数据 → 只给方案，等指令

| 项 | 建议修法 | 影响面 | 回滚 |
|---|---|---|---|
| A1/A2 `breadth_ma_daily` | 用 `_staging/breadth_ma_20261112/breadth_ma_daily.parquet`（8,653 行，与复算 9 列 100% 一致）替换正式表；替换前把现有正式目录整体移到 `_backup/`（`_backup/meta/breadth_20261113/` 已有一份同内容快照，可直接复用，不用再复制） | 8,653 行 × 12 列；所有读该表的下游（宽度统计、报告）在 546–1,893 天上的旧值会变 | 快照已在 `_backup/meta/breadth_20261113/`，可整目录回滚 |
| A3 `st_intervals` 25 行 `*ST`→`ST` | 同日多 flag 合并时取「最强 flag」（`*st` 优先），或在 `_segments_to_intervals` 落盘前剔除 `start > end` 的空区间（与 `_close_snapshot_rows` 口径对齐） | 22 只、25 个起点；只影响区分 `*ST/ST` 的消费方，布尔「是否 ST」不变 | 表级快照回滚 |
| A4 `st_intervals` evidence | 在 `_segments_to_intervals` 落 `evidence`（深交所变更记录序号 / 巨潮 `notice_id`），或在 manifest 改述为「仅人工复核层带证据」 | 纯追溯字段，不改任何判定 | 无需回滚（只增列值） |
| A5 推断层 | 基于新 `st_price_inferred` 重跑；3 条（`600710.SH`/`600738.SH`/`688053.SH`）重跑后按新表裁定 | 19 行推断面，可能牵动 `status_at` 的起止日 | 表级快照回滚 |
| A6 179 只开口 | 用退市日/行情末行闭合 `end_date`，并把 `st_coverage.coverage_end` 同步封口（本地数据即可，不需外部采集） | 375 行开口中的 179 只；`status_at` 对死码不再返回 ST | 表级快照回滚 |
| A7 688328 边界 | 旧段 `end_date` 改为 2026-11-11（或新段 `effective_date` 改 11-13），保证 as-of 唯一 | 1 只 / 1 天 | 表级快照回滚 |
| A8 `stale_status` | 重跑 `src/data/quality/stale_status.py`（常规入口，口径不变） | 9,761 行全部时点前移 4 个交易日 | 表级快照回滚 |
| A9 负 lag | 重跑时补负滞后校验（`lag` 允许为负或直接报错），不要静默截断 | 1 行 | 同上 |
| C1 行情缺 bar | 由行情侧补 `sz000002.SZ 1992-04-18`（TDX `.day` 即证据），再重建 `breadth_daily` | 1 只 / 1 天；补后校验脚本期望自动对齐 | 行情侧按单 bar 补，可逆 |

### 7.2 不需要停手、也不需要外部采集的

- B1/B2 `new_factors`：无动作。建议把 `governance_order.md` B4 对 G-085 的描述改为「已修（24 个月守卫，25,248 格已置空）」，避免后续再按「待修 + 改口径」处理。
- B6 manifest mtime：下次写 manifest 时顺手纠正即可。

### 7.3 需要人工裁定（数据本身判不出来）

- B7 的 14 只「有 `reviewed_coverage` 覆盖、无 ST 区间」：需人确认是"确无 ST"还是"未补"。
- B2 的 16 个 `turnover_20 = 0`：需确认是否长期停牌月。

---

## 8 复现命令与实测文件

```powershell
# 五表指纹 + 内部一致性 + 跨表对拍
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4_slice1_probe.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4_slice1_probe2.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4_slice1_probe3.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4_slice1_probe4.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4_slice1_probe5.py

# breadth_ma 三项判因 + 全历史独立复算
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4_breadth_probe.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4_breadth_probe2.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4_breadth_recompute.py

# 1992-04-18 独立源（通达信 .day，需读 h:\zxzq\vipdoc）
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4_tdx_1992.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4_tdx_1992b.py

# st_price_inferred 收口后补做
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4_stpi_check.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4_stpi_check2.py

# 校验脚本（报告写到工作目录，不覆盖既有 reports/）
.venv\Scripts\python.exe tools\verify_breadth_ma.py --report 2026-11-12\data-lineage\work\verify_breadth_ma_run.md
```

实测 JSON 落点（全部在 `2026-11-12/data-lineage/work/`）：`b4_slice1_probe.json`、`b4_slice1_probe2.json`、`b4_slice1_probe3.json`、`b4_slice1_probe4.json`、`b4_slice1_probe5.json`、`b4_breadth_probe.json`、`b4_breadth_probe2.json`、`b4_breadth_recompute.json`、`verify_breadth_ma_run.md`。

---

## 9 本次自行决定并执行的事（供事后复核）

1. 为判定 1992-04-18 性质，读了仓库外 `h:\zxzq\vipdoc` 的通达信 `.day` 原始文件（只读、只读 400 个文件）；这是唯一一次仓库外读取，用于替代 G-122 那套循环论证判据。
2. `st_intervals` 的 evidence 空值判定为「数据缺失（证据未回写）」，未采纳「schema 设计为仅人工层填写」这一解释——依据是 manifest 语义段明写每行带 evidence，两者不符需以登记为准；若维护者认为应以实现为准，则应改 manifest 而不是补数据。
3. 未在 `breadth_ma_daily` 的「正式表陈旧」上给出「删行修绿校验脚本」的路径，反而明确反对——因为 1992-04-18 经 TDX 原始证明是真实交易日。
4. 报告与探针脚本均未提交 Git、未写入 `data/local_store/`、未改任何 manifest。

---

## 10 修订记录（2026-11-13，修复链执行后）

本节只记录被后续实测推翻或细化的条目，原文保留以便追溯。

| 条目 | 原结论 | 修订后 |
|---|---|---|
| A1 | 正式表陈旧，`_staging/breadth_ma_20261112`（20:59）与独立复算 100% 一致 | 差值不是「一天」，是 **2,687 天数值差 + 1 天日期集合差**。逐日清单见 `b4_a1_diff_days.csv`；2,687 天全部落在 11-12 被改写的 70 个 quotes 文件跨度内 |
| A8 | `stale_status` 停在 11-05、已过期 | **已修**：重建为 9,787 行、基准日 11-11，与 `daily_index` 逐值 0 差异 |
| A9 | 1 行负 lag 被记成 0 | **已修**（`src/data/quality/stale_status.py:119` 改有符号），并加反例单测 |
| C1 | 「表是对的，缺的是 quotes 里那一条 bar」；把 1992-05-03 当作可疑行 | **半对**：表在 04-18 这一天确实对，但成因是 quotes 把 `000002.SZ` 该 bar 的日期改写成了 04-20；**1992-05-03 不是多余 bar**，TDX 中 SZ 多只标的当日确有记录（周日特别交易），此前怀疑应撤回 |

详见 `b4_fix_chain_20261113.md`。本报告第 7 节「停手项」中 A1/A8/A9 的执行状态以下一份文档为准。
