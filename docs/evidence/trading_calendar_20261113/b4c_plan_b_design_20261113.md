# 方案 B 落地设计稿：把 `get_calendar()` 收口到「四指数并集 + 单一 session 台账」

- 状态：**只出设计，未改代码、未改数据、未重建、未提交**
- 生成日：2026-11-13
- 上游依据：`b4b_calendar_semantics_eval_20261113.md`（阶段二评估）、`b4_fix_chain_20261113.md`（阶段一修复链）
- 本文所有天数/行数均由本机实测得出，复现命令见 §7

---

## 0 前提与边界

### 0.1 前提

`1992-04-18` 已定案为 **B**：真实交易日是 `1992-04-20`，本地现状正确，**不执行改回**。
因此阶段一/二里「两处 adv20 翻转由日历多一天 `1992-04-18` 导致」的归因**作废**，本稿按 B 重核（§0.2）。

### 0.2 两处翻转的真实成因（按 B 重核，已用数字闭环）

先排除 `04-18`：六层正式表 `l4_liquid.parquet` 的日期集合

```text
has_1992_04_18 = false
has_1992_04_20 = true
l4_date_min    = 1990-12-19
```

六层走的是 SH 指数日历，**从 1990-12-19 起从未包含 `1992-04-18`**；`1992-04-18` 只影响 `breadth_ma`（自建 quotes 日历）。所以两处翻转与它无关。

真实成因是 `_quote_panel` **旧版本**的日期映射缺陷（`tools/build_universe_layers.py:172` 起，现行版本已改成精确匹配）：

| 日期 | 000009.SZ 周日 bar | 当日真 bar | 旧实现（`searchsorted(side='left')` + `unique(keep='first')`） | adv20 结果 |
|---|---|---|---|---|
| 1992-10-12 | 1992-10-04（周日）amount **11,884,000** | 10-05 amount **8,445,000** | 周日 bar 被顶到 10-05，其实 bar 被 `keep='first'` 丢弃 | `(11,884,000−8,445,000)/20 ≈ +171,950` → 19,995,600 → **20,167,550 ≥ 2e7 = true**（正式层 true） |
| 1993-01-18 | 1993-01-03（周日）amount **14,173,000** | 01-04 amount **21,671,000** | 同上 | `(14,173,000−21,671,000)/20 ≈ −374,900` → 20,257,450 → **19,882,550 < 2e7 = false**（正式层 false） |

用当前行情 + 精确匹配重算两天：`1992-10-12 = 19,995,600 (false)`、`1993-01-18 = 20,257,450 (true)`，与 `recomp_l4_liquid.parquet` 完全一致。

**结论（对本稿的影响）**：即使日历一个字节都不动，只要重跑六层，这两处**必然翻转**。所以本轮要把两笔影响分开记账——

- 影响 A：`_quote_panel` 已修但正式表仍是旧版 → 重跑即翻转 2 处；
- 影响 B：日历扩张 `8,761 → 8,819` → 网格与 adv20 窗口变化。

验收时必须能分别给出 A、B 两份差异清单（§5.4 的三次构建法），不得合并成一句「日历改了所以变了」。

### 0.3 不在本稿范围

- 不裁定 `1993-05-01/02`、`1992-04-18` 的最终归属（前者已由 11-12 20:28~20:39 的位移消解，后者已结案）。
- 不新增第三方数据源、不改 `quotes/**`、不动 `breadth_ma_daily` / `state_vector_daily`。

---

## 1 三源并集后的日期集合

### 1.1 逐项增减

以现行权威日历（`000001.SH` 权威段，8,761 天）为基线：

| 步骤 | 规则 | 结果天数 | 增减 | 分布 |
|---|---|---|---|---|
| S0 | 现状：`000001.SH` 权威段 | **8,761** | — | 1990-12-19 ~ 2026-11-11 |
| S1 | ∪ `399001.SZ` / `399002.SZ` / `399003.SZ` / `000300.SH` | **8,806** | **+45** | 1991: 40 / 1992: 4 / 1993: 1；**全部是周末** |
| S2 | ∪ registry session（`weekend_session_1991` 53 天 + `upstream_omitted_session_1992` 3 天 = 56 天，其中 43 天已被 S1 覆盖） | **8,819** | **+13** | 全部 1991：01-05/12/19/26、02-02/09/23、03-02/09/16/23/30（12 个周六）+ 1991-11-17（周日） |
| S3 | ∪ G-112 `keep` 日期（175 行 / 51 天） | **8,819** | **+0** | 51 天全部已被 S1/S2 覆盖 |
| S4 | − 显式排除 | **8,819** | **−0** | 见 §1.3 |
| — | **最终** | **8,819** | **+58（+0.66%）** | |
| S5 | （对照，**不采纳**）∪ 个股 bar 并集（不设白名单） | 8,825 | +6 | 1993-06-05/19、07-03/17、08-07/21，**6 天全被 G-112 判 `drop`** |

**为什么 S5 不采纳**：个股 bar 并集的 19 个「指数并集里没有」的日期，构成为

```text
12 个 1991 早期周六  → 已在 S2（registry weekend_session_1991）
 1 个 1991-11-17 周日 → 已在 S2（registry weekend_session_1991）
 6 个 1993 周六       → G-112 全部判 drop
```

即：**个股 bar 并集作为「日期来源」，贡献的全是「已被其它证据覆盖」或「已被判无效」的日子，没有一天是它独有的新证据**。把它写成日期来源，等于让 6 个被 G-112 否掉的周六借道进入网格，并给未来任何一条错标日期的个股 bar 开后门（`tests/test_stock_basic_calendar_source.py` 的原始目的是挡这个）。

设计取舍：**个股 bar 并集只做对拍项，不做日期来源**。

### 1.2 1991-01-05~03-30 那 12 个周六能否进入网格

**能，但通道是 registry，不是个股 bar 并集。**

- 这 12 天全部在 `weekend_session_1991`（53 天）里；G-112 在这 12 天共判 **25 行 keep**，逐日 `1/1/2/1/3/3/0/3/2/2/2/2/3`。
- `1991-02-16` keep 行数为 **0**，且不在 registry、不在任何指数 → 不进网格（无 bar，无影响）。
- 若只做四指数并集（8,806）而不并 registry：这 12 天全部落在网格外，`_quote_panel` 的精确匹配（`tools/build_universe_layers.py:172`）会把这 **25 行静默丢弃**（这正是阶段一 A1 里「日历缺一天 → bar 无声消失」的同一机制）。
- 所以「那批 bar 是不是永远被丢弃」的答案是：**只要 S2 落地就不是**；只做 S1 则一定是。
- 另需注意：`1991-11-17`（registry session、G-112 判 drop 1 行）会进网格但当天无有效 bar——**日历含该日 ≠ 该 bar 有效**，两层职责分开（§2.4）。

### 1.3 排除表实际生效 0 天，但必须保留

`meta/anomalies/weekend_calendar_registry.json` 的 19 个「非 session」日期，分两类：

| 类 | 天数 | 是否在任何并集里 | 处置 |
|---|---|---|---|
| `nav_placeholder_weekend`（2025-11-29 ~ 2026-10-31，基金净值占位，`volume=0`/`amount=0`） | 16 | **否** | 必须留在排除表：一旦删掉，`session_guard.filter_nontrading_rows` 会开始放行基金周末净值占位行 |
| `upstream_closed_local_bar_1992`（1992-02-01、02-02、10-04） | 3 | 是（SZ 指数有 bar） | 02-02 / 10-04 同日另有 `upstream_omitted_session_1992`（30 行）→ 日期级取 **session**；02-01 见下 |

`1992-02-01` 是本轮唯一真分歧：G-112 判 **keep 2 行**（`factor_lib` 白名单口径写明「1991 年 50 个周六 173 行 + 1992-02-01 2 行」），registry 判 **上游休市 / 本地独有**。按 DG-18：挂 `unresolved`、不择一覆盖、保留本地存量 → **本稿选择「日期进网格」**（否则那 2 行真实观测在六层里被永久丢弃），金额代价是 1 个日期 / 约 15 行网格，且当日只有 2 只有 bar、其余标的一律 `traded=False`（不影响任何永久结论）。

> 决策点（已带建议，按「带建议即执行」）：`1992-02-01` 类型记 `unresolved_session`，日历取 **8,819**。若日后第三源证明上游确实休市，则从台账删该行 → 日历回到 **8,818**，并把那 2 行改判为 drop。

---

## 2 排除表与 registry 收敛为一份（DG-1）

### 2.1 结论与路径

- **唯一正式路径**：`data/local_store/meta/trading_calendar/session_ledger.parquet`（同目录 `manifest.json`）
  - 选这个目录的理由：`meta/trading_calendar/` 已存在且已登记权威路径（`docs/数据权威路径清单.md` L106 的 `coverage.json`），日历语义不新开目录；`official_future.parquet` 已于 2026-11-12 退役到 `_backup/trading_calendar_official_future_20261112/`，该目录当前只有 `coverage.json`（587 B），无重名风险。
- **被取代**：`meta/anomalies/weekend_calendar_registry.json`（1,079 行 / 41,244 B / SHA256 `BB21CDDA…FDEBE`，当前**零运行期消费者**）→ 按 DG-13 整体移到 `_backup/meta/anomalies_registry_20261113/`，并在权威路径清单的「已废弃路径」表登记 SHA256。
- **只登记例外，不物化全量**（关键设计选择）：台账只有 **73 行**（57 行 `session` + 16 行 `closed`），占 8,819 天的 0.83%；普通交易日的日期由「四指数并集」这一条**代码规则**给出，不落表。
  - 理由：若物化成 8,819 行全量日历，`get_calendar()` 就依赖一张必须每日刷新的表，一旦刷新链断掉，日历会停在旧日期（比现在更脆）；而并集规则有单测覆盖、可审计、无版本号，不构成「第二处真相」——**DG-1 管的是同一语义只能有一个正式路径，台账是唯一一份例外裁定，并集是唯一一条推导规则**。

### 2.2 台账 schema

| 列 | 类型 | 必填 | 语义 |
|---|---|---|---|
| `date` | `Date` | ✓ | 主键（唯一），自然日 |
| `status` | `Utf8` | ✓ | `session` / `closed`（**二值，不允许空**） |
| `kind` | `Utf8` | ✓ | 子类型：`weekend_session_1991` / `upstream_omitted_session` / `unresolved_session` / `nav_placeholder_weekend` / `upstream_closed_local_bar` |
| `evidence` | `Utf8` | ✓ | 判据一句话（例如「1991 深市六天工作周，index:399001.SZ 当日有 bar」） |
| `source` | `Utf8` | ✓ | `index:399001.SZ` / `baostock+sina` / `tdx_only` / `g112` / `manual` |
| `evidence_path` | `Utf8` | ✓ | 仓库相对路径（+ 行号或代码），如 `docs/evidence/g112_nontrading_bars/nontrading_bars_verdict.csv` |
| `effective_from` | `Date` | ✓ | 生效起点（本稿统一 `1990-01-01`） |
| `effective_to` | `Date` | ✓ | 生效终点（本稿统一 `2099-12-31`） |
| `unresolved` | `Boolean` | ✓ | DG-18 挂起标记；`true` 的行必须在 `note` 写明缺哪份证据 |
| `decided_at` | `Date` | ✓ | 裁定日（迁移行统一 `2026-11-13`） |
| `note` | `Utf8` | 可空 | 同日多标的裁定冲突、位移记录等 |

强制约束（构建脚本 + 单测双向校验）：

1. `status ∈ {session, closed}`，`date` 唯一，无空值；
2. `status='closed'` 的行**优先级最高**——即便四指数在该日有 bar 也必须剔除（这是排除表相对并集的唯一特权）；
3. `unresolved=true` 的行必须 `status='session'`（按 DG-18 保留存量）；
4. `kind='nav_placeholder_weekend'` 必须 `status='closed'`；
5. 台账**不得包含** `1993-05-01`、`1993-05-02` 任一 status 行（防误放行，§6.3）。

### 2.3 迁移映射

| 原 registry kind | 行 / 天 | 目标 `status` | 目标 `kind` | 备注 |
|---|---|---|---|---|
| `weekend_session_1991` | 218 / 53 | `session` | `weekend_session_1991` | 1991 深市六天工作周；1991-09-29、11-17 与 G-112 drop 冲突 → **日期级 session、行级 drop**，冲突写 `note` |
| `upstream_omitted_session_1992` | 30 / 3 | `session` | `upstream_omitted_session` | 02-02、05-03、10-04 |
| `upstream_closed_local_bar_1992` | 5 / 3 | 02-02、10-04 → `session`；02-01 → `session` + `unresolved=true` | `upstream_omitted_session` / `unresolved_session` | 同日多标的裁定不同，日期级取「有 session 证据即 session」 |
| `weekend_zero_volume_placeholder_1992` | 1 / 1（10-04） | 不单列 | — | 行级属性（`volume=0`），已由 10-04 的 session 行覆盖 |
| `weekend_date_shift_1992` | 1 / 1 | 不迁移 | — | 04-18→04-20 改期已执行且已结案（G-133），留痕进 `logs/data_changelog.csv` |
| `nav_placeholder_weekend` | 824 / 16 | `closed` | `nav_placeholder_weekend` | 基金净值占位；`evidence_path` 指向 registry 旧快照 |
| 1993 年 49 行（`uncovered_scope`） | 49 / N | **不迁移** | — | 保持未覆盖，写进 `manifest.json` 的 `coverage.uncovered_1993={rows:49}`；其中 01-03 由 S1 进入网格、6 个周六由 G-112 挡住 |

去重后 **73 行**（57 session + 16 closed）。迁移脚本必须断言：`去重前行数 = 1,079`、`去重后行数 = 73`、新增/减少逐行可解释。

### 2.4 `get_calendar()` 怎么读它

`_build()`（`src/data/trading_calendar.py:170-207`）改成分三步：

1. **并集**：`dates = ∪ _read_index_dates(root, code) for code in BENCHMARK_CODES`（四指数 + `000300.SH`）
2. **台账覆盖**：
   - `dates |= ledger.filter(status=='session').date`
   - `dates -= ledger.filter(status=='closed').date`  ← 排除优先，权限高于并集
3. **尾部近似**：`tail = _weekday_dates(dates.max()+1, as_of+366, holidays)`，`authoritative_end = dates.max()`（与现逻辑一致）

台账缺失/不可读时**不得回落成「只读 000001.SH」**（DG-2）：直接抛 `RuntimeError('交易日台账缺失或为空')`，并保留 `weekday_fallback`（全无指数文件）这条既有退化分支——它退化的是「所有源都不可用」，不是「新路径缺失回落旧路径」。

`source` 语义（改）：`local_index:000001.SH` → **`local_index:union(5)+ledger76`**
- 前段写明是并集、括号里给成员数，便于 `session_guard` / `build_stock_basic` 的日志与列值一眼看出日历代际；
- `ledger76` 是台账行数（73 行 → `ledger73`），随台账变化自动变，等于给日历打了一个免费的代际指纹。

### 2.5 行级 vs 日期级：两层职责必须分开写死在文档里

| 层 | 决定 | 载体 |
|---|---|---|
| 日期级 | 该自然日是否进网格 | `session_ledger` + 四指数并集 |
| 行级 | 该 `(ts_code, date)` bar 是否可用 | `docs/evidence/g112_nontrading_bars/nontrading_bars_verdict.csv`（keep 175 / drop 45） |

`keep_dates_not_in_final` 实测为 **`['1992-02-01']`** 的旧账在本稿下归零（该日已进网格）；反过来 `reject/drop 日期在 final` 有 **10 天**（1991-09-29、1991-11-17、1992-05-03、1993-01-03、1993-06-05/19、07-03/17、08-07/21）——**日历含该日 ≠ 该 bar 有效**，不得因为「日期进了日历」就把 drop 行放回面板。

---

## 3 `get_calendar()` 改动点与 11 处调用方

### 3.1 代码改动清单

| # | 位置 | 现状 | 改成 | 说明 |
|---|---|---|---|---|
| C1 | `src/data/trading_calendar.py:53` `BENCHMARK_CODES` | `('000001.SH', '000300.SH')` | `('000001.SH', '399001.SZ', '399002.SZ', '399003.SZ', '000300.SH')` | 常量名保留（多处 import），语义改为「并集成员」；docstring 同步 |
| C2 | `src/data/trading_calendar.py:193-200` `_build` 取数 | `for code in ...: 取第一个非空就 break` | 全部读、求并集 | 去掉 `break`；`source_code` 改为成员列表 |
| C3 | `src/data/trading_calendar.py:201-207` 返回 | `source=f'local_index:{source_code}'` | `source=f'local_index:union({len(codes)})+ledger{ledger_len}'` | §2.4 |
| C4 | 新增 `_read_session_ledger(root)` | — | 读 `meta/trading_calendar/session_ledger.parquet` 的 `date/status`；缺失抛错 | DG-2 不回落 |
| C5 | `src/data/trading_calendar.py:214-222` 缓存 key | `stamps` 只含 `BENCHMARK_CODES` 的 mtime，取 `max` | 5 个指数 mtime + **台账 mtime**，取 `tuple` 或 `sum` | 现用 `max(stamps)`：台账在同目录且更晚写入时不会失效 → 必须改 |
| C6 | `src/data/trading_calendar.py:210-228` 签名 | `(root, *, as_of, holidays, refresh)` | **不变** | 见 §3.2 |
| C7 | `tools/build_universe_layers.py:172` `_quote_panel` | 已精确匹配 | **不许退回**；补「孤儿 bar 必须报错」（§4 T5） | 这是阶段一起因的守卫 |
| C8 | `tools/build_universe_layers.py:83-94` `UPSTREAM_FILES` | 只登记 `000001.SH` | 加 `399001.SZ/399002.SZ/399003.SZ` + `meta/trading_calendar/session_ledger.parquet` | manifest 上游 SHA 必须能证明用的是哪一代日历 |
| C9 | 新增 `tools/build_session_ledger.py` | — | 从 registry + index 并集 + G-112 + 旧排除表生成 73 行台账，走 staging→对拍→`write_parquet_atomic` | 唯一写入口 |
| C10 | `tools/build_stock_basic.py:206-222` docstring | 「权威段 = 本地基准指数 `000001.SH` 的实际交易日」 | 改为「四指数并集 + 台账」 | 描述失真会误导后续判读 |

### 3.2 签名要不要加区间参数：**不加**

- 日历对象是全历史序列，`between/count/previous/next` 已经承担区间语义；再加 `start/end` 会让同一份数据在不同调用点得到不同日期集，破坏「一份日历」可比性。
- 现有 `as_of` 已经控制「近似尾部到哪里」，这是唯一需要按调用场景变化的维度。
- 加区间会让缓存 key 维度爆炸（每个窗口一个 key），而 `TradingCalendar` 本身才 8,819 个 date（约 70 KB），没必要分段。
- 唯一新增的读参数是台账路径——由 `root` 推导，不开新形参（避免调用方忘传）。

### 3.3 13 个调用点逐处结论

| # | 位置 | 用法 | 要不要改 | 要不要重建/重跑 |
|---|---|---|---|---|
| 1 | `src/data/quality/clean_set.py:599` | `self.calendar` + `calendar_ord`；`gaps = max(0, expected − quote_dates.size)` | 不改 | **要重跑**（`gaps` 会变，见 §5.2） |
| 2 | `src/data/quality/extended_builders.py:173-178` | `position_of = {day: pos for pos, day in enumerate(calendar.dates)}` | 不改 | **要重跑**（全表 `cal_pos` 后移；输出若有位置列必变） |
| 3 | `src/data/quality/tradable_universe.py:316-352` | 权威段 + `count` 算 `missing_rows`；指数连续 ≥20 日空洞 | 不改 | **要重建**（口径说明见 §5.2） |
| 4 | `src/data/quality/freshness.py:272-279` | `count(latest, today)`，`latest` 均在 2026 | 不改 | 不必 |
| 5 | `src/data/quality/freshness_check.py:292-305` | 同上 | 不改 | 不必 |
| 6 | `src/data/events.py:740-752` | `trading_dates()` → `blocked_map` | 不改 | **要重跑对拍 0 差异**（1991–1993 无事件） |
| 7 | `src/data/universe_filter.py:539-549` | `_trading_window` + `calendar.previous(day, inclusive=False)` | 不改 | 需补单测：`day` 落在新增周末时 `previous` 结果（§4 T7） |
| 8 | `src/quant/factor_lib/data.py:60-140` | `calendar_filter`（默认 `False`）/`calendar_filter_mode='whitelist'` | 不改 | 默认口径**不必重建**；开启白名单的面板需清缓存重跑 |
| 9 | `tools/advice_stateful_backtest.py:2483` | `between(start, end)` 定窗口 | 不改 | 仅当回测窗口覆盖 1991–1993 才变 |
| 10 | `tools/advice_stateful_backtest.py:3226` | 同上 | 不改 | 同上 |
| 11 | `tools/build_multi_asset_etf_pool.py:109-113` | `authoritative_dates()` | 不改 | ETF 起始 2004+，**不必重跑** |
| 12 | `tools/build_stock_basic.py:216-222` | `authoritative_dates()` | 仅改 docstring（C10） | **要重跑对拍**（权威段 +58） |
| 13 | `src/data/local_store/session_guard.py:103` | 用 `calendar` 过滤入站行 + 日志 `calendar.source` | 不改 | 需补单测：`nav_placeholder` 仍被拒（§4 T8） |

补充（第 14 个，不是 `get_calendar` 调用但同源）：`tools/build_breadth_ma.py:135-146`、`tools/build_state_vector.py:271-276`、`tools/verify_breadth_ma.py:116-119` 各自用 `quotes/stock` 并集建日历，**不读 `get_calendar`** → 本轮**不重建、不修**，`verify_breadth_ma` 的 `8,653 vs 8,654` 不会因本方案消解（阶段二结论在方案 B 下依然成立）。

---

## 4 需要同步改的单测与断言

### 4.1 现存用例：预期变红 1 条

| 用例 | 断言 | 结果 |
|---|---|---|
| `tests/test_trading_calendar.py::test_local_index_is_authoritative_and_excludes_holidays` | `cal.source == 'local_index:000001.SH'` | **必红** → 改为断言 `source.startswith('local_index:union(5)+ledger')`，并加断言「台账缺失时抛错」 |
| `tests/test_trading_calendar.py` 其余（fallback / count / between / previous / next / 空目录） | 3 天夹具、无台账 | 需在夹具里补一张最小台账（或让台账缺失可注入），否则会因 C4 抛错而**连带变红** |
| `tests/test_stock_basic_calendar_source.py::test_trade_calendar_ignores_breadth_weekend_bars` | 周六不出现在 `load_trade_calendar` | 通过（夹具只有 `000001.SH`，并集仍是 3 天）；但语义已不够——必须新增 T3 |
| `tests/test_build_trading_calendar.py` | `read_quote_authoritative_end` 遍历 `BENCHMARK_CODES` 取最大末行 | 通过（5 个里只有 1 个存在时结果不变） |
| `tests/test_universe_layers_quote_mapping.py`（3 条） | `_quote_panel` 给显式日期表 | 全部通过（函数级，不依赖 `get_calendar`） |
| `tests/test_freshness_check.py:111`、`tests/test_factor_lib.py:356/366`、`tests/test_extended_builders.py:22` | monkeypatch `get_calendar` / `reset_calendar_cache` | 通过 |

### 4.2 新增用例（8 条，全部要能在缺陷下失败）

| 编号 | 用例 | 断言（要点） | 反例自测方式 |
|---|---|---|---|
| T1 | `test_calendar_union_includes_sz_index_weekend_session` | 夹具写 `399001.SZ` 一个周六 bar、其余指数缺失 → 该周六 `is_trading_day=True`；`source` 含 `union(5)` | 把 `_build` 改回「取第一个非空」→ 必红 |
| T2 | `test_calendar_ledger_session_adds_and_closed_removes` | 台账 `session` 日进日历；`closed` 日**即使指数有 bar 也剔除** | 删掉 `dates -= closed` 一行 → 必红 |
| T3 | `test_calendar_ignores_unregistered_stock_only_weekend_bar` | `quotes/stock` 有周六 bar、指数无、台账无 → **不进日历** | 把个股并集写成日期来源 → 必红 |
| T4 | `test_calendar_excludes_1993_05_01_02` | 真库上 `is_trading_day(1993-05-01/02) == False`；台账无 `1993-05-01/02` 行 | 手工往台账加一行 session → 必红 |
| T5 | `test_build_universe_layers_raises_on_bar_outside_calendar` | **日历缺一天时 `build_universe_layers` 必须报错而不是静默丢弃**：网格外 bar 的日期集合必须严格等于白名单 `expected_orphan_dates`（当前为 6 个 1993 周六），多一天即 `SystemExit` | 把 `_quote_panel` 换回 `searchsorted` 旧实现 → 孤儿数骤增 → 必红；从台账删 `1991-09-29` → 孤儿多 1 天 → 必红 |
| T6 | `test_layer_l4_adv20_uses_calendar_positions` | adv20 窗口按 `cal_idx` 连续取 20 行，新增日插入后窗口行数不变、空值不参与均值 | 改成按 `ts_code` 行号滚动 → 必红 |
| T7 | `test_previous_trading_day_across_registered_weekend` | `previous(1991-04-08)` 在含 1991 周六的日历下返回 1991-04-06 而非 04-05 | 用旧日历 → 必红 |
| T8 | `test_session_guard_still_rejects_nav_placeholder_weekend` | 台账 `closed` 的 16 个净值占位周末仍被 `filter_nontrading_rows` 剔除 | 从台账删 `closed` 行 → 必红 |

另加一条静态断言（照 `tests/test_patrol_local.py::test_module_has_no_delete_calls` 的写法）：

| T9 | `test_single_calendar_truth_path` | 全仓 `src/ tools/ tests/` 中 `weekend_calendar_registry` 命中数 = 0；`session_ledger.parquet` 只在 `trading_calendar.py` 与 `build_session_ledger.py` 出现 | 恢复任一旧引用 → 必红 |

**断言必须能对空值失败**（DG-31）：T5 的孤儿计数用「集合相等」而非 `!=`；T2/T3 用 `is_trading_day` 布尔而非「长度差」；台账校验用 `status.is_null().any()` 显式判空，不允许仅靠 `filter` 后行数。

---

## 5 重建与执行顺序

### 5.1 逐表结论

| 顺序 | 产物 | 要不要动 | 依据 |
|---|---|---|---|
| 1 | `meta/trading_calendar/session_ledger.parquet`（新） | **新建** | §2 |
| 2 | `src/data/trading_calendar.py` | **改代码** | C1–C6 |
| 3 | `meta/universe/layers/L0..L5`（18,922,964 行 × 6） | **必须重建** | 网格直接来自日历（`_calendar_dates`，`build_universe_layers.py:121`） |
| 4 | `meta/universe/universe_exclusions.parquet` / `.json` / `universe_price_jumps.parquet` | **必须重建** | `tradable_universe.py:316` 用权威段算 `missing_rows` |
| 5 | `meta/clean/`（`clean_set` 日快照 + 月序列 + summary） | **要重跑** | `clean_set.py:599` 的 `gaps` 分母变（只进报告，不参与剔除） |
| 6 | `research/events/**` | **只重跑对拍，不替换** | `events.py:749` 只用事件日附近窗口，1991–1993 无事件 |
| 7 | `factor_lib` 面板缓存 | **不重建** | `calendar_filter` 默认 `False`（`data.py` 的 `PanelRequest`）；白名单模式仅在显式开启时生效 |
| 8 | `meta/stock_basic.parquet` | **重跑对拍** | `build_stock_basic.py:216` 取权威段（+58 天） |
| 9 | `breadth_ma_daily` / `state_vector_daily` | **不动** | 自建 quotes 日历，不读 `get_calendar` |
| 10 | `meta/suspension/suspension_daily.parquet` | **重跑对拍** | 语义是「日历 − 实际 bar」，日历一变停牌集合可能变；`build_universe_layers` 的 L2 依赖它 |

### 5.2 关键预期数字（用于行数守恒）

| 项 | 现值 | 预期 | 说明 |
|---|---|---|---|
| 日历天数 | 8,761 | **8,819** | +58 |
| L0..L5 行数（每层） | 18,922,964 | **18,923,736** | +772 = 58 个新日期在当日「已上市且未退市」的标的累加（按 `stock_basic` 实测：1992-02-01 当日 15 只，其余 57 天共 757 只次）；构建时须精确复算，差 1 行即停 |
| L0/L1/L2/L3 `in_layer=true` | 18,922,964 / 18,223,740 / 17,442,546 / 16,785,784 | 旧 8,761 天上**逐值 0 差异**；新增 58 天按原有规则计算 | L0–L3 不含滚动窗口 → 日历扩张不得改变既有日期的判定。**出现任何 1 行差异即说明还有未识别的日历耦合，停手** |
| L4 `in_layer=true` | 13,333,780 | 变化，且**逐条可归因** | 两类：① `_quote_panel` 修复（`000009.SZ` 2 处）；② adv20 窗口跨新增日期（影响面限于新增日后 20 个 `cal_idx`） |
| L5 `in_layer=true` | 13,198,843 | 随 L4 | 硬约束 `l5 ⊆ l4`，`l5_not_subset_l4` 必须 = 0 |
| `stale_status` / `listing_intervals` | 9,787 / 9,787 | 不变 | 与日历无直接耦合；重跑对拍 0 差异 |
| `clean_set.gaps` 汇总 | — | 早期标的单只最多 +58 | 只进报告 |

### 5.3 备份落点（DG-11 / DG-13）

| 被替换对象 | 快照落点 |
|---|---|
| `meta/universe/layers/`（整目录：6 parquet + `manifest.json` 19,063 B） | `_backup/meta/universe_layers_20261113/` |
| `meta/universe/universe_exclusions.parquet` + `.json` + `universe_price_jumps.parquet` | `_backup/meta/tradable_universe_20261113/` |
| `meta/clean/`（整目录） | `_backup/meta/clean_20261113/` |
| `meta/suspension/suspension_daily.parquet` | `_backup/meta/suspension_20261113/` |
| `meta/stock_basic.parquet` | `_backup/meta/stock_basic_20261113/` |
| `meta/anomalies/weekend_calendar_registry.json`（退役） | `_backup/meta/anomalies_registry_20261113/`（给 SHA256 前后一致证明） |

基线 SHA256（六层，用于回滚自证）：

```text
L0 f1b1a58ccfc8b2b9c640ab08818d85ff47270c1cbae7401ebc69103180f4178e  18,922,964 行
L1 999330f17307ec16873ed4acedd44f4267ae2c03a28e197b995b751c930ccb5d
L2 1f09ca975172bdd8c4d72e97e9432a192e39ce5e0a59fabb65813cc4f18c7803
L3 9604b549f85fc1390570b0e3f97caccf934a9be5a153039d418812a51b50a3ea
L4 813f821cba4bc9d46d954c3f882acb7a93f2cbce05050b3587dd3f33673124a5
L5 8f109b9ad688190499d17bbd0b9e484c630e0a3d94370db7119bb2f86cad98d3
```

### 5.4 串行顺序（不可并发）

```text
① build_session_ledger（staging 对拍 → 替换 + manifest）
② 代码改动 C1-C10 + 单测全绿（T1-T9）
③ 构建实验：(A) 新代码 + 旧日历  (B) 新代码 + 新日历  (C) 旧代码 + 旧日历(=现状)
     用 A−C 隔离「_quote_panel 修复」影响，B−A 隔离「日历扩张」影响
④ build_stock_basic（对拍 0 差异）
⑤ suspension_daily（对拍，允许差异但需逐条解释）
⑥ build_universe_layers（六层）
⑦ tradable_universe（exclusions）
⑧ clean_set
⑨ events / factor_lib 面板：只跑对拍，不替换
⑩ manifest 回写（`python -m src.data.manifest_refresh`）+ 权威路径清单登记
```

**为什么必须串行、不能并发**：

1. `get_calendar()` 是**进程内按文件 mtime 缓存**的对象（`trading_calendar.py:210-228`）。两个进程各持一份按「启动那一刻的磁盘状态」冻结的日历；中途替换台账或代码，先启动的进程仍用旧日期集 → 同一批产物里混两代日历，而 manifest 里记的上游 SHA 只能证明「文件是哪个版本」，证明不了「进程读的是哪一代」。
2. 六层是**全量网格**（每层 18,922,964 行、按 `cal_idx` 位置对齐）。日历一变，所有既有行的 `cal_idx` 后移；任何「部分重建」都会产出一套与旧层不可比的层，`l5 ⊆ l4` 这类跨文件约束会在两代之间假成立。
3. 六层与剔除表虽写不同文件，但共享 `meta/universe/manifest.json` 的 `files` 段；`write_parquet_atomic` 只保证单文件原子，跨文件不是事务（DG-10），并发写同一 JSON 会互相覆盖。
4. 同一工作区并发构建会让 `git status` 出现他路在途产物，无法按 DG-24 自证提交范围。

### 5.5 DG-20 四项验收口径（逐产物）

| 项 | 口径 |
|---|---|
| ① 行数守恒 | 六层每层 18,922,964 → 18,923,721（或构建时精确复算值，误差必须 = 0）；`stale_status`/`listing_intervals` 9,787 不变；exclusions 行数变化逐 `(ts_code, reason_code)` 解释 |
| ② 逐值对拍 | 旧 8,761 天上 L0–L3 `in_layer` **改动行数 = 0**；L4/L5 改动行必须全部落在 A/B 两份归因清单里；空值计数前后各报一次（`in_layer` 空值 = 0） |
| ③ SHA256 | 六层 + 3 个 exclusions 文件 + manifest，新旧各一份，写进回执 |
| ④ 抽样断言 | ①`1991-01-05` 网格存在且 000009.SZ 等 keep 行被读到（`_quote_panel` 命中数 = 25/25）；②`1992-02-01` 存在且 adv20 非空（2 只）；③`1993-05-01/02` 不存在；④`1993-06-05` 不存在；⑤`l5_not_subset_l4 = 0`；⑥`000009.SZ` 两处翻转值与 refit 一致（19,995,600 / 20,257,450） |

### 5.6 回滚

1. 六层 / exclusions / clean / suspension / stock_basic：把 `_backup/meta/<语义>_20261113/` 整目录移回原位（移动，不是复制），逐文件 SHA256 与 §5.3 基线比对，全等即回滚完成。
2. 台账：`session_ledger.parquet` 首次新建，回滚 = 从 `meta/trading_calendar/` 移出到 `_backup/`（不删）。
3. 代码：`git revert <本笔提交>` 生成前向修复提交；**禁止 reset / amend / force push**。
4. 回滚后必须重跑 `pytest tests/test_trading_calendar.py tests/test_universe_layers_quote_mapping.py` 与一次 `build_universe_layers` 的干跑，确认日历回到 8,761。

---

## 6 风险清单

### 6.1 历史结论会变的范围

| 结论 | 是否变 | 说明 |
|---|---|---|
| `breadth_ma_daily` / `state_vector_daily` | **不变** | 自建 quotes 日历，不读 `get_calendar`；`8,653 vs 8,654` 不消解 |
| `universe_layers` L0–L3（既有 8,761 天） | **不变** | 无滚动窗口，日历只增加行、不改变既有行判定 |
| `universe_layers` L4/L5 | **变** | ①`_quote_panel` 修复导致 `000009.SZ` `1992-10-12`、`1993-01-18` 翻转；②新增 58 天进入 adv20 窗口，影响其前后 20 个 `cal_idx` 的判定 |
| 任何以 1993 年前为起点的回测 | **可能变** | 池成员在 1991–1993 窗口变化 → 选股集变化 → 收益曲线变化；**必须在回测报告的样本说明里写明日历代际** |
| `tradable_universe.internal_gap` | 预期不变 | 新增日期间隔 ≥ 5 个自然日（中间都有工作日），任何序列的最长连续缺失段仍为 1 天 < 20 → 不新增登记；**仍须实测前后行数差 = 0**，若出现新增即为假阳性，停手 |
| `tradable_universe.missing_rows` | 不变 | `_stale_rows` 只对 `end_day < as_of` 的标的算，存量标的 `end` 均在 2026 |
| `clean_set.gaps` | **变** | 早期标的单只最多 +58；`gaps` 只赋值+汇总，无过滤分支 → 不影响剔除结果，只影响报告数字 |
| `stock_basic` | 权威段 +58 | 若输出的 `trade_calendar` 是单值（当前是一次 `pl.Series` 展开），需重跑后逐值对拍 |

### 6.2 会不会误放行 `1993-05-01/02`：**不会**，三道防线

1. **数据侧**：`quotes/stock` 已无这两个日期的 bar（11-12 20:28–20:39 的位移把 17 只移到 05-03/05-04 并清重复行）；四指数在 1993-05-01（周六）/05-02（周日）无 bar。
2. **台账侧**：台账不含 `1993-05-01/02` 任何 status 行；`kind` 白名单里没有能覆盖它们的类型（1993 年 49 行属 `uncovered_scope`，不迁移）。
3. **测试侧**：T4 反向断言 + T5 孤儿日期集合等于 6 个 1993 周六（若 05-01/02 被放行，孤儿集合会变 → 必红）。

同理，6 个 1993 周六（06-05/19、07-03/17、08-07/21）**不进网格**：它们只出现在个股 bar 里，S5 不采纳 + G-112 判 drop → 由 T3/T5 守住。若未来要把它们纳入，必须先在台账写 `session` 行并给出上游证据，否则永远进不来。

### 6.3 其它风险

| 风险 | 影响 | 缓解 |
|---|---|---|
| `session_guard.filter_nontrading_rows` 放行面扩大 | 1991 周末 bar 会通过入站过滤（这是本意），但若台账 `closed` 漏行，基金周末净值占位会一起进来 | T8 + 台账强制约束 4（`nav_placeholder_weekend` 必须 `closed`） |
| 新增日期全是周末，`adv20` 窗口里多一个空槽 | L4 在 1991–1993 有小幅变化 | §5.4 的 A/B 隔离，逐条归因；不接受「整体重算所以变了」 |
| 台账只有 73 行、并集规则在代码里 | 有人只改台账以为能改日历 | T9 静态断言 + `manifest.parameters.rule` 写明并集规则 |
| `source` 字符串变化 | `session_guard` 日志、`stock_basic` 列值变化 | 显式列入 §3.3 第 12/13 行，重跑对拍 |
| 1992-02-01 口径未定 | 日历 8,819 vs 8,818 差 1 天 | 台账 `unresolved=true` + `note` 写缺哪份证据；第三源到位后只需改台账一行 |

---

## 7 复现命令与本次自行决定并执行的事

### 7.1 复现（全部只读）

```powershell
# 1) 日历并集算术 + 逐项增减 + 个股独有日期清单
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4c_plan_b_probe.py

# 2) 按日期级裁决（session 优先 / 排除优先）重算最终日历
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4c_ledger_rule.py

# 3) 六层基线行数与 SHA256
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4c_baseline.py

# 4) 各产物行数（单行）
.venv\Scripts\python.exe -c "import polars as pl; print(pl.read_parquet('data/local_store/meta/universe/layers/l4_liquid.parquet').height)"
```

关键实测值：

```text
idx_union            8,806   (000001.SH 8,761 + 45)
sess - idx_union        13
G-112 keep            175 行 / 51 天
G-112 drop             45 行
ledger               73 行 (57 session + 16 closed)
final                 8,819   (+58)
stock_only           19 天 = 12(1991 周六) + 1(1991-11-17) + 6(1993 周六，全 drop)
grid delta            772 行/层（58 天 × 当日在市标的；1992-02-01 15 只 + 其余 57 天 757 只次）
```

### 7.2 本次自行决定并执行的事（只读，可回滚 = 删脚本）

| 项 | 决定 | 依据 |
|---|---|---|
| 新建 `2026-11-12/data-lineage/work/b4c_plan_b_probe.py` | 只读探针 | 用户要求「留关键行号与数字」，需可复现 |
| 新建 `2026-11-12/data-lineage/work/b4c_baseline.py` | 只读统计六层行数/SHA | 同上 |
| 新建 `2026-11-12/data-lineage/work/b4c_ledger_rule.py` | 只读复算日期级裁决 | 同上 |
| 新建本设计稿 | 交付物 | 用户明确要求 |
| 未改任何 `src/` / `tools/` / `tests/` / `data/` | — | 用户明确「不改代码、不改数据、不重建、不提交」 |

---

## 附：施工顺序速查（进入执行阶段时照此逐条勾）

- [ ] `tools/build_session_ledger.py`：registry + 4 指数 + G-112 → 73 行，`manifest.json` 写 `rule` / `coverage.uncovered_1993=49` / `sha256`
- [ ] `src/data/trading_calendar.py`：C1–C6（并集 + 台账 + 缓存 key + source）
- [ ] `tools/build_universe_layers.py`：C7 孤儿守卫 + C8 `UPSTREAM_FILES`
- [ ] `tools/build_stock_basic.py`：C10 docstring
- [ ] `tests/`：改 1 条（source）、补 9 条（T1–T9）
- [ ] 备份 6 处快照（§5.3）→ 构建实验 A/B/C → 逐表重建 → DG-20 四项 → manifest 回写
- [ ] `docs/数据权威路径清单.md`：新增 `session_ledger` 行；`weekend_calendar_registry.json` 移入「已废弃路径」表并记 SHA256
