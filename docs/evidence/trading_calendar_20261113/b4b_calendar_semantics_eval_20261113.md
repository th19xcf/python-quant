# `get_calendar()` 日历口径修复方案评估（只读）

- 评估时点：2026-11-13 02:05–02:25（Asia/Shanghai）
- 边界：**未改代码、未改数据、未重建、未提交**。全部数字来自只读探针 `2026-11-12/data-lineage/work/b4b_calendar_probe.py` → `b4b_calendar_probe.json`
- 不预设 1992-04-18 的归属：该项按 DG-18 挂 unresolved，本文只把它作为「注册表第 4 类」单列，不参与任何推荐结论
- 前序约束沿用：`docs/adj_factor独立源对拍_2026-11-11.md §14.1` 的「残差归零」判据（G-122）不作基准；独立对拍只用通达信 `.day` 原始 OHLC

---

## 0 现状口径（先钉死事实）

| 项 | 值 |
|---|---|
| 实现 | `src/data/trading_calendar.py::_build`，`BENCHMARK_CODES = ('000001.SH', '000300.SH')`，按顺序取**第一个**有数据的指数 |
| 当前来源 | `local_index:000001.SH` |
| 权威段 | 8,761 天，1990-12-19 ~ 2026-11-11（`authoritative_end`） |
| 近似尾部 | 262 个工作日 → `dates` 共 **9,023** 天（用户所述 9,023 即含尾部） |
| 节假日配置 | `src/data/quality/freshness_config.json` 的 `holidays` 当前为 **空集**（0 条） |
| 校验 | 权威段与 `quotes/index/sh/000001.SH.parquet` 日期集合**逐日相同，0 差** |

`authoritative_dates()` 只返回 ≤ `authoritative_end` 的部分；`dates` 含尾部近似。改动任何一处都会同时影响这两个出口。

---

## 1 消费者清单（`local_index:000001.SH` 派生这条日历的调用方）

按 `get_calendar` 的调用点逐处列，共 11 处（含 1 处不消费本日历）。

| # | 调用点 | 用途 | 日历多/少若干天会不会改变结论 |
|---|---|---|---|
| 1 | [tradable_universe.py](file:///e:/code/python/quant/src/data/quality/tradable_universe.py#L308-L352) `_benchmark_calendar` → `_stale_rows` | `missing_rows =` 日历落在 `(end, as_of]` 的天数，写 `stale_feed`（warn） | **只改数字不改命中**：触发条件是 `end < as_of`，与日历无关。1991-1993 末行的标的 `missing_rows` 最多 +56，分母约 8,700 → +0.6% |
| 2 | 同上 `_benchmark_calendar` → 指数连续空洞（`CONTINUOUS_GAP_MIN_DAYS=20`） | 指数在日历上连续 ≥20 个交易日无 bar 才登记 `internal_gap` | **不会触发**：1991 周六是孤立 1 天空洞，达不到 20；但若成批并入且指数缺 bar，会虚增 |
| 3 | [clean_set.py](file:///e:/code/python/quant/src/data/quality/clean_set.py#L597-L606) `calendar` / `calendar_ord` | ① `_source_lag_trading_days(data_last, calendar_latest, …)`；② `info.gaps = 上市区间内应有交易日 − 实际行数` | ① **不变**（两端都在 2026）；② **变**：1991 年在市的 SH 标的 `gaps` 最多 +52。`gaps` 当前只落诊断字段，仓内无过滤逻辑读取它（`grep '\.gaps'` 仅 dataclass 定义 + 赋值两处）；一旦将来被当过滤阈值用，就会误伤 |
| 4 | [events.py](file:///e:/code/python/quant/src/data/events.py#L740-L752) `trading_dates()` | 事件区间展开到交易日，供 `blocked_map` 等 | **变**：1991-1993 的事件区间会多出周末行 → 阻断天数增加 |
| 5 | [universe_filter.py](file:///e:/code/python/quant/src/data/universe_filter.py#L539-L549) `_trading_window` / `previous(day)` | 停牌回看窗口、前一交易日 | 仅当 `as_of` 落在 1991-1993 时窗口位移；近年 as_of **无影响** |
| 6 | [factor_lib/data.py](file:///e:/code/python/quant/src/quant/factor_lib/data.py#L48) | 因子面板的日期轴 | **变**：面板多/少列 |
| 7 | [build_universe_layers.py](file:///e:/code/python/quant/tools/build_universe_layers.py#L52) `_calendar_dates` | 网格日期轴、L1「上市满 120 交易日」、行情 bar 的**精确匹配**（非日历日 bar 直接丢弃） | **最敏感**：① 新增周末日会把 L1 门槛提前 1 天；② 现在被丢弃的 1991 周六 bar 会转为计入 → L2/L4 行集变化 |
| 8 | [build_multi_asset_etf_pool.py](file:///e:/code/python/quant/tools/build_multi_asset_etf_pool.py#L109-L113) | ETF 池的 as_of 与窗口 | 仅历史 as_of 受影响 |
| 9 | [advice_stateful_backtest.py](file:///e:/code/python/quant/tools/advice_stateful_backtest.py#L69) | 回测交易日轴（`hold-days` 计数） | 跨 1991-1993 的持仓天数计数会变 |
| 10 | [build_stock_basic.py](file:///e:/code/python/quant/tools/build_stock_basic.py#L85) | 用日历校验 `list_date` 是否落在交易日 | **变**：1991-1993 的 `list_date` 判定结果可能翻转 |
| 11 | [freshness.py](file:///e:/code/python/quant/src/data/quality/freshness.py#L272-L279) / [freshness_check.py](file:///e:/code/python/quant/src/data/quality/freshness_check.py#L292-L305) | `count(latest, today)` / 全量新鲜度 | **不变**：`latest` 均在 2026，1991-1993 的天数不进区间 |

**明确不消费本日历的产物**（对应第 4 节的「不需要重建」）：

- `breadth_ma_daily`：[build_breadth_ma.py](file:///e:/code/python/quant/tools/build_breadth_ma.py#L135-L146) 自建日历 = `quotes/stock` 日期并集，注释里写明「不用交易日历模块的近似尾部」
- `state_vector_daily`：[build_state_vector.py](file:///e:/code/python/quant/tools/build_state_vector.py#L271-L276) 同样自建 = `quotes/stock` 并集
- `listing_intervals`：读 `daily_index`（由 quotes 文件派生），不碰日历
- [build_trading_calendar.py](file:///e:/code/python/quant/tools/build_trading_calendar.py#L62-L65)：只取 `BENCHMARK_CODES` / `configured_holidays` 常量，写的是 `official_future_trading_calendar`，不消费本日历

---

## 2 接入注册表已登记的 1992 周末 session：日期集合怎么变

注册表：`data/local_store/meta/anomalies/weekend_calendar_registry.json`。1992 年共 6 类 / 37 行 / 8 个日期。

### 2.1 四类逐类判定

| kind | 行数 | 日期 | 是否并入日历 | 理由 |
|---|---|---|---|---|
| `upstream_omitted_session_1992` | **30** | **1992-02-02 / 1992-05-03 / 1992-10-04** | **是** | 上游有 session、本地个股缺 bar |
| `upstream_closed_local_bar_1992` | 5 | 1992-02-01 / 1992-02-02 / 1992-10-04 | **否** | 上游休市、本地有 bar → 本地多 bar，不是掉 session |
| `weekend_date_shift_1992` | 1 | **1992-04-18** | **不预设**（DG-18 unresolved） | 注册表判定为「日期位移」而非 session；另派第三源取证中 |
| `weekend_zero_volume_placeholder_1992` | 1 | 1992-10-04 | 否 | 零成交占位行，不是 session |

### 2.2 逐日增减（只并入 `upstream_omitted_session_1992`）

| 日期 | 动作 | 并入后日历天数 | 已在 `breadth_ma_daily`？ |
|---|---|---|---|
| 1992-02-02（周日） | +1 | 8,762 | 是 |
| 1992-05-03（周日） | +1 | 8,763 | 是 |
| 1992-10-04（周日） | +1 | 8,764 | 是 |
| 移除 | 0 | — | — |

净增 **3 天**，权威段 8,761 → **8,764**。

**必须先裁定的一处内部冲突**：`1992-02-02` 同时出现在 `upstream_closed_local_bar_1992`（000001.SH 记为休市）与 `upstream_omitted_session_1992`（000009/399001 记为 session）。同日按标的分歧 → 需要定一条「至少一只为 session 即算 session」还是「全部为 session 才算」的判定规则，否则 02-02 的归属随取值顺序漂移。1992-10-04 同理（13 只 session + 399002 休市 + 399003 零成交占位）。

### 2.3 若同时并入 1991 的 `weekend_session_1991`（53 天）

| 场景 | 权威段天数 | 新增 | 移除 |
|---|---|---|---|
| 现状 | 8,761 | — | — |
| 只加 1992 session（3 天） | 8,764 | +3 | 0 |
| 只加 1991 周末（53 天） | 8,814 | +53 | 0 |
| 两者都加 | **8,817** | +56 | 0 |

1991 的 53 天全部**不在**现有日历里（0 命中），逐日清单：

```text
1991-01-05 01-12 01-19 01-26 02-02 02-09 02-23 03-02 03-09 03-16
03-23 03-30 04-06 04-13 04-20 04-27 05-04 05-11 05-18 05-25
06-01 06-08 06-15 06-22 06-29 07-06 07-13 07-20 07-27 08-03
08-10 08-17 08-24 08-31 09-07 09-14 09-21 09-28 09-29* 10-05
10-12 10-19 10-26 11-02 11-09 11-16 11-17* 11-23 11-30 12-07
12-14 12-21 12-28
（* 09-29、11-17 为周日，其余为周六；02-16 春节周不在此列）
```

### 2.4 `verify_breadth_ma` 的 8,653 vs 8,654 会不会自然消解

**不会。两层原因**：

1. **该脚本根本不读 `get_calendar()`**。[verify_breadth_ma.py](file:///e:/code/python/quant/tools/verify_breadth_ma.py#L116-L119) 的期望日期集是 `quotes/stock` 的日期并集（8,653），日历怎么改都不进入这个比较。
2. 那 1 天缺口就是 **1992-04-18**，恰好是 unresolved 的 `weekend_date_shift_1992`。只并入 `upstream_omitted_session_1992`（02-02 / 05-03 / 10-04）不动它 → 缺口仍是 1 天。

顺带把「日历 vs 正式表」的差集变化算清（`table−cal` / `cal−table`）：

| 日历口径 | 天数 | table−cal | cal−table |
|---|---|---|---|
| 现状 | 8,761 | 37 | 144 |
| +1992 session | 8,764 | 34 | 144 |
| +1991 周末 | 8,814 | 12 | 172 |
| 两者都加 | 8,817 | 9 | 172 |
| +1992-04-18（**仅测算，不推荐**） | 8,762 | 36 | 144 |

读法：并入 1992 session 会把「表有日历无」的 37 天降到 34（02-02/05-03/10-04 表里本来就有）；并入 1991 会把 `cal−table` 从 144 抬到 172，因为 1991 上半年个股不足 60 个 bar、宽度表本来就算不出来——**日历变全不等于表能变全**。

---

## 3 换独立第三方 / 多指数并集日历的可行性

### 3.1 多指数并集（本地已有，零外部依赖）

候选：`000001.SH ∪ 399001.SZ ∪ 399002.SZ ∪ 399003.SZ`

| 项 | 值 |
|---|---|
| 并集天数 | **8,806**（比现状 +45、−0，是严格超集） |
| 新增日期性质 | **45 天全部是周末**：1991 年 40 天 / 1992 年 4 天 / 1993 年 1 天（1993-01-03） |
| 最早可覆盖 | **1991-04-03**（399001.SZ 首日；399002/399003 自 1992-10-04） |
| 覆盖不到 | 1991-01-05 ~ 1991-03-30 的 12 个周六 + 1991-02-16（春节周，注册表也未登记）+ 1991-11-17（周日）= **13 天** |
| 1993 年 | 只有 1993-01-03 一天；1993-06-05~08-21 的 6 个周六只有 000012 一只个股有 bar，指数侧无记录 |

对比「个股 bar 并集」：1991 年 52 天、1992 年 4 天、1993 年 7 天 = **63 天**；与注册表 1991 的 53 天相比，只缺 `1991-04-27`（注册表有、个股无）。

**能补到哪一年**：多指数并集只能补到 **1991-04-03**；1991 年 1-3 月必须靠个股 bar 或外部交易所年鉴，指数侧无论如何补不上。

### 3.2 独立第三方（baostock / sina）

按 `docs/早期深市日历口径审计_2026-11-12.md` 的既有取证：两者在 1991-1993 **都不含任何周末 session**，与 `000001.SH` 同为「工作日历」。因此：

- 作为**加法源**：**能补 0 天**，1991-1993 的周末 session 一天也补不到；
- 作为**减法证据**：可反向证明「上游当天休市」（这正是 `upstream_closed_local_bar_1992` 的取证方式），但无法证明「上游当天开市」。

### 3.3 差集汇总

| 口径 | 天数 | 与 9,023（`dates` 全量）/ 8,761（权威段）的关系 |
|---|---|---|
| 现状（000001.SH） | 8,761 / 9,023 | 基准 |
| ∪ 399001/399002/399003 | 8,806 | +45，−0 |
| ∪ 注册表 1991 周末（53） | 8,814 | +53，−0 |
| ∪ 注册表 1992 session（3） | 8,764 | +3，−0 |
| ∪ 个股 bar 并集 | 8,824 | +63，−0 |
| ∪ 全资产 bar 并集（含基金） | 8,843 | +82（+19 是 2025-2026 的基金净值周末占位，不属于交易日） |

---

## 4 影响面：哪些已落盘产物会变、要不要重建

| 产物 | 读 `get_calendar` | 会不会变 | 需不需要重建 |
|---|---|---|---|
| `meta/breadth/breadth_ma_daily.parquet` | **否**（自建 quotes 日历） | **不变** | **不需要**（与本次日历口径无关） |
| `meta/universe/state_vector_daily.parquet` | **否**（自建 quotes 日历） | **不变** | **不需要** |
| `meta/universe/listing_intervals.parquet` | **否**（读 `daily_index`） | **不变** | **不需要** |
| `meta/universe/stale_status.parquet` | 否（读 `daily_index`） | **不变** | **不需要** |
| `meta/universe/layers/l0..l5` | **是** | **变** | **需要重建**（L1 门槛前移 + 周末 bar 从丢弃变计入） |
| `tradable_universe` 清单（`internal_gap` / `stale_feed`） | **是** | `stale_feed.missing_rows` 变（+N）；指数孤立 1 天不触发 ≥20 阈值 | **需要重跑**（幂等，`detected_at` 取快照日） |
| `clean_set` 快照 | **是** | `gaps` 变（1991 年 SH 标的 +最多 52）；`source_lag` 不变 | **需要重跑** |
| `events.blocked_map` | **是** | 1991-1993 阻断天数变 | 用到历史事件时需重跑 |
| `multi_asset_etf_pool` | **是** | 历史 as_of 的池日期轴变 | 视是否重算历史池 |
| `factors/*` 面板（`factor_lib`） | **是** | 面板日期轴变 | 若包含 1991-1993 需重算 |

### 4.1 1993-05-01/02 会不会被误放成真 session

**数据现状已变，必须先修正前提**：`quotes/stock` 现在**已经没有** 1993-05-01 与 1993-05-02 的 bar（1993 年周末 bar 只剩 7 天：01-03、06-05、06-19、07-03、07-17、08-07、08-21）。11-12 20:28–20:39 那批日期位移已把 17 只的 05-01/02 移到 05-03/05-04，并清掉了重复行。以 000001.SZ 为例：05-03 = 58.05 / 2,795、05-04 = 58 / 3,410、05-05 = 56.7 / 4,692、05-06 = 55.3 / 12,940，05-03~05-06 逐日都有 bar（67~70 只）。

因此：

- 若只并入「注册表已判定的 session」（1991 周末 + 1992 的 3 天）→ 05-01/02 **不在其中**，不会放行；
- 若采用「TDX 六天工作周整体当 session」这种粗口径 → 05-01/02 会变成 **0 根 bar 的交易日**，后果是 L2 当天全体 `tradable=false`、`internal_gap` 虚假缺口、`clean_set` 缺口率抬升。**必须显式排除**，且注册表 `uncovered_scope` 已把 1993 年 49 行标为「含 1993-05 位移块、未覆盖」，说明这批本就不该走 session 通道。

### 4.2 一处需要如实记账的现场观察（不改动）

`_backup/quotes_stock_sz_1992_shift_20261113/` 已存在（内含 `000002.SZ.parquet` 快照）。但当前 `quotes/stock/sz/000002.SZ.parquet` 仍是「有 1992-04-20、无 1992-04-18」，与 DG-18 unresolved 的状态一致，即**快照已做、数据未动**。本评估不触碰该目录。

---

## 5 修法选项（只给方案，不实施）

### 方案 A：单点并入「注册表已判定 session」（最小改动）

- **改动点**：`src/data/trading_calendar.py::_build`——在 `index_dates` 之后并入一份显式 session 白名单（`weekend_session_1991` + `upstream_omitted_session_1992`），`sorted(set(...))` 去重，`authoritative_end` 仍取指数末日；白名单来源可选「读 `meta/anomalies/weekend_calendar_registry.json` 的指定 kind」或新增一个纯日期清单文件。同时补一条单测：并入后 1992-05-03 在 `authoritative_dates()` 内、1993-05-01 不在。
- **重建清单**：`universe_layers`（L0-L5）、`tradable_universe` 清单、`clean_set` 快照、`events.blocked_map`；`breadth_ma` / `state_vector` / `listing_intervals` / `stale_status` **不动**。
- **风险**：① 1992-02-02 的「至少一只/全部为 session」规则未定，先落会固化一个待改结论（DG-18）；② 1991 年 1-3 月的 12 个周六 + 1991-02-16 + 1991-11-17 仍缺，日历仍是「部分周末」状态；③ 日历从「纯指数派生」变成「指数 + 白名单」，`source` 字段语义要同步改，否则 manifest 记账撒谎。
- **回滚**：日历是纯派生、无独立数据文件（直接读 `quotes/index/**`），回滚 = 还原 `trading_calendar.py` 并 `reset_calendar_cache()`；受影响产物按各脚本重跑一遍即回到旧口径。

### 方案 B：改成「多源并集 − 显式排除表」（结构性修法）

- **改动点**：`BENCHMARK_CODES` 改为并集组（`000001.SH, 399001.SZ, 399002.SZ, 399003.SZ`），`_build` 从「取第一个」改为「全部取并集」；再引入一份排除表（`upstream_closed_local_bar_1992` 等），并集后扣除。`source` 记为 `local_index:union(4)`。
- **产出**：权威段 8,806 天（+45），其中 1991 年 40 天、1992 年 4 天、1993 年 1 天；1991-01-05~03-30 的 12 天仍缺。
- **重建清单**：同方案 A（`universe_layers` / `tradable_universe` / `clean_set` / `events`），另加任何读面板日历的因子重算。
- **风险**：① 并集会把 `1992-02-01`（399001 有、000001.SH 无）这类「只在 SZ 休市口径下成立」的日子带进来，需要排除表先定稿；② 排除表与注册表两处真相，若不收敛会违反 DG-1；③ 45 天里 40 天是 1991 周六，而宽度表/状态向量根本不读日历，收益集中在 `universe_layers` 的早期段。
- **回滚**：同 A，单文件还原 + 缓存重置；排除表若单独成文件，删除即可。

### 方案 C：日历不动，另开「历史周末 session」查询接口

- **改动点**：`TradingCalendar` 增加方法 `historical_sessions()`（只读注册表），`get_calendar()` 的默认行为不变；需要早期段的调用方（实际只有 `build_universe_layers`）显式取并集。
- **重建清单**：只有 `universe_layers`。其余产物一行不动。
- **风险**：**口径分裂**——同一语义（「哪些天是交易日」）存在两套路径，直接违反 DG-1/DG-2；下游各自选择会让同一份回测在不同入口得出不同结论。
- **回滚**：删除新方法，零数据影响。

### 推荐顺序（均不实施，等授权）

1. 先只读取证把 **1992-04-18**（date_shift 还是 session）与 **1992-02-02**（1:2 分歧的裁定规则）定案——这两项不定，任何并集都会边修边错。
2. 定案后走 **方案 B**，但把范围限制在「注册表已判定」的部分，排除表与注册表合并为同一份文件（避免两处真相）；1993 年 49 行未覆盖项单列下一批。
3. 不采用方案 C（口径分裂），除非明确接受「早期段与主段两套日历」的长期代价。

---

## 6 复现命令与产物

```powershell
# 只读探针（不写 data/、不改代码）
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b4b_calendar_probe.py

# 关键事实自查
.venv\Scripts\python.exe -c "from src.data.trading_calendar import get_calendar; c=get_calendar('data/local_store'); print(c.source, len(c.authoritative_dates()), c.authoritative_end)"
```

| 产物 | 说明 |
|---|---|
| `2026-11-12/data-lineage/work/b4b_calendar_probe.json` | 日历口径、各源覆盖、周末 bar、注册表合并模拟、1993-05 自查 |
| `2026-11-12/data-lineage/work/b4b_calendar_probe.py` | 只读探针源码（无写 data/ 逻辑） |

---

## 7 本次自行决定并执行的事

| # | 决定 | 依据 | 回滚 |
|---|---|---|---|
| 1 | 只写 `work/` 下探针脚本与 JSON，不碰生产代码与 `data/` | 任务明写「只读方案评估，不改代码、不改数据、不重建、不提交」 | 删两个文件即可，无副作用 |
| 2 | 1992-04-18 不做任何归属判断，只作「注册表第 4 类」单列 | 用户已按 DG-18 挂 unresolved 并另派第三源 | 无需回滚 |
| 3 | 把 1993-05-01/02 的现状写成「bar 已不存在」而非沿用旧审计的「17 只有 bar」 | 实测 11-12 20:28–20:39 的位移已生效，`quotes/stock` 无 05-01/02 | 若另有路径回滚该位移，本文该段需同步更正 |
