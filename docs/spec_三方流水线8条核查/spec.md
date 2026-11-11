# Spec：第三方因子选股流水线蓝图 8 条机制只读核查

- 状态：待批准（Spec Mode）
- 日期：2026-11-11
- 任务性质：**只读核查**。不改任何 `src/`、`tools/`、`scripts/` 代码；不写 `data/`；产物只落 `docs/`。
- 唯一交付物：`docs/第三方因子选股流水线8条机制核查_2026-11-11.md`

## 1. 目标与验收

对第三方蓝图的 8 条机制逐条给出「实现位置 file:line / 是否吻合 / 差异 / 风险」，并按三档归并：

| 档位 | 判定含义 |
|---|---|
| 吻合 | 实现与蓝图语义一致，有代码位置为证 |
| 不吻合但影响可控 | 差异可量化且不改变已有结论方向（无断言、口径更粗、覆盖不全等） |
| 不吻合且会污染结论 | 差异会改变收益标签、可成交判定或横截面排序，必须给出受影响标的数、行数、波及的已有结论 |

最后一档必须写清三件事：**多少标的 / 多少行 / 影响哪些已有结论**。

验收标准：
1. 8 条每条都有 `file:line` 证据，且至少一条为「该实现不存在」的否定证据（grep 结果或注释原文）。
2. 三档归类无遗漏，每条只落一档。
3. 第三档的量化数字可由报告中给出的只读命令复现。
4. 报告内所有数字标注来源（文件路径或命令）。

## 2. 已取得的证据（核查基线）

### 第 1 条 涨跌停分板块

| 位置 | 内容 |
|---|---|
| `src/risk/execution_rules.py:96-127` | `_resolve_board_limit_pct`：北交所 30%（`.BJ` 后缀或 83/87/88/920 号段）、创业板 2020-08-24 前 10% / 后 20%、`BOARD_RULES` 科创板 20% / B 股 10%、主板兜底 10% |
| `src/risk/execution_rules.py:129-162` | `resolve_limit_pct`：仅「常规 10% 板块」的 ST 收窄到 5%；20%/30% 板块 ST 不另设 5% |
| `src/risk/execution_rules.py:148-150` | 注释原文：「`listing_date` 预留用于新股上市首日不限涨跌幅的判定，当前数据层未提供，暂未启用」 |
| `src/risk/rules.py:566-585` | `PriceLimitTradable.DEFAULT_LIMITS` 五档齐全 |
| `src/data/services/breadth_service.py:203-214, 246-254, 281` | `_limit_pct_map` 复用同一真源，ST 取自 `st_history`，首个交易日 `is_first_day` 排除 |
| `src/advice/eligibility.py:57, 129` | 传入 `is_st=None` |
| `src/advice/factor_eval.py:741, 775` | 传入 `is_st=None`（前向检验 / 固定持有回测） |

初判：分板块**吻合**；**ST 5% 在选股与回测链路上未生效**（只有 breadth 用真 ST 状态）；**新股前 5 日不设限未实现**（只有首日排除）。
待量化：ST 标的数、ST 处于 5%~10% 区间的行数；上市未满 5 个交易日的样本行数。

### 第 2 条 停牌期间收益

| 位置 | 内容 |
|---|---|
| `tools/research/prepare_monthly_samples.py:128-133` | `fwd_ret = close.shift(-21).over(ts_code) / open.shift(-1).over(ts_code) - 1`，按**行**位移 |
| `src/advice/factor_eval.py:697-808` | `simulate_fixed_hold`：t 日分数、t+1 开盘买入；停牌时 `mark += shares * (float(row['close']) if row else pos['price'])` |
| `data/local_store/meta/suspension/coverage.json` | 缺口型停牌 986,886 行 / 4,328 只；另有 `implied_suspension.parquet` 占位 bar 型 |
| `src/data/quality/suspension_union.py:1-11` | 两类来源并集：`derived_from_quote_gap` 与「volume/amount 为 NULL、四价相等」的占位 bar |

初判：标签侧**不存在**「停牌日收益记 0、复牌日只算单日」——缺行被 `shift` 跳过，整个停牌区间的累计涨跌确实落在复牌后的 bar 上。但存在两个次级问题：
1. `shift(-21)` 按行位移，窗口在日历上被拉长，horizon 不可比（21 根 bar ≠ 21 个交易日）；
2. 若停牌期写的是 `volume=0` 的占位 bar（而非缺行），这些 bar 收益为 0 且占掉窗口槽位 → 21 日收益被系统性低估。
3. `simulate_fixed_hold` 停牌期估值用**买入价**而非最近收盘价 → 净值曲线在停牌期冻结且复牌跳变，属回测净值失真，不是标签污染。

待量化：占位 bar 型停牌行数 / 标的数；月末样本中前向 21 根 bar 窗口内含缺行或含占位 bar 的行数与标的数。

### 第 3 条 信号日与建仓日

- `src/advice/factor_eval.py:697-808`：t 日收盘算分，t+1 开盘成交。
- `tools/research/prepare_monthly_samples.py:130-131`：`close.shift(-21) / open.shift(-1) - 1`，建仓用 t+1 开盘、平仓用 t+21 收盘。
- `tools/advice_stateful_backtest.py:802`：cohort 在 T+1 开盘等权买入。

初判：**吻合**，无偷价。

### 第 4 条 中性化验收口径

- `tools/research/build_multifactor_signals.py:69-76`：`_neutralize` = 行业内去均值后对 `ln_mv` 回归取残差（FWL）。
- `tools/research/build_multifactor_signals.py:153-158`：对 `FACTORS` 逐个中性化再标准化。
- `src/alpha/cross_section/cross_sectional.py:109-161`：行业组内去均值 + 单连续暴露一元 OLS 残差。
- **不存在**「中性化后与行业/市值相关系数 < 0.03」的计算与断言。

初判：**不吻合但影响可控**。缺三样：① 相关系数计算（行业用哑变量或原行业列、市值用 `ln_mv`）；② 阈值断言（>0.03 即失败退出码非 0）；③ 落盘验收表（逐月 × 逐因子）。

### 第 5 条 `financials_pit` asof join 是否防前视

| 位置 | 内容 |
|---|---|
| `src/advice/fundamental_factors.py:70-78` | `_dedupe_period`：按 `(ts_code, end_date)` 分组，取 `ann_date.last()` 与各值列的 `last()` |
| `src/advice/fundamental_factors.py:151-164` | `_latest_period_per_announcement`：同一 `ann_date` 多期取最新 `end_date` |
| `src/advice/fundamental_factors.py:247-254` | `build_fundamentals` → `_asof_join(base, income_view, 'date', 'ann_date')`（backward） |

初判：实现确实落在蓝图点名的模式上（先按 `(code,end_date)` 只留最新修订版，再 asof）。但因为**值与 `ann_date` 同取最新**，合并后不会出现「值来自未来公告、公告日却是旧的」这种典型前视；实际代价是：
1. 中间修订版本被丢弃，无法回溯；
2. 更正公告发布**之前**的日期，该期数据整体取不到（回退到更早一期或为空），属于**可用性缺口**，不是错值。
待量化：多版本 `(code,end_date)` 的数量、涉及标的数；月末样本因该缺口取不到目标期的行数。

### 第 6 条 `daily_index` 混装

- `src/advice/universe.py:42-65`：`list_local_stock_codes` 扫 `quotes/stock/` 文件系统，显式排除 `is_index_code` / `is_fund_code` / `is_superseded`。
- `tools/benchmark_alignment.py:18-26`：`equal_weight_daily` 只吃传入 frame 的 `date/ret`，分池责任在调用方。
- `data/local_store/meta/daily_index.parquet` 混装 stock/index/fund，但有 `data_type` 列可切分。

初判：选池侧**已分池**；基准侧需确认 `equal_weight_daily` 的调用方传入的是 `data_type='stock'` 子集。待量化：`daily_index` 中 stock/index/fund 各多少标的。

### 第 7 条 `adj_factor` 校验口径

- `scripts/verify_data_20261104.py:280-321`：因子快照 vs 行情内 `qfq_factor/hfq_factor` 列逐值比对（5,832 只 mismatch=0）→ **自洽校验**，两端同源。
- `src/data/quality/factors.py:1-16, 104-160`：用「本地行情 + 分红」重算因子并反查跳变日 → 仍是本地推导链，非独立源。

初判：**不吻合但影响可控**。差别：自洽校验只能发现「快照与行情列不同步」，无法发现「整条推导链共模偏差」（分红金额错、除权日错、送转比例错会同时污染两端）。补独立源需要：Tushare `adj_factor`（逐日）或通达信 `.day` 复权序列或交易所除权公告原文；按 `(ts_code, trade_date)` 逐行对拍，缺任一项即无法完成。

### 第 8 条 退市股末日前 20 日复权因子

- `src/data/quality/anomalies.py:390-407`：`factor_tail_lag_rule` 覆盖因子末端 ≥ 2026-10-01 的 1,604 只，清单 `factor_tail_lag_codes.csv`；判据是「因子末端落后行情末端」的**覆盖性**检查。
- `src/data/quality/tradable_universe.py:19-20`：`placeholder_price`（价格恒为 1.0 的占位行情）、`stale_feed`（本地末行早于最近交易日）。
- `tools/backfill_delisted_quotes.py:13-25`：退市后因子列按本地末行前向填充，会触发 `pct_chg_mismatch` 告警。

初判：**不吻合**（无「退市前 20 日复权因子」专项值校验）。待确认 `tradable_universe` 的尾部占位/停更识别是否已覆盖该窗口，以及 `factor_tail_lag_codes.csv` 是否含退市标的。

## 3. 待执行的量化任务（批准后跑，全部只读）

| 编号 | 指标 | 只读取数方式 | 落点 |
|---|---|---|---|
| Q1 | ST 标的/行数、ST 落在 5%~10% 区间的行数 | `meta/st_status/*` 与 `quotes/stock/**` 按 `pre_close` 与 `close` 计算 | 报告第 1 条 |
| Q2 | 上市未满 5 个交易日的样本行数 | `meta/daily_index.parquet` 首行日 + 交易日历 | 报告第 1 条 |
| Q3 | 占位 bar 型停牌行数/标的数；月末样本前向窗口内含缺行或占位 bar 的行数/标的数 | `implied_suspension.parquet` + `suspension_daily.parquet` + `reports/research_tmp/analysis_monthly.parquet` | 报告第 2 条 |
| Q4 | 中性化断言缺口 | 读代码 + `reports/stock_multi_factor_v2_20261105/signals/*.csv` 是否含相关系数列 | 报告第 4 条 |
| Q5 | 多版本 `(code,end_date)` 数量、涉及标的；月末样本受影响行数 | `meta/financials_pit/` 四表按 `(ts_code,end_date)` 计数 `ann_date` 去重后 > 1 | 报告第 5 条 |
| Q6 | `daily_index` 三类标的数；`equal_weight_daily` 调用方传入的池 | `meta/daily_index.parquet` group by `data_type` + grep 调用点 | 报告第 6 条 |
| Q7 | 自洽校验覆盖面 | 读 `verify_data_20261104.py` 输出 JSON 的 `factor_*` 字段 | 报告第 7 条 |
| Q8 | 退市标的数、末日前 20 行因子缺口数 | `meta/stock_basic.parquet` 的 `status='D'` + 各票末 20 行 | 报告第 8 条 |

执行纪律：
- 所有量化脚本只写仓库根 `_staging/audit_8checks_20261111/`（DG-5）或 `docs/`，绝不写 `data/`。
- 只读 `data/`，不 `copy2`、不 `os.replace`、不 `unlink`。
- 遇数据文件缺失立即停手报告，不猜数（DG-27）。

## 4. 交付物结构

`docs/第三方因子选股流水线8条机制核查_2026-11-11.md`：

1. 结论摘要（三档表 + 第三档受影响范围）
2. 三档归类明细（8 条 → 档位 + 一句话理由）
3. 逐条核查（实现位置 / 是否吻合 / 差异 / 风险 / 量化数字 / 复现命令）
4. 不吻合且会污染结论的条目：受影响标的、行数、波及的已有结论
5. 附件：只读取数命令与输出摘要

## 5. 需要用户批准的事项

1. 是否同意在 `docs/spec_三方流水线8条核查/` 下保留 spec.md 与 tasks.md（用户原约束是「产物只写 docs/」，此路径仍落在 docs 内）。
2. 是否同意第 2 条按「标签侧无污染、回测净值侧有失真」定档（当前初判为「不吻合但影响可控」，若 Q3 显示占位 bar 覆盖大量样本则改判第三档）。
3. 是否同意第 5 条按「可用性缺口、非前视错值」定档（初判「不吻合但影响可控」）。

无代码改动，无数据改动，无可回滚项。
