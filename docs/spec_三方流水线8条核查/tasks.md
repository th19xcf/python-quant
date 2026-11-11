# Tasks：第三方流水线蓝图 8 条机制只读核查

约束：只读 `data/`，不改 `src/` / `tools/` / `scripts/`，产物只落 `docs/`。
量化脚本（如需）只写仓库根 `_staging/audit_8checks_20261111/`。

## T1 证据固化（已完成）

- [x] 第 1 条：读 `src/risk/execution_rules.py`、`src/risk/rules.py`、`src/data/services/breadth_service.py`、`src/advice/eligibility.py`
- [x] 第 2 条：读 `src/advice/factor_eval.py:697-808`、`tools/research/prepare_monthly_samples.py:117-133`、`meta/suspension/coverage.json`、`src/data/quality/suspension_union.py`
- [x] 第 3 条：读 `factor_eval.py`、`prepare_monthly_samples.py`、`tools/advice_stateful_backtest.py:802`
- [x] 第 4 条：读 `tools/research/build_multifactor_signals.py:69-95,153-168`、`src/alpha/cross_section/cross_sectional.py:109-161`
- [x] 第 5 条：读 `src/advice/fundamental_factors.py:70-78,151-164,247-254`
- [x] 第 6 条：读 `src/advice/universe.py:42-65`、`tools/benchmark_alignment.py:18-26`
- [x] 第 7 条：读 `scripts/verify_data_20261104.py:280-321`、`src/data/quality/factors.py:1-16,104-160`
- [x] 第 8 条：读 `src/data/quality/anomalies.py:390-407`、`src/data/quality/tradable_universe.py:19-20`、`tools/backfill_delisted_quotes.py:13-25`
- [x] 第 1 条补充：读 `src/data/universe_filter.py:1-121`（15 条选池规则，明确「不在选池剔除涨停」）

## T2 量化取证（待批准后执行，只读）

- [ ] Q1 ST 覆盖：ST 标的数 / 行数；ST 落在 5%~10% 涨幅区间（会被 10% 阈值漏判）的行数
- [ ] Q2 新股：上市未满 5 个交易日的月末样本行数（依赖 `daily_index` 首行日）
- [ ] Q3 停牌：占位 bar 型停牌行数 / 标的数；月末样本前向 21 根 bar 窗口内含缺行或含占位 bar 的行数 / 标的数
- [ ] Q4 中性化：确认 `reports/stock_multi_factor_v2_20261105/signals/` 下无相关系数产出，列出缺失的三件产物
- [ ] Q5 财务多版本：`meta/financials_pit/` 四表按 `(ts_code,end_date)` 的 `ann_date` 版本数 > 1 的组数 / 标的数；月末样本因该缺口取不到目标期的行数
- [ ] Q6 混装：`meta/daily_index.parquet` 按 `data_type` 计数；确认 `equal_weight_daily` 调用方传入池
- [ ] Q7 自洽校验：读 `logs/verification_20261104/market_scan.json` 的 `factor_*` 字段，记录 5,832 / mismatch=0 的原始键名
- [ ] Q8 退市尾部：`meta/stock_basic.parquet` `status='D'` 标的数；各票末 20 行与因子快照末行的缺口；`factor_tail_lag_codes.csv` 中退市标的占比

## T3 三档定档（依赖 T2）

- [ ] 逐条落档，第三档写出：受影响标的数 / 行数 / 波及的已有结论（对应 `docs/评价指标体系_2026-11-11.md` 的 §1.4、§2.6、§2.7 等）
- [ ] 每条给「复现命令」，命令只用只读取数

## T4 报告落盘

- [ ] 写 `docs/第三方因子选股流水线8条机制核查_2026-11-11.md`（结构见 spec.md §4）
- [ ] 自检：8 条均有 `file:line`；三档无遗漏；第三档数字可复现

## T5 里程碑汇报

- [ ] 三档归类 + 第 2、5 两条结论 → 父会话
- [ ] 报告完成后回报交付路径

## 不做什么

- 不改代码、不加断言、不改阈值（如需补中性化断言，只写「建议 + 实现位置」，不落地）
- 不动 `data/`、不跑任何写正式路径的脚本
- 不碰其它会话在途的 `M` / `??` 文件
