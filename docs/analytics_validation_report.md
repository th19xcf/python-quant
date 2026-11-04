# 只读分析层接入实测报告（Alphalens-reloaded + QuantStats）

> 生成时间：2026-11-03 14:17　｜　环境：Python 3.14.9 / pandas 3.1.0 / Windows
> 口径：行情均取自本地列存 `data/local_store/quotes`，价格为后复权（`raw OHLC × hfq_factor`），
> 全程未写数据库、未改列存、未改撮合引擎。

## 1. 依赖与安装

| 项 | 值 |
|---|---|
| 依赖组 | `[tool.poetry.group.analytics]`（`optional = true`） |
| 直接依赖 | `alphalens-reloaded==0.5.1`、`quantstats==0.0.86` |
| 传递依赖 | matplotlib 3.11.2、statsmodels 0.15.0、seaborn 0.13.2、empyrical-reloaded 0.7.2、yfinance 1.7.0 等 37 个包 |
| 安装体积 | **约 117.6 MiB**（含 matplotlib 22.2、statsmodels 38.1、pillow 14.2、jedi 13.9、fonttools 12.0） |
| 默认安装影响 | 无。`poetry install` 不安装该组；未安装时适配层抛 `RuntimeError` 并提示 `poetry install --with analytics` |

## 2. Alphalens 实测（250 只个股 / 2021-01-01 ~ 2025-12-31）

- 抽样：从 3,629 只覆盖度达标个股中确定性随机抽取 250 只（seed=2024）。
- 面板：343,415 行；因子评估区间 302,577 行。
- 因子：`price_momentum / momentum_3m`（项目现有因子层，hfq 价格计算）。
- 口径：**信号 T 日产生 → T+1 以开盘价建仓 → 持有 N 个交易日**；与回测撮合 T+1 规则一致。
- 耗时：因子计算 0.09s，Alphalens 核心评估 **3.88s**，整脚本（含列存扫描/读取）约 21s。
- Alphalens 清洗丢弃 1.7%（全部为前向收益不可得，未触发 `max_loss=35%`）。

| 持有期 | IC 均值 | IC 标准差 | ICIR | IC>0 占比 | 多空收益(Q5-Q1) |
|---|---|---|---|---|---|
| 1D | -0.0299 | 0.1969 | -0.1519 | 44.42% | -0.0972% |
| 5D | -0.0543 | 0.1925 | -0.2818 | 39.04% | -0.4454% |
| 10D | -0.0668 | 0.1868 | -0.3574 | 37.70% | -0.7639% |
| 20D | -0.0798 | 0.1869 | -0.4270 | 31.91% | -1.3445% |

分层收益（20D，Q1→Q5）：1.837% / 1.641% / 1.508% / 1.226% / 0.493%；
换手率：顶分位 10.20%，底分位 11.95%；因子秩自相关均值 0.972（3 个月动量信号非常稳定）。

**结论**：该样本内 3 个月动量呈显著**反转**特征（IC 全为负且随持有期加深），
20D 多空组合为负；这不是适配层 bug，而是因子本身在 A 股截面上的真实表现。
旧的单序列时序 IC 无法暴露这一点，现已保留为 `ts_ic` 兼容别名。

产物路径：

- HTML tearsheet：`reports/analytics_validation/alphalens/alphalens_momentum_3m_250stocks.html`
- 机器可读 JSON：`reports/analytics_validation/alphalens/alphalens_momentum_3m_250stocks.json`

## 3. QuantStats 实测（真实回测净值）

- 回测标的：`000002.SZ`（指定 600519.SH 未进入抽样池，自动改用样本内标的）
- 策略：`MAStrategy(5, 20)`，区间 2021-01-01 ~ 2025-12-31
- 权益点 1,212 个，成交 66 笔（`PerformanceAnalyzer` 配对后 trades_count=33）
- QuantStats 生成耗时：约 1.4s

| 指标 | PerformanceAnalyzer | QuantStats | 差异 | 结论 |
|---|---|---|---|---|
| 总收益 | -33.1142% | -33.1142% | ≈0（1e-17 浮点噪声） | 完全一致 |
| 年化收益 | -7.7409% | -8.0285% | +0.2876 pp | **口径差异**（见下） |
| 夏普比率 | -0.21716 | -0.21707 | -0.00009 | 口径差异（ddof） |
| 最大回撤 | 48.7854% | 48.7854% | ≈0（1e-15） | 完全一致 |
| 年化波动率 | 24.6960% | 24.7062% | -0.0102 pp | 口径差异（ddof） |

差异解释（均为口径问题，非 bug）：

1. **年化收益**：`PerformanceAnalyzer` 用自然日 `365 / 日历天数`（本次 1,822 天 ≈ 4.99 年）；
   QuantStats CAGR 用 `252 / 收益期数`（1,211 期 ≈ 4.81 年）。亏损策略下 QuantStats
   年化更负，二者相差 0.29 个百分点。若统一为 252 交易日口径即可对齐。
2. **夏普 / 波动率**：`PerformanceAnalyzer` 用总体标准差（ddof=0），QuantStats 用样本标准差
   （ddof=1），比值恰为 `sqrt((n-1)/n)=0.99959`，与实测差异完全吻合。
3. **胜率不可直接对比**：`PerformanceAnalyzer` 的 `winning_rate=24.24%` 是**配对交易胜率**；
   QuantStats 的 `win_rate=43.51%` 是**日收益为正占比**，定义不同，不应横比。
4. 最大回撤：两者都基于同一权益序列，结果在浮点精度内完全一致；本模块已统一输出正的
   小数口径（`0.4879` = 48.79%），而 `PerformanceAnalyzer` 输出百分数。

QuantStats 关键指标（小数口径）：CAGR -8.03%，Sharpe -0.217，Sortino -0.343，
MaxDD 48.79%，年化波动 24.71%，Calmar -0.165，Ulcer Index 0.334。

产物路径：`reports/analytics_validation/quantstats/quantstats_000002.SZ.html`
汇总 JSON：`reports/analytics_validation/validation.json`

## 4. 验证与回归

- 全量 `pytest`：**1399 passed, 1 skipped**，总覆盖率 **65.14%**（门禁 63%）。
- 新增无网络单测 17 个：锁定 T+1 开盘/收盘建仓口径、重复 `(date, asset)` 去重、
  缺失日期前向填充、空面板/单日/重复索引/非正净值显式报错、JSON/HTML 产物完整性。
- `ruff` / `mypy`：本次改动文件全部通过。
- 旧时序 IC 实现保留，仅新增 `calculate_ts_ic` / `calculate_ts_rank_ic` 别名，不破坏既有调用方。

## 5. 剩余风险

1. `data/local_store` 的 `daily_index.parquet` 当前仅含 `fund` 分类（其他会话正在重建），
   实测脚本按目录文件而非元数据索引选股，因此不受影响；但正式接口层若依赖该索引，需等重建完成。
2. Alphalens 对停牌采用 `prices.ffill()` 前向填充，与撮合层“停牌不可交易”不同；
   因子研究属通行口径，但结论不能直接等价于可交易收益。
3. 抽样只覆盖 3,629 只具备 5 年覆盖度的个股，退市/新股样本存在生存偏差。
4. QuantStats HTML 依赖内联 SVG，单文件约 676 KB；若后续接入 UI 需考虑加载体积。
5. 年化口径不统一的问题仍存在于项目内其他报表；本次仅新增可选路径，未修改既有报告逻辑。
