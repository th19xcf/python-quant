# 架构与数据流

本文档描述中国股市量化分析系统的模块依赖、数据流与关键设计决策。

## 模块依赖（文本式 Mermaid）

```mermaid
graph TD
    subgraph "UI 层（PySide6）"
        MW[main_window.py]
        BD[ui/dialogs/strategy_backtest_dialog.py]
        FAD[factor_analysis_dialog.py]
        IR[indicator_renderer.py + renderers/]
    end

    subgraph "业务应用层"
        BA[api/data_api.py]
        RA[api/business_api.py]
    end

    subgraph "核心业务"
        DM[data_manager.py / async_data_manager.py]
        DF[data_fetcher.py — 多源故障转移]
        AC[adj_factor_calculator.py — Polars 向量化]
        DE[db_manager.py — SQLAlchemy]

        BE[BacktestEngine — 单股回测封装]
        PE[PortfolioBacktestEngine — 多股组合封装]
        EC[execution_core.py — 统一撮合内核]
        OB[BaseBacktestEngine — 共享工具]
        SM[BaseStrategy — TrendFollowing / MeanRev / MultiFactor]

        TE[technical_analyzer.py + analyzer/ + indicators/]
        AC2[alpha_calculator.py + factors/]
    end

    subgraph "数据源（src/data 内置注册）"
        TDX[tdx_handler.py]
        BS[baostock_handler.py]
        AK[akshare_handler.py]
    end

    subgraph "基础设施"
        CFG[utils/config.py — pydantic_settings]
        DB[MySQL 8+]
        REDIS[(Redis 可选)]
        LOG[utils/logger.py — Loguru]
    end

    MW --> BD & FAD & IR
    BD --> BE & PE
    FAD --> AC2 & TE
    BE --> EC & OB & SM & TE   %% 使用 _precompute_indicators 预计算指标列
    PE --> EC & OB & SM

    DM --> DF
    DM --> DE
    DF --> TDX & BS & AK
    DF --> AC     %% 取分红数据算复权
    AC --> DE

    CFG --> DE
    CFG --> DF
    LOG -. 全模块 .-> LOG
    DB --- DE
    REDIS --- DM
```

## 数据流（单股回测）

```
┌────────────────────────────────────────────────────────────────────────────┐
│ UI: StrategyBacktestDialog                                                 │
│   用户输入: stock_code + start/end date + strategy type                    │
└──────────────────────────┬─────────────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ DataManager.get_stock_data(code, start, end, adjustment_type='hfq')        │
│   ├─ global_data_cache 命中 → 直接返回                                     │
│   ├─ _get_data_from_database → 原始 OHLC + qfq_factor/hfq_factor 因子列    │
│   │    └─ _apply_adjustment('hfq') → 主列 × hfq_factor，并保留 raw_open/  │
│   │       raw_close 供撮合内核判定涨跌停                                   │
│   └─ _get_data_from_sources → 按优先级 TDX → BaoStock → AkShare 故障转移 │
└──────────────────────────┬─────────────────────────────────────────────────┘
                           │  Polars DataFrame 含 date/open/high/low/close/volume
                           │                    + qfq_factor/hfq_factor 因子列
                           │                    + raw_open/raw_close（hfq 时保留原始价）
                           ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ BacktestEngine / PortfolioBacktestEngine（同一内核的参数化封装）           │
│   ① _precompute_indicators() → Polars 向量化一次算好:                     │
│      ma_{short}, ma_{long}, macd, macd_signal, macd_hist, rsi, atr        │
│   ② EventBacktestRunner 事件循环:                                         │
│      ├─ ExecutionRules — 涨跌停幅度 + 停牌/开盘触板不可成交               │
│      │    （携带 raw_* 列时用原始价判定：交易所规则定义在原始价上，        │
│      │     除权日复权比值会失真）                                         │
│      ├─ strategy.generate_signal(current_data, index) → O(1) dict lookup  │
│      ├─ pending_signal 延迟 T+1 开盘成交                                   │
│      ├─ AccountBook — 整手取整、买入即时扣费、卖出资金累加               │
│      ├─ RiskGate — 可选风控闸门，拒单写入 rejected_orders                  │
│      └─ equity_curve 按收盘价盯市                                          │
│   ③ PerformanceAnalyzer 算 Sharpe / Drawdown / WinningRate                │
└──────────────────────────┬─────────────────────────────────────────────────┘
                           │ backtest_results dict
                           ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ UI 展示: equity_curve 折线 + trades 表格 + 绩效数字                       │
└────────────────────────────────────────────────────────────────────────────┘
```

## 关键设计决策

### 为什么策略无状态 + 引擎向量化预计算

修复了 P0-1（状态污染）、P0-2（EMA 截断错误）、P0-3（lru_cache 误用）。
策略调用从 O(窗口长度) 的 list 累积 + O(n×period) 的 EMA 递推，
变为 O(1) dict lookup。全市场多年回测的瓶颈转移到：
- 参数优化串行跑（已由 ProcessPoolExecutor worker 解决）
- DB 逐股票拉取（可加批量 preload 缓存）

### 为什么回测用后复权价

消除分红送转对收益率计算的干扰。DB 的 stock_daily 表只存原始
`open/high/low/close` 与两列复权因子 `qfq_factor` / `hfq_factor`
（旧的 `qfq_open`/`hfq_open` 等 8 列预计算价格已整体废弃删除）；
`data_fetcher._apply_adjustment` 在数据出口按请求类型把因子实时乘进主列
（`price × factor`），下游拿到的 `open/high/low/close` 即为所请求的复权价。
hfq 路径额外保留 `raw_open` / `raw_close`：交易所涨跌停规则定义在原始价上，
用复权价比值会在除权日失真。

### 单一执行内核（已收口）

撮合、T+1、整手、费用、权益估值与风控只有一个实现：`src/backtest/engine/execution_core.py`。

| 组件 | 职责 |
|---|---|
| `ExecutionRules` | 涨跌停幅度推导 + 停牌/开盘触板不可成交判定 |
| `AccountBook` | 现金与持仓记账、整手取整、买入即时扣费、卖出资金累加、收盘盯市 |
| `RiskGate` | 唯一的风控闸门，拒单写入 `rejected_orders` |
| `EventBacktestRunner` | 唯一事件循环：T 日信号 → T+1 开盘成交、逐日盯市、结果组装 |

`BacktestEngine` 与 `PortfolioBacktestEngine` 只是同一内核的参数化封装（单标的/多标的、资金分配方式），
不再各自维护撮合分支；`OptimizedBacktestEngine` 已删除。结果字典键保持
`trades / final_equity / total_return / equity_curve / performance / risk_summary / rejected_orders`。

### 为什么 _calculate_factors 用事件逐行更新而不是全因子公式

标准除权参考价公式：

```
除权价 = (prev_close - cash_div + rights_price × rights_ratio)
        ─────────────────────────────────────────────────────
           1 + share_div + rights_ratio
```

每一条分红事件都是独立的"价格跳变边界"，用 `pl.when(trade_date < ex_date)`
逐事件应用 factor，复杂度 O(分红事件数 × 价格行数)，但每轮都是 Polars 表达式
广播而非 Python list comprehension。
相比一次性 cum_prod 公式，事件驱动更透明、更容易加/删事件，
且 Polars 广播常数系数很快，单只股票 20 年数据（~5000 行 × ~20 次分红）
毫秒级完成。

### 涨跌停自适应规则

唯一真源是 `ExecutionRules.resolve_limit_pct(ts_code, is_st, trade_date)`
（`src/backtest/engine/execution_core.py`），判定顺序固定为
**交易日 → 板块常规幅度 → ST 状态**：

| 情形 | 幅度 | 判定依据 |
|---|---|---|
| 科创板 | 20% | 代码 688 / 689 段 |
| 创业板 | 20%（2020-08-24 起）/ 10%（改革前） | 代码 300 / 301 段 + `trade_date` |
| 北交所 | 30% | `.BJ` 后缀优先，无后缀时用 83 / 87 / 88 / 920 号段 |
| 沪深 B 股 | 10% | 900（沪）/ 200（深）显式号段 |
| 主板 | 10% | 兜底 |
| ST / *ST | 5%，仅常规幅度为 10% 的板块 | `is_st=True` 且板块常规幅度为 10%；20%/30% 板块的 ST 股票与普通股同幅度 |
| 数据层未提供 ST 标识 | 按板块常规幅度 | `is_st=None`，只标记 `st_unknown` / `fill_confidence=low`，不改变成交判定 |

涨跌停价按 `round(prev_close × (1 ± limit), 2)` 计算：先转 `Decimal` 再相乘并
`ROUND_HALF_UP`，与交易所"四舍五入到分"口径一致。开盘价触及涨停不可买、
触及跌停不可卖、成交量 0（停牌）不可交易；`PRICE_EPS` 只吸收双精度乘法噪声。

两条回测路径共用同一判定。未传 `ts_code` 时回退主板 10%。

---

## 核心公式与口径说明

所有指标、因子、复权计算统一用 Polars 表达式（`pl.col().ewm_mean() / rolling_mean() / when.then.otherwise`），**不复刻通达信/同花顺/MetaTrader 的分支实现**。指标公式的唯一实现在 `src/tech_analysis/indicators/formulas.py`，`indicator_calculator.py` 只负责编排；图表渲染层不实现公式，只读取预计算列。

| 指标/因子 | 口径 | 公式 / 表达式 | 备注 |
|---|---|---|---|
| **MA** | `rolling_mean(window_size=N)` | `close.rolling_mean(N)` | Polars 内置实现 |
| **EMA** | Wilder `alpha = 2/(N+1)` | `close.ewm_mean(span=N)` | Polars 默认，通达信/同花顺一致 |
| **MACD** | EMA(12) - EMA(26), Signal = EMA(9) | 同上 | 种子：Polars 第一个有效值；通达信用前 26 行 SMA 作种子——**两者在种子附近最多差 1 根 bar**，之后一致 |
| **RSI** | Wilder 平滑 (非简单平均) | `delta=close.diff(); gain=pl.when(delta>0).then(delta).otherwise(0); avg_gain=gain.ewm_mean(alpha=1/14, adjust=False)` | Polars `ewm_mean(alpha, adjust=False)` 即 Wilder 递推 |
| **KDJ** | RSV 含当日 `HHV/LLV(14)`；K/D 为 SMA(X,3,1) 递推 | `rsv=(close-LN14)/(HN14-LN14)*100; K=2/3 K_prev + 1/3 rsv; D=2/3 D_prev + 1/3 K; J=3K-2D` | 唯一实现见 `formulas.kdj_exprs`，种子取首个 RSV；图表与 Alpha 因子层同源 |
| **ATR** | Wilder 14 日 | `TR = max(H-L, \|H-prev_close\|, \|L-prev_close\|)`；`ATR = wilder_expr(TR, 14)`（`ewm_mean(alpha=1/14, adjust=False)`，非 `rolling_mean`） | 唯一实现 `formulas.atr_expr`，引擎 `_precompute_indicators` 一次性算好；首行无 prev_close 时 TR 退化为 `H-L` |
| **后复权价** | `price × cum_prod(复权因子)` | 复权因子在分红 ex_date 前：`(prev_close - 现金分红) / (prev_close × (1 + 送转比例) + 配股比例 × 配股价)` | 上一轮 A1-2 已补配股项，公式统一 |
| **前复权价** | `price × cum_prod(1/复权因子)` | 与后复权互逆 | 同上，只方向相反 |

**回测引擎向量化预计算（只算一次，不复刻策略实现）**：

```python
# 引擎 run_backtest 开头
self.pl_df = self._precompute_indicators(self.pl_df, self.strategy)
# → 自动在 DF 上生成 ma_{short_window} / ma_{long_window} / macd / macd_signal / rsi / atr
```

策略的 `generate_signal(data, index)` 现在是 O(1) dict lookup（读第 index 行的 ma/macd/rsi 值），不再自己维护 Python list。

**整列向量化信号（可选快路径）**：策略可实现 `generate_signals_vectorized(df) -> pl.Series`，
一次 `select` 产出一整列信号，语义必须与逐行 `generate_signal` 严格一致——
如 `MAStrategy` 用 `when(short.is_null() | long.is_null()).then('hold')`
对齐逐行版读取缺失列/窗口未满返回 `'hold'` 的行为。事件循环仍逐 bar 消费该列，
T+1 撮合语义不变。

### 与通达信口径的微小差异（可接受）

| 项 | 我们 | 通达信 | 影响 |
|---|---|---|---|
| **MACD 种子** | Polars 第一个有效值 | 前 26 行 SMA 作种子 | 种子附近最多差 1 bar，20 日线级别几乎不可见 |
| **KDJ M 参数** | 通达信用 3 | 同花顺/东方财富用 2 | 我们按通达信口径 M=3；需同花顺口径自行调参 |
| **MA 空值填充** | Polars 默认前 N-1 行为 null | 通达信用 0 或 NaN 继续算 | 引擎 `_is_tradable` 和持仓都有 `position > 0` 保护，null 不会穿透 |
| **ATR / RSI** | Wilder 平滑 | 通达信一致 | 一致，无差异 |
| **复权因子公式** | 补了配股项 | 通达信只覆盖送转+现金分红 | 我们**更完整**（配股除权日通达信跳空被我们修正） |
