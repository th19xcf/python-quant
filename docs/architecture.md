# 架构与数据流

本文档描述中国股市量化分析系统的模块依赖、数据流与关键设计决策。

## 模块依赖（文本式 Mermaid）

```mermaid
graph TD
    subgraph "UI 层（PySide6）"
        MW[main_window.py]
        BD[strategy_backtest_dialog.py]
        FAD[factor_analysis_dialog.py]
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

        BE[BacktestEngine — 单股回测]
        PE[PortfolioBacktestEngine — 多股组合]
        OB[BaseBacktestEngine — 共享工具]
        SM[BaseStrategy — TrendFollowing / MeanRev / MultiFactor]

        TE[technical_analyzer.py + indicators/]
        AC2[alpha_calculator.py + factors/]
    end

    subgraph "数据源插件"
        TDX[plugins/tdx_datasource.py]
        BS[plugins/baostock_datasource.py]
        AK[plugins/akshare_datasource.py]
    end

    subgraph "基础设施"
        CFG[utils/config.py — pydantic_settings]
        DB[MySQL 8+]
        REDIS[(Redis 可选)]
        LOG[utils/logger.py — Loguru]
    end

    MW --> BD & FAD
    BD --> BE & PE
    FAD --> AC2 & TE
    BE --> OB & SM
    BE --> TE   %% 使用 _precompute_indicators 预计算指标列
    PE --> OB & SM

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
│   ├─ _get_data_from_database → 拿到 qfq + hfq 两套列                      │
│   │    └─ _apply_adjustment('hfq') → 把 hfq_open/high/low/close 写进主列  │
│   └─ _get_data_from_sources → 按优先级 TDX → BaoStock → AkShare 故障转移 │
└──────────────────────────┬─────────────────────────────────────────────────┘
                           │  Polars DataFrame 含 date/open/high/low/close/volume
                           │                    + qfq_*/hfq_* 双列
                           ▼
┌────────────────────────────────────────────────────────────────────────────┐
│ BacktestEngine.run_backtest(initial_capital, lot_size, ts_code)           │
│   ① ts_code → _get_limit_pct() 自适应涨跌停阈值                          │
│   ② _precompute_indicators() → Polars 向量化一次算好:                     │
│      ma_{short}, ma_{long}, macd, macd_signal, macd_hist, rsi, atr        │
│   ③ bar-by-bar 撮合循环:                                                  │
│      ├─ _is_tradable() — 停牌过滤 + 一字涨跌停识别                       │
│      ├─ CostModel.calculate_buy/sell_cost() — A 股现行规则                │
│      ├─ strategy.generate_signal(current_data, index) → O(1) dict lookup  │
│      ├─ pending_signal 延迟 T+1 执行                                       │
│      └─ equity_curve 按收盘价盯市                                          │
│   ④ PerformanceAnalyzer 算 Sharpe / Drawdown / WinningRate                │
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

消除分红送转对收益率计算的干扰。DB 在 stock_daily 表中同时存储
`qfq_*` 和 `hfq_*` 两套列，data_fetcher 按请求类型写入主列，
下游拿到的 `open/high/low/close` 即为所请求的复权价。

### 三引擎为何不合并

| 引擎 | 独特之处 | 合并难度 |
|---|---|---|
| BacktestEngine | 单股撮合，生产路径 | — |
| OptimizedBacktestEngine | 历史副本（lru_cache + 卖出记账 bug 均已修） | 低，仅保留 |
| PortfolioBacktestEngine | 按交易日并集循环、多股独立信号、独立 capital | 高，与单股撮合语义不同 |

当前策略：三者共享基类工具（_is_tradable / _precompute_indicators / _select_best_params），
业务撮合循环各自维护。未来若需统一，可把撮合骨架抽成模板方法，
但当前成本收益比不划算。

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

```python
def _get_limit_pct(ts_code, is_st=False):
    if is_st:        return 0.05  # ST / *ST
    if 300/301xx    return 0.20  # 创业板
    if 68/688xx     return 0.20  # 科创板
    if 4/8/9xxxxxx  return 0.30  # 北交所
    return 0.10                    # 主板默认
```

三个引擎均已对齐。向后兼容：未传 ts_code 时回退 `limit_pct=0.1`。
