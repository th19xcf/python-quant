# 中国股市量化分析系统

基于 Windows 平台的 Python 量化分析系统，专注于 A 股技术分析、回测引擎、多因子选股与可视化。

**技术栈**：Python 3.14（`>=3.14,<3.16`）/ PySide6 / Polars / SQLAlchemy + MySQL / Loguru / Poetry（`poetry.lock` 锁定全部传递依赖）

---

## Quickstart（10 行命令跑通第一次回测）

```bash
# 1. 克隆并进入项目
git clone <your-repo> && cd quant

# 2. 安装依赖（Poetry 按 poetry.lock 精确安装，跨机器版本可复现）
poetry install              # 运行时 + dev（pytest / ruff / mypy）
poetry install --with gpu   # 可选：仅 CUDA 环境，额外安装 cupy

# 3. 配置数据库与通达信路径
cp .env.example .env            # 已被 .gitignore 保护
# 编辑 .env，至少填 DATABASE__USERNAME / DATABASE__PASSWORD / DATABASE__DATABASE

# 4. 启动桌面 UI（行情图 + 策略回测对话框）
poetry run python -m src.main
```

UI 中打开「策略回测」对话框 → 选股票代码 / 日期范围 / 策略类型 → 运行。
回测内部使用**后复权价**，涨跌停幅度按板块自动推导（创业板/科创板 20%、ST 5%、北交所 30%、主板 10%）。

### 数据源前置条件（四选其一或组合）

项目内置三套数据源，按 `DataFetcher.register_source` 注册顺序故障转移：

| 数据源 | 类型 | 启动条件 | 依赖 |
|---|---|---|---|
| **通达信本地**（TdxHandler） | 本地文件 | `.env.DATA__TDX_DIR` 指向通达信 VIPDOC 目录 | Windows 上需已安装通达信客户端，默认 `C:\new_tdx\vipdoc` |
| **BaoStock** | 网络 API | `pip install baostock`（可选依赖，未写入 pyproject；缺失时该源自动跳过） | 国内网络，无需额外配置 |
| **AkShare** | 网络 API | `pip install akshare`（pyproject.toml 已声明） | 国内网络，无需额外配置 |

推荐开发期把通达信 VIPDOC 准备好（2GB 左右，官网下载），BaoStock/AkShare 作为故障转移兜底。

### 数据库配置

`.env.example` 有模板，必填项：

```
DATABASE__HOST=localhost
DATABASE__PORT=3306
DATABASE__USERNAME=<your-user>
DATABASE__PASSWORD=<your-password>   # .env 已被 .gitignore 保护
DATABASE__DATABASE=quant_db
```

当前代码默认只支持 **MySQL 8.0**（SQLAlchemy 方言 `mysql+pymysql`），其他数据库未测试。首次启动系统会尝试建表。
无数据库也能跑（单文件自包含回测见下），但缺少实时行情 + 因子分析。

### 端到端 Quickstart（一条命令跑通「取数 → 因子 → 回测 → 报告」）

```bash
python examples/quickstart.py                                            # 合成数据，零配置
python examples/quickstart.py --bars 1200                                # 加长样本
python examples/quickstart.py --real 600000.SH --start 2023-01-01 --end 2024-12-31   # 真实数据源
```

流程：合成/取数 → `calculate_factors_cross_sectional` 计算动量因子 → 按因子截面排名选前 2 只
→ `run_backtest` 组合回测（MA 择时）→ 报告落到 `examples/output/`（`report.json` /
`trades.csv` / `equity_curve.csv`）。

脚本默认不读 `.env`、不连数据库，克隆后即可运行；带 `--verbose` 可打开框架日志。
关键不变量断言失败时返回非 0，可直接当作冒烟测试接入 CI——这也是回归验证的最短路径。

---

## 项目结构

```
quant/
├── .env.example                 # 配置模板（密码占位，复制为 .env）
├── .gitignore
├── pyproject.toml               # Poetry 依赖
├── README.md                    # 本文件
│
├── plugins/                     # 插件：可视化（数据源统一在 src/data 内置注册）
│   └── kline_visualization.py
│
├── tools/                       # 运维脚本：通达信精度校准、复权因子重算
├── scripts/                     # 一次性数据修复 / 抽样查询脚本
├── examples/
│   └── quickstart.py            # 端到端样例：取数 → 因子 → 回测 → 报告
├── docs/                        # 架构文档 + 校准指南
│   ├── architecture.md          # 模块依赖与数据流
│   └── tdx_calibration_guide.md
│
└── src/
    ├── main.py                  # 入口：启动 PySide6 主窗口 / 或仅跑数据初始化
    │
    ├── api/                     # 应用层 API（业务 API / 数据 API / DTO）
    ├── business/                # 业务规则引擎
    │
    ├── data/                    # 数据获取与管理
    │   ├── managers/data_fetcher.py   # 多数据源故障转移（TDX → BaoStock → AkShare）
    │   ├── async_data_manager.py      # 数据更新任务调度
    │   ├── adj_factor_calculator.py   # 复权因子计算（Polars 向量化 + 配股公式）
    │   └── sources/                   # 数据源抽象基类
    │
    ├── database/                # SQLAlchemy 模型 + 会话管理
    │   ├── db_manager.py       # connect / disconnect / list_stock_codes
    │   └── models/             # stock_daily / stock_basic / stock_dividend / ...
    │
    ├── backtest/                # 回测引擎
    │   ├── engine/             # execution_core（统一内核）+ backtest_engine / portfolio + base_engine
    │   ├── strategies/         # TrendFollowing / MeanReversion / MultiFactor（无状态）
    │   ├── costs/              # A 股成本模型（佣金 / 印花税 0.05% / 过户费 / 滑点）
    │   ├── performance/        # 绩效分析（Sharpe / Drawdown / ...）
    │   └── visualization/      # 回测结果 PySide6 可视化
    │
    ├── quant/analysis/         # 上层分析对话框：策略回测 / 因子分析 / 推荐
    ├── alpha/                  # Alpha 因子计算器（price_momentum / risk / technical / ...）
    ├── strategy/               # 信号生成 + 策略分析（上层辅助）
    ├── tech_analysis/          # 技术指标计算与注册（MA / MACD / RSI / KDJ / 布林 / ...）
    ├── ui/                     # PySide6 主窗口 + mixin（chart / event / ui / data）
    └── utils/                  # config（pydantic_settings）/ logger / memory / event_bus / ...
```

---

## 8 阶段路线图的真实落地状态

README 旧版宣称 8 阶段，代码现状已完成/部分完成/待补齐，用对照表表示：

| # | 阶段（原 README） | 现状 | 落地位置 |
|---|---|---|---|
| 1 | 基础框架搭建 | ✅ 已完成 | pydantic_settings Config、SQLAlchemy 模型、Polars 全量接管 Pandas |
| 2 | 数据获取功能 | ✅ 已完成 | DataFetcher 多数据源故障转移、DB 双复权列（qfq/hfq）、adj_factor_calculator 含配股公式 |
| 3 | 技术分析 | ✅ 已完成 | tech_analysis/ 20+ 指标（Trend/Oscillator/Volatility/Volume/MarketBreadth），指标插件化注册 |
| 4 | UI 界面开发 | ✅ 已完成 | main_window + 4 个 mixin、pyqtgraph 图表、策略回测对话框 |
| 5 | 量化分析集成 | ⚠️ 部分完成 | **已有**：BacktestEngine（单股）+ PortfolioBacktestEngine（多股组合）；**缺失**：QLib / Valuecell 未集成（独立 repo 体量，按 P2 规划排期） |
| 6 | 股票推荐 | ⚠️ 部分完成 | **已有**：多因子选股 UI + AlphaCalculator 因子回写；**缺失**：策略化回测 / Walk-Forward |
| 7 | 宏观经济与情绪分析 | ❌ 未开始 | 原规划目录不存在（macro_analysis/ sentiment_analysis/），未来按数据源覆盖进度排期 |
| 8 | 自动交易 | ❌ 未开始 | auto_trading/ 目录不存在；需券商 API 对接前置，暂不考虑 |

---

## 设计要点

### 回测引擎定位

| 类 | 用途 | 调用入口 |
|---|---|---|
| `BacktestEngine` | 单股回测（生产路径） | `src/ui/dialogs/strategy_backtest_dialog.py` → `run_backtest` 用例 |
| `PortfolioBacktestEngine` | 多股组合回测（等权分配 + 个股独立信号） | 组合回测用例 |

两者都是 `src/backtest/engine/execution_core.py` 的参数化封装：撮合、T+1、整手、费用、权益估值与风控只有这一份实现。
共享 `BaseBacktestEngine` 的 `_precompute_indicators`（Polars 向量化 MA/MACD/RSI/ATR）与 `_generate_param_combinations`（参数优化）。
`OptimizedBacktestEngine` 已删除。

### 策略无状态约定

所有 `BaseStrategy` 子类**不得**持有 per-bar 累积的 list / dict（见 P0-1 修复）。引擎先在 `run_backtest` 开头一次性向量化预计算全部指标列，策略 `generate_signal(data, index)` 只读当前 bar 的 dict。

### 成本模型（A 股现行规则）

```
buy_cost  =  max(佣金最低5元, 成交额 × 0.03%)           + 过户费 0.001% × 成交额 + 滑点 0.02% × 成交额
sell_cost =  max(佣金最低5元, 成交额 × 0.03%) + 印花税 0.05% × 成交额 + 过户费 0.001% × 成交额 + 滑点 0.02% × 成交额
```

### 数据复权

回测内部统一使用后复权价（`adjustment_type='hfq'`），消除分红送转对收益率计算的干扰。DB 在 stock_daily 表中同时存储 `qfq_*` 和 `hfq_*` 两套列，data_fetcher 按请求类型写入 `open/high/low/close`。

### 幸存者偏差

数据源侧：Baostock / AKShare 自带退市股覆盖；TDX 本地行情文件不含退市股。
代码侧：`DatabaseManager.list_stock_codes(include_delisted=False)` 可过滤退市股（默认 True 时全部返回）。
**注意**：TDX 本地模式下，即便 `include_delisted=True`，也会因源文件缺失拿不到退市股数据——需切换到在线数据源才能彻底消除偏差。

---

## 配置

系统使用 `pydantic_settings BaseSettings`，默认读取项目根目录 `.env`（已被 .gitignore 保护）。字段定义见 [config.py](src/utils/config.py)。

```bash
cp .env.example .env
# 编辑 .env 填入 MySQL 账号密码 + 通达信数据路径
```

---

## 开发约定

- 依赖管理：Poetry；运行时直接依赖固定为 `==` 版本，`poetry.lock` 锁定全部传递依赖（不要在未更新锁文件的情况下手工 `pip install` 升级依赖，否则回测结果不可复现）
- 可选依赖组：`dev`（pytest / ruff / mypy / black，默认安装）、`gpu`（cupy，`poetry install --with gpu`）；GPU 缺失时 `gpu_acceleration.py` 自动降级为 NumPy
- 提交路径：项目目录 `e:\code\python\quant` 为 Git 仓库；每次提交前先同步备份目录 `E:\git\python-quant`（保持相同相对路径）
- 测试：pytest（`tests/` 下 332 个用例，`venv\Scripts\python.exe -m pytest tests -q`）
- 质量门禁（本地与 CI 一致）：
  - `ruff check src`（语法错误、无效比较、未定义名称）
  - `mypy`（当前只检查统一执行内核 `execution_core.py`）
  - `pytest --cov=src --cov-fail-under=39`（当前覆盖率 40%）

---

## 许可证

MIT License
