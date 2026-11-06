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

项目内置三套数据源，但**默认只注册本地通达信一套**：`src/utils/config.py` 的
`data.allow_remote_sources` 默认为 `False`，`DataService` 在该开关关闭时只注册
TDX、不发起任何网络请求（见 `src/data/services/data_service.py`）。只有显式设置
`DATA__ALLOW_REMOTE_SOURCES=true`，才会按 `DataFetcher.register_source` 的注册顺序
追加 BaoStock → AkShare，形成三源故障转移。

| 数据源 | 类型 | 启动条件 | 依赖 |
|---|---|---|---|
| **通达信本地**（TdxHandler） | 本地文件 | 恒注册；`.env` 中 `DATA__TDX_DATA_PATH` 指向通达信 VIPDOC 目录 | Windows 上需已安装通达信客户端，默认 `C:\new_tdx\vipdoc` |
| **BaoStock** | 网络 API | 需 `DATA__ALLOW_REMOTE_SOURCES=true`；缺失依赖时该源自动跳过 | `pyproject.toml` 已声明可选组 `baostock`，国内网络 |
| **AkShare** | 网络 API | 需 `DATA__ALLOW_REMOTE_SOURCES=true`；缺失依赖时该源自动跳过 | `pyproject.toml` 已声明 `akshare`，国内网络 |

推荐开发期把通达信 VIPDOC 准备好（2GB 左右，官网下载）；需要在线源兜底或回补退市股时再显式打开远程开关。

### 数据库配置

`.env.example` 有模板，必填项：

```
DATABASE__HOST=localhost
DATABASE__PORT=3306
DATABASE__USERNAME=<your-user>
DATABASE__PASSWORD=<your-password>   # .env 已被 .gitignore 保护
DATABASE__DATABASE=stock_quant
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
├── alembic.ini                  # Alembic 迁移配置（数据库 URL 由 migrations/env.py 从 .env 读取）
├── migrations/                  # 数据库迁移脚本（versions/ 下为增量版本，禁止手写 DDL）
│
├── plugins/                     # 插件：可视化（数据源统一在 src/data 内置注册）
│   └── kline_visualization.py
│
├── tools/                       # 运维脚本：通达信精度校准、复权因子重算
├── scripts/                     # 一次性数据修复 / 抽样查询脚本
├── examples/
│   └── quickstart.py            # 端到端样例：取数 → 因子 → 回测 → 报告
├── docs/                        # 架构文档 + 校准指南
│   ├── 架构与数据流.md          # 模块依赖与数据流
│   └── 通达信单股票校准指南.md
│
└── src/
    ├── main.py                  # 入口：启动 PySide6 主窗口 / 或仅跑数据初始化
    │
    ├── api/                     # 应用层 API（业务 API / 数据 API / 表现层 DTO）
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
    ├── quant/                  # analysis/（分析对话框）+ reports/
    ├── core/usecases/          # 应用用例编排
    ├── recommendation/         # 多因子选股推荐（stock_recommender.py）
    ├── risk/                   # 风控规则（rules.py）
    ├── plugin/                 # 插件基类 / 注册表 / 管理器
    ├── alpha/                  # Alpha 因子（calculator / cross_section / evaluation / factors）
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

所有 `BaseStrategy` 子类**不得**持有 per-bar 累积的 list / dict（见 P0-1 修复）。引擎先在 `run_backtest` 开头一次性向量化预计算全部指标列，策略 `generate_signal(row, index)` 只读当前 bar 的 dict。

### 成本模型（A 股现行规则）

单一真源是 `src/backtest/costs/cost_model.py` 的 `COST_DEFAULTS`；本段数值由
`tests/test_p0_hardening.py::test_readme_cost_constants_match_code` 自动校验，
改常量必须同步改文档。

| 项目 | 代码常量 | 现行值 |
|---|---|---|
| 佣金（双边） | `commission_rate` | **0.05%**（单笔最低 5 元） |
| 印花税（仅卖出，2023-08-28 起） | `stamp_tax_rate` | **0.05%**（ETF/LOF 豁免） |
| 过户费（双边，2015-08-01 起沪深统一） | `transfer_fee_rate` | **0.002%** |
| 滑点兜底（仅 ETF/LOF 缺 ADV20 时） | `slippage` | **0.02%** |

```
buy_cost  =  max(5元, 成交额 × 0.05%)           + 成交额 × 0.002% + 滑点档 × 成交额
sell_cost =  max(5元, 成交额 × 0.05%) + 印花税 × 成交额 + 成交额 × 0.002% + 滑点档 × 成交额
```

历史区间传 `trade_date` 时按 `STAMP_TAX_SCHEDULE` / `TRANSFER_FEE_SWITCH` 取当日起费率，
不得用现行费率回测 2015 / 2023 切换前的区间。

**分档滑点（2026-11-06 起主撮合链路已真实生效）**：

- 个股按总市值（亿元）：≥500 亿 **15bp**、≥100 亿 **20bp**、其余或市值缺失 **30bp**。
  输入列 `total_mv_yi`（亿元）或 `total_mv`（元，自动 /1e8）。
- ETF/LOF 按 20 日均成交额（元）：≥1 亿 **5bp**、≥5,000 万 **7bp**、其余 **10bp**；
  `category=sector/industry/theme` 取相邻更高档。输入列 `amount`。
- **因果约束**：订单在 t 日开盘成交，分档输入一律按 `shift(1)` 只用 t-1 及之前数据，
  不使用当日全天成交额/收盘市值（避免前视偏差）。
- 完全没有分档输入时，个股一律 30bp、ETF 一律 2bp 兜底；结果中的
  `cost_inputs.tiered_symbols` 会显式列出覆盖情况并输出 warning，不再静默兜底。

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
- 数据库迁移：Alembic（`.\.venv\Scripts\python.exe -m alembic upgrade head`；改表必须先 `alembic revision --autogenerate -m "..."` 生成脚本，禁止手写 DDL，`create_tables()` 内部已走 `alembic upgrade head`）
- 测试：pytest 全量回归（`.\.venv\Scripts\python.exe -m pytest tests -q`）
- 质量门禁（本地与 CI 一致）：
  - `ruff check src tests`（pyflakes / bugbear / bandit / isort / mccabe C901 等全量规则）
  - `mypy src`（覆盖 src 下全部业务模块，配置见 `pyproject.toml [tool.mypy]`）
  - `pytest --cov=src`（覆盖率门禁 `[tool.coverage.report] fail_under = 63`，当前约 64%）

---

## 许可证

MIT License
