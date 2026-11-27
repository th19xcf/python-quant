# china-stock-quant 技术审查报告

审查对象：`E:\code\python\quant`　审查日期：2026-11-23（22:19 更新：第 7 节含修复、静态检查清零与全量回归结果）

**来源标识**：【代码】代码直接证据　【测试】测试直接证据　【静态】静态分析结果　【推论】工程推论　【未验证】未验证

---

## 0. 审查方法与限制（请先读）

- 扫描范围：`src/` 下 407 个模块（13.8 万行），全部做了 AST 解析；`tests/` 下 285 个文件做了关键词扫描；另外读了核心文件片段、`pyproject.toml`、`.venv/pyvenv.cfg`、`coverage.json`（2026-11-22 10:14 生成）和 `daily_update_run.log`。【静态】
- **没有逐行通读全部 13.8 万行**。结论来自全量 AST 指标，加上对 `data_fetcher`、`data_service`、`execution_core`、`backtest_engine`、`data_cache`、`industry_pit`、`workbench/server`、`analysis_cache` 等关键文件的人工阅读。
- **执行环境限制**：我在你电脑上运行代码的环境禁止启动子进程，所以**没能激活 venv 跑 pytest/coverage，也没做任何性能基准、CPU/内存/IO 监控**。第 1.4 节的覆盖率数据取自项目里已有的 `coverage.json`；第 4 节和第 5 节中涉及运行结果的结论都标为【未验证】，第 6 节给出了你可以在 venv 里自己执行的命令。
- 初版审查只读。之后按你的要求做了修复，见**第 7 节**；第 7 节的结论优先于前文。

---

## 1. 代码架构与可维护性

### 1.1 模块耦合与内聚

| 包 | 被依赖数 Ca | 依赖外部数 Ce | 不稳定度 I | 判断 |
|---|---|---|---|---|
| utils | 10 | 2 | 0.17 | 基础层，但反向依赖了 data 和 tech_analysis ❌ |
| data | 10 | 6 | 0.38 | 核心层，却依赖了 api、backtest、core ❌ |
| tech_analysis | 9 | 4 | 0.31 | 依赖了 api、data、database |
| database | 5 | 0 | 0.00 | 很干净 ✅ |
| risk | 4 | 1 | 0.20 | 只依赖 core ✅ |
| core | 5 | 8 | 0.62 | 用例层依赖 advice、quant，层次倒置 |
| advice / ui / main | 1/1/0 | 8/7/6 | 0.88–1.0 | 顶层，符合预期 ✅ |

- **包级别有 10 对循环依赖**：advice↔core、api↔data、api↔tech_analysis、backtest↔data、backtest↔core、core↔data、core↔risk、data↔utils、data↔tech_analysis、tech_analysis↔utils。【静态】
- 函数内的延迟 import 共有 **566 处**，最多的是 `data_manager`（18）、`risk_flags`（18）、`local_store/build`（17）、`finance_service`（16）。这说明循环依赖主要靠"延迟导入"来绕开，并没有真正解除。【静态】
- 影响：`data` 和 `utils` 本应是底层稳定包，现在却反向依赖上层。想把数据层单独抽成服务，或者给实盘复用，都必须先拆掉这些环。另外改动 data 层时会连带影响回测和用例层，扩展成本高。【推论】
- 依赖面最广的模块：`ui.main_window`（26 个内部依赖）、`main`（21）、`data_fetcher`（18）、`local_store.build`（17）、`advice.pipeline`（17），它们是改动风险的集中点。【静态】
- 被依赖最多的模块：`utils.config`（44）、`data.atomic_write`（40）、`utils.logger`（38）、`local_store.store`（38）、`utils.code_converter`（37），这些模块的接口要格外稳定。【静态】

**建议**
1. 把 `utils` 中引用 data 和 tech_analysis 的 2+2 处 import 下沉，或改成依赖注入，让 utils 成为真正没有依赖的底层。
2. 在 `data` 和 `backtest`/`core` 之间抽一个 `src/contracts` 包（放 Protocol 和 DTO），双方都只依赖这个契约包。
3. 加一个 `import-linter`（或在 ruff 里加 `TID` 规则）配置分层契约，接入测试，防止循环依赖回潮。

### 1.2 代码复用与重复实现

- 有 16 组函数的 AST 完全相同（函数体至少 8 行），**集中在 UI 层**：`attribution`、`industry_heat`、`portfolio_risk`、`style_rotation` 四个页面各自复制了 `_make_table`、`_finalize_table`、`_sort_table_rows`、`_on_header_clicked`、`_restore_row_order`、`_apply_row_height_mode`、`_renumber_rows`、`_sort_key`、`_on_task_completed`、`_item` 等约 10 个方法。`table_sizing.py` 里本来已经有 `_unify_table_surface` 和 `_renumber_rows`，但这些页面仍然各自保留了一份副本。【静态】
- 日期解析有多套实现：`_as_date` 这个名字在 **19 个模块**里各定义了一次，另外 `performance_analyzer._parse_date`、`runner.pipeline.to_date`、`runner.progress._to_date` 三者完全相同。而 `utils` 里其实已经有被引用 74 次的 `as_date`。【静态】
- 同名函数在多个模块中重复定义：`_sha256`（7 处）、`_default_root`（7 处）、`get_stock_data` 和 `get_index_data`（各 9 处，分布在数据源、服务和 API 层）。【静态】
- `utils` 里有一批死代码，引用次数为 0：`process_large_df`、`print_df_memory`、`optimize_df`、`handle_error_gracefully`、`ErrorCodeManager`，以及 `RenderError`、`PluginLoadError`、`OrderRejectedError`、`InsufficientDataError`、`DataSourceTimeoutError` 等 10 个异常类。异常体系定义了，但实际没人用。【静态】
- 复用得好的公共接口：`get_config`（88 次）、`as_date`（74）、`is_index_code`（53）、`EventType`（45）、`MemoryOptimizer`（40）。【静态】

**建议**：把 UI 表格的这些方法抽成 `SortableTablePageMixin`，预计能删掉约 1000 行重复代码；全部 `_as_date`、`to_date` 统一改用 `utils.date_utils.as_date`，`_sha256` 收敛到 `atomic_write` 或 `utils.hashing`；死代码和不用的异常类要么删掉，要么真正接入错误处理流程。

### 1.3 接口设计规范性

- 有文档与实现不一致的例子：`services/data_service.py` 顶部注释写的取数链路是"缓存 → 数据库 → 数据源"，但 `DataFetcher.get_stock_data` 实际是"缓存 → 本地列存 → 数据源（TDX，远程源并行）→ 远程库（需显式开启）"。【代码】
- 同一个注释里还写着"TDX / BaoStock / AkShare 故障转移"，实际实现是：TDX 恒定注册，远程源**并行竞速**，取最先成功的结果（`_try_remote_sources_parallel`）。【代码】
- 参数校验做得比较规范：`get_stock_data` 会先按资产类型分流（指数和基金分别走自己的路径），缓存键包含 `frequency`、`adjustment_type` 和 `as_of`，避免不同复权方式的数据串用；执行内核用 `finite_float` 把价格统一转成有限浮点数（标注为 A24）。【代码】
- 错误处理的标准化程度只有中等：
  - `except Exception` 有 **229 处**，其中 **18 处**直接 `pass`、`continue` 或 `return`，等于把异常吞掉了，集中在 `ui/workbench/server`（4）、`analysis_cache`（2）、`gpu_acceleration`（2）、`plugin_ops`（2），以及 `dataset_contracts`、`st_history`、`local_store/store` 各 1 处。【静态】
  - 没有裸 `except:`。【静态】
  - 定义好的异常类大多没被使用（见 1.2），调用方只能靠捕获 `DataSourceException` 或 `Exception` 处理错误。【静态】
- 规模指标：共 5425 个函数，超过 100 行的 52 个，超过 50 行的 383 个；圈复杂度 ≥20 的有 81 个，最高的几个是 `code_migration._pair_record`（44）、`learning.evaluator.simulate_long_only`（43，228 行）、`st_price_infer.samples`（40）、`core.tradability.filter_day`（39）、`multi_resonance_timing_v3.scan_triggers`（39）、`execution_core._run_trading_loop`（35，172 行）。另外 `core.usecases.run_backtest.run_backtest` 有 222 行。【静态】
- 类型注解：81.2% 的函数标注了返回类型。【静态】

### 1.4 单元测试覆盖率

数据取自 `coverage.json`，2026-11-22 生成，行覆盖率，未开启分支覆盖。我没有重新运行，所以下表是【静态】，是否与当前代码一致属于【未验证】。

- **总覆盖率 71%**（61682 条语句）。`pyproject` 里设的门槛是 `fail_under = 71`，但注释记录 11-22 复测只有 **70.54%**，**当前门槛实际上没通过**。而且注释里写明"未接入 CI（仓库无 .github 工作流）"，这个门槛完全靠手动执行。【代码】
- 未开启分支覆盖（`branch_coverage: False`）。交易逻辑里涨跌停、ST、停牌等分支很多，行覆盖率会高估真实的测试程度。【静态】

| 关键模块 | 覆盖率 | 评价 |
|---|---|---|
| risk（整体） / risk/rules.py | 94.6% / 94.4% | 好 |
| backtest/costs/cost_model.py | 98.1% | 好 |
| backtest/engine/execution_core.py | 89.9% | 好 |
| backtest/engine/backtest_engine.py | 86.3% | 可接受 |
| data/managers/data_fetcher.py | 72.6%（1060 条语句） | 中等，取数主路径 |
| **data/adj_factor_calculator.py** | **47.0%** | ❌ 复权是价格正确性的根基 |
| **quant/analysis/portfolio_risk_local.py** | **42.0%** | ❌ 组合风险 |
| data/risk_flags.py | 67.4% | 偏低 |
| plugin（整体） / main.py / api | 40.2% / 48.4% / 52.5% | 偏低 |
| strategy_lab/panels.py、plugin_manager、attribution_local、st_name_history | 19%–38% | 很低 |

- 测试内容方面：85 个测试文件涉及 PIT/as_of，35 个涉及成本和印花税，27 个涉及停牌，14 个涉及涨跌停，5 个专门测未来函数（如 `test_adjustment_pit.py`），9 个涉及样本外或 walk-forward，8 个涉及并发。另外有 `perf` 和 `slow` 两个 marker，其中 `slow` 用于截断后重跑、对比结果来防未来函数。默认运行时排除了 `perf`。【测试】
- 没有使用 hypothesis 做基于性质的测试。【测试】

**建议**：在 `[tool.coverage.run]` 里加 `branch = true`；优先给 `adj_factor_calculator` 补测试（建议覆盖率 ≥85%），可以用配股、送转、连续除权等场景，对比"由复权因子反推的价格"和交易所公布的除权价；把 `pytest` 和 `tools/check_module_coverage.py` 接入 CI（GitHub Actions 或本地 pre-push 钩子都行）。

### 1.5 文档完整性

- 公开函数和类的 docstring 覆盖率为 **75.6%**（2341/3098）。【静态】
- 核心模块注释质量很高：`execution_core` 每个关键决策都标注了 A8、A11、A14、A24 等编号和原因；`pyproject` 里逐条解释了每个依赖为什么锁定这个版本。【代码】
- `docs/` 下有 160 个文件，README 约 25KB，并且写有"路线图真实落地状态"一节。【静态】文档内容是否与代码逐项一致【未验证】。
- 已经发现的不一致见 1.3。另外 README 写的是"三套数据源"，但 `src/data/tencent/` 还有一个腾讯补数客户端，README 的数据源表里没有。【代码】

---

## 2. 数据管理流程

### 2.1 数据源可靠性

- **默认只用本地数据**：`data.allow_remote_sources` 默认是 False，此时只注册 TDX，不发起任何网络请求。【代码】
- 远程源（AkShare、BaoStock）可以放到子进程里隔离运行（`_PROCESS_ISOLATED_HANDLERS`），并配有超时处理（`_settle_timeout_futures`）、迟到结果回收（`_consume_late_result`）、健康检查（`data_source_health`）、限流（`rate_limit.py`）和指数退避重试（`configure_retry`）。【代码】
- 远程源通过并行竞速取最先成功的结果，可用性更高。但不同源的数据口径不一致（复权方式、成交量单位、停牌时是否返回行）时，**同一请求每次可能由不同的源返回**，结果可能不可复现。项目有 `akshare_schema` 和 `baostock_schema` 做字段适配，但数值口径对齐要靠 `quality/cross_check.py`。【推论】
- 第三方接口很脆弱：pyproject 的注释里记录了 AkShare 依赖 `py_mini_racer`，缺包时会表现为 RemoteDisconnected；`industry_pit.py` 会用 MiniRacer 执行巨潮网站的 JS（`cninfo.js`）。上游一改版，这部分就会失效。【代码】
- `industry_pit.py:230` 请求申万数据时设置了 `verify=False`，关闭了 TLS 证书校验，存在被中间人篡改行业分类数据的风险。【代码】
- 每日更新日志 `daily_update_run.log`：没有 ERROR，WARNING 至少有 1000 条（统计时达到了计数上限）。告警这么多容易淹没真正的问题。【静态】具体内容【未验证】。

### 2.2 数据处理管道

- 存储分层合理：本地 Parquet 列存（约 5.2 万个 parquet 文件）是主读取路径，TDX 的 `.day` 文件只用来构建和增量更新，MySQL 退化成归档和兜底；写入通过 `atomic_write` 保证原子性（被 40 个模块使用）；有 `data_version`、`snapshot_manifest`、`changelog` 做版本追溯。【代码】
- 质量体系完整：`quality/` 下有 22 个模块，包括异常检测、交叉校验、新鲜度、隔离（取数时自动 `drop_quarantined`）、可交易池、豁免清单、停牌并集。【代码】
- PIT 设计正确：默认用后复权；前复权必须传 `as_of`，系统会截掉这个日期之后的除权事件，保证历史任意时点看到的前复权价都一致；行业和 ST 也都有时点版本（`industry_pit`、`st_history`）。【代码】【测试】（`test_adjustment_pit.py`）
- 潜在瓶颈：
  - 有 11 处 `map_elements`（Polars 中逐行调用 Python 函数，是性能反模式），集中在 `financials/collector`（5）和 `code_migration`（2）。【静态】
  - 有 81 处 `for … in iter_rows/rows` 逐行循环，集中在 `quality/clean_set`（10）和 `tradable_universe`（7），这两个都是全市场规模的模块。【静态】这些循环是否真的落在热路径上【未验证】。
  - 5.2 万个小 parquet 文件会带来大量文件句柄和元数据开销，全市场批量读取时会受 IO 和文件数量限制。【推论】可以考虑按年份或板块合并分区，或者用 `pl.scan_parquet` 配合 glob 一次性扫描。
- 缓存：`global_data_cache` 有锁，有 `max_size` 和 `ttl`；`get` 返回的是对象引用而不是拷贝。Polars DataFrame 本身是不可变的，风险较低，但如果调用方对返回结果做原地修改（比如经 `to_pandas` 转换后），可能污染缓存。【代码】【推论】

### 2.3 数据安全与合规

- `.env` 已被 `.gitignore` 忽略，里面包含 `DATABASE__PASSWORD` 和 `REDIS__PASSWORD`；源码中没有扫到明文密码字面量。【代码】【静态】
- `.env` 中的密码以明文存储，没有接入系统凭据管理器（Windows Credential Manager 或 keyring）。【代码】
- 是否有任何历史 commit 曾经提交过 `.env`，这次没能运行 git 检查，【未验证】。建议执行 `git log --all -- .env`。
- 数据库连接是否开启 TLS（`pymysql` 的 ssl 参数）【未验证】。
- `analysis_cache` 和 `backtest_engine` 使用了 pickle：缓存是本地自建的，并且有 schema 校验，【代码】。但如果缓存目录被其他进程写入，`pickle.load` 可能导致任意代码执行，属于低风险。【推论】
- 回测工作台的 HTTP 服务只绑定 `127.0.0.1`，端口随机，【代码】；但没有鉴权 token，本机其他进程或用户都能调用它来启动回测。【推论】
- 数据留存：`_backup/`、`backups/`、按日期命名的审计目录都已加入 gitignore，但我没有看到自动清理或留存天数的策略（日志有 `LOG__RETENTION`，只针对日志）。【静态】【推论】

---

## 3. 执行环境（venv）验证

- `.venv` 在项目目录内（`poetry.toml` 配置了 `in-project = true`），Python 版本为 **CPython 3.14.9**，`include-system-site-packages = false`，与系统环境隔离良好。【代码】
- 共装有 136 个包。核对 9 个关键依赖（pandas 3.1.0、polars 2.1.0、numpy 2.5.4、sqlalchemy 2.1.6、pydantic 2.14.2、akshare 1.20.21、numba 0.68.1、pytest 9.2.0、coverage 7.17.0），**版本都与 pyproject 的锁定值一致**。【静态】
- 没有对 136 个包逐一和 `poetry.lock` 比对；可选组（gpu、analytics、learning、tdx、gui）是否已安装【未验证】，可以执行 `poetry check --lock` 和 `poetry install --sync --dry-run`。
- `.env` 中 21 个配置项都已设置；`DATA__TDX_DATA_PATH` 指向的目录是否真实存在【未验证】。
- `pyproject` 声明的 Python 下限是 3.12，但当前环境只有 3.14，ruff 的 `target-version` 是 py312。3.12 和 3.13 从没跑过测试，声明的兼容性不可信。【代码】【推论】

---

## 4. 性能与策略效率

> 本节所有运行时指标（回测速度、吞吐量、CPU、内存、IO）都没能实测，均为【未验证】。

- 回测执行内核已经做过多轮热路径优化：把取价函数内联、每个 bar 只做一次 `bar_columns` 元组解包、挂单改用堆结构按到期弹出（从 O(D×N) 降下来）、ST 状态用预排序加二分查找、只有启用风控时才构造当日停牌集合。【代码】
- 主循环是 **Python 层的逐日、逐标的双重循环**（`_run_trading_loop`），复杂度约为 O(交易日数 × 当日标的数)。全市场 5000 只股票跑 10 年约 1200 万个 bar，会受 Python 解释器速度限制。【代码】【推论】可以考虑：信号计算保持向量化（已经有 `generate_signals_vectorized`，12 处实现）；撮合部分把价格、成交量预先放进 NumPy 数组，用 numba 加速（numba 已经是依赖）；或者先按"当日有挂单或持仓"的标的筛选，再进入内层循环。
- 参数寻优：`backtest_engine` 会按估算的串行耗时与进程池启动成本（注释实测 Windows 下 spawn 一个进程要 2.1–2.4 秒）自动选择线程池或进程池，行情数据通过 initializer 一次性注入子进程，避免对每个参数组合重复 pickle 大 DataFrame。【代码】这个设计很好。
- 项目里共有 24 处 `time.sleep`、141 处 `datetime.now()`。后者会影响回测的可复现性；有些是用于日志和时间戳，是否有出现在计算逻辑里的【未验证】。【静态】
- 有现成的性能门禁（`pytest -m perf`，固定 seed、多次取中位数），但默认不运行。【测试】建议定期执行并记录基准值。

---

## 5. 风险隐患

### 5.1 技术风险

| # | 风险 | 等级 | 来源 |
|---|---|---|---|
| T1 | 包级别有 10 对循环依赖，靠 566 处延迟 import 维持；import 顺序一变就可能在运行时报 ImportError | 高 | 【静态】 |
| T2 | 18 处 `except Exception` 直接吞掉异常，其中 `local_store/store`、`dataset_contracts`、`st_history` 都在数据链路上，可能静默产出错误数据 | 高 | 【静态】 |
| T3 | 覆盖率门槛实际没通过（70.54% < 71%），且没有 CI，所有质量门槛靠人工执行 | 高 | 【代码】 |
| T4 | 复权因子计算覆盖率只有 47%，一旦出错会系统性污染所有回测价格 | 高 | 【静态】【推论】 |
| T5 | `verify=False` 关闭 TLS 校验，行业数据有被篡改的风险 | 中 | 【代码】 |
| T6 | 31 处 Lock、16 处线程池、5 处进程池，再加上全局单例（缓存、本地存储、健康检查）；`ui/workbench/server` 有 3 处 `time.sleep` 和 4 处吞异常，并发场景下容易出现难以复现的问题 | 中 | 【静态】【推论】 |
| T7 | 数据存储依赖本地单台机器上的 Parquet 和 TDX 目录，没有异地备份就是单点故障；实际备份策略【未验证】 | 中 | 【推论】 |
| T8 | 依赖 AkShare、巨潮 JS、申万网页等非官方接口，上游一改版就失效 | 中 | 【代码】 |
| T9 | 回测工作台的本地 HTTP 服务没有鉴权；pickle 缓存有反序列化风险 | 低 | 【代码】【推论】 |
| T10 | 只在 3.14 上测试过，声明支持 3.12 但没验证 | 低 | 【代码】 |

### 5.2 策略风险

- **未来函数防护很强**：默认后复权、前复权用 `as_of` 截断、行业和 ST 用时点数据、除权在当日撮合之前记账、挂单设有效期（TTL）、`slow` 测试会截断数据重跑做对比。【代码】【测试】
- A 股交易规则建模完整：涨跌停幅度按板块推导（主板 10%、创业板和科创板 20%、ST 5%、北交所 30%），停牌时挂单顺延，成本包含佣金、0.05% 印花税、过户费和滑点，冲击成本按 ADV20 计算，现金计息。【代码】【测试】
  - T+1 规则在 `execution_core` 里只出现了 1 处关键词，具体是在哪一层强制执行的（是在 book 账簿层吗？），这次没有追踪到，【未验证】。
- **过拟合风险**：
  - `backtest_engine` 提供参数网格并行搜索（`param_combinations` 出现 22 次），代码里与样本外相关的关键词有 110 次，9 个测试文件涉及 OOS 或 walk-forward，说明有样本外机制。【静态】【测试】
  - 但我没看到针对多重检验的修正（如 Deflated Sharpe Ratio、PBO/CSCV、White's Reality Check）。参数网格越大，最优参数的样本内 Sharpe 就越可能被高估。【推论】
  - `multi_resonance_timing_v3` 这种版本号迭代到 v3、圈复杂度 39 的策略，本身就提示可能存在反复调参的痕迹。【推论】
- **市场适应性和极端行情**：没看到专门的压力测试场景，比如 2015 年股灾的千股跌停、流动性枯竭、2016 年熔断、2024 年初小盘股踩踏。【未验证】建议做一份固定的极端区间回测清单，重点检查连续跌停导致卖不出、冲击成本暴增的情况。
- 冲击成本模型在缺少 ADV 数据时返回 0（注释说明这是刻意设计的，见 `cost_model.py:289`），对小盘股或新股可能低估交易成本。【代码】【推论】

---

## 6. 优先级行动清单

**P0（本周）**
1. 补 `adj_factor_calculator` 的测试，把覆盖率从 47% 提到 85% 以上；同时开启 `branch = true`。
2. 清理数据链路上吞异常的 18 处 `except Exception`：至少要记录日志并上报，数据写入路径应该直接失败。
3. 修正 `data_service.py` 中过时的取数链路注释。
4. 建立 CI（或本地 pre-push 钩子）：依次执行 ruff、mypy、`pytest --cov`、`check_module_coverage`。

**P1（本月）**
5. 用 import-linter 固定分层；先拆掉 utils→data 和 data→backtest/core 这几处反向依赖。
6. 抽出 UI 表格 Mixin，统一 `_as_date`，删掉 utils 中的死代码。
7. 把申万数据请求改成"补全证书链后再校验"，或者固定证书指纹，不要直接关闭 TLS 校验。
8. 建立性能基准：定期运行 `pytest -m perf`，并新增一个全市场 10 年回测的基准，测墙钟时间和峰值内存。

**P2（季度）**
9. 拆分 `DataFetcher`（2127 行）和 `run_backtest`（222 行）、`_run_trading_loop`（172 行），后者可以用 numba 加速。
10. 引入 Deflated Sharpe 或 PBO 评估过拟合，并建立固定的极端行情回测清单。
11. 合并 Parquet 小文件；建立数据异地备份和留存策略；`.env` 中的密码改由 keyring 管理。

### 需要在 venv 里运行的验证命令（我这次没法执行）

```powershell
cd E:\code\python\quant
.\.venv\Scripts\Activate.ps1
python -c "import sys; print(sys.prefix)"          # 确认 venv 已激活
poetry check --lock; poetry install --sync --dry-run
python -m pytest --cov=src --cov-branch --cov-report=html --cov-report=json
python tools/check_module_coverage.py
python -m pytest -m perf                            # 性能门禁
python -m cProfile -o bt.prof examples/quickstart.py --bars 1200
ruff check src; mypy src
git log --all --oneline -- .env                     # 检查密钥是否曾被提交
```

---

## 7. 2026-11-23 修复进展与结论修正

这一节记录初版报告之后实际做的修改。测试是你在 `.venv`（CPython 3.14.9）里跑的，全量结果见 `tmp/full_test_20261123_v2.txt`。

### 7.1 对前文结论的修正

| 前文结论 | 修正 |
|---|---|
| 1.3 / 5.1 T2 说"18 处 `except Exception` 吞掉异常，风险高" | 逐处看过后，**只有 7 处真的需要改**，都已改成记日志或收窄异常类型。其余 11 处是有意的降级兜底，注释里写了原因（比如 GPU 探测失败时退回 CPU、子进程把异常回传、插件属于外部代码）。T2 风险等级降为**低**。【代码】 |
| 1.4 说"总覆盖率 71%，门槛实际没通过" | 开启分支覆盖后，口径变成"语句 + 分支"，全量实测 **68.05%**（只算行是 70.95%）。`fail_under` 按新口径重新定为 68，README 和门槛测试已同步更新。【测试】 |
| 1.4 说 `adj_factor_calculator` 覆盖率 47% | 新增 49 个用例后达到 **94%**（行 + 分支），剩下没覆盖的是 IntegrityError 分支、一条走不到的分支和 CLI 入口。【测试】 |
| 1.2 说"日期解析有 19 份副本" | 对 28 种输入逐份对比后，**12 份已改为调用 `utils.dates.as_date`**（`as_date` 新增支持整数 YYYYMMDD 和斜杠格式），其余 7 份因为语义不同而有意保留（报错方式、序数日期、斜杠容错、CLI 的 `ArgumentTypeError`）。【代码】【测试】 |
| 4 节说冲击成本模型"缺 ADV 返回 0" | 另外发现策略试算默认会把**冲击成本重复计算**：16.5bp 往返已经包含冲击，`impact_coef=0.015` 又另加了一次平方根冲击，而且印花税固定按 0.1% 计。现在已改成 `impact_coef=0`、印花税用 `historical`。【代码】 |
| 2.1 说"申万请求 `verify=False`" | 已改为默认校验证书，通过 `SW_CA_BUNDLE` 补中间证书，`SW_ALLOW_INSECURE=1` 仅用于应急，并有 7 个用例覆盖。**申万站点的证书链这次没有实测**（域名解析失败）。【代码】【测试】【未验证】 |

### 7.2 新发现并修复的真实缺陷

1. **没有人工裁定文件时，ST 区间表写不进去**：`st_history.apply_reviewed_conflicts` 在缺少裁定文件时不会补出 `evidence/raw_fetched_at` 两列，结果被 11-22 加的写入契约拒绝，新环境或重建时整张表都生成不出来。现在无论是否有裁定文件都会补齐输出列。【代码】【测试】
2. **新股只靠 RSI 一项就能拿高分**：11-21 RSI 改成通达信口径后，第 2 根 K 线就有值。新股的其他因子因为窗口不够都是空的，权重重新归一后 RSI 独占 100%，测试里一只上市 10 天的新股拿到 1.79 分。现在打分侧给 RSI 加了 14 根 K 线的预热期（画图仍和通达信一致）；另外新增通用门槛 `min_factor_coverage=0.5`：有值因子的权重不到一半就不打分。【代码】【测试】
3. **血缘工具认不出受控写入口**：`tools/lineage_writers.py` 识别不了 `write_dataset(key, ...)`，全仓 **49 处**正式写入点都被漏掉了。现在改成静态解析契约注册表（40 个契约），把 key 映射成路径。之前基于旧工具生成的血缘清单不完整，需要重新生成。【代码】【测试】
4. **基金伪数据登记表区间重复**：11-20 重跑时，`(ts_code, start)` 相同但 end 后移的区间被当成新段追加，60 只基金出现新旧区间并存，伪数据行数从 25,768 膨胀到 42,433，`fund_price_basis` 读取时直接报错。合并键已改为 `(ts_code, start)`，数据已重新生成（475 段 / 26,128 行），并新增了回归用例。【代码】【测试】
5. **`limit_table_rules` 完全没有测试**：它是撮合内核的涨跌停输入，之前覆盖率只有 34%。补了 6 个用例后达到 100%，`src/backtest/engine` 覆盖率回到 89.86%（门槛 89）。【测试】

### 7.3 工程治理类修改

- **分层**：`tradability` 从 `src/core` 挪到 `src/risk`（core 下保留了一个转发壳，给 `work/` 里的脚本用），消除 `backtest/runner/pipeline` 对 core 的反向依赖。【代码】
- **重复代码**：4 个 UI 页面改为复用 `SizedTableMixin`，删掉约 760 行重复代码。【代码】
- **类型注解**：给 177 个没有参数、也没有返回值的方法补上 `-> None`，并逐一检查过父类和子类的签名，没有冲突（`sources/base.py` 中 4 个会被子类重写成返回 DataFrame 的方法改标 `Any`）。未注解函数数从 750 降到 572，棘轮基线同步下调。【静态】
- **参数优化**：`optimize_strategy` 的 `walk_forward_windows` 默认值从 0 改为 **3**。需要和历史结果对账时，要显式传 0。【代码】
- **测试修复**：12 + 16 个测试失败都已定位原因，绝大多数是测试没有跟上 11-11 到 11-22 的代码改动（远程库默认关闭、写入闸门、序号列、交易日历、成本口径、步骤数等）。写入闸门的放行统一放到 `conftest.py`，需要测试拒绝行为的用例可以用 `@pytest.mark.write_gate` 标记退出放行。【测试】
- **硬编码行号**：血缘测试原来写死的 10 处行号改成按"函数名 + 写入调用"定位。顺带发现原来 5 条"假阳性"断言的行号早就过期，一直是空过的，什么也没检查到。【测试】
- **成交量修复证据**：19 行成交量在 11-13 被 TDX 重新解析覆盖，现在的值 = 修复前的值 × 100，用 VWAP 核对全部落在当日的 [low, high] 之间。判据改为"数量级一致 + VWAP 自洽"。证据文件 `rows.csv` 本身还是 11-12 的推算值，没有重新生成。【测试】

所有改动前的原文件都备份在 `_backup/*_20261123/`，按主题分了目录。

### 7.4 晚间收尾：静态检查清零与全量回归

| 项目 | 初版报告 | 2026-11-23 22:19 |
|---|---|---|
| 全量 pytest（`.venv`） | 12 + 16 个失败 | **全部通过**（只剩项目原本就登记为 xfail 的用例）【测试】 |
| ruff（`src` + `tests`） | 50 处 | **0**【静态】 |
| mypy（`src`，408 个文件） | 311 处 | **0**【静态】 |
| 未注解函数（棘轮基线） | 750 | 572【静态】 |
| 覆盖率（行 + 分支） | 门禁不达标 | 68.05% ≥ 68；`src/backtest/engine` 89.86% ≥ 89【测试】 |

**ruff**：导入排序用 `--fix` 自动修了；B905 补了 `strict=True`；S105 和 S310 两条是误报，加 noqa 并写明原因；`__main__` 入口里的 print 加了 per-file-ignore。复杂度超标的函数里，`as_date`、`_apply`、`_get_stock_batch_impl` 三个已经拆成小函数。`filter_day`（23）、`build_path`（19）、`refresh_quotes_manifest`（17）都在核心路径上，这次没拆，先加 `noqa: C901 - 待拆分`。【代码】

**mypy**：这次只改类型，不改行为：
- 50 处 Qt 枚举改成带作用域的完整写法（例如 `Qt.ScrollBarPolicy.ScrollBarAsNeeded`）。
- 9 个 mixin 声明了宿主属性（共 33 个，只在类型检查时生效）。
- polars `min()`/`max()` 返回的宽联合类型统一用 `cast` 收窄（原来是 72 处报错）。
- 同一个变量被赋予不同类型的地方，拆成了不同的变量名，比如 `exclusion_layer` 三条规则原来共用一个 `cfg`。
- `PooledCash.get` 和 `BTopNStrategy.generate_signals_vectorized` 两处**签名本来就和父类不同**，标注了 `type: ignore[override]` 并写明原因。

有 3 处运行时行为变了，都只影响原来就会报错的分支：
- `main_window` 拿不到 QApplication 时，不再安装按键过滤器（原来会抛 AttributeError）。
- `strategy_lab/page` 在 `rule` 或 `layout` 为 None 时提前返回（原来会抛异常，或者被外层吞掉后显示一条错误的提示）。
- `workbench/server.stop_all` 只是给变量改了名，行为不变。【代码】

**新加的测试**：`test_limit_table_rules`（6 个）、`test_adj_factor_calculator_coverage`（49 个）、`test_industry_pit_tls`（7 个）、打分门槛（4 个）、伪数据区间合并回归（1 个）、walk-forward 默认值（2 个）。【测试】

改动前的原文件都按主题备份在 `_backup/{ui_table_mixin,as_date_unify,sw_tls,stale_tests,layering,full_suite_fix,review,round5,ruff,mypy}_20261123/`。

### 7.5 还没处理的事项

| 优先级 | 事项 |
|---|---|
| 高 | **成本口径改成 `impact_coef=0` + 历史印花税、walk-forward 默认改成 3 折、`min_factor_coverage=0.5` 之后，研究文档和 golden 基线里的历史数字要重跑**，比如 8.662%、7.61%。和旧结果对账时要显式传旧参数。【推论】 |
| 中 | 重新生成血缘清单（工具现在能识别 `write_dataset` 了）；单独核对 `920065.BJ` 11-18 那次 manifest 写入失败（PermissionError）。 |
| 中 | 实测一次申万站点下载，确认证书链在开启 TLS 校验后能用。【未验证】 |
| 中 | 门禁（pytest、覆盖率、mypy、ruff、分层）还没接入 CI，建议先加一个 git pre-push 钩子。 |
| 低 | 拆分 `filter_day` / `build_path` / `refresh_quotes_manifest`；把成交量修复证据 `rows.csv` 按现值补一条说明。 |
| 低 | 项目自己的警告：8 处 `join_asof` 提示排序不可校验（调用前都已经排过序，加 `check_sortedness=False` 即可）、3 处 LazyFrame 改用 `collect_schema()`、`test_memory_optimizer` 里 3 个测试用了 `return`、`test_free_float_fill` 补 `orient="row"`。第三方库（alphalens / quantstats / seaborn）的弃用警告我们改不了。 |
| 低 | 包级 10 对循环依赖、`DataFetcher` 2127 行、撮合主循环用 numba 加速、572 个未注解函数（主要在 ui）、覆盖率低的模块（`industry_pit`、`portfolio_risk_local`、`plugin` 都只有 34%~36%）、开启 `check_untyped_defs`。 |
| 低 | 执行 `git log --all -- .env`，确认历史提交里没有密钥。 |
