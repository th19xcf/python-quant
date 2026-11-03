# 本地数据迁移清单（另一台机器）

> 核查日期：2026-11-03。体积为本机实测（1 MB = 1,048,576 B），文件数为递归统计。
> 标注约定：「代码核对，未实测」= 依据源码分支判断、未在无库/跨机环境实际运行；「未验证」= 本机没有可复现依据。

## 一、最小迁移步骤

最少需要 6 样：**仓库代码、Python 3.14 + Poetry 依赖、通达信日线目录、3 个行业 cfg、`meta/names.json`、`factors/stock_dividend.parquet`**。行情 Parquet、复权因子、ST/行业快照都能由这几样重建。

1. **代码**：`git clone` 仓库（不要拷 `.venv`、`.git` 可选）。确认 `pyproject.toml`（9,549 B）与 `poetry.lock`（473,573 B）在，Python 版本满足 `>=3.14,<3.16`。
2. **环境**：在仓库根目录执行 `poetry install`（默认含 dev 组；可选组按需加装，见第 1 层清单）。
3. **配置**：复制 `.env.example` 为 `.env`，把 `DATA__TDX_DATA_PATH` 改成本机通达信 `vipdoc` 绝对路径。数据库段可保留也可不配——不配就走离线模式（行为见问题 2）。
4. **通达信**：目标机通达信安装目录需包含
   - `vipdoc/{sh,sz,bj}/lday/*.day`（本机实测 10,378 个文件、804.0 MB）；
   - `T0002/hq_cache/tdxhy.cfg`、`tdxzs.cfg`、`tdxzs3.cfg`（合计 201,558 B）。
   目标机的历史深度应不短于本机，否则重建出来的行情会变短。
5. **最小本地快照**（必须拷贝，无法离线重建）：
   - `data/local_store/meta/names.json`（1,346,680 B）——仓库内没有生成代码；
   - `data/local_store/factors/stock_dividend.parquet`（677,598 B）——只能从 MySQL 导出。
6. **重建其余本地列存**（在仓库根目录依次执行）：

```powershell
python -m src.data.local_store build --all
python -m src.data.local_store rebuild-factors
python -m src.data.risk_flags refresh --as-of 2026-11-03
python -m src.data.industry refresh
```

本机实测：行情全量构建 9,602 标的 / 25,056,094 行 / 179.6 秒；因子重建 17,577,983 行 / 24.0 秒。目标机耗时未验证。

7. **可选：推荐持仓状态**：拷 `reports/advice/state/{short,mid,long}.json`（合计 5,885 B）。不拷不会报错，但推荐会从空仓重新开始。
8. **自检**：

```powershell
python -m src.data.local_store stats
python -m src.data.risk_flags stats
python -m src.data.industry stats
```

`stats` 应显示 9,602 标的、约 2,500 万行；ST 快照应显示 5,654 标的（st/non_st/unknown 三态）；行业快照应显示约 95.9% 覆盖率。数值为本机基线，目标机允许因数据深度不同而略有差异。

---

## 二、分层清单

### 第 1 层：代码仓库本身

| 项 | 路径 | 体积（实测） | 是否被 `.gitignore` 忽略 | 能否命令重建 | 缺失后果 |
|---|---|---|---|---|---|
| 运行代码 | `src/` | 未单独统计 | 否（入库） | `git clone` | 无法运行 |
| 测试 | `tests/` | 未单独统计 | 否（入库） | `git clone` | 无法跑 pytest；不影响运行 |
| 依赖声明 | `pyproject.toml` | 9,549 B | 否（入库） | `git clone` | 依赖无法复现 |
| 锁定版本 | `poetry.lock` | 473,573 B | 否（入库） | `git clone` | 版本漂移，行为可能不同 |
| 配置模板 | `.env.example` | 2,211 B | 否（入库） | `git clone` | 不知道要配哪些键 |
| 实际配置 | `.env` | 未统计 | **是**（`.gitignore:85`） | 从 `.env.example` 复制后改路径 | `tdx_data_path` 为空，通达信取数全部失效 |
| 虚拟环境 | `.venv` | 1,602.8 MB | **是**（`.gitignore:86`） | `poetry install` | 不要跨机拷贝（与本机路径/二进制绑定） |

可选依赖组（`pyproject.toml` 实读）：

| 组 | 内容 | 安装命令 | 缺失后果 |
|---|---|---|---|
| 默认（含 dev） | pytest、coverage、ruff、mypy、black、pyarrow 等 | `poetry install` | 无 |
| `baostock`（optional） | baostock | `poetry install --with baostock` | BaoStock 数据源不注册，相关契约测试跳过 |
| `gpu`（optional） | cupy | `poetry install --with gpu` | GPU 加速自动降级 NumPy |
| `analytics`（optional） | alphalens-reloaded、quantstats | `poetry install --with analytics` | 因子评估/绩效 tearsheet 提示「可选能力不可用」，不影响回测 |
| `learning`（optional） | lightgbm、scikit-learn | `poetry install --with learning` | 学习管道跳过 LightGBM，保留等权/IC/ICIR/Ridge |

**注意：仓库里没有 `factor_lib` 依赖组。** `reports/factor_lib/` 只是报告产物目录（12 个文件、0.18 MB），不是 Poetry 组。

### 第 2 层：通达信安装目录被读的部分

通达信目录在仓库外，`.gitignore` 不适用。代码实际读取位置：`src/data/local_store/build.py`（首次构建/增量）、`src/data/managers/data_fetcher.py`（TDX 兜底）、`src/data/tdx_handler.py`、`src/ui/data_market_scan.py`、`src/data/quality/__main__.py`。

| 路径（本机 `h:\zxzq\` 下） | 内容/数量（实测） | 体积（实测） | 代码是否读取 | 能否命令重建 | 缺失后果 |
|---|---|---|---|---|---|
| `vipdoc/sh/lday/*.day` | 5,127 个文件 | 426.7 MB | 是 | 不可（只能靠通达信客户端更新） | 沪市行情首次构建缺失；本地 Parquet 已建好时日常运行不受影响 |
| `vipdoc/sz/lday/*.day` | 4,893 个文件 | 368.7 MB | 是 | 不可 | 同上（深市） |
| `vipdoc/bj/lday/*.day` | 358 个文件 | 8.6 MB | 是 | 不可 | 同上（北交所） |
| `T0002/hq_cache/tdxhy.cfg` | 个股→行业代码 | 150,556 B | 是（`src/data/industry.py`） | 不可（通达信侧更新） | 行业快照全部 `unknown`，推荐行业分桶失真 |
| `T0002/hq_cache/tdxzs.cfg` | 行业代码→中文名 | 18,272 B | 是 | 不可 | 行业二级名称缺一部分 |
| `T0002/hq_cache/tdxzs3.cfg` | 行业代码→中文名（补充） | 32,730 B | 是 | 不可 | 同上 |
| `T0002/hq_cache/*.tnf` | 5 个文件 | 28,186,362 B | **否**（全仓 grep 无 `.tnf` 读取） | 不可 | 不影响仓库运行；仅在仓库外的 `names.json` 生成工具可能用到（未验证） |
| `T0002/hq_cache/tdxzs2.cfg`、`tdxzsbase*.cfg` | 行业/板块辅助 | 18,936 B / 237,897 B / 70,210 B | **否**（只读 `tdxzs.cfg`、`tdxzs3.cfg`） | — | 无影响 |
| `vipdoc/{sh,sz,bj}/minline/`、`eday/` 等分钟线 | 未统计 | 未统计 | **否**（`minline`/`fzline`/`eday` 全仓 0 引用） | — | 无影响 |
| `vipdoc/cw/` 财务数据（`gpcw*.dat` 等） | 本机未见/未统计 | 未统计 | **否**（`gpcw` 全仓 0 引用） | — | 无影响 |
| `T0002/hq_cache` 其他文件 | 未逐个核对 | — | 否 | — | 无影响 |

### 第 3 层：`data/local_store/`

整体实测：**9,615 个文件、927.1 MB**。`.gitignore:135` 已整体忽略。

| 项 | 路径 | 体积（实测） | 性质 | 能否命令重建 | 缺失后果 |
|---|---|---|---|---|---|
| 股票行情 | `quotes/stock/{SH,SZ,BJ}/*.parquet` | 5,654 文件 / 554.1 MB | 构建产物 | 能：`python -m src.data.local_store build --all` | DataFetcher 回退 TDX `.day`（可用但慢）；推荐引擎直接读 Parquet，缺则无行情/推荐为空 |
| 基金行情 | `quotes/fund/*/*.parquet` | 2,615 文件 / 96.5 MB | 构建产物 | 能（同上） | 同上（基金部分） |
| 指数行情 | `quotes/index/*/*.parquet` | 1,333 文件 / 165.8 MB | 构建产物 | 能（同上） | 同上（指数部分） |
| 覆盖度索引 | `meta/daily_index.parquet` | 157,472 B | 构建产物 | 能：`python -m src.data.local_store rescan-meta` | `codes()`/`coverage()` 为空，ST/行业快照会生成 0 行 |
| 构建状态 | `meta/state.json` | 305 B | 构建副作用 | 构建时自动写 | 无影响（仅展示/断点信息） |
| **名称快照** | `meta/names.json` | 1,346,680 B / 52,784 个键 | **外部生成的运行时资产** | **仓库内无生成入口**（`grep` 只有消费方；`git log` 无记录） | ST 快照无名称来源→ST 全 `unknown`，推荐默认策略「放行但登记」，**ST 拦截失效**；展示名称回退代码 |
| ST 快照 | `meta/st_status/st_status_20261103.parquet` | 49,683 B / 5,654 行 | 按日累积快照 | 能重建当日：`python -m src.data.risk_flags refresh --as-of 2026-11-03` | 推荐 ST 过滤失效（同上）；历史 ST **不可回溯** |
| 行业快照 | `meta/industry/industry_20261103.parquet` | 23,028 B / 5,654 行 | 按日累积快照 | 能重建当日：`python -m src.data.industry refresh` | 行业全部 `unknown`，行业分桶失真；历史行业快照同样不可回溯 |
| 复权因子 | `factors/stock_adj_factor.parquet` | 12,468,497 B / 17,577,983 行 | 构建产物 | 能：`python -m src.data.local_store rebuild-factors`（依赖行情+分红） | 复权计算缺失，回测/图表价格口径退化 |
| **分红快照** | `factors/stock_dividend.parquet` | 677,598 B / 53,974 行 | **MySQL 导出快照** | 仅能：`python -m src.data.local_store refresh-factors --allow-remote`（需数据库） | 回测**静默不记分红**（详见问题 1） |
| 因子元数据 | `factors/meta.json` | 354 B | 构建副作用 | 重建时自动写 | 无影响 |
| 因子覆盖清单 | `factors/covered_codes.txt` | 56,500 B | 构建副作用 | 重建时自动写 | 重建会退化为扫 Parquet 求唯一值（慢一点） |
| 基准临时文件 | `factors/_bench_last.parquet` | 100,879,795 B | 基准测试产物 | 可删/可重跑基准 | `src/` 无引用；是否被仓库外脚本使用**未验证**，可跳过 |

### 第 4 层：仓库外的运行时产物

| 项 | 路径 | 体积（实测） | 是否被 `.gitignore` 忽略 | 能否重建 | 缺失后果 |
|---|---|---|---|---|---|
| 推荐持仓状态 | `reports/advice/state/{long,mid,short}.json` | 2,380 + 2,366 + 1,139 = 5,885 B | 是（`reports/`） | 可让运行时重新生成 | 不报错，从空仓开始；连续性丢失 |
| 最近一次推荐输出 | `reports/advice/latest.json`、`all.csv`、`summary.json`、`replay_*`、`risk_*` | 约 130 KB（16 个文件） | 是 | 跑一次推荐即可 | 无影响 |
| 校准缓存 | `reports/advice/calibration/factor_panel.parquet` | 416,442,112 B（约 397 MiB） | 是 | 可重算（`scripts/calibrate_advice.py`，耗时未实测） | 运行时不读；仅影响校准脚本的首次速度 |
| 校准结果 | `reports/advice/calibration/{baseline,scan,refine,frontier,universe}.json` | 约 254 KB | 是 | 同上 | 同上 |
| 研究产物 | `reports/factor_lib/`、`reports/alphalens_test/`、`reports/analytics_validation/` | 0.18 + 0.31 + 0.86 MB | 是 | 对应脚本可重算 | 无影响 |
| 日志 | `logs/` | 29.1 MB / 50 个文件 | 是（`logs/`、`*.log`） | 运行时自动生成 | 无影响 |
| 临时目录 | `tmp/` | 90.4 MB / 142 个文件 | **否**（`.gitignore` 只忽略 `*.tmp`，`tmp/` 目前是 untracked） | 无 | 无影响（草稿/一次性脚本，不要提交） |
| 运行输出 | `runs/` | 0 个文件 | 未核对 | — | 无影响 |

---

## 三、三个问题的核查结论

### 问题 1：分红快照从哪来？无库时会不会静默不记分红？

**结论：`stock_dividend` 是 MySQL 表；本地 `factors/stock_dividend.parquet` 是它的导出快照。目标机没有数据库时，该快照不能刷新，只能拷贝；如果连快照也没有，回测会「没有除权事件」地继续跑，不报错也不补数据——即静默不记分红。**

依据：

- 表模型：`src/database/models/stock.py` 中的 `StockDividend`。
- 导出路径：`src/data/local_store/build.py:566` `refresh_factors()`；`:592` 执行 `FROM stock_dividend ORDER BY ts_code, ex_date`；`:606` 写 `factors/stock_dividend.parquet`；`:611` 元数据 `source='remote_mysql'`。
- 本地读取：`src/data/local_store/store.py:549-556` `dividends()` 读本地文件，文件不存在返回 `None`。
- 回测读取：`src/backtest/engine/corporate_actions.py:238-265` `load_local_dividend_frames()` **只读本地、无数据库回退**；docstring 明确「分红缺失在回测侧等价于无除权事件」。
- 关键细节：文件**不存在**时 `store.dividends()` 返回 `None`，不会抛异常，因此 `load_local_dividend_frames()` 连 `logger.warning` 都不会打——属于**完全静默**。只有读取异常（文件损坏等）才告警。
- 其他调用方：`src/data/data_manager.py:94-116` 先读本地快照；未命中时若 `db_manager` 为空，会 `logger.warning("数据库管理器未初始化，无法获取分红配股数据")` 并返回空表。

迁移要求：**必须拷 `data/local_store/factors/stock_dividend.parquet`**；拷了之后 `rebuild-factors` 可离线重算复权因子。

### 问题 2：MySQL 到底是不是必需？

**结论：离线模式可以启动并跑推荐/因子库/质检/K线，但「行情页」会因为没有 `stock_basic` 名称而显示空表；标准回测入口在未显式传入数据时会先建共享取数器并直连数据库，无库会抛异常（代码核对，未实测）。**

| 功能 | 无数据库时行为 | 依据 |
|---|---|---|
| GUI/程序启动 | 自动降级离线：`DatabaseManager.connect()` 失败后以 `DataManager(config, None, ...)` 启动 | `src/main.py:390-411` |
| 行情页（通达信扫描） | `get_stock_basic()` 返回空表 → 名称映射 `{}` → 每行因 `stock_name` 为空被丢弃 → **表格为空** | `src/ui/data_market_scan.py:50-56, 132-136`；`src/data/data_manager.py:270-272` |
| K 线 / 单票取数 | 路由为「本地 Parquet → 通达信 `.day` → 远程库（仅 `remote_database_fallback=true`）」，默认不查库 | `src/data/managers/data_fetcher.py:328-363`；默认值 `src/utils/config.py:114-117` |
| 组合/批量取数 | 同上（本地批量优先，逐票兜底 TDX） | `src/data/managers/data_fetcher.py:365-390` |
| 回测（标准入口） | `run_backtest(ts_code=...)` 且未传 `data`/`db_manager` 时，`_get_shared_data_fetcher()` **无条件** `db.connect()`，连接失败抛出 → 回测失败 | `src/core/usecases/run_backtest.py:51-64, 104-111`（代码核对，未实测） |
| 回测（显式传数据） | 传入 `data` 或已连接的 `db_manager` 可绕开上述连接 | 同上 `:97-99` |
| 推荐引擎 | 只读本地列存 + 本地名称/ST/行业快照，无数据库访问 | `src/advice/data.py:110-114`；`src/advice/pipeline.py:98-101`；`src/advice/risk_filters.py` |
| 因子库 | 读本地 Parquet；`rebuild-factors` 纯本地；只有 `refresh-factors --allow-remote` 需要库 | `src/data/local_store/store.py:459-556`；`build.py:566-620` |
| 质检 | 质检模块没有引用 `DatabaseManager`（`grep` 0 命中），走本地文件/可选在线源 → 可用 | `src/data/quality/`（代码核对，未实测） |
| DB 同步类命令 | `--sync-*`、`--update-*` 等会连接失败并 `logger.error`，不静默改数据 | `src/main.py:134-377` |

补充：推荐场景下若无 `names.json`，ST 状态全为 `unknown`，默认策略是 **keep（放行但登记）**，即 ST 拦截失效（`src/advice/risk_filters.py:40-41, 115-120`；`src/advice/eligibility.py:96-100`）。这比「没有数据库」更危险，务必拷 `names.json` 或每天重新生成 ST 快照。

### 问题 3：机器相关的硬编码路径

| 位置 | 当前值/行为 | 目标机要改 | 可覆盖的环境变量 |
|---|---|---|---|
| `.env` | `DATA__TDX_DATA_PATH=h:\zxzq\vipdoc` | **必须改**为目标机 `...\vipdoc` 绝对路径 | `DATA__TDX_DATA_PATH` |
| `.env` | `DATABASE__HOST=114.113.154.36`、`DATABASE__PORT=45608`、`DATABASE__DATABASE=hs` | 要么改成目标机可访问的库，要么保持离线（不要指望连上） | `DATABASE__HOST`、`DATABASE__PORT`、`DATABASE__USERNAME`、`DATABASE__PASSWORD`、`DATABASE__DATABASE` |
| `src/utils/config.py:85` | `tdx_data_path` 默认 `""`（不是 `h:\zxzq`） | 不配则通达信取数全部失效 | 同上 |
| `src/utils/config.py:108-111` | `local_store_path` 默认 `data/local_store`（相对项目根） | 若列存放在别处，改成绝对路径 | `DATA__LOCAL_STORE_PATH` |
| `src/utils/config.py:114-117` | `remote_database_fallback` 默认 `false` | 保持 false 即为离线读 | `DATA__REMOTE_DATABASE_FALLBACK` |
| `src/utils/config.py:91-94` | `allow_remote_sources` 默认 `false` | 需要 AkShare/BaoStock 时再开 | `DATA__ALLOW_REMOTE_SOURCES` |
| `src/data/industry.py:37-40` | `tdx_cache_dir = Path(tdx_data_path).parent / 'T0002' / 'hq_cache'` | 无需单独改；改 `DATA__TDX_DATA_PATH` 即可联动 | 无（`--cache-dir` 仅 CLI 单次覆盖） |
| `src/advice/pipeline.py:172-173` | 默认状态目录 `<repo>/reports/advice/state`（按源码位置推导） | 无需改 | CLI `--state-dir` |
| `src/advice/cli.py:118` | 默认输出目录 `<repo>/reports/advice` | 无需改 | CLI `--out-dir` |
| `src/utils/config.py:233-240` | `.env` 从**当前工作目录**读取，嵌套分隔符 `__` | 必须在仓库根目录启动，或用进程环境变量覆盖 | 全部 `*__*` 键 |

`h:\zxzq` 只出现在 `.env`、`.env.example` 注释、`docs/` 与 `tmp/` 草稿脚本；生产代码没有本机绝对路径硬编码（`grep zxzq` 结果）。

---

## 四、未验证项（不要把以下当确定结论）

1. `names.json` 的生成来源与生成命令：仓库内无生成代码、`git log` 无记录，**目标机能否重建未验证**；按现状应视为必须拷贝的资产。
2. 通达信 `*.tnf` 是否足以重建 `names.json`：代码不读 `.tnf`，外部工具是否可用**未验证**。
3. 通达信客户端如何补齐/回溯历史日线：**未验证**（本机数据由用户侧维护）。
4. 无数据库时标准回测入口必抛异常：来自源码分支判断，**未实测运行**。
5. 质检在无数据库环境的完整可用性：只做了引用检索，**未实测运行**。
6. `reports/advice/calibration/factor_panel.parquet` 重算耗时：**未实测**。
7. `factors/_bench_last.parquet` 是否被仓库外脚本使用：`src/` 无引用，**未验证**外部依赖。
8. 目标机的重建耗时、磁盘占用：本机 179.6 s / 24.0 s 为参考值，**目标机未验证**。
9. `names.json` 中 52,784 个键的资产类别构成（股票/基金/指数/退市）：只实测了总键数与 ST 构建结果，**未逐类拆分**。
