# 主机迁移本地数据清单（另一台机器 · 通达信本地数据）

> **一句话结论**：搬到另一台机器**最少需要 5 样**：仓库代码 + `poetry.lock`、Python 3.14 环境（`poetry install` 重建 `.venv`）、目标机通达信 `vipdoc/{sh,sz,bj}/lday/*.day` 与 `T0002/hq_cache` 下 3 个 cfg、`data/local_store/` 整目录（其中 `factors/stock_dividend.parquet` 与 `meta/names.json` **必须拷贝、代码无法重建**）、一份新 `.env`（改 TDX 路径、去掉生产库凭据）。
>
> 标注约定：**「实测」** = 本机 2026-11-03 只读测量/实际执行过；**「代码核对」** = 依据源码分支与行号判断，未在无库/跨机环境复现；**「未验证」** = 本机没有可复现依据，不要当确定结论。
> 本文由两份同主题清单合并而成，以实测结果为准；两稿冲突处的取舍理由集中记录在第 4 节。

---

## 0. 最小迁移步骤（照着搬）

### 前置条件（目标机）

| 项 | 要求 | 本机实测 |
|---|---|---|
| 操作系统 | Windows x64（项目路径与命令均按 Windows/PowerShell 编写） | Windows |
| Python | `>=3.14,<3.16`（见 `pyproject.toml:14`） | 3.14.9 |
| Poetry | 2.x（`poetry.toml` 已声明 `in-project = true`） | 2.5.1 |
| 通达信 | 已安装，且日线数据已下载完整（`vipdoc` 目录存在） | `h:\zxzq\vipdoc` |

### 步骤 1：搬代码，重建虚拟环境

```powershell
# 方式 A：git clone；方式 B：直接复制目录（务必排除 .venv / data / reports / logs / tmp）
git clone <仓库地址> E:\code\python\quant
Set-Location E:\code\python\quant

# poetry.toml 已声明 in-project=true，这里重复设置无副作用
poetry config virtualenvs.in-project true
poetry install                 # 只跑程序可改 `poetry install --only main`
```

- `.venv`（实测 1.6 GB）**不要拷贝**：里面是 Windows 本机编译产物，跨机复制会因绝对路径/解释器不一致失效，必须用 `poetry install` 重建。
- 需要按需装可选依赖组（见 1.1 表）：`poetry install --with analytics,learning`（`baostock`、`gpu` 非必需）。

### 步骤 2：新建 `.env`

```powershell
Copy-Item .env.example .env
# 编辑 .env，至少改这两处：
#   DATA__TDX_DATA_PATH=<目标机 vipdoc 的绝对路径>   # 例如 D:\new_tdx\vipdoc
#   DATABASE__HOST=localhost                        # 无数据库时指向本机空端口，快速失败并进入离线模式
```

- 不要原样拷贝源机的 `.env`：其中含生产 MySQL 地址与密码（`.env` 已被 `.gitignore:85` 忽略，且**整个文件不入库**）。
- `.env` 是**相对当前工作目录**加载的（`src/utils/config.py:233-240`），所以启动命令必须在仓库根目录执行；否则等价于没有配置。

### 步骤 3：搬数据（核心）

```powershell
# 3.1 通达信日线（目录结构必须原样保留）
robocopy <源机>\vipdoc        D:\new_tdx\vipdoc        /E
# 3.2 通达信板块/指数 cfg（只需 3 个文件；hq_cache 的位置由 vipdoc 上一级推导）
robocopy <源机>\T0002\hq_cache D:\new_tdx\T0002\hq_cache tdxhy.cfg tdxzs.cfg tdxzs3.cfg

# 3.3 本地列存（推荐整目录，实测 927.1 MB）
robocopy <源机>\data\local_store D:\code\python\quant\data\local_store /E
```

> 若只搬通达信、不搬 `local_store`，请继续步骤 4；但此时 **`factors/stock_dividend.parquet` 与 `meta/names.json` 仍需从源机单独拷贝**，否则无法恢复分红与股票名称。

### 步骤 4：仅搬通达信时的本地库重建

```powershell
poetry run python -m src.data.local_store build --all          # 从 TDX .day 生成 quotes/ 与 meta/
poetry run python -m src.data.local_store rescan-meta          # 只复制了 quotes/ 时，重建 daily_index.parquet
# 分红快照无法从 TDX 生成，必须先拷贝 factors/stock_dividend.parquet，再二选一：
poetry run python -m src.data.local_store rebuild-factors --include-existing   # 连同已有因子一起重算
poetry run python -m src.data.local_store rebuild-factors                      # 默认只补缺失标的
```

- `--include-existing` = 把快照里已有的标的**重新计算**；不加该参数则**只补缺失**（`src/data/local_store/factor_rebuild.py:203-229`）。两者都是离线命令，不写远程库；按需选择其一。
- 本机历史实测：行情全量构建 9,602 标的 / 25,056,094 行，耗时记录为 **115～180 s**（不同批次/缓存状态）；因子重建 17,577,983 行，约 **24 s**。目标机未复测（见 3.2 第 4 条）。

### 步骤 5：刷新当日参考数据快照

```powershell
poetry run python -m src.data.risk_flags refresh    # meta/st_status/st_status_<今天>.parquet
poetry run python -m src.data.industry refresh      # meta/industry/industry_<今天>.parquet
```

> `risk_flags refresh` 会尝试从远程 `stock_basic` 补充名称/退市状态（`src/data/risk_flags.py:274-301`），离线时异常被捕获后继续用 `names.json` 构建；离线等待时长**未验证**。`industry refresh` 只读本地 `tdxhy.cfg`，完全离线可用。

### 步骤 6：验证跑通（全部为只读命令）

```powershell
# 6.1 本地列存自检：本机期望 symbols=9602、rows≈25,056,094
poetry run python -m src.data.local_store stats

# 6.2 ST / 行业快照自检（旧稿实测基线：ST 5,654 标的；行业覆盖率约 95.9%）
poetry run python -m src.data.risk_flags stats
poetry run python -m src.data.industry stats

# 6.3 单票日线质检（本地源）
poetry run python -m src.data.quality check --codes 600000.SH --source local --dry-run

# 6.4 离线取数 + 离线回测冒烟（显式传 data，不触碰数据库；本机实测通过）
poetry run python -c "from src.utils.config import Config; from src.data.managers.data_fetcher import DataFetcher; from src.core.usecases import run_backtest; from src.backtest.strategies.ma_strategy import MAStrategy; d=DataFetcher(Config(), None).get_stock_data('600000.SH','2023-01-01','2023-12-31'); r=run_backtest(ts_code='600000.SH',start_date='2023-01-01',end_date='2023-12-31',strategy=MAStrategy(),data=d); print('total_return', r['total_return'], 'trades', len(r['trades']))"
# 本机结果：600000.SH 2023 全年 total_return -6.26%，trades 14（仅作冒烟基准，不同复权/数据会有差异）

# 6.5 启动 GUI（本机未做自动化启动验证 → 标注「未验证」）
poetry run python -m src.main
```

### 步骤 7（可选）：延续每日推荐持仓状态

```powershell
robocopy <源机>\reports\advice\state  D:\code\python\quant\reports\advice\state  /E
Copy-Item <源机>\reports\advice\latest.json D:\code\python\quant\reports\advice\latest.json
```

不搬该目录时：程序会以空持仓/空权益重新开始，推荐本身仍能生成（`reports/advice/state` 为空只是丢失历史仓位连续性）。

---

## 1. 完整清单

四层统一字段：**路径 / 体积 / 是否被 `.gitignore` 忽略 / 能否重建及确切命令 / 缺失后果**。

### 1.1 代码与依赖

| 路径 | 体积 | 被 .gitignore 忽略 | 能否重建 / 确切命令 | 缺失后果 |
|---|---|---|---|---|
| 仓库根（`.git` + 工作树） | `.git` 13.4 MB；`src/tests/scripts/tools/docs` 合计 12.9 MB | 否 | `git clone` 或整目录复制（**排除** `.venv/data/reports/logs/tmp`） | 无代码，全部不可用 |
| `src/`（运行代码） | 含在上行合计，未单独统计 | 否 | `git clone` | 无法运行 |
| `tests/`（测试） | 含在上行合计，未单独统计 | 否 | `git clone` | 无法跑 pytest；不影响运行 |
| `pyproject.toml` | 9,549 B | 否 | 随仓库自带 | 无法安装依赖 |
| `poetry.lock` | 473,573 B | 否 | **必须随仓库一起搬**；丢失后只能 `poetry lock` 重解（版本可能漂移） | 依赖版本不可复现 |
| `poetry.toml` | 46 B | 否 | 随仓库自带（`in-project=true`） | `.venv` 可能建到用户目录，步骤 6 命令路径失效 |
| `.env.example` | 2,211 B | 否 | 随仓库自带，`Copy-Item .env.example .env` | 新机没有配置模板 |
| `.env` | 586 B | **是**（`.gitignore:85`） | 手工新建，见步骤 2 | 无 TDX 路径 → 取数报「本地 TDX 源不可用」；无 DB 配置 → 启动尝试连默认库 |
| `.venv`（虚拟环境） | 1.6 GB | **是**（`.gitignore:86`） | `poetry install`（`poetry.toml` 强制建在项目内 `.venv`） | 无法运行；**不要跨机拷贝** |
| Python 解释器 | — | — | 安装 `>=3.14,<3.16` | 依赖无法安装/运行 |
| 可选依赖组 `analytics` | — | — | `poetry install --with analytics` | 因子评估/绩效 tearsheet 提示「可选能力不可用」，不影响回测 |
| 可选依赖组 `learning` | — | — | `poetry install --with learning` | 学习管道跳过 LightGBM，保留等权/IC/ICIR/Ridge |
| 可选依赖组 `baostock` | — | — | `poetry install --with baostock` | BaoStock 数据源不注册，相关契约测试跳过 |
| 可选依赖组 `gpu` | — | — | `poetry install --with gpu` | GPU 指标加速不可用，自动退化为 CPU（`src/tech_analysis/gpu_acceleration.py`） |
| `dev` 组（pytest/ruff/mypy 等） | — | — | `poetry install`（默认含 dev） | 无法执行步骤 6 的质检/测试（运行主程序不受影响） |

依据：`pyproject.toml:14`（Python 约束）、`pyproject.toml:61-95`（可选组）、`poetry.toml:1-3`、`.gitignore:85-86`。
**注意：仓库里没有 `factor_lib` 依赖组**；`reports/factor_lib/` 只是报告产物目录（12 个文件、189,281 B），不是 Poetry 组。

### 1.2 通达信安装目录中被读取的部分

代码实际只读以下位置，**不读取 TDX 的分钟线、财务数据、`.tnf` 等任何其他文件**（`src` 内无 `minline|fzline|gpcw|.tnf` 调用点）：

| 路径（相对通达信根目录） | 体积 / 文件数（本机 `h:\zxzq`） | 被 .gitignore 忽略 | 能否重建 / 确切命令 | 缺失后果 |
|---|---|---|---|---|
| `vipdoc\sh\lday\*.day` | 426.7 MB / 5,127 个 | 是（仓库外，天然不入库） | 通达信客户端「盘后数据下载」；或从源机直接复制 | 沪市日线取数失败 / `build --all` 沪市为空 |
| `vipdoc\sz\lday\*.day` | 368.7 MB / 4,893 个 | 同上 | 同上 | 深市日线缺失 |
| `vipdoc\bj\lday\*.day` | 8.6 MB / 358 个 | 同上 | 同上 | 北交所日线缺失（该市场标的整体不可回测/推荐） |
| `T0002\hq_cache\tdxhy.cfg` | 150,556 B | 同上 | 通达信「板块数据」下载；或复制 | `python -m src.data.industry refresh` 无法生成行业快照 |
| `T0002\hq_cache\tdxzs.cfg` | 18,272 B | 同上 | 同上 | 指数/板块名称映射缺失 |
| `T0002\hq_cache\tdxzs3.cfg` | 32,730 B | 同上 | 同上 | 扩展指数映射缺失 |
| `T0002\hq_cache\` 其余 100 个文件（含 `*.tnf`） | 合计 81.9 MB（其中 3 个 cfg 仅约 0.2 MB） | 同上 | — | **本仓库不读**，缺失不影响运行 |

- 路径推导：`vipdoc` 根由 `DATA__TDX_DATA_PATH` 指定（`src/utils/config.py:85`、`.env:19`），日线路径由 `src/data/tdx_day_parser.py:117-119` 拼接；`hq_cache` 由 `vipdoc` 同级上一级推导（`src/data/industry.py:37-40`），所以**目录层级必须保持** `<TDX根>/vipdoc/...` 与 `<TDX根>/T0002/hq_cache/...`。
- 分钟线：项目不读 TDX 分钟文件；只有远程腾讯 CLI 会联网补齐（`python -m src.data.tencent minute --allow-remote --codes 600000.SH --freq m1`，见 `src/data/tencent/__main__.py:202-211`；`--codes` 为必填），**不属于本机迁移必需项**。
- 财务数据：UI「财务分析」标签页仅为 `QLabel("财务分析区域")` 占位（`src/ui/main_window_layout_mixin.py:275-280`），代码无财务数据文件读取路径（`src` 内无 `gpcw` 等财务文件解析）。

#### 1.2.1 明确不读取的文件（不必搬）

| 路径 | 体积 / 数量（本机实测或旧稿实测） | 代码是否读取 | 缺失后果 |
|---|---|---|---|
| `T0002\hq_cache\*.tnf` | 5 个文件 / 28,186,362 B | **否**（全仓 grep 无 `.tnf` 读取） | 不影响仓库运行；仅在仓库外的 `names.json` 生成工具可能用到（**未验证**） |
| `T0002\hq_cache\tdxzs2.cfg`、`tdxzsbase*.cfg` | 18,936 B / 237,897 B / 70,210 B | **否**（只读 `tdxzs.cfg`、`tdxzs3.cfg`） | 无影响 |
| `vipdoc\{sh,sz,bj}\minline\`、`fzline\`、`eday\` | 目录存在但文件数为 0（本机实测） | **否**（全仓 0 引用） | 无影响 |
| `vipdoc\cw\` 财务数据（`gpcw*.dat` 等） | 本机未见 | **否**（`gpcw` 全仓 0 引用） | 无影响 |
| `T0002\hq_cache` 其他文件 | 未逐个核对 | 否 | 无影响 |

### 1.3 `data/local_store/`（构建产物 + 4 项不可重建资产）

总：**9,615 个文件 / 927.1 MB**（其中行情 9,602 个文件 / 816.4 MB）。`.gitignore:135` 整目录忽略，**必须整目录搬或按下列命令重建**。

| 路径 | 体积 / 规模 | 被忽略 | 能否重建 / 确切命令 | 缺失后果 |
|---|---|---|---|---|
| `quotes/stock/{sh,sz,bj}/*.parquet` | 5,654 个 / 554.1 MB | 是 | ✅ `poetry run python -m src.data.local_store build --all`（源＝TDX 日线） | 股票 K 线退化为直读 TDX（仍可用，但全市场扫描变慢）；参数优化/因子库批量读取显著变慢 |
| `quotes/fund/{sh,sz}/*.parquet` | 2,615 个 / 96.5 MB | 是 | ✅ 同上（TDX 基金日线） | 基金页/基金回测缺数据 |
| `quotes/index/{sh,sz,bj}/*.parquet` | 1,333 个 / 165.8 MB | 是 | ✅ 同上（TDX 指数日线） | 指数行情、基准对比、指数推荐缺数据 |
| `meta/daily_index.parquet` | 157,472 B / 9,602 行（stock 5654 / fund 2615 / index 1333） | 是 | ✅ `poetry run python -m src.data.local_store rescan-meta`（扫 quotes 重建） | 「代码/区间索引」缺失，`LocalQuoteStore` 无法判断覆盖区间，取数回退 TDX；ST/行业快照会生成 0 行 |
| `meta/state.json` | 305 B（记录 stock_adj_factor 17,577,983 行、stock_dividend 53,974 行、5650 代码等） | 是 | ✅ 由 `build` / `rebuild-factors` / `rescan-meta` 自动写 | 因子/缓存状态统计缺失；不影响取数主流程 |
| `meta/names.json` | **1,346,680 B（1.35 MB）/ 52,784 条**（含股票、指数、基金名称） | 是 | ❌ **代码无任何写入口**（全仓库直接引用该文件名的仅 5 处读取：`src/advice/pipeline.py:100`、`src/data/risk_flags.py:101`、`scripts/calibrate_advice.py:117`、`scripts/smoke_risk_filters.py:50`、`tests/test_reference_data.py:53`） | 推荐/ST 判定中的中文名缺失：展示退化为代码；ST 未知代码按「非 ST」处理（可能放行 ST 股）。**必须从源机拷贝** |
| `meta/st_status/st_status_20261103.parquet` | 49,683 B / 5,654 行 | 是 | ⚠️ 仅能生成**当天**：`poetry run python -m src.data.risk_flags refresh --as-of 2026-11-03`；**历史日期不可补** | 回测/推荐按日取不到 ST 快照 → 按未知处理（可能把 ST 纳入候选）；缺失当天的会即时降级 |
| `meta/industry/industry_20261103.parquet` | 23,028 B / 5,654 行 | 是 | ⚠️ 仅能生成**当天**：`poetry run python -m src.data.industry refresh --as-of 2026-11-03`（读 `tdxhy.cfg`） | 行业分散度约束失效/行业过滤退化为未知；历史日期不可补 |
| `meta/events/events.parquet` | 413 KB / 22,101 行（coverage 2025-01-01～2025-12-31） | 是 | ⚠️ 需原始公告 CSV/Parquet：`poetry run python -m src.data.events import --path <文件>`（`src/data/events.py:493-510`）；**仓库内无联网抓取命令**，离线只能拷贝已有文件或 `research/events/data/*.parquet` | 事件风险过滤（减持/立案/业绩预告）失效，推荐不再规避事件雷 |
| `factors/stock_adj_factor.parquet` | 12,468,497 B / 17,577,983 行 / 5,650 代码 | 是 | ✅ `poetry run python -m src.data.local_store rebuild-factors --include-existing`（依赖本地行情 + 分红快照） | 复权因子缺失 → **前/后复权价等于原始价**（`src/data/local_store/factor_rebuild.py:93-112`），回测收益口径失真 |
| `factors/stock_dividend.parquet` | 677,598 B / 53,974 行 / 5,376 代码 | 是 | ⚠️ **本地快照是唯一权威源**：日常只能从源机拷贝（或上游同步任务落地后本地生成）；`refresh-factors` 已改为**危险覆盖操作**，默认拒绝，仅首次冷启动可用 `refresh-factors --allow-remote --force-overwrite --confirm OVERWRITE-LOCAL-SNAPSHOT`（写前比对事件条数/最新除权日/因子末值，出现回退即中止）；**TDX 无分红文件、仓库无解析实现** | 无库且缺此文件时：复权因子恒为 1.0（静默）+ 回测除权记账不计分红/送转/配股（见 Q1）。**必须从源机拷贝** |
| `factors/covered_codes.txt` | 56,500 B | 是 | ✅ 由 `rebuild-factors` 自动生成 | 无影响（仅重建进度辅助；缺失会退化为扫 Parquet 求唯一值，稍慢） |
| `factors/meta.json` | 354 B（`source: local_dividend`、17,582,589 行、5,650 代码） | 是 | ✅ 自动生成 | 因子来源/覆盖率统计缺失，不影响计算 |
| `factors/rebuild/manifest.json` | 440 B | 是 | ✅ 重建进度标记，`rebuild-factors` 自动维护 | 丢失后重建从头开始（耗时增加），不影响结果 |
| `factors/_bench_last.parquet` | **100,879,795 B（96.2 MB）** | 是 | ❌ 代码中**无任何引用**（`grep` 仅命中文件自身）；可删/可重跑基准 | 无影响（历史基准/校准遗留产物）；是否被仓库外脚本使用**未验证** |

**必须拷贝、无法用命令重建的资产共 5 项**（`data/local_store/` 内 4 项 + 仓库外 1 项）：`meta/names.json`、`factors/stock_dividend.parquet`、`meta/st_status/`（历史日期）、`meta/industry/`（历史日期），以及仓库外的 `reports/advice/state/{long,mid,short}.json`（3 个文件 / 5,885 B，约 6 KB，见 §1.4）。`meta/events/` 需原始公告文件才能重导入。

### 1.4 仓库外运行时产物（不入库，可选搬运）

下表体积/文件数均为**本次实测**（除注明外）；`reports/`、`logs/`、`tmp/` 被 `.gitignore:132,110,138` 忽略。**结论性内容统一看 `docs/`（含新增的 `docs/factor_lib_report.md`），可重建产物与运行时状态留在 `reports/`。** 注意 `reports/` 整体忽略，若迁移时用 `robocopy /XD reports` 排除该目录，会连带丢掉 `reports/advice/state/` 这 3 个不可重建的状态文件，需单独拷贝。

| 路径 | 体积 / 内容（实测） | 被 .gitignore 忽略 | 能否重建 | 缺失后果 |
|---|---|---|---|---|
| `reports/advice/` 顶层 | 15 个文件 / 144,389 B（`latest.json`、`all.csv`、`summary.json`、`replay_*`、`risk_*`） | **是**（`.gitignore:132`） | ✅ 重新运行每日推荐生成 | 无历史报告可查，功能正常 |
| `reports/advice/state/{long,mid,short}.json` | 3 个文件 / 5,885 B（2,380 + 2,366 + 1,139，约 6 KB） | 是 | ❌ **不可重建**：只能从空仓重新开始（无法反推出历史持仓） | 推荐组合的历史仓位/净值断档。**必须拷贝**；`robocopy /XD reports` 会跳过，需单独加拷 |
| `reports/advice/calibration/` | 6 个文件 / 416,698,819 B：`factor_panel.parquet` 416,442,112 B（约 397 MiB）+ 5 个 JSON 共 256,707 B（`baseline/scan/refine/frontier/universe`） | 是 | ✅ `python scripts/calibrate_advice.py`（需全市场本地数据，耗时**未实测**） | 运行时不读；仅影响校准脚本首次速度与校准结论复算 |
| `reports/factor_lib/` | 12 个文件 / 189,281 B | 是 | ✅ `python -m src.quant.factor_lib --size 80` 可重算（耗时未实测） | 无影响；结论已整理进 `docs/factor_lib_report.md`，目录内明细仅作复核 |
| `reports/alphalens_test/` | 4 个文件 / 328,712 B | 是 | ✅ 对应研究脚本可重算 | 无影响 |
| `reports/analytics_validation/` | 4 个文件 / 905,526 B | 是 | ✅ 对应研究脚本可重算 | 无影响 |
| `logs/` | 50 个文件 / 30,491,712 B（`app.log` + 质检/对拍/因子审计报告） | **是**（`.gitignore:110`） | ✅ 运行自动生成 | 无历史日志与历史质检报告，不影响运行 |
| `tmp/`（基准与调试脚本输出） | 2 个文件 / 4,252 B（清理后仅余 `db_snapshot_20261103_094535/` 下 `manifest.txt` 与 `stock_daily.csv` 留证；数量随工作区变化） | **是**（`.gitignore:138` 新增整目录规则，此前只忽略 `*.tmp`） | — | 无影响，**不需要搬**（草稿/一次性脚本，不要提交） |
| `runs/` | **不存在**（实测 missing；此前记录为 0 文件） | 未核对 | — | 无影响 |
| `.coverage` | 98,304 B | 是 | `pytest --cov` 重新生成 | 无影响 |
| `research/`（代码 + 数据） | 23 个文件 / 13,409,819 B | 部分 | 代码与 `research/events/data/` 下 15 个数据文件（原始/规范化 parquet、汇总 CSV、映射表）均已入库；本次新增忽略 2 个可重建中间产物（`.gitignore:141-142`：`announcement_event_level_car.csv`、`announcements_partial.parquet`） | 不影响运行；已入库数据是离线重导 `meta/events/events.parquet` 的原始来源之一 |

---

## 2. 三个必须查清的问题

### Q1. 分红数据从哪来？无库时会怎样？

**结论（代码核对，已确认）**：`stock_dividend` 是 MySQL 表；本地 `data/local_store/factors/stock_dividend.parquet` 是它的导出快照（实测存在，677,598 B / 53,974 行）。目标机没有数据库时该快照不能刷新，只能拷贝；如果连快照也没有，回测会「没有除权事件」地继续跑，不报错也不补数据——即**静默不记分红**。

1. 运行期分红**只读本地快照** `data/local_store/factors/stock_dividend.parquet`（`src/data/local_store/store.py:473,486,549-556`），不是实时查 MySQL 表。
2. **本地快照是唯一权威数据源**：运行期不再优先读 MySQL，`refresh-factors` 默认拒绝用远程库反向覆盖（需 `--allow-remote` + `--force-overwrite` + `--confirm OVERWRITE-LOCAL-SNAPSHOT` 三重显式开关，且写前校验事件条数/最新除权日/因子末值，回退即中止）。快照的日常来源是源机拷贝或上游同步任务落地，之后用 `rebuild-factors` 重算因子（`src/data/local_store/build.py`、`src/data/local_store/factor_rebuild.py`）。
3. **仓库内没有任何从通达信读取分红/送转/配股的实现**（TDX 分红数据不在 `vipdoc`，代码也未解析），所以目标机无库时**无法自行产出分红数据**。
4. 降级行为是「**显式告警一次 + 静默不计**」，不是报错中断：
   - `DataManager.get_stock_dividend`：本地快照缺失 → 若 `db_manager is None` 则 `logger.warning(... 数据库管理器未初始化 ... )` 后返回**空 DataFrame**（`src/data/data_manager.py:94-116`）。
   - 回测装载分红：`load_local_dividend_frames` 的文档注释明确写了「分红缺失在回测侧等价于无除权事件，不阻塞回测」（`src/backtest/engine/corporate_actions.py:238-252`），文件不存在时直接得到空帧。
   - 关键细节：文件**不存在**时 `store.dividends()` 返回 `None` 而非抛异常，因此回测侧连 `logger.warning` 都不会打——属于**完全静默**；只有读取异常（文件损坏等）才告警。
   - 因子重建：分红为空时 `qfq/hfq` 因子**恒为 1.0**，无任何报错（`src/data/local_store/factor_rebuild.py:93-112`）。
   - **后果**：回测照常跑完，但除权日不做现金分红/送转/配股调整 → 价格跳空被当成正常下跌、收益与持仓数量失真；因子全 1.0 时「前复权/后复权」退化为原始价。属于**静默错误**，比直接失败更危险。
   - **实测佐证**：本次离线冒烟（`DataFetcher(Config(), None)` + 本地快照存在）正常完成；分红快照缺失时的空帧路径为**代码确认**，本次未实删文件复现（标记未验证）。
   - **未验证**：目标机实际资金曲线偏差幅度（需在目标机用有分红标的做前后对比）。

迁移要求：**必须拷 `data/local_store/factors/stock_dividend.parquet`**；拷了之后 `rebuild-factors` 可离线重算复权因子。

### Q2. MySQL 是不是必需？

`allow_remote_sources` 默认关闭、`remote_database_fallback` 默认关闭（`src/utils/config.py:91-94,114-117`），故**读行情本身不依赖 MySQL**；但有两处会强制连库。

| 功能 | 无 MySQL 时 | 依据 |
|---|---|---|
| GUI 启动 / 主界面 | ✅ 可用：连库失败自动降级 `DataManager(config, None)`（`src/main.py:390-409`） | 离线模式 |
| 行情页 · 列表扫描 | ❌ 空表：报价来自 TDX `.day`，但名称来自 `stock_basic`；`db_manager=None` 时 `get_stock_basic` 直接返回空表（`src/data/data_manager.py:270-272`），扫描时 `stock_name` 为空的行被整行丢弃（`src/ui/data_market_scan.py:132-136`）→ 表格为空。3 个硬编码默认只在「库已连接但表为空」时插入（`data_manager.py:292-304`），无库不会生效。基金/指数列表同样读库为空（`src/ui/fund_section.py:100-113`、`src/ui/data_index_controller.py:161-165`） | 代码核对 |
| K 线 / 个股图表 | ✅ 可用：`DataFetcher` 走 本地 Parquet → TDX 直读，`db_manager=None` 不触发远程（`src/data/managers/data_fetcher.py:203-233,269,328-350`） | **实测**取到 600000.SH 2023 全年 242 行 |
| 组合/批量取数 | ✅ 可用：本地批量优先，逐票兜底 TDX | `src/data/managers/data_fetcher.py:365-390` |
| 回测 · 显式传入 `data` | ✅ 可用 | **实测** 600000.SH 2023 全年 `total_return=-6.26%`、14 笔交易，全程未连库 |
| 回测 · GUI（仅传 `ts_code`） | ❌ **失败**：`_prepare_backtest_data` 无 `data` 时强制 `_get_shared_data_fetcher()` → `DatabaseManager.connect()`，无库抛异常（`src/core/usecases/run_backtest.py:51-64,88-110`；`src/ui/dialogs/strategy_backtest_dialog.py:46-52`） | 代码核对；**未实测**异常文案/超时时长 |
| 参数优化（走同一 `run_backtest` 取数分支） | ⚠️ 同 GUI 回测，仅传代码时受阻；先批量取数再传入则可用 | 部分未验证 |
| 每日推荐 | ✅ 可用：`AdviceEngine` 只读 `LocalQuoteStore` + `names.json` + 快照（`src/advice/pipeline.py:96-101`、`src/core/usecases/get_daily_recommendations.py:222-257`），无库路径 | 代码核对；**未实测**完整推荐 |
| 因子库 | ✅ 可用：`src/quant/factor_lib/data.py:6-20` 直读本地 Parquet（含复权因子快照），不查库 | 代码核对；**未实测** |
| 数据质检 | ✅ 可用：`quality check --source local`（`src/data/quality/__main__.py:79-92`）；跨源对拍需要远程源，离线不可用 | 代码核对；本机未跑该命令 |
| 除权记账 | ⚠️ 仅当 `factors/stock_dividend.parquet` 已拷贝时可用；否则按 Q1 静默不计分红 | 代码核对 |
| DB 同步类命令 | ❌ `--sync-*`、`--update-*` 等会连接失败并 `logger.error`，不静默改数据 | `src/main.py:134-377` |
| 远程辅助（AkShare/Baostock/腾讯补数/分红刷新） | ❌ 不可用（默认关闭且目标机离线） | 配置默认值 |

**结论**：MySQL 不是运行期「读行情」的必需品，但**当前 GUI 策略回测**与**分红快照刷新**两处是硬依赖。目标机若不架设数据库，建议：① 回测改用「显式传 data」的调用方式（脚本/CLI）；② 从源机拷贝 `stock_dividend.parquet`。

### Q3. 机器相关硬编码路径 / 必须改哪几处

| 位置 | 现状 | 目标机处理 | 是否可用环境变量覆盖 |
|---|---|---|---|
| `.env:19` `DATA__TDX_DATA_PATH` | `h:\zxzq\vipdoc` | **必改**为目标机 `vipdoc` 绝对路径 | ✅ `DATA__TDX_DATA_PATH` 环境变量优先（pydantic-settings 嵌套前缀） |
| `.env:2-6` `DATABASE__HOST/PORT/USERNAME/PASSWORD/DATABASE` | 生产远程库（IP + 凭据，本文不展开） | **必须处理**：删凭据并指向 `localhost`（快速失败进离线）或改为目标机本地库 | ✅ `DATABASE__HOST` 等 |
| `src/utils/config.py:85` `tdx_data_path` | 默认 `""`（不是 `h:\zxzq`） | 不配则通达信取数全部失效 | ✅ `DATA__TDX_DATA_PATH` |
| `src/utils/config.py:104-111` `local_store_path` | 默认 `data/local_store`（相对项目根） | 列存放在别处时改成绝对路径 | ✅ 配置项 / `DATA__LOCAL_STORE_PATH` |
| `src/utils/config.py:114-117` `remote_database_fallback` | 默认 `false` | 保持 false 即为离线读 | ✅ `DATA__REMOTE_DATABASE_FALLBACK` |
| `src/utils/config.py:91-94` `allow_remote_sources` | 默认 `false` | 需要 AkShare/BaoStock 时再开 | ✅ `DATA__ALLOW_REMOTE_SOURCES` |
| `.env` 其余（`LOG__FILE_PATH=logs/app.log` 等） | 均为相对路径 | 无需改（保持仓库根运行） | ✅ |
| `.env` 加载方式 | `SettingsConfigDict(env_file=".env")` 相对**当前工作目录**（`src/utils/config.py:233-240`） | 必须在仓库根执行 `poetry run python -m src.main`，否则配置不生效 | ✅（改用真实环境变量即可不依赖 `.env`） |
| 本地列存根路径推导 | 相对仓库 `data/local_store`（`src/data/local_store/store.py:92-98`，基于 `Path(__file__).parents[3]`） | 无需改 | ✅ 同上 |
| 日志文件 | 相对 `logs/app.log`（`src/utils/config.py:173`） | 无需改（目录自动创建） | ✅ `LOG__FILE_PATH` |
| 推荐报告/状态 | 相对仓库 `reports/advice`（`src/core/usecases/get_daily_recommendations.py:163-169`）；`src/advice/pipeline.py:172-173` 默认状态目录、`src/advice/cli.py:118` 默认输出目录 | 无需改 | ✅ CLI `--state-dir`、`--out-dir` |
| 通达信 cfg | `vipdoc` 上一级推导 `T0002/hq_cache`（`src/data/industry.py:37-40`） | 保持 `TDX根/vipdoc` + `TDX根/T0002/hq_cache` 层级 | ❌ 无单独配置项（只能通过 TDX 路径间接决定；`--cache-dir` 仅 CLI 单次覆盖） |
| 文档注释里的 `D:\TDX\vipdoc` | `src/utils/config.py:429` 仅示例注释 | 无需改（不参与运行） | — |

**补充（代码核对）**：生产代码中未发现其它写死的绝对路径；`grep` 命中的 `E:\...` 仅在 `tmp/` 调试脚本里，不属于运行链路。`tools/`、`scripts/` 均以仓库根/参数传路径方式工作。`h:\zxzq` 只出现在 `.env`、`.env.example` 注释、`docs/` 与 `tmp/` 草稿脚本。

---

## 3. 核查方式与未验证项

### 3.1 本次只读核查手段
- 代码检索：`grep` 覆盖 `vipdoc|lday|.day|hq_cache|.tnf|tdxhy|tdxzs|minline|fzline|gpcw|names.json|stock_dividend|local_store|db_manager` 等关键字，逐条核对调用点与行号。
- 体积/规模：`Get-ChildItem -Recurse` 统计；`polars.read_parquet` 读取 `meta/daily_index.parquet`、`factors/stock_adj_factor.parquet`、`stock_dividend.parquet`、`st_status_20261103.parquet` 的行数与代码数。
- CLI 实测：`python -m src.data.local_store stats`（9602 标的 / 25,056,094 行 / 816.4 MB）；离线取数 242 行；离线回测 `-6.26% / 14 笔`。
- CLI 参数核对：`build --all`、`rebuild-factors [--include-existing]`、`rescan-meta`、`stats`、`refresh-factors [--allow-remote --force-overwrite --confirm OVERWRITE-LOCAL-SNAPSHOT]`、`risk_flags/industry refresh|stats`、`quality check --source local`、`events import --path` 均由 argparse 定义确认。
- 全程未修改数据、未写远程库、未改生产代码；仅维护本文档。

### 3.2 明确未验证（不要当确定结论使用）
1. 目标机 GUI 实际启动与页面渲染（本机未做自动化启动验证）。
2. 「GUI 回测无库必定抛异常」的具体异常类型、超时时长（代码路径已确认会 `connect()`，但未真实断网复现）。
3. `names.json` 能否用外部工具（mootdx/pytdx 读 `*.tnf`）重建：**本仓库代码不支持**；外部工具读 `*.tnf` 的可行性仍未验证。mootdx 整体可行性已有实测报告且已入版本库（`docs/mootdx_feasibility_report.md`），结论为「不值得接入」，不能作为实现依据。
4. 目标机 TDX 日线与源机的完整度差异（未逐文件比对），以及 `build --all` 在目标机的实际耗时（源机历史记录 115～180 s，未在目标机复测）。
5. 分红快照与生产 MySQL 的时效差（本机快照 53,974 行 / 5,376 代码，未与线上库比对）。
6. 全量推荐链路与因子库在纯离线环境的端到端结果（仅确认代码不连库，未跑全流程）。
7. `names.json` 中 52,784 个键的资产类别构成（股票/基金/指数/退市）：只实测了总键数与 ST 构建结果，**未逐类拆分**。
8. 通达信客户端如何补齐/回溯历史日线：**未验证**（本机数据由用户维护）。
9. 数据质检在无数据库环境的完整可用性：只做了引用检索与参数核对，**未实测运行该命令**。
10. `reports/advice/calibration/factor_panel.parquet` 重算耗时：**未实测**。
11. `factors/_bench_last.parquet` 是否被仓库外脚本使用：`src/` 无引用，**未验证**外部依赖。
12. 目标机的重建耗时、磁盘占用：本机数值仅作数量级参考，**目标机未验证**。

---

## 4. 两稿合并取舍记录（便于日后复核）

| 冲突点 | 合并后取值 | 取舍理由 |
|---|---|---|
| 必需项是 5 样还是 6 样 | **5 样** | 合并口径：通达信日线与 3 个 cfg 合为 1 样，并把 `.env` 计入必需（缺 `.env` 时 `tdx_data_path=""`，代码核对会直接取数失败）；旧口径拆成 6 样且未计 `.env`。按「缺任一项就跑不起来」取 5 样 |
| 列存子目录大小写 | **小写 `sh/sz/bj`** | 本次实测 `data/local_store/quotes/stock/` 下为 `sh`、`sz`、`bj`；旧稿写 `{SH,SZ,BJ}` 与实测不符 |
| `rebuild-factors` 参数 | **两种都保留** | 旧稿只写 `rebuild-factors`（默认只补缺）；新稿写 `--include-existing`（重算已有）。语义不同（`factor_rebuild.py:209,218,228-229`），文档按场景二选一说明 |
| `tmp/` 文件数 | **145 个 / 94,796,793 B（本次实测）** | 该目录随工作区变化，此前记录为 142、122；统一标注为当日快照 |
| 构建耗时 | **115～180 s（区间）** | 旧稿记 179.6 s、新稿记约 115 s，应为不同批次/缓存状态；不写单一值，且标注目标机未复测 |
| `reports/advice/` 顶层规模 | **15 个文件 / 144,389 B（本次实测）** | 旧稿记「约 130 KB / 16 个文件」，本次实测更新 |
| 分红快照路径 | **`data/local_store/factors/stock_dividend.parquet`** | 两稿一致，且本次实测存在（677,598 B / 53,974 行），无冲突 |
| `tencent minute` 命令 | **补 `--codes`** | 旧/新稿均写成 `minute --allow-remote`，但 argparse 中 `--codes` 为必填（`src/data/tencent/__main__.py:204`），按实际 CLI 修正 |
