# Git 跟踪文件必要性审计

- 审计对象：`e:\code\python\quant`，`HEAD` 全部跟踪文件
- 审计日期：2026-11-12
- 审计性质：**只读**。未改任何文件、未删文件、未动索引、未提交
- 工作脚本与原始数据：`2026-11-12/git-tracked-audit/work/`（`collect.py` / `stats.py` / `refs.py` / `groups2.py`，结果落 `files.json`）

## 0. 结论摘要

1. 实际跟踪文件是 **806 个 / 13,924.5 KB（14,258,706 字节）**，任务书写的「805 个」与实际差 1（`git ls-files | wc -l` 实测 806，见 §1.1）。本报告按实测 806 计。
2. **主体是干净的**：`src/` 321 个 `.py` 全部是生产代码，`tests/` 197 个 `.py` 全部是测试，**没有任何 `.parquet` / `.pkl` / `.db` / `.sqlite` / `.xlsx` / `.zip` 被跟踪**。数据层唯一一条漏网是 1 个 5.7 KB 的 `manifest.json`。
3. 找到 **34 个问题项**（占 4.2%，占体积 2,960,351 B / 20.8%），可分为 22 个建议取消跟踪、2 个建议删除、10 个需人工判定；**772 个 11,298,355 B 判定必须保留**。
4. 单个最贵的问题项不是脚本而是 **`logs/data_changelog.csv`（1,945,970 B，占全部跟踪体积 13.6%）**——它是台账真源、有代码与测试消费者，因此归「需人工判定」而非「该删」。
5. 唯一与 `.gitignore` 硬矛盾的是 **`data/local_store/meta/anomalies/manifest.json`**：`git check-ignore -v` 命中 `.gitignore:168:data/local_store/`，却仍在跟踪区。它描述的台账 CSV 本身不在 Git 里，形成「有清单无数据」的半套。
6. `research/events/data/` 的 8 个 CSV + 3 个 JSON（≈103 KB）**不是**漏网：`232a1ae` 的提交说明明确「json 与小体积 csv 元数据继续跟踪」，且这 11 个文件全部被 `research/events/*.py` 读取，属有意保留。

## 1. 审计方法与判定标准

### 1.1 数据来源（全部可复算）

| 项 | 命令 | 说明 |
|---|---|---|
| 清单 | `git -c core.quotepath=false ls-files -s` | 806 条，含 mode/blob |
| 大小 | `git cat-file --batch-check`（对 806 个 blob 去重后批量查） | 汇总 14,258,706 B |
| 首次进库 | `git log --reverse --diff-filter=A --name-only`；对 17 个无 `A` 记录的再跑 `git log --follow --diff-filter=AR` | 17 个是英文件名→中文名重命名（如 `architecture.md`→`架构与数据流.md`），`A` 记录不存在，用 `R` 补齐 |
| 忽略命中 | `git check-ignore -v --no-index --stdin`（分 200 条/批） | 3 条命中，其中 2 条是 `!` 取反规则（假阳性，见 §3.5） |
| 引用 | `git grep -F <文件名/模块名> HEAD` | 内容检索，**不匹配文件名本身**，故「0 引用」= 没有任何文件正文提到它 |
| 首行说明 | `ast.get_docstring` 取模块文档首句 | 用于判断脚本是一次性探测还是长期入口 |

> 差异说明：`Measure-Object -Line` 计 806，`Measure-Object` 的 `Count` 也计 806，`ls-files -s` 解析同样 806。三个口径一致，任务书的 805 应为笔误或统计时点差异，本报告以 806 为准。

### 1.2 判定标准（四条，每条可判是/否）

- **S1 生产者闭环**：库内有产出脚本，且产物不被任何代码当输入读 → 可重算，不该入库。
- **S2 消费者存在**：产物被 `src/` / `tests/` / `tools/` 的正文读取（路径常量、`glob`、`import`）→ 入库有理。
- **S3 单次性证据**：文件名或模块文档自述「一次性 / 探测 / mockup / 演示」，或目录被 README 自述为「一次性数据修复 / 抽样查询脚本」。
- **S4 路径合规**：路径是否被 `.gitignore` 覆盖（`check-ignore` 非取反规则命中即不合规）。

四条里 S1+S3 同时成立且 S2 不成立 → 建议取消跟踪；S4 成立 → 硬性问题；S2 成立 → 必须保留。

## 2. 分类统计表

| 分类 | 文件数 | 体积 | 占比 | 主要扩展名 |
|---|---|---|---|---|
| 源码（`src/`） | 322 | 4,203.8 KB | 30.2% | `.py`×321、`.json`×1 |
| 文档（`docs/`） | 107 | 3,337.1 KB | 24.0% | `.md`×101、`.png`×3、`.json`×3 |
| 数据台账（`logs/`） | 2 | 1,903.2 KB | 13.7% | `.md`×1、`.csv`×1 |
| 工具脚本（`tools/`） | 104 | 1,845.3 KB | 13.3% | `.py`×104 |
| 测试（`tests/`） | 198 | 1,668.8 KB | 12.0% | `.py`×197、`.json`×1 |
| 根目录配置与说明 | 7 | 502.9 KB | 3.6% | `.toml`×2、`.lock`×1、`.ini`×1、`.md`×1、`.example`×1、`.gitignore`×1 |
| 研究与数据（`research/`） | 24 | 201.2 KB | 1.4% | `.py`×11、`.csv`×8、`.json`×5 |
| 一次性/分析脚本（`scripts/`） | 12 | 115.1 KB | 0.8% | `.py`×12 |
| 规则（`.trae/`） | 6 | 56.0 KB | 0.4% | `.md`×5、`.ps1`×1 |
| 迁移（`migrations/`） | 17 | 45.5 KB | 0.3% | `.py`×16、`.mako`×1 |
| 插件示例（`plugins/`） | 2 | 20.1 KB | 0.1% | `.py`×2 |
| 示例（`examples/`） | 3 | 19.0 KB | 0.1% | `.py`×3 |
| 正式数据层（`data/`） | 1 | 5.6 KB | 0.0% | `.json`×1 |
| 配置（`config/`） | 1 | 0.8 KB | 0.0% | `.json`×1 |
| **合计** | **806** | **13,924.5 KB** | 100% | |

非代码文件只有 **24 个**：`.json`×12、`.csv`×9、`.png`×3。**没有** 任何 `.parquet`、`.pkl`、`.db`、`.sqlite`、`.feather`、`.zip`、`.xlsx`——这一点按扩展名穷举确认（`stats.py` 输出），不是抽样。

## 3. 逐类问题项

### 3.1 数据文件被跟踪（24 个，逐个判定）

| # | 路径 | 字节 | 首次进库 | 类型 | 该不该在 Git |
|---|---|---|---|---|---|
| 1 | `logs/data_changelog.csv` | 1,945,970 | `cc362717` 2026-11-05 | **结论/台账真源** | **该**（见下） |
| 2 | `docs/因子IC快照_2026-11-11.json` | 10,141 | `8e4eab9f` 2026-11-11 | **测试 fixture** | **该** |
| 3 | `docs/策略参数基线_2026-11-11.json` | 11,643 | `f3c6593d` 2026-11-11 | 修前快照（已失效） | **不该**（§3.6） |
| 4 | `docs/策略参数基线_2026-11-11_v2.json` | 12,110 | `cb7cb72f` 2026-11-11 | 当前基线快照 | 该（命名待修，§3.6） |
| 5-7 | `docs/screenshots/advice_dark.png`<br>`advice_light.png`<br>`advice_empty.png` | 224,338<br>227,428<br>80,243 | `b49c2a14` 2026-11-03 | **可重算产物** | **不该**（§3.3） |
| 8-15 | `research/events/data/` 8 个 CSV：`announcement_event_summary.csv`(15.2K)、`announcement_event_summary_ma.csv`(15.0K)、`event_sector_map.csv`(6.1K)、`macro_event_summary.csv`(38.9K)、`macro_events.csv`(7.3K)、`macro_mapping_verdict.csv`(2.3K)、`news_source_sample.csv`(3.5K)、`sector_etf_universe.csv`(0.4K) | ≈88,700 | `4b8fc1b4` 2026-11-03 | **结论报告数据 + 脚本输入** | **该** |
| 16-18 | `research/events/data/` 3 个 JSON：`fetch_meta.json`(0.7K)、`news_source_probe.json`(2.5K)、`backfill_supplement/_audit.json`(11,278) | ≈14,200 | `4b8fc1b4` / `823a5b5b` | 采集元数据 / 补数审计 | 前两个**该**；`_audit.json` 需人工判定 |
| 19-20 | `research/mootdx/mootdx_probe_servers.json`(20,164)<br>`mootdx_scan_all.json`(17,566) | 37,730 | `e26627fe` 2026-11-03 | **可重算探测结果** | **不该**（§3.3） |
| 21 | `src/data/quality/freshness_config.json` | 19,447 | `26e356d5` 2026-11-06 | 配置（新鲜度阈值） | **该** |
| 22 | `config/patrol_targets.json` | 806 | `34f1152d` 2026-11-10 | 配置（巡检登记） | **该** |
| 23 | `tests/golden/baseline.json` | 8,724 | `829e58a6` 2026-11-06 | **测试 fixture（黄金基线）** | **该** |
| 24 | `data/local_store/meta/anomalies/manifest.json` | 5,717 | `01e850ad` 2026-11-12 | 派生数据 manifest | **不该**（§3.5） |

判定依据补充：

- #1 `logs/data_changelog.csv` 不是「顺手带进来的 csv」。`src/data/changelog.py:1` 自称「数据变更台账（logs/data_changelog.csv）的**唯一写入入口**」，`:61` 有路径常量，`tests/test_changelog.py` 有 7 处引用。台账约定「只追加、不改历史行」，属**结论级真源**，删了无法重建（历史变更理由只存在于这份 CSV）。体积大是结果不是错。
- #2 `docs/因子IC快照_2026-11-11.json` 被 `tests/test_factor_registry_direction.py:32` 用 `SNAPSHOT_GLOB = '因子IC快照_*.json'` 走 glob 读，用来判定期限 h=60 的 OOS IC 符号。**它是测试基准，不是普通文档数据**，重算会改变测试判定，必须钉住。
- #8-18 `research/events/data/`：`232a1ae` 当初只撤了 40 个 parquet，正文写明「json 与小体积 csv 元数据继续跟踪」。11 个文件全部被 `research/events/announcement_event_study.py`、`macro_event_study.py`、`fetch_news_sample.py`、`fetch_announcements.py` 读写，S2 成立。
- #21/#22/#23 三种配置/fixture 均由代码按路径读取，S2 成立。

### 3.2 临时或一次性脚本

先按文件名（`_tmp_` / `tmp_` / `scratch` / `probe` / `debug` / `demo` / `test_manual` / `check_`）扫，再按模块文档首句扫「一次性 / 临时 / mockup / 演示」，共 9 + 52 条初筛；去掉「一次性」出现在业务描述里（如「每期一次性扣除」）的假阳性后，实质候选如下。

| # | 路径 | 字节 | 首次进库 | 一次性证据 | 引用数 | 判定 |
|---|---|---|---|---|---|---|
| 1 | `scripts/query_etf.py` | 709 | `6de957f5` 2026-10-26 | 顶层裸执行、无 `main()`、无 argparse、硬编码 `like('159%')`、无文档串 | **0** | 建议删除 |
| 2 | `scripts/correct_stock_basic.py` | 4,163 | `6de957f5` 2026-10-26 | 文档首句「以通达信数据源为准」校正；同职能已由 `tools/build_stock_basic.py`（43 KB，含守卫与对拍）承接 | **0** | 建议取消跟踪 |
| 3 | `scripts/verify_data_20261104.py` | 42,344 | `0986c017` 2026-11-04 | 文件名带日期「2026-11-04 数据独立验证线」 | 8（全部是 docs 与同批脚本互相引用） | 建议取消跟踪 |
| 4 | `scripts/verify_gap_sina.py` | 3,801 | `0986c017` 2026-11-04 | 对拍 236 条当次候选 | 1（`docs/数据验证与对拍报告_2026-11-04.md`） | 建议取消跟踪 |
| 5 | `scripts/verify_zero_event_codes.py` | 2,420 | `0986c017` 2026-11-04 | 当次抽样推断 | 1（同上） | 建议取消跟踪 |
| 6 | `scripts/analyze_dividend_full.py` | 9,682 | `0986c017` 2026-11-04 | 当次全市场分红对拍 | 1（同上） | 建议取消跟踪 |
| 7 | `scripts/analyze_factor_detail.py` | 6,400 | `0986c017` 2026-11-04 | 当次因子列核查 | 1（同上） | 建议取消跟踪 |
| 8 | `scripts/analytics_validation.py` | 10,977 | `f246b176` 2026-11-03 | 「只读分析层实测脚本」 | 2（迁移清单、分析层报告；`.gitignore` 那条是旧英文副本 `analytics_validation_report.md` 的子串误命中） | 建议取消跟踪 |
| 9 | `scripts/smoke_risk_filters.py` | 5,657 | `9bbb59f1` 2026-11-03 | 「最小冒烟（功能验证，非策略评估）」 | 1（`docs/主机迁移本地数据清单.md`） | 建议取消跟踪 |
| 10 | `scripts/verify_advice_backtest.py` | 5,044 | `0986c017` 2026-11-04 | 当次台账一致性核对 | **0** | 建议取消跟踪 |
| 11-16 | `research/mootdx/` 6 个 `.py`（`coverage`/`minute_probe`/`probe_funcs`/`probe_servers`/`scan_all`/`stability`） | 14,488 | `e26627fe` 2026-11-03 | 文档首句均自述「探测 / 实测 / 行为确认」 | 0 生产引用（仅 docs 与其自身） | 建议取消跟踪 |
| 17 | `tools/chip_layout_mockup.py` | 20,206 | `ec691789` 2026-11-05 | 文档首句「界面布局示意图（mockup），**非正式界面实现**」，产出落 `reports/chip_prototype`（`reports/` 已不入库） | 0（仅被 `chip_prototype.py` 反向提及） | 建议删除 |
| 18 | `tools/chip_prototype.py` | 24,907 | `ec691789` 2026-11-05 | 文档首句「B 档原型试画」 | 2（`src/tech_analysis/chip.py:6` 引为口径基准；`chip_layout_mockup.py:35` 真 import） | 需人工判定 |
| 19 | `tools/recalculate_adj_factor.py` | 2,420 | `f10e7135` 2026-10-22 | 单票重算并回写 MySQL（`stock_adj_factor` + `stock_daily`） | **0** | 需人工判定 |
| 20 | `tools/recalculate_adj_factor_batch.py` | 3,046 | `f10e7135` 2026-10-22 | 同上批量版 | **0** | 需人工判定 |
| 21 | `tools/update_calibrate_stock_dividend.py` | 12,495 | `f10e7135` 2026-10-22 | 旧口径「更新并校准」（现役是 `collect_stock_dividend.py` 33K + `derive_total_dividend.py`） | 1（工作日志） | 需人工判定 |

判断依据与背景事实：

- `README.md:94` 自己把 `scripts/` 描述为「**一次性数据修复 / 抽样查询脚本**」，`README.md:95` 才列 `examples/`。目录定位就是一次性，`.gitignore:161-165` 也已经按这个理由排除了 4 个同类脚本（`add_399260.py` 等）。所以 `scripts/` 里 0 消费者、且职能已被 `tools/` 正式工具取代的脚本，与那 4 个被排除的是同一类，只是没被同一规则覆盖。
- #19-#21 三个 2026-10-22 的脚本**依赖模块仍在**（`src/data/adj_factor_calculator.py`、`src/data/tdx_handler.py`、`src/database/db_manager.py`、`src/utils/config.py` 均已跟踪），import 不会报错，所以不能按「死代码」直接删；但它们的落点是 MySQL 回写，而现行口径是本地列存 + 复权因子快照（`refresh_quote_factor_columns`），存在双口径风险，故列需人工判定。
- `tools/sync_st_name_history.py` 体积极小但**不是**一次性脚本：它是 `python -m src.data.st_name_history` 的薄 CLI 包装，逻辑在 `src/`，保留。

### 3.3 可重算的生成物

| # | 路径 | 字节 | 产出脚本 | 是否有消费者当输入 | 判定 |
|---|---|---|---|---|---|
| 1-3 | `docs/screenshots/advice_{dark,light,empty}.png` | 532,009 | `scripts/render_advice_screenshots.py`（文档首句「离屏渲染真实 grab，不是设计稿」） | **否**——`git grep "screenshots/advice"` 只命中产出脚本自身，README 与 101 篇 md 均未引用 | 建议取消跟踪 |
| 4-5 | `research/mootdx/mootdx_{probe_servers,scan_all}.json` | 37,730 | `research/mootdx/mootdx_scan_all.py` / `mootdx_probe_servers.py`（同目录，脚本写这些 json） | 仅 `docs/mootdx接入可行性验证报告.md` 正文引用 | 建议取消跟踪 |
| 6 | `docs/策略参数基线_2026-11-11.json` | 11,643 | `tools/tune_advice.py:327` `export_baseline()` | `latest_baseline()` 用 `sorted(glob)` 取**最后一个**，实际取到 `_v2`；此文件仅被 5 篇 md 正文提及 | 建议取消跟踪（§3.6） |

补充：`docs/` 里 101 篇 `.md` 绝大多数是**当次审计/实验的证据留档**（含行数、SHA256、逐值对拍数字），不属于「可由一条命令重算」的产物，且项目规则明确要求「值得长期留存的报告另存一份进 `docs/` 纳入 Git」。这类不列入问题项。

### 3.4 体积异常项（单文件 > 500 KB）

全库只有 **1 个**：

| 路径 | 大小 | 首次进库 | 是什么 | 判断 |
|---|---|---|---|---|
| `logs/data_changelog.csv` | 1,945,970 B（1,900.4 KB） | `cc362717` 2026-11-05 | 数据变更台账，**只追加**，每次改动数据追加一行（含理由、SHA256、行数） | 保留；但需注意每追加一次就在 Git 历史里多存一份完整快照 |

次大项（300–500 KB）只有 `poetry.lock`（462.5 KB，`c0bdf918` 2026-10-29），锁文件必须入库，无问题。

> 台账的 Git 膨胀值得记一笔：`logs/data_changelog.csv` 每变一次就是 +1.9 MB 的历史增量，全年可累积到 GB 级。但这属于「保留策略」而非「该不该跟踪」，不作为取消跟踪的理由；若日后要瘦身，正确做法是台账滚动归档到 `_backup/` 并从库内截断旧行，需单独决策。

### 3.5 与 `.gitignore` 矛盾项

`git check-ignore -v` 命中 3 条，逐条核实：

| 路径 | 命中的规则 | 是否真矛盾 | 说明 |
|---|---|---|---|
| `data/local_store/meta/anomalies/manifest.json` | `.gitignore:168:data/local_store/` | **是** | 父目录被整条忽略，该文件却处于跟踪区（`01e850ad` 2026-11-12 提交） |
| `logs/data_changelog.csv` | `.gitignore:117:!logs/data_changelog.csv` | 否 | `!` 是**取反**（白名单），即「只允许这一个日志文件入库」 |
| `logs/data_changelog.README.md` | `.gitignore:118:!logs/data_changelog.README.md` | 否 | 同上，`!` 取反，是有意放行 |

`manifest.json` 的问题具体在哪：

1. **规则层面**：它是派生数据的 manifest，按 DG-3 应与数据同目录；但 `data/local_store/` 是「一律不入库」的数据层（`.gitignore:167-168`），manifest 单独进 Git 就绕过了这条边界。
2. **完整性层面**：它登记的 `fund_fake_bar_bars.csv` 与 `no_ex_rights_events.csv` **都不在 Git**（`git ls-files` 无这两个文件）。只跟踪清单、不跟踪被描述的数据，读者拿到仓库无法校验任何一条 SHA256。
3. **职责层面**：该文件的价值是「台账可复现性说明（A 级可重生成 / B 级仅归档）」，这属于文档职责，`docs/数据缺陷修复_行情漏建与台账可复现_2026-11-12.md` 第 232 行已经在正文复述了同一份内容。

### 3.6 多版本并存（附加发现）

`docs/` 下同一天存在两份基线快照：`策略参数基线_2026-11-11.json`（`f3c6593d`，14:41）与 `策略参数基线_2026-11-11_v2.json`（`cb7cb72f`，15:43）。

逐字段对比（`config` 拍平后）：**9 个字段不同**，其中 3 个因子在 3 个档位上方向翻转：

```
tiers.{long,mid,short}.factors.gtja_003.direction  v1=1.0  v2=-1.0
tiers.{long,mid,short}.factors.gtja_010.direction  v1=1.0  v2=-1.0
tiers.{long,mid,short}.factors.gtja_024.direction  v1=1.0  v2=-1.0
```

即 **v1 记录的是修正前的错误方向**（对应 `docs/因子方向冲突诊断_2026-11-11.md` 那批结论）。`tools/tune_advice.py:348` 的 `latest_baseline()` 用 `sorted(glob)[-1]` 取值，`_`(0x5F) > `.`(0x2E)，所以程序取到的是 v2，行为没坏；但**人读 v1 会拿到错方向**，且两份同日在库，违反「同一语义只保留一个最终版本」。v1 是可重建的（`_git_commit=af0b37a6`，随时能重新导出），因此建议把 v1 移出跟踪，并把 v2 重命名为不带 `_v2` 的日期名以消除排序歧义。

## 4. 每个问题项「删了会丢什么」

| 问题项 | 能否由库内脚本重算 | 是否被代码当输入读 | 只是历史留档？ | 删了实际会丢什么 |
|---|---|---|---|---|
| `data/local_store/meta/anomalies/manifest.json` | 台账 CSV 可由 `tools/anomalies/build_*.py` 重跑（文档记 A 级 14/14、7/7 逐字段 0 差异） | 否（`git grep` 仅两篇 doc 提及） | 否，是「可复现性声明」 | 丢一份与 Git 内其它内容重复的说明；磁盘文件不受影响（取消跟踪不删盘） |
| `docs/screenshots/*.png` ×3 | 是，`scripts/render_advice_screenshots.py`（离屏渲染） | 否 | 是 | 只丢界面展示图；需要时重跑脚本，但依赖 Qt 离屏环境，换机可能渲染不出同尺寸 |
| `research/mootdx/*.py` ×6 + `*.json` ×2 | 脚本不需重算（本身就是脚本）；json 可重跑，但依赖外部行情服务器可用性，结果不会逐字节一致 | 否 | 是——结论已固化进 `docs/mootdx接入可行性验证报告.md` | 丢「结论是怎么测出来的」的原始探测代码；报告里的 142 节点、回溯年限等数字将失去可复算入口 |
| `scripts/verify_data_20261104.py` | 是（只读核查，输入可重建） | 否 | 是——`docs/数据验证与对拍报告_2026-11-04.md` 引用 | 丢当次独立验证线 42 KB 的复算入口；结论数字仍在报告里 |
| `scripts/analytics_validation.py` | 是 | 否 | 是——`docs/分析层接入实测报告.md` 引用 | 丢 Alphalens/QuantStats 实测的复算入口 |
| `scripts/analyze_dividend_full.py` | 是 | 否 | 是——同上报告引用 | 丢全市场分红跨源对拍脚本 |
| `scripts/analyze_factor_detail.py` | 是 | 否 | 是——同上报告引用 | 丢复权因子精度核查脚本 |
| `scripts/smoke_risk_filters.py` | 是 | 否 | 是——迁移清单引用 | 丢风险过滤冒烟脚本 |
| `scripts/verify_advice_backtest.py` | 是 | 否 | 是（0 引用） | 丢台账与本地行情一致性核对脚本 |
| `scripts/correct_stock_basic.py` | 是 | 否 | 是 | 丢旧校正逻辑；同职能已在 `tools/build_stock_basic.py` |
| `scripts/verify_gap_sina.py` | 是 | 否 | 是——对拍报告引用 | 丢 236 条跳空候选的新浪对拍脚本 |
| `scripts/verify_zero_event_codes.py` | 是 | 否 | 是——对拍报告引用 | 丢零事件标的抽样统计脚本 |
| `docs/策略参数基线_2026-11-11.json` | 是，`tools/tune_advice.py baseline` 可重导出（`_git_commit` 已记录） | 否（程序取 v2） | 是——修前状态留证 | 丢「修前方向」的对拍基准；但 v2 + 诊断文档已记录翻转事实 |
| `scripts/query_etf.py` | 是（20 行 SQL 查询） | 否 | 否，无任何留档价值 | 丢一个随手写的查询片段 |
| `tools/chip_layout_mockup.py` | 否（本身是产出脚本） | 否（仅 `chip_prototype.py` 提到它依赖 matplotlib） | 是 | 丢 UI 布局示意图的生成代码；产出图在 `reports/chip_prototype/`，本就不入库 |
| `logs/data_changelog.csv` | **否**（只追加的历史，无重建来源） | **是**（`src/data/changelog.py:61`） | 否 | 丢全部数据变更历史。**不能删** |
| `docs/数据缺口登记.md` | 否 | 否 | 是 | 丢旧登记表正文；`_正式表_2026-11-12.md` 已声明「旧文件不删除，降级为历史正文」 |
| `_audit.json` | 否（补数审计记录） | 否 | 是 | 丢 11 KB 补数审计元数据 |
| `tools/recalculate_adj_factor*.py` | — | 否 | 否 | 丢一条仍可运行的 MySQL 回写入口（与现行快照口径并行） |
| `tools/update_calibrate_stock_dividend.py` | — | 否 | 否 | 丢旧分红校准入口 |
| `tools/chip_prototype.py` | — | `src/tech_analysis/chip.py` 正文引为口径基准（非 import） | 否 | 丢筹码算法口径的参考实现（生产实现仍可运行） |
| `examples/*.py` ×2 | — | 否 | 否 | 丢两个演示脚本；`demo_backtest.py` 与 `quickstart.py` 功能重叠 |
| `plugins/indicator_visualization.py` | — | 否（插件系统已暂停，`src/plugin/plugin_manager.py:6` 自述 `main.py` 不再调用） | 否 | 丢一个插件示例；恢复插件系统时 `plugin_manager.py:89` 会按 `plugins/` 目录扫描 |

## 5. 分组结论表

| 分组 | 文件数 | 合计字节 | 合计 KB | 占总体积 |
|---|---|---|---|---|
| **必须保留** | 772 | 11,298,355 | 11,033.5 | 79.2% |
| **建议取消跟踪（磁盘保留）** | 22 | 692,075 | 675.9 | 4.9% |
| **建议删除** | 2 | 20,915 | 20.4 | 0.1% |
| **需人工判定** | 10 | 2,247,361 | 2,194.7 | 15.8% |
| 合计 | 806 | 14,258,706 | 13,924.5 | 100% |

### 5.1 必须保留（772 个 / 11,298,355 B）

判据 S2（有代码/测试消费者）或属不可替代的生产资产：

- `src/**` 322（321 py + `src/data/quality/freshness_config.json`）
- `tests/**` 198（含 `tests/golden/baseline.json`）
- `docs/**` 102（100 md + `因子IC快照_2026-11-11.json` + `策略参数基线_2026-11-11_v2.json`；`数据缺口登记.md` 另列需人工判定）
- `tools/**` 99（104 − 1 个建议删除 − 4 个需人工判定）
- `.trae/**` 6、`migrations/**` 17、`config/patrol_targets.json`、`logs/data_changelog.README.md`
- `research/events/` 5 py + 10 data 文件（8 CSV + `fetch_meta.json` + `news_source_probe.json`；`_audit.json` 另列需人工判定）
- `examples/quickstart.py`、`plugins/kline_visualization.py`（README 结构树列出）
- `scripts/calibrate_advice.py`（`tests/test_advice_calibration.py` 引用）、`scripts/render_advice_screenshots.py`（截图的唯一产出入口）
- 根目录 7 个（`pyproject.toml`、`poetry.lock`、`poetry.toml`、`alembic.ini`、`.env.example`、`.gitignore`、`README.md`）

### 5.2 建议取消跟踪（磁盘保留，22 个 / 692,075 B）

执行方式应为 `git rm --cached`，**磁盘文件一律保留**，先落一份 `_backup/` 快照再改索引：

| 路径 | 字节 | 理由（判据） |
|---|---|---|
| `data/local_store/meta/anomalies/manifest.json` | 5,717 | S4 矛盾；所描述的 CSV 不在 Git |
| `docs/screenshots/advice_dark.png` | 224,338 | S1 可重算 + 无消费者 |
| `docs/screenshots/advice_light.png` | 227,428 | 同上 |
| `docs/screenshots/advice_empty.png` | 80,243 | 同上 |
| `research/mootdx/mootdx_probe_servers.json` | 20,164 | S1 可重算 + S3 探测 |
| `research/mootdx/mootdx_scan_all.json` | 17,566 | 同上 |
| `research/mootdx/mootdx_coverage.py` | 2,601 | S3 一次性探测 |
| `research/mootdx/mootdx_minute_probe.py` | 2,283 | S3 |
| `research/mootdx/mootdx_probe_funcs.py` | 1,995 | S3 |
| `research/mootdx/mootdx_probe_servers.py` | 2,411 | S3 |
| `research/mootdx/mootdx_scan_all.py` | 1,798 | S3 |
| `research/mootdx/mootdx_stability.py` | 3,400 | S3 |
| `scripts/verify_data_20261104.py` | 42,344 | S3 日期戳一次性验证线 |
| `scripts/analytics_validation.py` | 10,977 | S3 只读实测 |
| `scripts/analyze_dividend_full.py` | 9,682 | S3 当次对拍 |
| `scripts/analyze_factor_detail.py` | 6,400 | S3 当次核查 |
| `scripts/smoke_risk_filters.py` | 5,657 | S3 冒烟验证 |
| `scripts/verify_advice_backtest.py` | 5,044 | S3 + 0 引用 |
| `scripts/correct_stock_basic.py` | 4,163 | S3 + 被 `tools/build_stock_basic.py` 取代 |
| `scripts/verify_gap_sina.py` | 3,801 | S3 当次对拍 |
| `scripts/verify_zero_event_codes.py` | 2,420 | S3 当次抽样 |
| `docs/策略参数基线_2026-11-11.json` | 11,643 | §3.6 修前错误快照，与 v2 并存 |

### 5.3 建议删除（2 个 / 20,915 B）

| 路径 | 字节 | 理由 |
|---|---|---|
| `scripts/query_etf.py` | 709 | 顶层裸执行、无 `main()`、硬编码 `like('159%')`、0 引用、无留档价值；一条 SQL 即可重现 |
| `tools/chip_layout_mockup.py` | 20,206 | 模块自述「非正式界面实现」，产出落 `reports/`（已不入库），0 消费者，且真 import 的 `chip_prototype.py` 另有判定 |

### 5.4 需人工判定（10 个 / 2,247,361 B）

| 路径 | 字节 | 待判定的问题 |
|---|---|---|
| `logs/data_changelog.csv` | 1,945,970 | 台账真源必须留，但要决定「是否定期归档截断」以控制 Git 历史膨胀 |
| `docs/数据缺口登记.md` | 218,176 | 已被 `_正式表_2026-11-12.md` 取代，但正文声明「旧文件不删除」，属留档还是冗余需拍板 |
| `tools/chip_prototype.py` | 24,907 | 被 `src/tech_analysis/chip.py:6` 引为口径基准，删了会失去参考实现 |
| `plugins/indicator_visualization.py` | 16,452 | 插件系统暂停中；`plugin_manager.py:89` 仍按 `plugins/` 目录扫描加载 |
| `tools/update_calibrate_stock_dividend.py` | 12,495 | 旧分红校准入口，与现役 `collect_stock_dividend.py` 并存 |
| `research/events/data/backfill_supplement/_audit.json` | 11,278 | 补数审计元数据，小体积、无消费者，留或移 `_backup/` 均合理 |
| `examples/china_market_analysis_example.py` | 7,707 | 0 引用、README 未列；需试跑确认是否还兼容现接口 |
| `examples/demo_backtest.py` | 4,910 | 同上，且与 `quickstart.py` 功能重叠 |
| `tools/recalculate_adj_factor_batch.py` | 3,046 | 旧 MySQL 回写链路，依赖模块仍在，非死代码 |
| `tools/recalculate_adj_factor.py` | 2,420 | 同上 |

## 6. 明确「没有问题」的几类（避免误伤）

- **数据层**：`.parquet` / `.pkl` / `.db` / `.sqlite` / `.feather` / `.zip` / `.xlsx` 跟踪数为 **0**。`data/local_store/` 除 §3.5 那 1 个 manifest 外**没有任何文件入库**。
- **`src/`**：322 个文件全部是 321 个 `.py` + 1 个配置 json，无脚本、无产物、无数据。
- **`tests/`**：198 个文件全部是 197 个测试 py + 1 个 golden fixture，无临时调试文件。
- **`migrations/`**：alembic 迁移 17 个，属 schema 历史，必须入库。
- **`.trae/`**：6 个规则/技能文件，是项目治理规则本体，必须入库。
- **`research/events/`**：11 py + 11 小体积 data，`232a1ae` 已明确为「有意保留的小体积元数据」，且被脚本读取。

## 7. 复现命令

```powershell
# 清单与大小
git -c core.quotepath=false ls-files -s | Measure-Object -Line
git -c core.quotepath=false ls-files -s | ForEach-Object { ($_ -split '\s+')[1] } |
  Sort-Object -Unique | git cat-file --batch-check

# 忽略矛盾
git ls-files | git check-ignore -v --no-index --stdin

# 引用检索（注意 git grep 只搜正文，不搜文件名）
git -c core.quotepath=false grep -F "data_changelog" HEAD -- src tools tests

# 两份基线差异
.venv\Scripts\python.exe 2026-11-12\git-tracked-audit\work\collect.py
.venv\Scripts\python.exe 2026-11-12\git-tracked-audit\work\groups2.py
```

## 8. 本轮边界

- 未执行 `git rm`、`git add`、`git commit`、`git checkout`、`git reset` 任何写操作。
- 未删除、未移动、未修改任何被审计文件。
- 临时脚本与中间结果全部写在 `2026-11-12/git-tracked-audit/work/`（该目录被 `.gitignore:200` 整体忽略）。
- 需要落地「取消跟踪」时，正确顺序是：`_backup/` 快照 → `git rm --cached` → 提交 → `git read-tree HEAD` 回填共享索引，本轮不执行。
- 需如实记录的一处副作用：审计过程中执行 `git status --short` 时，Git 自身的索引刷新会生成并删除 `.git/index.lock`，该临时锁文件被环境的删除拦截层移入回收站（`E:\code\python\quant\.git\index.lock`）。核验结果：`git rev-parse HEAD` 仍为 `f2e2a32a`，`git ls-files` 仍 806 条，`git status --short` 输出 0 条，锁文件当前不存在——索引内容与历史均未改变，无需恢复该锁文件。
