# B1 第一片体检：行情四族（只读，不修数据、不修代码）

- 日期：2026-11-12
- 取片依据：`2026-11-12/data-lineage/outputs/base_layer_calibration.csv` 前 4 行，与「行情四族」划分一致（`quotes/stock`、`quotes/index`、`quotes/fund`、`quotes/futures`）
- 范围边界：只做基础层四族；**未触碰任何派生表**（`stock_adj_factor`、`stock_turnover`、`breadth`、`layers`、`state_vector` 等），第 3 项也没有为了对拍去反算派生数据
- 结论落点：`b1_slice1_check.csv`（一表一行，已成型，保留）+ 本文件（判定与归因正文）

## 1. 结论速览

| 表 | ① 写入口可反复跑 | ② manifest 一致性 | ③ 与权威源对拍 | ④ 第二份同语义 | ⑤ 权威清单登记 | 综合 |
|---|---|---|---|---|---|---|
| 个股日线 `quotes/stock` | 通过 | 通过 | **有缺陷**（1993-05 深市日期错位 36 行；6 只沪市大盘股量纲口径 19 行） | 通过（副本均非正式） | **有缺陷**（消费者列过窄；工具描述过时） | **有缺陷** |
| 指数日线 `quotes/index` | 通过 | 通过 | 通过（价格/量/额差异 0；425 行 high/low 差异全部为 TDX 自身越界） | 通过（副本均非正式） | **有缺陷**（消费者列漏 14 个文件） | **有缺陷**（仅登记项） |
| ETF/LOF 日线 `quotes/fund` | 通过 | 通过 | **有缺陷**（1 行占位值异常；量/额差异 0） | 通过（副本均非正式） | **有缺陷**（消费者列漏 13 个文件） | **有缺陷** |
| 期货合约日线 `quotes/futures` | 通过（有条件：依赖一次性 staging） | 通过 | 通过（cffex 官方 6/6 日 0 差异）；shfe/ine **对不了**（无官方独立源） | 通过（副本均非正式） | **有缺陷**（漏 1 个消费者） | **有缺陷**（仅登记项） |

规模（2026-11-12 实测，与权威清单登记数字逐值一致）：

| 表 | 文件 | 行 | 覆盖 |
|---|---|---|---|
| stock | 5,832 | 18,130,902 | 1990-12-19~2026-11-11 |
| index | 1,335 | 4,649,808 | 1990-12-19~2026-11-11 |
| fund | 2,620 | 2,979,395 | 2004-03-22~2026-11-11 |
| futures | 997 | 153,176 | 中金所 2010-04-16 / AG 2012-05-10 / SC 2018-03-26 起 |

## 2. 取证口径（可复算）

### 2.1 第 2 项：manifest 只用现有工具，不自造判据

```
.venv\Scripts\python.exe tools\manifest_tool.py --check --only quotes/stock --only quotes/index \
  --only quotes/fund --only quotes/futures --json 2026-11-12\data-lineage\work\manifest_check_quotes.json
```

结果 `verdict=PASS`：manifest **10,785 份** / 文件记录 **21,569 条** / 不一致 **0** / sha256 格式告警 **0** / 无法定位 **0** / 无 manifest **0** / 语义未登记 **0**；`coverage_gaps` 三个桶（`no_manifest`、`not_located`、`no_manifest_nonparquet`）全为空。唯一一条「非数据集声明」是 `quotes/fund/backfill_2019_gap.manifest.json`，四项闸门全部通过。

逐族：stock 5,832 份、index 1,335 份、fund 2,621 份（含上述 1 份 non_dataset）、futures 997 份，均 0 不一致。

### 2.2 第 3 项：对拍对象、口径与容差

- 对拍对象：通达信本地日线 `H:\zxzq\vipdoc\{sh,sz,bj}\lday\<code>.day`，解析走仓库既有 `src/data/tdx_day_parser.py`（不另写格式判据）
- 单位口径：个股/指数价格 = 整数 ÷ 100；基金价格 = 整数 ÷ 1000；成交量按解析器口径（个股/基金 ÷100 得「手」，指数原样为「手」）；成交额为 `.day` 的 float32 原值（元）
- 容差：价格 `|Δ| ≤ 1e-6`（同一整数源）；成交量 `|Δ| ≤ 1e-9`（整数 ÷100 精确）；成交额相对差 `≤ 2e-6`（float32 约 7 位有效数字）
- 比对列：`open/high/low/close/volume/amount` 六列逐 bar；同时核日期集合差（`.day` 有本地无、本地有 `.day` 无）
- 覆盖：**三族全量**（不是抽样）——stock 5,832 文件、index 1,335、fund 2,620，脚本 `work/scan_all_tdx.py`

| 族 | 可对文件 | 对拍 bar 数 | 价格差 | 量差 | 额差 | `.day` 有本地无 | 本地有 `.day` 无 |
|---|---|---|---|---|---|---|---|
| stock | 5,814 / 5,832 | 17,903,097 | 36 行 | 108 行 | 66 行 | 35 | 25 |
| index | 1,333 / 1,335 | 4,628,445 | 0 行（另 425 行 high/low 加固） | 0 | 0 | 0 | 1,091 |
| fund | 2,620 / 2,620 | 2,875,804 | 1 行 | 0 | 0 | 0 | 3,856 |

对不了的部分：stock 18 只、index 2 只在 `vipdoc` 下无对应 `.day`（已退市 / 非通达信覆盖的指数），属「本地无第二源可比」，不判缺陷。

### 2.3 期货：唯一可用的官方独立源

```
.venv\Scripts\python.exe tools\verify_futures_official.py --days 6
```

6/6 个分层交易日、逐合约 `close/settle/oi/volume` 与中金所官方 `get_futures_daily` **完全相等（0 差异）**。`shfe` / `ine` 没有对接官方接口，只能与 tushare `fut_daily` 同源比对，**判对不了**（同源不构成独立证据）；`H:\zxzq\vipdoc\ds\lday` 为空，TDX 亦无期货日线可作第二源。

## 3. 逐表五项明细

### 3.1 个股日线 `quotes/stock/{sh,sz,bj}/<code>.parquet`

1. **写入口**：通过。常规入口 `python -m src.data.local_store build`（`build` 子命令自述「构建/增量更新本地行情列存，不指定模式时默认增量」），落地经 `LocalQuoteStore.write_daily` → `write_parquet_atomic`（`src/data/local_store/store.py:494-534`，单文件原子：临时文件 → fsync → `os.replace`）。补缺旁路 `src/data/tencent/daily_fill.py`、`tools/fill_internal_gaps.py` 同样走原子写。全仓未发现一次性 `_staging` 脚本直写正式路径。
2. **manifest**：通过（见 2.1）。
3. **对拍**：有缺陷，差异全部归入两类，无第三类：
   - **（a）1993-05-03~05-06 深市日期错位**：17 只 `000xxx`（000001/000006/000007/000008/000011/000012/000014/000016/000018/000019/000020/000501/000502/000503/000504/000506/000507），价格差 36 行、量差 66 行、额差 66 行。成因可复算：TDX 该段含 1993-05-01（六）、05-02（日）两根周末 bar，本地把它们改标到后续工作日 05-03、05-04，其后 2 根顺延，05-07 起又重新对齐，于是本地 05-03~05-06 这 4 根与 TDX 同日 bar 不对应（例 000001.SZ：本地 05-04 = TDX 05-02 的 `58.0/58.1/56.6/56.7`）。**该窗口已有另一路在修**：`_backup/quotes_sz_1993b_20261113/data/local_store/quotes/stock/sz/` 下正是这 17 只改数前快照，本片只登记、不推进。
   - **（b）成交量量纲 19 行**：6 只沪市大盘股（600010、600157、600221、601288、601868、601988）+ 000725.SZ 的个别交易日，本地/`.day` 比值 ≈100，而本地成交额反算 `amount/(volume×close) ≈ 100`（说明本地按「手」正确入库）。成因：TDX `.day` 成交量字段是 uint32，成交股数超过 2^32-1 时改按「手」存储；`tdx_day_parser.py` 只对**基金**分支做了这种识别（同文件 169-179 行注释即写此事），**个股分支无此判断**，故本片对拍脚本读出的 `.day` 量被少算 100×。**本地数据正确，缺陷在解析/对拍工具口径**，不影响正式表。
   - 其余 23 行（11 只代码）为比值 ≈1 的舍入差，最大 0.01 手（如 688496.SH 2026-05-18：`.day` 360,198.57 vs 本地 360,198.56）。
   - **本地有、`.day` 无的 25 个日期**：1991~1996 全年均为工作日，是 TDX 缺行、本地更全，不判缺陷。**`.day` 有、本地无的 35 个日期**含上述 1993-05-01/02 周末 bar，属非交易日 bar 已知项（G-112）。
4. **第二份同语义**：通过（有副本，均非正式）。`_backup/**` 909 个 parquet（`quotes_sz_1993b_20261113` 17、`split_fix_20261110` 11、`internal_gap_fix` 221、`quotes` 633 等回滚快照）、`logs/**` 11,195 个（`tdx_update_backup_20261105` 8,911、`factor_copy_backup_20261104` 5,655 等改数前快照）、`_staging/restore_drill_20261112` 4 个、`G:\git\python-quant` 镜像；`reports/**` 与 `backups/**` 为 0。MySQL `stock_daily` 存在同语义镜像（`local-wins` 覆盖，只做旁路导出、不参与本地取数）。
5. **权威清单登记**：有缺陷。已登记（`docs/数据权威路径清单.md` L54），文件数/行数/日期段与实测逐值一致；但 ——
   - 消费者列只写「store.py、build.py、洁净集/宽度/换手/复权全链路」，实测 **29 个文件 / 68 处**，漏项包括 `src/data/services/local_store_daily_sync.py`、`src/data/quality/cross_check.py`、`src/data/quality/code_migration.py`、`src/advice/universe.py`、`src/quant/factor_lib/data.py`、`tools/build_stock_basic.py`、`tools/build_universe_layers.py` 等；
   - 该行称「`manifest_tool` 对 `quotes/` 显式跳过逐文件核对」，与现行工具不符：`tools/manifest_tool.py:766-769` 已明确取消整段豁免（注释原文「quotes/ 不享受整段豁免…现在与其它子树一视同仁，逐目录判定」），本次实测逐文件核对 0 遗漏。

### 3.2 指数日线 `quotes/index/{sh,sz,bj}/<code>.parquet`

1. **写入口**：通过。同 `build` 入口；补缺旁路 `src/data/index_daily_fill.py`。
2. **manifest**：通过（1,335 份，0 不一致）。
3. **对拍**：通过。
   - 4,628,445 bar 逐 bar：价格、成交量、成交额差异 **全为 0**；`.day` 有本地无的日期 **0**。
   - 425 行 high/low 与 TDX 不同，逐行都等于 `high=max(open,high,close)`、`low=min(open,low,close)` 的加固结果——即 TDX 自身存在 `high<max(open,close)` 或 `low>min(open,close)` 的越界记录（1991~1995 早期为主），本地入库时做了边界加固，属合理修复，不判缺陷。全库反查：本地 1,335 文件 / 4,649,808 行中 OHLC 越界 **0 行**。
   - 本地有、`.day` 无的 1,091 个日期：`880581.SH` 1,087 + `000904.SH` 4，**全部是工作日**，是 TDX 该文件缺行（本地更全），不判缺陷。
   - `000922.SH` / `000985.SH` 无 TDX 文件，二者在权威清单另有单独登记行（来源东财/baostock，已做过双源交叉核对），属「对不了」但已有独立取证，不计入本片缺陷。
4. **第二份同语义**：通过（副本均非正式）。`_backup` 77、`logs/**` 1,284、`_staging` 0、`reports` 0、`backups` 0、`G:` 镜像。
5. **权威清单登记**：有缺陷（仅消费者列）。已登记 L56，1,335 / 4,649,808 / 1990-12-19~2026-11-11 与实测逐值一致；消费者列写 `store.py`、`trading_calendar.py`、`src/advice/*`，实测 **19 个文件 / 30 处**，漏 `src/data/index_daily_fill.py`、`src/data/local_store/build.py`、`src/data/quality/tradable_universe.py`、`tools/build_universe_layers.py`、`tools/build_state_vector.py`、`tools/build_trading_calendar.py` 等。

### 3.3 ETF/LOF 日线 `quotes/fund/{sh,sz}/<code>.parquet`

1. **写入口**：通过。同 `build` 入口，基金侧派生走 `src/data/local_store/fund_factors.py`。
2. **manifest**：通过（2,621 份，含 1 份 non_dataset 记录，0 不一致）。
3. **对拍**：有缺陷（1 行）。
   - 2,875,804 bar：成交量、成交额差异 **0**；价格差 **1 行**——`163503.SZ 2007-10-16`：本地 `open=high=low=close=1.00246`、`volume=0`、`amount=0`；TDX 同日为 `1.956`（折算前旧价结转）。经 2026-11-12 只读取证（B1S1-03），该值是 G-066 修复脚本 `2026-11-12/gap-rootcause/work/apply_g066_163503.py --apply` 写入的**折算日除权参考价** = `1.956 ÷ 1.9512`（`tmp/fund_cache/splits.json` 登记 2007-10-15 份额折算 `1:1.9512`；`hfq_factor` 同日 `3.988115 → 7.781611`，比值 `1.9512003`），**非源端占位、非抓取失败填充、也非成交价**。
   - 本地有、`.day` 无 3,856 个日期：其中 **748 个是周六/周日**（非交易日 bar，属 G-112 范畴）、**477 行属 G-004 九只占位基金**（`160425/160646/160727/161730/161838/164212/164826/166109/169108` 各有整段占位行），其余集中 2025-11~2026 的个别 LOF（如 160137.SZ 59 个日期，均为工作日）。本片不重复深挖 G-112。
4. **第二份同语义**：通过（副本均非正式）。`_backup` 151 + `_backup/quotes/` 下 633、`logs/**` 2,297、`_staging/restore_drill_20261112` 2、`reports` 0。
5. **权威清单登记**：有缺陷（仅消费者列）。已登记 L55，2,620 / 2,979,395 / 2004-03-22~2026-11-11 与实测逐值一致（且已写明基金 `pre_close` 为除权口径的硬约束）；消费者列只写 `fund_factors.py` + `store.py`，实测 **15 个文件 / 25 处**，漏 `tools/build_etf_equity_pool.py`、`tools/mark_fund_tradability.py`、`src/data/quality/fund_price_basis.py`、`src/data/quality/anomaly_lists.py`、`tools/build_fund_corporate_actions.py` 等。

### 3.4 期货合约日线 `quotes/futures/{cffex,shfe,ine}/<合约>.parquet`

1. **写入口**：通过（有条件）。入口 `tools/build_futures_continuous.py`，经 `write_parquet_atomic` 落盘；唯一输入 `_staging/multi_asset_tier2_20261110/daily/{cffex,ine,shfe}` 下 **997 个 parquet 仍在**，可反复重跑。但该 staging 是**一次性采集产物**：只有 `progress_cffex.csv / progress_ine.csv / progress_shfe.csv`，**没有 `failed.csv`**（历史任务，按 `project_rules.md`「`_staging` 断点只约束新任务」不追溯补建）。**重建不出来时怎么办**：staging 一旦被清理，必须用 `tools/fetch_futures_daily.py` 从 tushare `fut_daily` 逐合约重抓（需 token、单连接 + 节流，按 DG-25 限流），无法从本地行情推导。
2. **manifest**：通过（997 份，0 不一致）。**注意**：本片上一版校准表 `base_layer_calibration.csv` 该行写「无任何 manifest（三张派生表同样）」，与实测不符，已在缺陷登记 B1S1-06 中记账（属旧产物笔误，不是数据缺陷）。
3. **对拍**：通过（仅 cffex 有独立源）；shfe/ine **对不了**。
   - cffex：实跑官方对拍 6 个交易日、逐合约 `close/settle/oi/volume` **完全相等**（0 差异），与权威清单 L43 记载的「2026-11-10 抽样 20 日最大绝对差 0」一致。
   - shfe/ine：无官方接口对接，只能与 tushare 同源对拍，同源比对不构成独立证据 → 判对不了；`H:\zxzq\vipdoc\ds\lday` 为空，本地也无 TDX 期货日线。
4. **第二份同语义**：通过（副本均非正式）。`_backup` 2（`futures_settle_g058_20261111`）、`logs` 0、`reports` 0、`backups` 0；`_staging/daily` 的 997 个 parquet 是采集输入而非副本。
5. **权威清单登记**：有缺陷（漏 1 个消费者）。已登记 L43，997 / 153,176 与实测逐值一致，来源与字段口径（含 `settle`、`oi`）描述正确；消费者列写「`tools/build_futures_continuous.py` 的唯一输入」，实测还有 `tools/verify_futures_official.py` 读本层做官方对拍。

## 4. 缺陷登记（只登记，不修）

| 编号 | 表 / 项 | 缺陷 | 证据 | 建议归属 |
|---|---|---|---|---|
| B1S1-01 | stock ③ | 1993-05-03~05-06 深市 17 只 `000xxx` 日期错位：TDX 周末 bar 被改标到后续工作日，其后 2 根顺延；价格 36 行 / 量 66 行 / 额 66 行 | 逐 bar 对拍 + `work/tdx_compare.json`；`_backup/quotes_sz_1993b_20261113` 已有 17 只改数前快照 | **已有另一路在修，本片不派活**，收口时与其对账 |
| B1S1-02 | stock ③（工具侧） | `tdx_day_parser.py` 个股分支未识别「成交量 uint32 溢出改按手存储」，导致对拍把 19 行本地正确值误判为 100× 偏大（基金分支已识别） | 同文件 169-179 行注释 + 19 行 `ratio≈100`、本地 `amount/(volume×close)≈100` | 工具链修复（单独派，改 `src/data/tdx_day_parser.py` + 用例） |
| B1S1-03 | fund ③ | `163503.SZ 2007-10-16` OHLC=1.00246 与 TDX 源值 1.956 不符（当日 volume/amount=0） | **已定性（2026-11-12 只读取证）**：`apply_g066_163503.py --apply` 写入的折算日除权参考价 = `1.956 ÷ 1.9512`。证据链：① TDX 原始该日 1.956 / 0 / 0（源端结转旧价）；② `1.956 ÷ 1.00246 = 1.9512000` 与 `hfq 7.781611 ÷ 3.988115 = 1.9512003` 一致（差 2e-7）；③ `tmp/fund_cache/splits.json` 登记 2007-10-15 份额折算 `1:1.9512`；④ 2007-10-17 `pre_close` 接续该值，复权收益由 `−48.006%` 修正为 `+1.4504%`；⑤ 已在 `meta/anomalies/placeholder_quote_codes.csv` 登记为 `converted_leftover` 段（2007-10-15~16，2 行），`fund_price_basis.is_pseudo=True` 由读取侧过滤，`tests/test_fund_price_basis.py` 13 条锁定。**结论：不是坏行，不需改数**。唯一留痕缺口：该修复未写入 `logs/data_changelog.csv`（该文件仅有 2026-11-05 一次 create），属记账缺口而非数据缺陷 | 不派改数任务 |
| B1S1-04 | 四表 ⑤ | 权威清单四行消费者列全部不完整（实测 stock 29 / index 19 / fund 15 / futures 2 个文件） | `work/consumers_quotes.json`（本轮重 grep，未沿用旧表） | 文档修复（单派，只改 `docs/数据权威路径清单.md`） |
| B1S1-05 | stock ⑤ | 权威清单 L54 称「`manifest_tool` 对 `quotes/` 显式跳过逐文件核对」已过时；工具 `manifest_tool.py:766-769` 已取消整段豁免 | 工具注释 + 本次 `--check` 逐文件 0 遗漏 | 同上，与 B1S1-04 合并派 |
| B1S1-06 | futures ② | 上一版校准表 `base_layer_calibration.csv` 写「期货无任何 manifest」，实测 997 份逐文件 manifest 全部通过 | `manifest_check_quotes.json` 中 `quotes/futures` 997 份 0 不一致 | 旧产物勘误（本片已在本文件与 CSV 中更正，无需另派） |

## 5. 明确不深挖的已知项

- **非交易日 bar（G-112）**：stock 1993-05-01/02 周末 bar、fund 748 个周末日期，均标「已知，见 G-112」，本片不重复取证、不推进。
- **`stock_adj_factor` 三只标的**：已知，已定性 unresolved，属派生层，按要求本片不触碰。
- **派生层一律不动**：`breadth`、`layers`、`state_vector`、`stock_turnover` 等本轮零操作；凡需要派生数据才能对的口径（如用复权因子反算价格）一律记「对不了」，不现算派生数据。

## 6. 复现命令

```powershell
# ② manifest（只用现有工具）
.venv\Scripts\python.exe tools\manifest_tool.py --check --only quotes/stock --only quotes/index `
  --only quotes/fund --only quotes/futures --json 2026-11-12\data-lineage\work\manifest_check_quotes.json

# ③ 三族全量逐 bar 对拍（只读）
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\scan_all_tdx.py index fund stock
# 差异归因
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\explain_tdx_diffs.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\diag_vol_diff.py

# ③ 期货 cffex 官方对拍（只读，6 个分层交易日）
.venv\Scripts\python.exe tools\verify_futures_official.py --days 6

# ⑤ 消费者重 grep（不沿用旧表）
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\grep_consumers.py
```

中间产物：`work/tdx_fullscan_{stock,index,fund}.json`、`work/tdx_compare.json`、`work/stock_vol_diff_rows.csv`、`work/extra_dates_stats.json`、`work/consumers_quotes.json`、`work/manifest_check_quotes.json`。
