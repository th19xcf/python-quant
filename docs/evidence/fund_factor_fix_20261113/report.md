# 批 5：G-118 / G-123 收口证据（2026-11-13）

本轮只动复权因子相关正式表与由它派生的复权列，未动行情 OHLCV 与其它语义的表。
候选与脚本落 `_staging/fund_factor_fix_20261113/`（含 `progress.csv` / `failed.csv`）。

## 1. G-118：判定为误报，不改数据

原文主张：`factors/stock_dividend.parquet` 的 `300163.SZ` `2015-09-25` 行 `cash_div=0` 漏计同日现金分红，
理由是「表内重建 ref = (29.92−0)/3 = 9.973333 → 跌停 8.98，而当日 8.95 破跌停」。

复核结论：**该行 `cash_div=0` 是正确的**，原文漏计了同一复牌行上的另一条事件。

1. 该股 `2015-06-30~2015-09-24` 停牌，行情无中间行（`quotes/stock/300163.SZ.parquet` 在
   `2015-06-29` 与 `2015-09-25` 之间零行）。`2015-07-03` 的派息事件（`cash_div=0.1`，
   `record_date=2015-07-02`、`pay_date=2015-07-03`）与 `2015-09-25` 的 `share_div=2.0` 事件
   **落在同一复牌行**上，因子链按乘积生效。
2. 因此表内合计现金为 `0.1`（不是 0）：ref = (29.92−0.1)/3 = **9.940000**，
   跌停 = round(9.94×0.9, 2) = **8.95**，与当日 `open=high=low=close=8.95` 精确闭合。
3. 交付因子台阶 `qfq(2015-06-29)/qfq(2015-09-25) = 3.010060 = 1/0.33221925`，
   而 `(29.92−0.1)/(29.92×3) = 0.33221925` —— **交付值与「应有参数」下的值逐位相同，因子级误差为 0**。
   原文的 `+0.335%` 是把单事件因子 `0.33333333` 误当成交付值所致（两个事件前收同为 29.92、
   现金同为 0.1，故乘积与「单事件带 0.1 现金」同值）。
4. 独立第二源：通达信权息库 `H:\zxzq\T0002\hq_cache\gbbq`（`pytdx.reader.gbbq_reader.GbbqReader`）
   category=1 记录 `300163` 为 `2015-07-03 hongli=1.0`（每 10 股派 1 元）、
   `2015-09-25 hongli=0.0 / songgu=20`（每 10 股转 20，**无现金**）。

复算入口：

```bash
.venv\Scripts\python.exe -c "
from pytdx.reader.gbbq_reader import GbbqReader
df=GbbqReader().get_df(r'H:\zxzq\T0002\hq_cache\gbbq')
print(df[df['code'].astype(str).str.zfill(6)=='300163'].to_string())"
```

结论：不修任何数据，G-118 转「已收口（核实为误报）」。

## 2. G-123：qfq/hfq 不自洽，已修

### 2.1 实测范围（不照抄原文）

| 口径 | 原文（2026-11-13 00:53 重建前） | 本轮实测（重建前现表） |
|---|---|---|
| `qfq/hfq` 极差 > 1e-7 的标的 | 221 / 2,620 | **218 / 2,620** |
| 其中 `150xxx` | 180 / 218 | **178** |
| 其中 `502xxx` | 22 / 36 | **21** |
| 其它前缀 | 19 | **19** |
| 极差最大值 | 3.8444 | **3.8444**（`150316.SZ`：`qfq/hfq` 在 5.335183~25.845804 间跳变，k=4.844408） |
| 判据 | `max(qfq/hfq)/min(qfq/hfq)-1` | 同 |

差异来自 00:53 的重建已修掉 3 只，其余仍不自洽。

### 2.2 根因

`fund_adj_factor.parquet` 是**按标的增量补齐**的产物：事件表（`fund_dividend.parquet`、
`fund_share_change.parquet`）新增事件后，只有部分标的的因子行被重算，其余标的保留旧行。
旧行与新行拼在同一张表里 → 同一标的下 `qfq/hfq` 的比值跳变，而该比值本应等于
`1/∏{全部事件因子}`（`AdjFactorCalculator.calculate_factors` 中 `hfq = qfq / suffix[0]`）。

反例 `150022.SZ` `2020-01-06`：修前 `qfq` 步长 1.0、`hfq` 步长 0.875108，事件表 `2020-01-02
share_ratio 1.0372`，三者互不相等；修后 `qfq` 步长 **1.0372**，与事件表逐位一致。

进一步证据：`161911.SZ` 修前只有 24 个因子台阶、末台阶停在 `2019-06-21`（之后的派息事件全未反映），
修后 47 个台阶、末台阶 `2025-03-19`，新增台阶逐日落在事件表的 `ex_date` 上。

### 2.3 修法

按当前事件表**全量重算**（`src/data/local_store/fund_factors.py::_compute_code_factors`，
逐标的 `progress.csv` 记账，`failed.csv` 为空），再按 `(ts_code, trade_date)` 主键对拍后整体替换。

```bash
.venv\Scripts\python.exe _staging\fund_factor_fix_20261113\rebuild_fund_factor.py build
.venv\Scripts\python.exe _staging\fund_factor_fix_20261113\rebuild_fund_factor.py apply
.venv\Scripts\python.exe _staging\fund_factor_fix_20261113\rebuild_fund_factor.py lock_hold
.venv\Scripts\python.exe _staging\fund_factor_fix_20261113\rebuild_fund_factor.py verify
```

### 2.4 DG-20 四项

| 项 | 结果 |
|---|---|
| 行数守恒 | 改前 **2,979,381** → 改后 **2,979,381**（`rows_only_old=0`、`rows_only_new=0`）；标的 2,620 → 2,620 |
| 逐值对拍 | 共同行 **2,979,381**；`qfq` 改动 **209,024** 行、`hfq` 改动 **118,480** 行、合计 **243,731** 行 / **241** 只；其中实质改动（相对差 > 1e-12）**240,377** 行 / **227** 只，其余 3,354 行为末位浮点噪声（最大相对差 4.44e-16） |
| 受影响文件 SHA256 | `factors/fund_adj_factor.parquet`：`89CA0CB2…372B17`（1,220,996 B）→ `089C002D…D1FD9415`（1,421,338 B）；`quotes/fund/**` 共 **241** 个文件被回刷复权列 |
| 抽样断言 | `150022.SZ` 9 个台阶全部等于事件 `share_ratio`（1.0091 / 1.0556 / 1.065 / 1.0071 / 1.1141 / 1.0567 / 1.0467 / 1.045 / 1.0372）；`quote_consistency`（fund）2,620 文件 / 2,979,393 行 **mismatch 0**；`verify_reversibility` 5 只 / 14,903 行最大相对误差 **2.07e-16** |

### 2.5 DG-31 / DG-34（空值与等价口径）

| 指标 | 改前 | 改后 |
|---|---|---|
| `qfq_factor` 空值 | 0 | **0** |
| `hfq_factor` 空值 | 0 | **0** |
| 既有非空值被改动（`qfq`） | — | **209,024**（全部为事件链重算所致，逐只可定位到事件日） |
| 既有非空值被改动（`hfq`） | — | **118,480** |

表内无 `built_at` / `fetched_at` / `source_mtime_ns` 等运行时字段，等价性按
`(ts_code, trade_date)` 主键 join 逐值比对，不以 SHA 论证内容等价（DG-34）。

### 2.6 DG-33（旁路改写同轮回写索引与 manifest）

替换与回刷均走 `write_parquet_atomic` + `refresh_quote_factor_columns` → `flush_meta()`：

- `meta/daily_index.parquet`：241 只基金的 `rows` / `file_size` / `built_at` 已刷新，
  逐行 `file_size` 与磁盘 **241/241 一致**；`meta/daily_index.manifest.json` 校验 PASS（2 条记录）。
- `factors/manifest.json` 校验 PASS（11 条记录，不一致 0）。
- `factors/fund_meta.json` 增加 `rebuild_reason` / `rebuilt_at` / `factor_rows` / `quote_factor_refresh` / `g120_hold`。
- 逐文件侧车与 `quotes/manifest.json`：`refresh_quote_factor_columns` 重写 241 个行情文件后，
  G-106 维护的 `quotes/**/<code>.manifest.json` 由代码链不写，`manifest_tool.py --check --only quotes`
  实测 FAIL（不一致 **483** 条 = 241 份侧车 × `bytes`/`sha256` + 1 条汇总 `bytes`；既有侧车原本一致）。
  已补 `fix_sidecars` 步骤同轮回写：241 份侧车 `bytes` / `sha256` / `generated_at`；
  目录级汇总按磁盘实测重算 `bytes`（**898,279,237 → 898,240,072**，全部差额来自 fund 分项
  104,137,768 → 104,098,603，stock / index / futures 三种与磁盘本就一致、改后数值不变），
  `universe.*.markets_detail` 分项 bytes 与 `dataset` 文案里的总字节同步更新；
  `files` / `rows` / `codes_hash` 未动（只重写复权列，行数不变）。原侧车与汇总副本
  `copy2` 到 `_backup/quotes_manifest_20261113/`（242 份）保留回滚。
  重跑 `--check --only quotes` → **PASS，manifest 10,786 份 / 文件记录 21,569 条 / 不一致 0**。

### 2.7 DG-23 / DG-30（消费方实际读路径）

- `LocalFactorStore.factors('150022.SZ')` etc.(消费方读入口)：`ratio_spread` = 0.0，
  `502003.SH` / `510300.SH` 为 2.22e-16（末位）。
- `LocalQuoteStore.read_file('fund', code)` 内嵌复权列与因子快照全量一致（2,620 只 / 2,979,393 行，mismatch 0）。
- 行情 OHLCV 未动：以 `G:\git\python-quant\data\local_store\quotes\fund\**`（改前镜像）为对照，
  241 个被回刷文件在共同日期上 `open/high/low/close/volume/amount` **差异 0 行**。

### 2.8 与 G-120 的口径隔离

全量重算会为 6 只 G-120 标的补出 `2026-11-10` / `2026-11-11` 共 12 行（事件隐含口径）。
G-120 已明确「整只挂起、不混口径、保留 11-09 末行」，故 `lock_hold` 步骤把这 12 行剔出再落盘：
6 只末行保持 `2026-11-09`，`quotes/fund` 对应两日复权列按既有 `join_asof` 规则回落到 11-09 因子值。
剔除后行数与改前完全相等（2,979,381），其余非空值不受影响。

## 3. 回滚

```powershell
Copy-Item _backup\fund_adj_factor_20261113\fund_adj_factor.parquet data\local_store\factors\fund_adj_factor.parquet -Force
.venv\Scripts\python.exe -c "from src.data.local_store.store import *; ..."  # 或直接重跑 build/apply（备份目录已存在时 apply 会拒绝覆盖）
```

行情副本回滚须重跑 `refresh_quote_factor_columns(kind='fund', codes=<241 只>)`，走同一 `flush_meta` 回写索引。

## 4. 未解决 / 遗留

- G-119（`000667.SZ` 送转比例 unresolved）、G-120（6 只 2 日缺口）仍为未收口，本轮未触碰。
- 本轮未改事件表；若事件表后续再变，仍须全量重算（增量补齐会再次制造同类不一致，属待改造点，不在本轮范围）。
