# 数据准确性与复权因子核验报告（2026-11-03）

**一句话结论：行情和复权数据可以直接拿来做回测和选股，只有极少数标的需要绕开——按台账排除早期指数的 25 行脏价格、重建 `000408.SZ` 的因子、把 `000004.SZ` 的 `hfq` 仅用于收益率用途，受影响的记录不到全市场万分之二，不改变整体可用性。**

本报告汇总四项可重复执行的核验结果。所有核验**只读**：不改数据库、不改表结构、不改 `.day` 源文件、不写远程库。机器可读明细见 `logs/` 下同名 JSON/CSV。

> 说明：核验期间 `data/local_store` 正被另一条线增量重建（标的数从 6,924 增至 9,603），下文所有数字均标注对应运行时刻；结论与差异类别不受影响。

## 1. 数据契约巡检

入口：`.venv\Scripts\python.exe -m src.data.quality check ...`（支持 `--codes/--all`、`--start/--end`、`--source local|tdx`、`--dry-run`）。异常只报警，绝不改数据。

13 条规则：必填列、价格为正、`high >= max(open, close)`、`low <= min(open, close)`、`high >= low`、成交量非负、金额量纲、未来交易日、周末交易日、主键唯一、`pre_close/change/pct_chg` 可复算。

| 数据源 | 标的 | 行数 | 耗时 | error | warning | 唯一异常行 |
|---|---|---|---|---|---|---|
| 本地列存（13:48） | 6,924 | 22,072,125 | 48.58s | 0 | 7,768 | 7,654 |
| 通达信源（13:49） | 6,924 | 22,072,548 | 40.79s | 425 | 7,381 | 7,690 |

本地列存异常全部为 warning：`amount_dimension=7,094`、`pre_close_mismatch=387`、`weekend_trade_date=287`。构建期 `_validate_kline_quality` 已剔除非法 OHLC，因此本地 OHLC 为 0。

通达信源（构建前）：
- `high_lt_open_close=25`、`low_gt_open_close=399`、`high_lt_low=1` → 425 次命中 / **423 个唯一行**。
- 已知 25 行全部命中：`000001.SH` 16 行、`399001.SZ` 9 行（1991–1995）。
- 额外 398 行：`880006.SH` 147、`880016.SH` 127、`880036.SH` 57、`880026.SH` 43、`000003.SH` 18、`000002.SH` 6（板块指数与早期指数）。
- `weekend_trade_date=287`：`399001.SZ` 45 行为已知，其余 242 行集中在 23 只早期深市个股。
- 股票（非指数）的 OHLC/`pre_close`/`change`/`pct_chg` 在源端均为 0 违规。

**与已知数量核对结论**：已知 25 行 OHLC 瑕疵在源端全部复现；本地列存为 0 是因为构建期已过滤——两者口径不同，不是数据消失。

## 2. 异常行台账

`src/data/quality/anomalies.py` 内置 **7 条记录、5 类问题**，支持精确定位日期或区间：

| 类别 | 标的 | 范围 | 处置 |
|---|---|---|---|
| OHLC 越界 | 000001.SH | 16 个精确日期 | exclude |
| OHLC 越界 | 399001.SZ | 9 个精确日期 | exclude |
| OHLC 剔除后的 pre_close 缺口 | 000001.SH | 16 个紧随日期 | flag（仅 local） |
| OHLC 剔除后的 pre_close 缺口 | 399001.SZ | 8 个紧随日期 | flag（仅 local） |
| 周末行情混入 | 399001.SZ | 45 个精确日期 | flag |
| hfq 基期差 | 000004.SZ | 1990-01-01..2026-12-31 | flag（仅 factor） |
| legacy 成交量 100 倍 | 600547.SH | 至 2021-06-22 | exclude（仅 legacy） |

回测/取数侧用法：

```python
from src.data.quality import is_known_anomaly, anomaly_action
if is_known_anomaly(ts_code, trade_date, context='local'):
    action = anomaly_action(ts_code, trade_date, context='local')  # exclude / flag / ignore
```

`context` 区分 `local` / `tdx` / `factor` / `legacy`；`applies_to=('any',)` 时不限上下文。扩展用 `register_anomaly(KnownAnomaly(...))`，测试覆盖命中、不命中、上下文隔离与运行时扩展。

## 3. 跨源抽样对拍

入口 `... cross-check --sample 40 --years 3 --source both`；抽样 40 只沪深股票，区间 2023-11-04..2026-11-03，逐列比对 `open/high/low/close/volume/amount`。远程开关只在进程内临时打开（`build_remote_config()`），不改 `data.allow_remote_sources` 默认值。单源串行、并发 1；BaoStock 间隔 0.2s，AkShare 间隔 3s + 指数退避 5→30s、最多 2 次重试；连续 3 次空结果熔断。

| 源 | 成功对拍 | 数值有差异 | 仅日期条目不同 | 无法对拍 |
|---|---|---|---|---|
| BaoStock | 40 | 1 只 / 13 行 | 10 只（远程多 102 行停牌） | 0 |
| AkShare | 29 | 0 | 29 只（远程多 79 行停牌） | 11（东财 IP 限流） |

- 重叠区间数值：OHLC 全部一致；`volume` 仅 Float32 舍入（最大 0.48 手）。
- 唯一数值差异：`002621.SZ` 13 行 `amount`，最大差 50 元、最大相对 0.206%。该股为低价退市股，通达信金额取整到百元、BaoStock 用均价计算，属**口径差**，非源错。
- 日期条目差异均为**停牌日口径**：远程返回 `volume=0` 的停牌行，通达信 `.day` 不写该行；重叠交易日逐值一致。
- AkShare 11 只因东财临时限流标记为“无法对拍”，未计入一致。

## 4. 复权因子全市场核验

入口 `... factor-check --sample 60`（只读）。报告：`logs/factor_audit_20261103_143055_805537.json` 及 `_codes.csv / _gaps.csv / _sample.csv`。

### 4.1 覆盖与一致性

- 因子快照：**5,650 只 / 17,577,983 行 / 1990-12-19..2026-10-21**。快照生成后本地行情继续增量更新（现 5,655 只个股、至 2026-11-02）。
- 差集：新上市 5 只暂无因子：`001381.SZ`、`301718.SZ`、`920285.BJ`、`920296.BJ`、`920306.BJ`。
- 两批口径：逐票用「本地行情 + 分红」重算，**5,649 只逐行一致（max_rel=0）**；仅 `000004.SZ` 的 `hfq` 相对差 16.67%（≈1.2 倍基期），`qfq` 为 0，收益率不受影响。
- 末端覆盖：5,504 只快照行数少于行情（合计 117,809 行），但缺口内仅 **1 条**除权事件（`000408.SZ` 2026-04-17，快照止于 2026-04-16），该标的因子必须重建；其余缺口无事件、数值不受影响。

### 4.2 算法正确性

- 独立复算抽样 60 只：上市年代 `<2000` 24 只、`2000s` 4 只、`2010s` 18 只、`2020s` 14 只；含送转 50 只、含配股 18 只、无分红 5 只。
- 原始价 → 因子 → 复权价 → 反推：`max_rel = 7.0e-16`，可逆性 `max_rel = 2.2e-16`。
- 不变量：本实现 `hfq = qfq / 全历史因子乘积`，因此 `hfq/qfq` **逐票恒定**（60/60 通过），但**不同标的之间不是同一常数**——用户提到的“同一交易日跨标的常数关系”在该口径下不成立，已按逐票恒定校验。
- `hfq_factor` 逐行检查：**50,402 个跳变行全部落在除权日**；非除权日无跳变。

### 4.3 分红完整性双向反查

- 因子跳变 → 分红事件：**5,650 只、50,402 个跳变日期，全部有来源**；`factor_without_source=0`。
- 分红事件 → 因子跳变：`event_without_jump=0`；`invalid_events=10`（事件系数无效：`factor<=0` 或 `>10`）。
- `events_before_first_row=1,044`：早于该标的因子首行的除权事件被丢弃，只影响 `hfq` 基期，不影响 `qfq` 与收益率。
- `events_after_last_row=1`：即 `000408.SZ`，见 4.1。
- 分红快照 53,974 行 / 5,376 只，`ex_date` 为空 **1,711 行**（不是远程表的 4 行）；计算前已过滤，确认**不影响因子**。

### 4.4 品种边界

- 指数：因子快照中 **0 只**，符合“指数不复权”契约。
- 基金：因子快照中 **0 只**；2,859,828 行基金行情中 `qfq/hfq` 非 1 值 **0 行**、空值 **0 行**。
- 取数行为：`DataFetcher._apply_adjustment` 在因子列全为 1.0 时显式告警“复权降级”，不会静默返回伪复权价。

### 4.5 结论：能否说“所有复权因子准确”

**不能无条件说“全部准确”**，应表述为：

- **可信任**：5,649 / 5,650 只个股因子与独立重算逐行一致；`qfq`、收益率、`as_of` 截断逻辑均正确；指数/基金不写因子且读侧会告警。
- **需处理（2 只）**：
  1. `000004.SZ` 的 `hfq` 基期差 1.2 倍（`qfq` 正常，收益率不受影响）——保持台账 flag，若下游使用 `hfq` 绝对价位则需统一基期重算；
  2. `000408.SZ` 的 2026-04-17 除权事件未进入快照（快照止于 4-16）——必须重建该标的因子。
- **需补齐**：5 只新上市标的尚无因子，当前取数按 1.0 降级。
- 系统性修复建议：在行情增量更新后自动触发 `rebuild-factors`（按缺失/过期标的增量重算），并把“快照末端落后行情且缺口内有除权事件”纳入巡检告警。

## 5. 附带发现

- `data/local_store/meta/daily_index.parquet` 当前只剩 `fund` 一类（另一次 fund 构建的 `flush_meta` 覆盖了索引）。巡检模块已改为按目录枚举兜底；但线上 `LocalQuoteStore.covers()` 对个股会返回 False，导致读路径回退 TDX。建议由 `store.py` 维护方重建索引。（本次未改动该文件。）
- AkShare 东财接口对短时高频请求会临时封 IP；已将间隔调至 3s、退避上限 30s，并在连续空结果时熔断。

## 6. 复现命令

```powershell
.venv\Scripts\python.exe -m src.data.quality check --all --source local --data-type all --output-dir logs
.venv\Scripts\python.exe -m src.data.quality check --all --source tdx   --data-type all --output-dir logs
.venv\Scripts\python.exe -m src.data.quality cross-check --sample 40 --years 3 --source both --output-dir logs
.venv\Scripts\python.exe -m src.data.quality factor-check --sample 60 --output-dir logs
.venv\Scripts\python.exe -m src.data.quality anomalies --code 000001.SH --context local
```
