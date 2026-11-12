# G-067 第二段：历史逐码候选清单补跑对账

- 基线 run：`reports\advice\stateful\20190131_20260803_h20`（2026-11-11 19:42，mid / month-end / hold-days=20）
- 新 run：`reports\advice\stateful\20190131_20260803_g067rechunk2`（本次，参数同基线，仅换了输出目录后缀）
- 逐码清单：`2026-11-12\g067-rechunk2\outputs\g067_candidates_rechunk.csv`（732 行，SHA256 `393EEAF99E82354567FB3C5F9B17D6483539F7AA1C35FC9A85625EFE6EE5A990`）
- 长效逐码明细：`docs\evidence\g067_candidates_20261112\g067_candidates_detail.csv`（732 行，SHA256 `27B404347884D23C2B03B005D0FA3A7218C8A518B28626C3F1C378D7B020FED3`）
- 断点文件：`_staging/g067_rechunk2_20261112/progress.csv`、`_staging/g067_rechunk2_20261112/failed.csv`（采集与对账全程只读 `data/local_store`，断点保持不动）
- 输入指纹：`data/local_store` 全树 15925 个文件，跑批前后 新增 5831 / 删除 0 / 改动 6（非零，逐项见 §8）

## 1. 三组对账数字

| 项 | 值 |
|---|---:|
| 基线候选数（`h20/summary.json` funnel.candidate_signals） | 731 |
| 基线 8 片 `chunk_state.json` 的 candidate_signals 之和 | 731 |
| 新 run 实际落盘候选行数 | 732 |
| 新 run `funnel.candidate_signals` | 732 |
| 新 run 8 片 `chunk_state.json` 的 candidate_signals 之和 | 732 |
| 原 575 缺口（731 − 156 笔成交）补齐后无依据候选 | 575 |
| 新清单与基线的总数差（新 − 731） | +1 |
| (trade_date, ts_code) 重复行 | 0 |

## 2. 逐片候选数

| 片 | 基线 signals | 新 signals | 差 | 新行数 | 决策日数 | 行数=计数 | 基线意图 | 基线成交 | candidates.csv SHA256 |
|---|---:|---:|---:|---:|---:|:--:|---:|---:|---|
| 2019 | 83 | 83 | +0 | 83 | 4 | 是 | 39 | 13 | `326D91C8AEA98DEB…` |
| 2020 | 22 | 23 | +1 | 23 | 4 | 是 | 19 | 18 | `2C5B80CCB66C434A…` |
| 2021 | 19 | 19 | +0 | 19 | 3 | 是 | 19 | 7 | `8BEFFE08250A014D…` |
| 2022 | 71 | 70 | -1 | 70 | 6 | 是 | 46 | 34 | `E13EA8EF9EEBD47A…` |
| 2023 | 71 | 72 | +1 | 72 | 5 | 是 | 28 | 16 | `CA74A99FCD902050…` |
| 2024 | 202 | 202 | +0 | 202 | 4 | 是 | 41 | 21 | `CC3EE0A57DEE85E4…` |
| 2025 | 189 | 189 | +0 | 189 | 8 | 是 | 46 | 30 | `5B892BEA8D7C83C3…` |
| 2026 | 74 | 74 | +0 | 74 | 4 | 是 | 19 | 17 | `FCDF5C2D1BFF3B5D…` |
| **合计** | **731** | **732** | **+1** | **732** | — | — | **257** | **156** | — |

## 3. 四组复核（日历 / 面板日 / 选池 / 规则计数）

| 片 | dates 旧→新 | panel_dates 旧→新 | universe.kept 旧→新 | rule_counts |
|---|---|---|---|---|
| 2019 | 223→223 | 667→667 | 2993→2993 | 无差异 |
| 2020 | 243→243 | 684→684 | 3199→3199 | 无差异 |
| 2021 | 243→243 | 682→682 | 3408→3408 | 无差异 |
| 2022 | 242→242 | 682→682 | 4043→4043 | 无差异 |
| 2023 | 242→242 | 682→682 | 4359→4359 | 无差异 |
| 2024 | 242→242 | 682→682 | 4785→4785 | 无差异 |
| 2025 | 243→243 | 681→681 | 4894→4894 | 无差异 |
| 2026 | 140→140 | 577→577 | 4930→4930 | 无差异 |

- 四组汇总：8 片全部相同

## 4. 逐码 diff（历史真实被消费过的候选 ⊆ 新清单）

- 历史侧可核对的逐码集合：成交买入 156 对 + 换手上限未成交 99 对，去重后 255 对
- 未出现在新清单中的对数：**1**

| 缺失 trade_date | 缺失 ts_code | 历史状态 |
|---|---|---|
| 2022-02-28 | 601619.SH | filled |

## 5. 三处逐片差异归因（2020 +1 / 2022 −1 / 2023 +1）

净差 +1 不能当作「只差一笔」；下表逐处给出片 / 标的 / 日期 / 边界值 / 成因。
三只标的自身的 quote 分片 mtime 均在基线 run（2026-11-11 19:42）之前且本轮全程未再改写，因此成因均为「数据未变、判定变」，驱动量是同日横截面的 rank 分位轻微推移。

| 片 | 标的 | 日期 | 差 | 方向 | 本轮分位 | 判定 | 成因 |
|---|---|---|---:|---|---:|---|---|
| 2020 | 600853.SH | 2020-02-28 | +1 | 从无变有 | 0.90025 | 数据未变、判定变 | 同标的自身行情零改动；分位是当日横截面 rank，11 只有逐值差异的标的改变了同日截面，边际名次微移 |
| 2022 | 601619.SH | 2022-02-28 | -1 | 从有变无 | 0.899505 | 数据未变、判定变 | 同标的自身行情零改动；分位是当日横截面 rank，11 只有逐值差异的标的改变了同日截面，边际名次微移 |
| 2023 | 600293.SH | 2023-06-30 | +1 | 从无变有 | 0.900092 | 数据未变、判定变 | 同标的自身行情零改动；分位是当日横截面 rank，11 只有逐值差异的标的改变了同日截面，边际名次微移 |

- 基线侧三只标的的当日分位**未落盘**（`h20` 只留计数、`unfilled_records` 仅覆盖换手上限未成交），因此不存在「基线分位 X → 本轮分位 Y」的逐值对；能给出的只有本轮分位，以及下述「当日候选池最低分位」界。
- 2022 −1 已关掉（见 §5.1）：`601619.SH @ 2022-02-28` 复跑后实测 score=0.408787、分位=0.899505，判定为**分位跌破 0.90**，软筛与可推荐性均通过。
- §4 里唯一缺失的历史消费对 `601619.SH @ 2022-02-28`，与本处 2022 −1 是同一件事，不是两笔。
- 2020-02-28（2020）本轮候选池 1 只，最高 600853.SH 0.90025，最低 600853.SH 0.90025；600853.SH 在池内，分位 0.90025。
- 2022-02-28（2022）本轮候选池 4 只，最高 000751.SZ 0.922772，最低 600107.SH 0.901485；601619.SH 不在池内，诊断复跑分位 0.899505，`pct_ok=false`。
- 2023-06-30（2023）本轮候选池 1 只，最高 600293.SH 0.900092，最低 600293.SH 0.900092；600293.SH 在池内，分位 0.900092。

其余 5 片（2019 / 2021 / 2024 / 2025 / 2026）逐片计数与基线相同，但「相同」的层级不同，必须分开说：

| 片 | 候选层 | trades 层 | 比对方式 |
|---|---|---|---|
| 2019 | 计数相同，逐码无法比（基线无清单） | 26 行**逐行相同、0 字段差** | 按 (signal_date, ts_code, side) 配对后逐字段比对 |
| 2021 | 计数相同，逐码无法比 | 行集合相同，shares/notional/cost 有差（36 处）、price 全同 | 同上 |
| 2024 | 计数相同，逐码无法比 | 行集合相同，shares/notional/cost 有差（84 处）、price 全同 | 同上 |
| 2025 | 计数相同，逐码无法比 | 行集合相同，shares/notional/cost 有差（207 处）、price 全同 | 同上 |
| 2026 | 计数相同，逐码无法比 | 行集合相同，shares/notional/cost 有差（111 处）、price 全同 | 同上 |

- 2020 / 2022 / 2023 的 trades 差异即 §5 那三处候选进出的直接后果：2020 多 2 行（600853 买/卖）、2022 少 2 行（601619 买/卖）、2023 多 2 行（600293 买/卖）。
- 2021 及以后的 shares/notional/cost 差不是独立事件：2020 新增的 600853 交易改变了此后各片起始现金与权益，`target_weight × equity` 随权益等比变化，price 全部相同即证明成交价未受影响。

### 5.1 2022 −1 收口：`601619.SH @ 2022-02-28` 单片诊断复跑

只重跑 2022 一片（输入片 2021 的 `chunk_state.json` 从 8 片版原样复制作种子，其余 7 片零改动），输出换后缀 `_g067rechunk2sc`，并加 `--dump-eval` 落盘决策日全截面门槛判定；断点落 `_staging\g067_rechunk2sc_20261112/progress.csv`、`failed.csv`。

| 项 | 值 |
|---|---|
| 复跑候选行数 / signals | 70 / 70 |
| 与 8 片版 2022 片逐行比对（日期+代码+分位） | 逐行相同 |
| `candidates.csv` | `reports\advice\stateful\20190131_20260803_g067rechunk2sc\chunks\2022\candidates.csv` |
| `signal_eval.csv` | `reports\advice\stateful\20190131_20260803_g067rechunk2sc\chunks\2022\signal_eval.csv`（4,243,275 B，SHA256 `FA2A3233440023A46057EA66D4D6C142…`） |

| 字段 | 值 |
|---|---|
| `trade_date` | 2022-02-28 |
| `ts_code` | 601619.SH |
| `score`（原始打分） | **0.408787** |
| `score_percentile`（当日横截面分位） | **0.899505** |
| `pct_ok`（≥0.90） | false |
| `soft_pass` | true |
| `eligibility_ok` | true |
| `entry_ok` | false |
| `reject_reason` | **pct_below_threshold** |

- 判定：**分位跌破阈值**，不是软筛剔除。分位 0.899505 距 0.90 仅差 0.000495，属同日横截面排名微移的典型边际效应；其自身 quote 分片全程未改写。
- 同日被选中的 4 只（分位）：000751.SZ 0.922772、003003.SZ 0.914604、600107.SH 0.901485、600846.SH 0.92104。
- 同日 4,040 只参与判定，`pct_below_threshold` 3,635 只、`soft_filter` 399 只、`eligibility` 2 只、选中 4 只（口径自 `signal_eval.csv` 逐行统计）。

## 6. `000536.SZ @ 2023-07-31` 换手上限拦截变化归因（12→11）

先说口径：这里的 `blocked_turnover_cap` **不是个股换手率因子**，而是组合级换手预算裁剪（`src/advice/turnover.py:136-162`）：`budget = 2 × max_turnover × equity`，卖出优先、买入按分位从高到低占用预算，放不下的买入意图计入 `blocked_turnover_cap` 并写入 `unfilled_records`。本项目 `max_turnover_per_rebalance=0.20`（`src/advice/config.py:76`）。

| 项 | 基线 | 本轮 |
|---|---:|---:|
| 决策日 `2023-07-31` 权益 equity | 857,407.04 | 882,157.15 |
| 持仓数（`holdings_count`） | 0 | 1 |
| 空余槽位（max_positions=15） | 15 | 14 |
| 单笔目标额 `target_weight × equity`（0.066667） | 57,160.47 | 58,810.48 |
| 换手预算 `2 × 0.20 × equity` | 342,962.82 | 352,862.86 |
| 当日被换手上限拦下的买入意图数 | 9 | 8 |
| 被拦金额合计 | 514,444.22 | 470,483.81 |
| 被拦清单最低分位标的 | 000536.SZ 0.930243 | 601718.SH 0.931620 |

- `000536.SZ` 当日分位：基线 `0.9302432308`（取自基线 `unfilled_records`），本轮候选清单 `0.930243` —— 两轮**同值**，说明它的分位没有变。
- 变化的是它能排进意图列表的位次：本轮 2023-07-03 买入 `600293.SH` 后，`2023-07-31` 持仓数 0→1，空余槽位 15→14；`_plan_buys` （`tools/advice_stateful_backtest.py:613-639`）只按分位从高到低取满空余槽位，最低分位的那只（`000536.SZ` 0.930243 < `601718.SH` 0.931620）被挤出意图列表，于是它既不在 `trades` 里、也不在 `unfilled_records` 里，`blocked_turnover_cap` 相应 12→11。
- 判定：这不是 `factors/stock_turnover` 重建的传导——该因子文件在本工具中从未被读取（`grep -n "stock_turnover" tools/advice_stateful_backtest.py src/advice` 零命中，`turnover` 全部指组合换手）。真正的成因是 2023-06-30 新增候选 `600293.SH` 经持仓链把 2023-07-31 的空余槽位从 15 压到 14，属槽位竞争；**不是**换手因子取值变化，也不是拦截口径被放宽；当日两轮行情本身零改动。

## 7. `hfq_factor` 重算与因子层重建 → 同日横截面排名推移（下游影响证据）

本节写给正在做独立取证的那一路：本 run 的三处候选进出，是该改动的下游表现，不是独立的回测口径变化。

- 跑批窗口内 `quotes/stock/**` 共 78 个分片被改写，其中 **11 个**在 2019-01-01 之后存在逐值差异：`600188.SH`、`600519.SH`、`600719.SH`、`600938.SH`、`600989.SH`、`000016.SZ`、`000409.SZ`、`000550.SZ`、`000953.SZ`、`002326.SZ`、`002352.SZ`。
- 差异列以 `hfq_factor` 为主（每只约 900～1,900 行），其中 8 只来源为 `_backup/quotes_stock_20261111`（2026-11-11 21:46，即基线 run 之后 2 小时）；`600989.SH` / `000953.SZ` 来源 `_backup/quotes_adj_recalc_20261110_002529`，`000016.SZ` 来源 `_backup/quotes_sz_1993b_20261113`。
- 机制：`score_percentile` 是**当日横截面 rank**。上述标的的复权价序列变化会改变它们自身的动量/波动类打分，进而使同日全部标的的分位发生微小推移；恰好压在 0.90 门槛附近的标的因此边际进出。本轮三处进出点全部贴在阈值上（0.900250 / <0.90 / 0.900092），与该机制一致。
- 逐文件明细（来源、改写时间、行数、差异列与差异行数）见 `2026-11-12/g067-rechunk2/outputs/g067_data_timeline.md`。

## 8. 非读路径例外附注（6 个改动件 + 5,831 个新增 manifest）

指纹比对覆盖 `data/local_store` 全树 (size, mtime_ns)。以下差异全部不在 `advice_stateful_backtest` 的取值链路内，故不构成本轮候选数值的输入变更；逐项列明以便复核。

### 8.1 改动的 6 个文件

| 路径 | 字节 旧→新 | 改写时间 | 为什么不进取值链路 |
|---|---:|---|---|
| `data/local_store/meta/anomalies/manifest.json` | 13,531→14,105 | 2026-11-12 21:41:51 | 同目录清单文件（元数据），无任何代码读取 |
| `data/local_store/meta/anomalies/placeholder_quote_codes.csv` | 11,534→21,424 | 2026-11-12 21:41:11 | 占位行情代码清单，仅数据质量标记；回测只读 quotes/factors |
| `data/local_store/meta/state/manifest.json` | 29,484→31,043 | 2026-11-12 21:48:06 | 同目录清单文件（元数据），无任何代码读取 |
| `data/local_store/meta/state/state_vector_daily.parquet` | 738,063→738,059 | 2026-11-12 21:48:05 | 状态向量派生表，`advice_stateful_backtest.py` 未导入、未读取 |
| `data/local_store/quotes/manifest.json` | 9,329→10,061 | 2026-11-12 21:46:26 | quotes 层清单文件（元数据），无任何代码读取 |
| `data/local_store/quotes/stock/sz/000001.SZ.manifest.json` | 15,424→16,267 | 2026-11-12 21:54:54 | 单只标的的侧车清单（元数据），无任何代码读取 |

### 8.2 新增的 5,831 个 sidecar manifest

全部为 `quotes/stock/<market>/<code>.manifest.json` 侧车文件，不进入任何取数/回测/展示链路：

| 目录 | 新增数 |
|---|---:|
| `data\local_store\quotes\stock\bj` | 357 |
| `data\local_store\quotes\stock\sh` | 2,420 |
| `data\local_store\quotes\stock\sz` | 3,054 |
| **合计** | **5,831** |

- 依据：`src/` 全目录没有任何 `*.manifest.json` 读取；这些文件由另一路侧车回填任务写入，属元数据，既不改 `quotes/stock/**/*.parquet`，也不改 `factors/**`。
- 判定：本轮 8 片产物有效（主控裁决 A），不整轮重跑。

## 9. 逐码清单与后续对账口径（G-067 病根治理）

- 长效明细：`docs\evidence\g067_candidates_20261112\g067_candidates_detail.csv`，732 行，SHA256 `27B404347884D23C2B03B005D0FA3A7218C8A518B28626C3F1C378D7B020FED3`。
- 列含义：`score` 为原始打分（仅 2022 段有值，该片用改后工具复跑；其余 7 片在改工具前跑完，留空不补造）；`score_percentile` 为当日横截面分位；`buy_percentile_threshold=0.9`；`above_threshold` 全部为 yes（进清单即已过阈）；`intent_status` ∈ filled / intent_unfilled / candidate_only；`blocked_reason` 仅对 `intent_unfilled` 有值。
- 状态计数：candidate_only 477、filled 157、intent_unfilled 98
- 每片 `candidates.csv` 的 SHA256 见 §2 末列，可按片独立核对。
- **此后对账一律逐行比，不再比总数**：基线 `h20` 没有逐码清单，是 G-067 反复出现的根因；自本清单起，任何新 run 都必须与`docs/evidence/g067_candidates_20261112/g067_candidates_detail.csv` 按 (trade_date, ts_code) 逐行对齐，计数差只能作为线索，不能作为结论。

## 10. 候选清单落盘字段缺 `score`：已在本轮补齐（G-067 病根）

- 缺陷：`candidates.csv` 原先只有 `trade_date / ts_code / score_percentile / reason`，没有原始 `score`，也没有阈值判据；跨 run 只能比分位与总数，无法复算「为何刚好跨过 0.90」，这就是 G-067 反复回到比总数的根因。
- 改动工具：`tools/advice_stateful_backtest.py`。
- 新增列：`CANDIDATE_COLUMNS` → `(trade_date, ts_code, score, score_percentile, reason)`，`score` 为原始打分（6 位小数，缺失写空）；另新增诊断产物 `signal_eval.csv`（`EVAL_COLUMNS` 10 列：`trade_date, ts_code, score, score_percentile, pct_ok, soft_pass, eligibility_ok, entry_ok, selected, reject_reason`），由新参数 `--dump-eval` 控制，不进撮合链路。
- 兼容性：`_read_chunk_candidates` / `_write_csv` 均按列名取值，旧 CSV 缺 `score` 列不会报错；`_merge_chunks` 只用 `candidates.csv` 核对行数、不写合并候选表，多一列不影响合并；旧 run 的 8 片产物无需重跑。新列只在下一轮写盘时出现。
- 本轮验收：2022 单片复跑 70 行与 8 片版 2022 片逐行相同，`score` 非空 70/70；`docs/evidence/g067_candidates_20261112/g067_candidates_detail.csv` 已加 `score` 列，2022 段有值、其余 7 片留空（那些片在改工具前跑完，按设计无 score，不补造数值）。
- 建议下一步（未实施）：把 `buy_percentile_threshold` 与当日截面样本数写进 `stateful.stats`，这样即使只看 `chunk_state.json` 也能复算阈值判据。

## 11. 结论与限制

- 历史 run 从未落过逐码清单（8 片 `chunk_state.json` 的 `stateful` 无 `candidates` 键、`trades.csv` 仅含成交），因此「731 个候选」只有计数、没有逐码基准；本次逐码对拍的对象是「当时确实进入撮合链路的候选」（成交 + 未成交意图），这部分是历史侧唯一存在的逐码证据，缺失对数见 §4。
- 新 run 与基线差 +1（732 vs 731），按实测报差值，不改基数、不改验收口径。
- provisional 那一轮（`..._g067rechunk`）跑在派生层搬移窗口内，已作废，文件仍保留作对照，不参与任何结论。
- 采集与对账全程未写 `data/local_store`（指纹以外的 6 处改动均来自其它任务，见 §8），未改 `_staging/g067_rechunk2_20261112/` 断点文件。
