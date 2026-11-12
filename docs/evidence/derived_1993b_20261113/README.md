# 派生表重建留证（1993-05 B 模板处置后）

- 任务口径：不联网、不改公式、不换口径、不放宽断言；`data/` 下只移不删（整体移动到 `_backup/`）。
- 时间：2026-11-12 20:29 ~ 21:44（layer 0~6 与第 9 步验收）。
- 本目录为**只读留证**：从 `_staging/derived_1993b_20261113/` 复制的三份原始证据未被修改，原件仍留在该 `_staging` 目录（该目录不进 Git，仅本地镜像）。
- 本流程**不含 `refresh_anomaly_lists`**：`meta/anomalies/placeholder_quote_codes.csv` 全程未读未写。

## 目录内容

| 文件 | 说明 |
|---|---|
| `验收报告.md` | 完整验收报告（10 节：结论、盘点、12 类不重建理由、传播链逐项对照、复现命令） |
| `layer6_turnover.json` | `factors/stock_turnover.parquet` 删 34 行前后的 rows / null 计数 / SHA256 / bytes / manifest totals / 34 行逐条清单 |
| `before.json` | 删除前备份额（`_backup/factors_turnover_1993b_20261113/before.json` 的副本）：源路径、SHA256、bytes、rows、copy 时间 |
| `README.md` | 本文件：13 张表 + 6 层 universe 对照表与三段传播链实测 |

## 1. 13 张表前后对照

「改动行数」= 与备份逐键 join 后至少一列发生变化的行数（`ne_missing` 空值安全比较）；「非 17 只改动」= 目标 17 只以外标的的值改动行数；「空值增量」= 逐列 null 计数之和的新旧差。

| # | 文件 | 旧行数 | 新行数 | 改动行数 | 非 17 只改动 | 空值增量 | 旧 SHA256 | 新 SHA256 |
|---|---|---|---|---|---|---|---|---|
| 1 | `factors/stock_adj_factor.parquet` | 18,129,902 | 18,129,870 | 8,189 | 0 | 0 | `b2e872f8a0da1d602bda53ae124d7b5abf07a738a77b03bd065bd7679f41313c` | `0196f654ff28c56497da543460ba66d452940ac03a0468179c33f5df9972ae7c` |
| 2 | `factors/stock_turnover.parquet` | 18,055,784 | 18,055,750 | 0（值未动，仅删 34 行） | 0 | 0 | `39de5003636ef7fdf712e30d8b6ec37dd768b01a1fc2c9731c7c908a10191449` | `eea4bf815de646b769b4d52a18b9b0610ba716154152cf446890cf37b99b3f66` |
| 3 | `meta/suspension/suspension_daily.parquet` | 987,196 | 987,194 | 0 | 0 | 0 | `1a6ff29cdafd7898f500eb0604488dbfff8b73405ed3d9ae6937ecaa5e8f4543` | `72dfdbff199fcf4ac5725bcdc5240f2644baa8b31439bd771e74b9d5f73861e9` |
| 4 | `meta/stock_basic.parquet` | 5,916 | 5,916 | 0 | 0 | 0 | `048335ccd9d81da7880dcd170d851e8b522eabb4d6bf5242019c7efdd31717b8` | `048335ccd9d81da7880dcd170d851e8b522eabb4d6bf5242019c7efdd31717b8`（回退，未采纳重建） |
| 5 | `meta/universe/listing_intervals.parquet` | 9,761 | 9,761 | 16 | 0 | 0 | `4ff2d608f41eb7e9a67ea2224e2395298dd2c3254e94e91b1100969776748f7a` | `ee51db26fc95fe8acd0f117f0d69f28dceea01c99aa9fa6fd006782366f1e726` |
| 6 | `meta/universe/stale_status.parquet` | 9,761 | 9,761 | 0 | 0 | 0 | `c783676db6acc086b59f8ce7649014f12c421699a86b1f630c1512231f67e58c` | `c783676db6acc086b59f8ce7649014f12c421699a86b1f630c1512231f67e58c`（回退） |
| 7 | `meta/universe/universe_price_jumps.parquet` | 9,910 | 9,910 | 0 | 0 | 0 | `adcfb57f245e13d2e76bc932edefae7ed08878ff7ec2169583c9de099a568963` | `adcfb57f245e13d2e76bc932edefae7ed08878ff7ec2169583c9de099a568963`（候选不采纳） |
| 8 | `meta/universe/universe_exclusions.parquet` | 1,474 | 1,474 | 0 | 0 | 0 | `f152841c777c735c8f23c5dfa01ef793363d6fd7a217b5d9b8366f25489d8291` | `f152841c777c735c8f23c5dfa01ef793363d6fd7a217b5d9b8366f25489d8291` |
| 9 | `meta/breadth/breadth_daily.parquet` | 8,826 | 8,824 | 8,166 日期行 | —（全市场日期行） | 0 | `caf175504cb5533a1f34f9ef1634b2d26d488d2769187b0688b0bc52c0357af9` | `322571b2c46f7ab0e392f472e5726b4e9525997727a842b4db3dd8d82fcc60e1` |
| 10 | `meta/breadth/breadth_ma_daily.parquet` | 8,656 | 8,654 | 46 日期行 | —（全市场日期行） | 0 | `2599c28437fa0ddbba1d9333f2851b9f810a8728c0e30f2931db8a89d66dbc1b` | `48ae8ac5bf2ed319f3f1a2cd0ae351fee1e8bc28a6eeea9c23d05b9a528913ea` |
| 11 | `meta/state/state_vector_daily.parquet` | 8,827 | 8,825 | 451 日期行 | —（全市场日期行） | −2（15 列各 −2，删掉的 2 行本身为 null） | `daea7d75436991a4a3913bc2d5a9840cdb0378f1c28e82f4c60d19f426bca4e9` | `02fda62b214d83a5bad1ca779cc4d2a54c074ca571d992f8be3eb2ebaca1e59f` |
| 12 | `quotes/stock/sz/000016.SZ.parquet` | 8,191 | 8,189 | 8,189 单元格（qfq 274 + hfq 7,915） | 0 | 0 | `6c4a4e8228dbeb928ee2762958ff3a61cc07a8726525ec17d8313572c46538c1` | `bc70f666783b4427b81ca4dd8e7e095ce14e17f4ff4227bf4f36ef99d80e675b` |
| 13 | `meta/daily_index.parquet` | 9,787 | 9,787 | （前序任务已刷新，本次未动） | — | 0 | `9ed94acb00a415a33c07664c6bafa2340db7f05d747141153c1b10f999556270` | `6f2e54541c8df6c2b6f2e2977a59cf0946d6a0cf40ec80c564df51ee7e9fae5d` |

行数变化逐表可归因：因子表 −32（17 只各 −2 共 −34，加 000001 新增 05-03/05-06 两行 +2）；换手表 −34（17 只 × 05-01/02 孤儿行，值未改未重算）；停牌表 −2（仅 000001 的 05-03/05-06）；breadth / breadth_ma / state 各 −2（05-01/02 两个非交易日合成行）。

bytes 变化（供复核编码差异）：adj 12,880,456 → 11,113,361（row group 182 → 144）；turnover 147,399,890 → 147,399,167；suspension 1,908,438 → 2,055,685（row group 8 → 12）；listing_intervals 94,802 → 94,803；breadth 135,580 → 135,009；breadth_ma 330,498 → 330,498；state 738,060 → 738,063；000016 234,243 → 234,200；daily_index 143,461 → 143,509。

> 第 1 行旧 SHA 以**备份副本实测** `b2e872f8…` 为准；`_staging/derived_1993b_20261113/progress.csv` 与 `layer1_after_sha.json` 记的 `581ca66a…` 同盘无任何副本，判定为记账不一致（已单列取证，不在此展开）。
>
> 第 11 行新 SHA 为本任务验收时刻的实测值 `02fda62b…`（同一内容副本：`_staging/derived_1993b_20261113/state_vector_daily_candidate.parquet`，bytes 738,063、SHA 恰为 `02fda62b…`）。**2026-11-12 21:43:24 另一路任务 `_staging/state_margin_patch_20261112/` 在该表追加 `lev_margin` 维（dims 5→6）**，此后磁盘文件 SHA 变为 `6a2cc6533f3979599e8029f6a7c1848003f794b6e881e89cc525d443c8aef189`（bytes 仍 738,063、行数仍 8,825），该改动不属本任务，本目录不把它计入本次重建结果。

## 2. 6 层 universe 前后对照

六层行数全部守恒 18,922,964（原位掩码 `when(mask).then(val).otherwise(col)` 替换，不增删行）；非 17 只改动 0、空值增量 0。

| 层 | 旧行数 | 新行数 | 改动行数 | 非 17 只改动 | 空值增量 | 旧 SHA256 | 新 SHA256 |
|---|---|---|---|---|---|---|---|
| l0_all_history | 18,922,964 | 18,922,964 | 0 | 0 | 0 | `f1b1a58ccfc8b2b9c640ab08818d85ff47270c1cbae7401ebc69103180f4178e` | `f1b1a58ccfc8b2b9c640ab08818d85ff47270c1cbae7401ebc69103180f4178e` |
| l1_listed_120d | 18,922,964 | 18,922,964 | 0 | 0 | 0 | `999330f17307ec16873ed4acedd44f4267ae2c03a28e197b995b751c930ccb5d` | `999330f17307ec16873ed4acedd44f4267ae2c03a28e197b995b751c930ccb5d` |
| l2_tradable | 18,922,964 | 18,922,964 | 2 | 0 | 0 | `63bc51195104c9b013bba1ccabe1527bf9e6192b4b11bc0a6f5344ca7a234aa2` | `706a13d126748a75aff6a4e8a8313884dcc93d146c46db679f4873a069ca7baa` |
| l3_risk_excluded | 18,922,964 | 18,922,964 | 2 | 0 | 0 | `1fb7851d7db0a3b4f60ebb77b4d9ce8eb8b0c5a93a8c30f13b7d0e27a514d784` | `9994d4cdc9ece459aaec81e0840454499848d1d1bd61aeda725864015fc7ebb7` |
| l4_liquid | 18,922,964 | 18,922,964 | 4 | 0 | 0 | `288e8cd85755414e1084f8a8bd3098e7cb119216d86a7081ebe06d20f8445732` | `42ff9a9d9a4cf442ac64218068296f6db4a8b6bc0a3fbe7d6d67d9dce3eea922` |
| l5_pool | 18,922,964 | 18,922,964 | 4 | 0 | 0 | `f2ee3b691ac603b53e91582f9fca27de89054af70fda715ce879f4177f18d939` | `7200fa5c7ad82a0bcfdb801dbaf7e4aae4d45c37eb25764970f0e9b8dcde782d` |

改动明细：l2 / l3 各 2 行为 000001 的 05-03 / 05-06 由 false 变 true；l4 / l5 各 4 行 = 000001 上述 2 行 + 000006 的 05-06 / 05-07 由 true 变 false。`counts_by_year.csv` 的 1993 年 L2 / L3 各 +2；`st_snapshot_vs_interval.json` 未变。旧 SHA 全值另存于 `_staging/derived_1993b_20261113/layer5_before_sha.json`（键 `data/local_store/meta/universe/layers/<name>`），新值见 `layer5_apply.json` 的 `files`。

## 3. 三段传播链实测

### 3.1 ADL（`meta/breadth/breadth_daily.parquet` 的 `adl`）

- 共同交易日逐日差：`1993-05-03 +1`、`1993-05-04 +15`、`1993-05-05 +13`、`1993-05-06 −2`。
- **`1993-05-07` ~ `2026-11-11` 共 8,161 个共同交易日，差值唯一值集合 `{-2}`**（ADL 为全历史累计量，最后一次净变化后只能整体平移）。
- 合计改动 8,166 日期行 = 窗口内 4 行 + 之后 8,161 行 + `1992-03-27` 1 行（该行 `flat 3→2`、`total 11→10`，因 000016 上市首日 `pre_close 0.0 → null`，属前序已落盘修复，非漂移）。
- 计数列窗口内：05-03 `up 30→21 / down 35→45`；05-04 `up 2→9 / down 64→57`；05-05 `up 31→30 / down 36→37`；05-06 `up 0→1 / down 52→68`。

### 3.2 breadth_ma_daily（MA 传播）

- 46 个改动日期，逐列改动行：`sample_size 4`、`above_ma20 3`、`above_ma60 4`、`above_ma120 20`、`pct_above_ma20 5`、`pct_above_ma60 7`、`pct_above_ma120 25`、`new_high_250 2`、`new_low_250 4`、`sample_size_ma60 6`、`sample_size_ma120 15`、`sample_size_250 26`。
- 末次差异落在各窗口内的交易日数：**20 日窗 6 个、60 日窗 31 个、120 日窗 97 个、250 日窗 228 个**；最后一天 **`1994-03-22` = 1993-05-06 + 228 个交易日**。
- 窗口外 `8,608` 行计数列逐值 0 改动（`untouched_equal = true`）。

### 3.3 state_vector_daily（1250 日滚动分位）

- raw 列改动：`sent_ff_turnover` 4 行（05-03/04/05/06）、`sent_ff_coverage` 2 行、`sent_ff_ffill_share` 2 行、`breadth_ma20` 3 行（05-03/05-06/05-14，最后一天为 20 日窗传播）、`dims_q_available` 4 行。
- 分位列：`sent_ff_turnover_q 279` 行、`breadth_ma20_q 282` 行，起 `1995-08-24`（分位序列首个有效观测），止 **`1998-05-05` = 1993-05-06 + 1,250 个交易日**，即 `QUANTILE_WINDOW = 1250` 观测窗的自然边界；`1998-05-06` 起逐值 0 改动。
- 其余 15 个 raw / q 列改动 0 行；这些列空值数 −2 是删掉的 2 行本身为 null，不是新增空值。
- 汇总：改动行 451（列并集），空值增量 −2，窗口内 4 个交易日全市场独立重算偏差 ≤ 3.1e-9。

## 4. 复现命令

以下脚本位于任务工作区 `_staging/derived_1993b_20261113/`（该目录不入 Git，仅 `G:\git\python-quant` 镜像保留一份）：

```powershell
# 第 9 步逐表对拍（只读）：输出 layer9_final_check.json / .log
.venv\Scripts\python.exe _staging\derived_1993b_20261113\layer9_final_check.py
# 13 表 + 6 层 rows / bytes / SHA 重算（只读）
.venv\Scripts\python.exe _staging\derived_1993b_20261113\gen_evidence_table.py
# 换手表 34 行删除的独立复核记录
Get-Content _staging\derived_1993b_20261113\layer6_turnover.json -Raw
```

## 5. 边界声明

1. `meta/financials/valuation_daily.parquet` **本次未动**：`pe/pb/total_mv/circ_mv/close` 均为联网财务源字段，按 DG-16 / DG-17 / DG-19 不得用本地行情推导修正；该表仍含 34 行 `1993-05-01/02` 且 `close` 与本地行情错开一个交易日，另按只读判定处理，不属本次重建范围。
2. `meta/universe/universe_exclusions` 只复核了行情派生四类（`price_jump` / `suspended` / `delisting` / `st` 依赖口径），`no_quotes` / `no_basic_info` / `internal_gap` / `tdx_truncated` 四类仍指向联网库与 TDX 源，本次未核未动。
3. `factors/stock_adj_factor.parquet` 旧 SHA 以备份副本实测为准（`b2e872f8…`），台账中 `581ca66a…` 为记账不一致，已单列取证。
4. 000001.SZ 的 22 个交易日因子缺口（G-107）未并入本次修复，仍按既有登记留待单独处理。
5. `meta/state/state_vector_daily.parquet` 在本任务验收后（2026-11-12 21:43:24）被另一路任务 `_staging/state_margin_patch_20261112/` 追加 `lev_margin` 维（dims 5→6，SHA `6a2cc653…`），行数与 bytes 不变；本目录第 11 行记录的是本任务自己的验收结果，两者不得混算。
