# `turnover_20` 多实现方向核查报告（只读）

- 核查日期：2026-11-12
- 范围：`src/`、`tools/`、`config/`、`_staging/`（仅读）、`reports/`（仅读）
- 性质：**只读核查**。未改任何代码、数据、文档；全部结论来自 `read_parquet` + 现场重算。
- 复算脚本：[recompute_turnover_sign.py](file:///e:/code/python/quant/2026-11-12/turnover-sign/outputs/recompute_turnover_sign.py)
- 机器可读指标：[turnover_sign_metrics.json](file:///e:/code/python/quant/2026-11-12/turnover-sign/work/turnover_sign_metrics.json)
- 表结构探查：[probe_schema.json](file:///e:/code/python/quant/2026-11-12/turnover-sign/work/probe_schema.json)

---

## 0. 结论速览

| 项 | 结果 |
|---|---|
| **判定** | **A（真缺陷，结构性）**：同一语义「20 日真实换手率均值」被两处独立实现，落盘符号相反，且其中一处已进入 L1 合成打分/回测链路 |
| 方向相反是否成立 | 成立。两表共同键 **ρ = −0.99999888**，逐日截面均值 **−0.99999873**，取负后 **+0.99999888** |
| 是否已造成历史结论错 | **否**。四条在用链路各自对存储方向自洽（详见 §3.3），未发现任何一处二次取负 |
| 另一条 `ff_turnover_20` | 归 **B**：成交额倍数，与真实换手 ρ = **−0.1163**，是异语义同名，不是方向冲突 |
| 纯死代码 | `new_factors.turnover_20` 这一列归 **C**：全仓零读取点 |
| 受影响链路 | `base_rebal → pre_h20 → 单因子筛选 → L1 等权合成 → 分域/换手/分位评估`；`eval_daily_advice` IC 筛选池；`single_window_diag` style_liquidity；`reports/factor_lib` 评估表 |

**一句话**：数值方向确实完全相反（等价于同一个量取负），但当前没有任何代码把两份拼到一起或对已取负的那份再乘 −1，所以是「埋着的地雷」而不是「已经踩过的地雷」。

---

## 1. 一、逐处定义

### 1.0 先更正任务书里的三处路径

| 任务书写的 | 实际位置 | 说明 |
|---|---|---|
| `src/quant/factor_lib/factors/fundamental_factors.py:278-280` | [fundamental_factors.py:278-280](file:///e:/code/python/quant/src/advice/fundamental_factors.py#L278-L280) | 目录是 `src/advice/`，不在 `factor_lib` 下 |
| `tools/research/research_candidates.py` 里的一条 | [research_candidates.py:396-399](file:///e:/code/python/quant/src/alpha/factors/research_candidates.py#L396-L399) | 实际是 `src/alpha/factors/`；且该文件**没有 `turnover_20`**，只有 `q_asset_turnover`（营收TTM / 期末总资产，资产周转率，与换手率无关），该文件也不被任何生产代码导入（仅 `tests/test_research_candidates.py` 导入） |
| `src/quant/factor_lib/factors/famafrench.py` 的 `ff_turnover_20` | [famafrench.py:85-90](file:///e:/code/python/quant/src/quant/factor_lib/factors/famafrench.py#L85-L90) | 路径正确 |

因此实际是 **3 处实现 + 1 处派生落盘**，不是 4 处平级实现。

### 1.1 对照表

| # | 实现（文件:行） | 列名 | 输入列 | 公式原文 | 单位 | 值越大代表 | 窗口 / 最小样本 | 空值处理 |
|---|---|---|---|---|---|---|---|---|
| ① | [build_new_factors.py:146-154](file:///e:/code/python/quant/tools/research/build_new_factors.py#L146-L154) | `new_factors.turnover_20` | `volume`、`float_share`（`meta/factors/stock_shares.parquet`，`join_asof` backward） | `_turn = volume·100 / float_share`；`_turn20 = rolling_mean(_turn, 20)`；落盘 `(-_turn20).alias('turnover_20')` | 换手率（小数），**已取负** | **越低迷**（约 −0.02 表示 2% 换手） | 20 / **15** | `volume<=0 or float_share<=0 → null`；不足 15 天 → null |
| ② | [fundamental_factors.py:278-280](file:///e:/code/python/quant/src/advice/fundamental_factors.py#L278-L280) + [:289](file:///e:/code/python/quant/src/advice/fundamental_factors.py#L289) | `ext_turnover_20` | 同上（`read_shares()`） | `turnover = volume·100 / _float_share`；`turnover_20 = rolling_mean(turnover, 20)`；`alias('ext_turnover_20')` | 换手率（小数），**未取负** | **越活跃** | 20 / **10** | 同上 |
| ③ | [famafrench.py:85-90](file:///e:/code/python/quant/src/quant/factor_lib/factors/famafrench.py#L85-L90) | `ff_turnover_20` | `amount` | `safe_div(amount, ts_mean(amount, 20))` | **成交额倍数**（无量纲，围绕 1） | 越活跃（成交额相对自身 20 日均值放大） | 20 / **20（满窗）** | `safe_div` 分母 0 → null |
| ④ | [_staging/…/run_oos.py:171-177](file:///e:/code/python/quant/_staging/factor_oos_verify_20261111/run_oos.py#L171-L177) → `base_rebal.parquet` | `base_rebal.turnover_20` | ② 的输出 | 直接 `ext_turnover_20` **原值 alias 成 `turnover_20`**，不改符号 | 换手率（小数），未取负 | 越活跃 | 同 ② | 同 ② |

**方向元数据**：② 在 [fundamental_factors.py:59-60](file:///e:/code/python/quant/src/advice/fundamental_factors.py#L59-L60) 登记 `FundamentalCandidate('ext_turnover_20', 'volume', '20日均真实换手率', -1.0, '…；高换手偏空')` —— **值不取负、符号放在 direction 里**。① 相反，**值取负、没有 direction 字段**（[build_new_factors.py:6](file:///e:/code/python/quant/tools/research/build_new_factors.py#L6) 只写注释「越高换手、未来收益越差」）。

> 这两种编码本身都合法，问题在于**同一句经济含义配了两套相反的数值编码，且列名只差前缀**，任何按列名合并的代码都会静默拿到相反方向。

---

## 2. 二、方向是否真的相反（实测）

全部数字由 [recompute_turnover_sign.py](file:///e:/code/python/quant/2026-11-12/turnover-sign/outputs/recompute_turnover_sign.py) 现场产生，可原样复跑。

### 2.1 存量两表直接对拍

| 项 | 值 |
|---|---|
| 样本 | `new_factors`（142 个月末）× `base_rebal`（575 周频），**共同月末 28 个**（2015-11-30 ~ 2026-04-30） |
| 行数 / 只数 | **113,501 行 / 5,391 只** |
| **ρ(`new_factors.turnover_20`, `base_rebal.turnover_20`)** | **−0.99999888** |
| **ρ(−`new_factors`, `base_rebal`)** | **+0.99999888** |
| 逐日截面（28 天，日均 4,054 对） | 均值 **−0.99999873**，中位 −1.0，min −1.0，max −0.99999017 |
| 值域镜像 | `new_factors` ∈ [−0.5584039, −0.0001876]，**100% 为负**；`base_rebal` ∈ [+0.0001876, +0.5584039]，**0% 为负** |

两端极值精确互为相反数（−0.5584038641633918 / +0.5584038641633918），即**逐值取负**，不是「趋势相反」。

### 2.2 现场重算，确认算术与空值

用 400 只随机样本（种子 20261112，全部命中行情文件）按三条源码公式逐字重算：

| 对拍 | 行数 | 最大绝对差 | 平均绝对差 | p99.9 |
|---|---|---|---|---|
| `base_rebal.turnover_20` vs 重算 ②（未取负） | 7,485 | **1.39e-17** | 6.7e-20 | 6.9e-18 |
| `new_factors.turnover_20` vs −重算 ①（取负） | 37,345 | **6.0e-4** | 1.6e-8 | **0.0** |
| 重算 ①（min_samples=15）vs ②（min_samples=10） | 37,345 | **0.0** | 0.0 | — |

- `base_rebal` 与 ② 的差在 1e-17 量级 = 浮点噪声，**证明 `base_rebal.turnover_20` 就是 `ext_turnover_20` 原值**。
- `new_factors` 与 −重算 的 p99.9 为 0、均值 1.6e-8，仅 1 行差 6.0e-4（约 0.04% 相对误差），来自 `read_shares()` 与直接读 `stock_shares.parquet` 的去重/取最新口径在股本变更日的边界差异，**不是语义差异**。
- `min_samples=10` 与 `15` 在月末键上差值为 0：上市满 20 日的票两者都有值，次新票两者都空，**样本门槛不是两者差异来源**。

### 2.3 相关矩阵（同一批 400 只 × 142 个月末，37,289 行）

| 对 | Spearman ρ | 解读 |
|---|---|---|
| ② `ext_turnover_20` vs `new_factors.turnover_20` | **−0.99999995** | 同一语义，符号相反 |
| ② vs −`new_factors.turnover_20` | **+0.99999995** | 取负后恢复到 +1 |
| ② vs `base_rebal.turnover_20` | **+1.0**（7,485 行） | 同源同号 |
| ② vs ③ `ff_turnover_20` | **−0.11633** | 异语义（逐日均值 −0.127，区间 [−0.514, +0.211]） |
| ③ vs `new_factors.turnover_20` | **+0.11633** | 同上，镜像 |

### 2.4 量纲证据（说明 ③ 确实不是换手率）

| 列 | min | p50 | p99 | max | mean |
|---|---|---|---|---|---|
| ② `ext_turnover_20` | 0.000207 | **0.0203** | 0.1999 | 0.4855 | 0.0330 |
| ① `new_factors.turnover_20` | −0.4855 | **−0.0203** | −0.0018 | −0.000238 | −0.0327 |
| ④ `base_rebal.turnover_20` | 0.000379 | **0.0185** | 0.1953 | 0.4514 | 0.0304 |
| ③ `ff_turnover_20` | 0.0 | **0.8603** | 4.162 | 12.10 | 1.043 |

①②④ 同一量纲（中位约 2% 换手）；③ 中位 0.86、p99 达 4.16，围绕 1 波动，是**倍数**。

### 2.5 实证方向（哪一边的经济符号是对的）

`reports/l1_baseline/single_factor_h20.csv` 里，**正原值**口径的 `turnover_20`：

| rank_ic | icir_annual | t_stat | ic_positive_rate | sign |
|---|---|---|---|---|
| **−0.089071** | −3.176 | **−10.700** | 17.5% | 负 |

即「换手越高 → 未来 20 日收益越差」，与 ② 的 `direction=-1.0`、④ 的 `PROD_DIR['turnover_20']=-1` 一致。因此 ① 落盘取负、实际等价于 direction=+1 —— **两个编码都对，但只在自己那套里对**。

对照 `ff_turnover_20`（`reports/factor_lib/factor_eval.csv`）：`mean_ic = −0.00754`、`t = −0.299`、`icir = −0.0399`、`monotonicity = 0.6` —— 与真实换手（−0.0891 / −10.70）量级差 12 倍，进一步支撑「异语义」。

---

## 3. 三、是否被同一处代码混用

### 3.1 全仓读取点（`src/`、`tools/`、`config/`）

| 实现 | 写 | 读（消费者） | 是否在生产链路 |
|---|---|---|---|
| ① `new_factors.turnover_20` | [build_new_factors.py:154](file:///e:/code/python/quant/tools/research/build_new_factors.py#L154) | **无**。`build_multifactor_signals.py:101` 只从 `new_factors` 取 `accruals`/`sue`；[:14](file:///e:/code/python/quant/tools/research/build_multifactor_signals.py#L14) 明确「turnover_20 未加入」 | 否 |
| ② `ext_turnover_20` | [fundamental_factors.py:289](file:///e:/code/python/quant/src/advice/fundamental_factors.py#L289) | [build_analysis_monthly.py:282](file:///e:/code/python/quant/tools/research/build_analysis_monthly.py#L282)（算了但输出列不含，`analysis_monthly` 实际无此列）；[eval_daily_advice.py:193-197](file:///e:/code/python/quant/tools/eval_daily_advice.py#L193-L197) IC 筛选池；[single_window_diag.py:78](file:///e:/code/python/quant/tools/single_window_diag.py#L78)（`style_liquidity`）、[:720](file:///e:/code/python/quant/tools/single_window_diag.py#L720)；`run_oos.py:171-177` | **是**（评估链路） |
| ③ `ff_turnover_20` | [famafrench.py:89](file:///e:/code/python/quant/src/quant/factor_lib/factors/famafrench.py#L89) 注册 | `src/quant/factor_lib` 的 `compute_all` → `reports/factor_lib/{factor_eval,factor_registry,recommended_factors,redundancy_clusters}.csv`；[eval_daily_advice.py:179](file:///e:/code/python/quant/tools/eval_daily_advice.py#L179) 同一因子矩阵 | 是（因子库评估 + 候选池） |
| ④ `base_rebal.turnover_20` | [run_oos.py:171-177](file:///e:/code/python/quant/_staging/factor_oos_verify_20261111/run_oos.py#L171-L177) | [eval_factor_candidates.py:61](file:///e:/code/python/quant/tools/research/eval_factor_candidates.py#L61)（LEGACY_16 正交化）；[run_l1.py:68](file:///e:/code/python/quant/tools/research/l1_baseline/run_l1.py#L68)、[:563](file:///e:/code/python/quant/tools/research/l1_baseline/run_l1.py#L563)；[turnover_curve.py:72](file:///e:/code/python/quant/tools/research/l1_baseline/turnover_curve.py#L72)；[check_ref.py:19-32](file:///e:/code/python/quant/tools/research/l1_baseline/check_ref.py#L19-L32) | **是**（打分/回测/评估） |

`config/` 全目录无 `turnover` 字样；`src/advice/factor_sets.py` 无 `turnover_20`（生产打分 17 因子不含换手，见 `docs/因子方向冲突诊断_2026-11-11.md` §11）；`src/backtest/`、`src/quant/learning` 均无引用。

### 3.2 有没有「同一段代码同时引用两个实现」

- **没有真正对撞，但确实同池共存一次**。`eval_daily_advice.build_factor_matrix`（[:177](file:///e:/code/python/quant/tools/eval_daily_advice.py#L177) 注释「163 条 factor_lib + 15 条扩展技术 + legacy 12 因子 + 7 条基本面/股本因子」）在同一个 DataFrame 里同时装 ③（`compute_all`，:179）和 ②（`compute_fundamental_factors`，:193）；由于列名不同（`ext_turnover_20` / `ff_turnover_20`），不会互相覆盖——**同池但不是同列，属「评估口径并列」而非「数据覆盖」**。
- 唯一同时装载多个研究表的是 `build_multifactor_signals.py`，但它从 `new_factors` 只取 `accruals`/`sue`，从 `analysis_monthly` 取 5 个基础因子——两张表的 `turnover_20` 都不在其中。
- 风格归类会让两者进**同一个配额桶**：[factor_eval.py:83-91](file:///e:/code/python/quant/src/advice/factor_eval.py#L83-L91) 中 ② 经 `STYLE_OF`（`FUNDAMENTAL_CANDIDATES` 的 `volume`）归为 `volume`，③ 因表达式含 `amount` 被关键词表归为 `volume`。同一风格桶里并列两只「换手」，但列名不同、不会覆盖，只是同一桶内的冗余候选。
- `eval_factor_candidates.py` 只读 `panel_cache.parquet`，从不读 `new_factors.parquet`（已核其全部 `read_parquet` 调用点）。

### 3.3 各在用链路的方向是否自洽（逐条验过）

| 链路 | 存储方向 | 施加的方向 | 是否二次取负 | 证据 |
|---|---|---|---|---|
| `base_rebal → pre_h20 → run_l1.evaluate_one(orient=-1)` | 正 | −1，**分组前**施加一次 | 否 | `pre_h20.turnover_20` 与 `base_rebal.turnover_20` 的 ρ = **+0.734**（预处理保号，衰减来自 winsorize/中性化）；`signs_h20_oo['turnover_20'] = -1.0`；`orient_assertions` **12/12 PASS** |
| `turnover_curve.py:72` | 正 | 先 `-col` 再取 Q5（= 低换手端） | 否，且注释与实现一致 | [:266](file:///e:/code/python/quant/tools/research/l1_baseline/turnover_curve.py#L266)「Q5 = 低换手端」 |
| `check_ref.py` | 正 | `orient=-1.0` | 否 | [:32](file:///e:/code/python/quant/tools/research/l1_baseline/check_ref.py#L32) |
| `run_oos.py` PROD_DIR | 正 | −1 | 否 | [:77](file:///e:/code/python/quant/_staging/factor_oos_verify_20261111/run_oos.py#L77) |
| `new_factors` | 负 | 无消费者 | — | §3.1 |
| `eval_daily_advice` / `single_window_diag` | 正 | 按 `direction=-1.0` 取向 | 否 | 走 `FUNDAMENTAL_CANDIDATES` 元数据 |

### 3.4 唯一的「已发生的错误」是注释，不是数值

[run_oos.py:60-61](file:///e:/code/python/quant/_staging/factor_oos_verify_20261111/run_oos.py#L60-L61) 把 `turnover_20` 写在**「对照因子（已知 OOS 正向）」**列表里，而 [:77](file:///e:/code/python/quant/_staging/factor_oos_verify_20261111/run_oos.py#L77) 设 `'turnover_20': -1`，注释紧接着写「经济方向 -1（高换手偏空）」。同一文件 17 行之内，同一因子被描述成「OOS 正向」和「方向 −1」。代码判定用的是 `PROD_DIR`，结论没错；错的是那句「已知 OOS 正向」——它描述的其实是**取负后**的 ①，混进了以正原值为准的 `base_rebal` 语境。这正是两套编码并存造成的认知污染。

---

## 4. 四、判定

### 4.1 判定：A（真缺陷，结构性）

四个条件逐条核对：

| 条件 | 是否成立 | 依据 |
|---|---|---|
| 同一语义 | 成立 | ①②④ 都是 `volume·100/float_share` 的 20 日均值；ρ = −0.99999888，值域精确镜像 |
| 两处独立实现 | 成立 | ① 自己读股本表自算；② 走 `read_shares()`；两套 `min_samples`（15 / 10） |
| 方向相反 | 成立 | 落盘符号相反；取负后 ρ = +0.99999888 |
| 至少一处进入打分/回测/评估链路 | 成立 | ②→④ 进入 L1 合成（`turnover_20` 在 26 只入选池内，`kept_icir = 0.8947`，为全池最高）；② 进入 `eval_daily_advice` IC 筛选与 `single_window_diag` |

### 4.2 但严重度是「未触发」，不是「已算错」

- 无任何历史结论因符号而错：四条在用链路逐条验过，方向只施加一次（§3.3），`orient` 回归断言 12/12 通过。
- ① 是死列，所以「取负的那份」从未参与任何计算。

### 4.3 触发条件（写成「一变就错」的形式）

满足任一即会把「潜在」变成「已发生」：

1. 任何代码把 `new_factors.turnover_20` 并入 `base_rebal`/`pre_h20`（例如「给 L1 补财务因子」时按列名 join）——同名列直接对撞，后到的覆盖先到的，符号随机取决于 join 顺序。
2. 任何代码读 `new_factors.turnover_20` 并按 `FUNDAMENTAL_CANDIDATES` 的 `direction=-1.0` 取向 —— 二次取负。
3. 任何代码把 `ext_turnover_20` 改名为 `turnover_20` 后与 `new_factors` 同名列合并（`run_oos.py` 已经在做「改名成 turnover_20」这一步，只差一个合并动作）。

### 4.4 影响面（若触发，多大）

- 单因子：rank IC 由 **+0.0891** 翻为 **−0.0891**，摆幅 **0.178**（|t| = 10.70，不是噪声级）。
- L1 合成：26 只等权，`turnover_20` 名义贡献由 **+0.0034** 变为 **−0.0034**，合成 IC 摆幅约 **0.0068**；且它是入选池中 ICIR 最高的一只（0.8947），反向会同时反转「低换手优于高换手」这一核心对冲假设。
- 可能受牵连的历史产物：`reports/l1_baseline/*`（`single_factor_h20.csv` / `eval_results.json` / `selection_h20.json` / `turnover_curve*.json`）、`_staging/factor_candidates_20261111/*`、`_staging/factor_oos_verify_20261111/*`、`reports/factor_lib/*`。**当前这些产物的数值是对的**（§3.3），受影响的是「复算可重现性」——换一个人重跑、按列名取数，就会得到相反结论。

### 4.5 另外两条的单独归类

| 实现 | 归类 | 理由 |
|---|---|---|
| ③ `ff_turnover_20` | **B** | 语义本来就不同（成交额倍数 vs 换手率），ρ = −0.1163、量纲差 40 倍、IC 量级差 12 倍。同名只是撞名。建议：改名为 `ff_amount_ratio_20`，或在登记表 `note` 已写明「非真实换手率」的基础上，把列名也带上 `amount` |
| ① `new_factors.turnover_20` | **C** | 全仓唯一写者，零读取点；`build_multifactor_signals.py:14` 记录了「正交化后仅 +0.066，已被现有簇吸收」的主动排除决定。建议：要么删除该列，要么改名为 `neg_turnover_20` 让符号进名字 |

> 这三条不冲突：**字段级判定取最严重的一条 = A**，因为②④这条活链路满足 A 的全部条件；③ 与 ① 分别是各自实现的问题，单独归 B/C。

### 4.6 修复建议（本次不实施，仅记录）

按代价从低到高：

1. **零风险**：把 ① 的列名改为 `neg_turnover_20`（或直接删列，它没有消费者），消除同名对撞的可能。
2. **零风险**：把 ③ 改名为 `ff_amount_ratio_20`，同步 `factor_registry.csv` 与 `reports/factor_lib/*`。
3. **需重算**：若要让 ① 与 ② 统一编码，应保留「值不取负 + direction 元数据」这一套（②④ 在用、且与 `FundamentalCandidate`/`PROD_DIR`/`orient` 三处约定一致），让 ① 改为落正原值 —— 但这会改动 `new_factors.parquet` 的正式内容，属「改变既定口径」，须按项目规则先报批。
4. **文档侧**：修掉 `run_oos.py:60-61` 那句「已知 OOS 正向」，改为「正原值口径，经济方向 −1（高换手偏空）」。

---

## 5. 复算方式

```powershell
cd e:\code\python\quant
.\.venv\Scripts\python.exe 2026-11-12\turnover-sign\outputs\recompute_turnover_sign.py
```

产物：`2026-11-12\turnover-sign\work\turnover_sign_metrics.json`（本报告 §2 全部数字的唯一来源）。脚本只做 `read_parquet`，不写任何 `data/`、`reports/`、`_staging/` 内容。

补充口径说明（现场核过，供复核）：

- 抽样：`numpy.random.default_rng(20261112)` 从 `new_factors` 的 5,391 只中抽 400 只，400 只全部命中 `data/local_store/quotes/stock/<ex>/<code>.parquet`。
- 股本：`join_asof(strategy='backward')` 按 `effective_date`，与两处源码一致。
- 复权：三处公式都用**未复权 `volume`**，故重算不做复权处理。
- 月末键：`new_factors` 的 142 个月末；与 `base_rebal` 的交集是 28 个，交集外的 114 个月末只用于 ②/③ 的分布与相关。
