# 片7 落地：`free_float_share` 可复现化（D1 / D2 / D6）+ 549 行违例决策材料

> 上游依据：[b1_slice7_check.md](file:///e:/code/python/quant/2026-11-12/data-lineage/outputs/b1_slice7_check.md) 的 D1 / D2 / D6 三条缺陷。
> 本文件只出 md，不产出 CSV。§1~§4 为「代码落地 + 决策材料」阶段（数据未改）；§5 为**用户批准截断口径后的数据步实测**（549 行截断 + 13 行补填，一次 `--apply` 落盘）。派生层（`state_vector_daily`）本轮**未重建**，见 §8。

## §0 结论先行

| 项 | 状态 | 一句话结论 |
|---|---|---|
| **D1** `free_float_share` 可复现 | ✅ 落地 | 新增 `tools/fill_free_float_shares.py`；用仓库内代码重跑，**既有 148,587 个非空 `free_float_share` 逐值差异 0 行**（两份 raw 各验一次） |
| **D2** `filled ≤ float` 断言 | ✅ 落地 | `check()` 额度**固定 0**（`--allow-filled-gt-float` 已删除）；截断内建在 `derive()`，现表 `--dry-run` 实测 `filled_gt_float_rows=0` 通过；反例单测真实触发 |
| **D6** meta `backup_file` 指向 | ✅ 修复 | 不存在的 `data/local_store/_backup/…` → 仓库根真实路径；仅 1 行变化，快照与新旧 SHA256 已记 |
| **549 行 / 479 只违例** | ✅ 已批准并落盘 | 100% 是 ffill 继承（跨源 0 行）；用户批准「截断到 `float_share`」，与 13 行补填**同一次 `--apply`** 完成，四项验收见 §5 |

---

## §1 D1：`free_float_share` 变成仓库内可复现产物

### 1.1 新入口

| 项 | 值 |
|---|---|
| 文件 | [tools/fill_free_float_shares.py](file:///e:/code/python/quant/tools/fill_free_float_shares.py)（新增，504 行 / 23,247 B / `sha256 9ec2209e…`） |
| 来源 | 东财 F10 `RPT_F10_EH_EQUITY` 的 `FREELIQCI_SHARES`（自由流通股本，单位股），与 `LISTED_A_SHARES` 同表同行 |
| 接口 | `https://datacenter.eastmoney.com/securities/api/data/v1/get`，`pageSize=5000`、`sortColumns=SECUCODE` |
| 判定规则（与一次性脚本一致） | 只填空值；`free > 0` 且 `free ≤ float_eff`；`float_eff = coalesce(本次 float_share, 东财 LISTED_A_SHARES>0)` |
| 逐行来源标记 | `--apply` 时在 staging 写 `free_float_provenance.csv`（`ts_code / effective_date / free_before / free_after / em_free / em_float / float_eff / action / source`），`source='eastmoney:F10_FREELIQCI_SHARES'` |
| 子命令 | `--fetch` / `--plan`（dry-run）/ `--verify`（只读对拍）/ `--apply`（落盘） |
| 离线复用 | 默认按 `--raw` → `_staging/<任务>_<日期>/raw.parquet` → `E:\www\tmp\em_equity_all_*.parquet`（取 mtime 最新）顺序定位，**不强制联网** |
| 联网口径 | 仅 `--fetch` 触发；单连接串行、每页 `0.25s` 节流、单页最多 3 次重试，连续失败即停压（DG-25） |
| 断点目录 | `_staging/free_float_shares_em_20261113/`，`progress.csv` 与 `failed.csv` 启动即建 |

### 1.2 逐值不变证明（核心验收）

命令：

```powershell
.venv\Scripts\python.exe tools\fill_free_float_shares.py --verify --raw "E:\www\tmp\em_equity_all_20261105.parquet"
.venv\Scripts\python.exe tools\fill_free_float_shares.py --verify      # 默认用 2026-11-13 新抓的 raw
```

实测输出（两次都相同的关键行）：

```text
[verify] 既有非空 free_float_share 148587 行，仓库内代码重算差异 0 行
```

以 2026-11-05 历史缓存（324,806 行 / 24,754 只，`sha256 45122378…`）为输入时的判定分布：

| action | 行数 | 含义 |
|---|---|---|
| `keep` | **148,587** | 已有非空值，原样保留（= 逐值对拍的行集合） |
| `fill` | 13 | 空值且规则内可填（即片7 的「13 行规则内漏填」，本次未写盘） |
| `no_source` | 16,321 | 配对成功但源端该字段为空 |
| `source_nonpositive` | 145 | 源端 `FREELIQCI_SHARES = 0` |
| `source_over_float` | 772 | 源端 `free > float`，按规则跳过 |
| `no_float` | 0 | 无可用的流通股上界 |
| `unmatched` | 446 | 源端无该 `(ts_code, effective_date)` 记录 |
| 合计 | 166,284 | 行数守恒 |

空值闭合：17,697 → 17,684（恰好 `-13`），与片7 空值分解式（446 + 16,321 + 145 + 772 + 13 = 17,697）逐项吻合。

用 2026-11-13 新抓缓存（325,640 行 / 24,761 只，`sha256 3d513e75…`）复跑，148,587 行差异**仍为 0**，只是 `fill` 由 13 升到 341、`unmatched` 由 446 降到 118（新快照多覆盖了 1 个月的新增报告期记录）。这说明证明不依赖某一份缓存。

### 1.3 staging 断点目录（本次真跑，不是造文件）

`.venv\Scripts\python.exe tools\fill_free_float_shares.py --fetch` 实测：

| 项 | 值 |
|---|---|
| 目录 | `_staging/free_float_shares_em_20261113/` |
| `parts/` | 66 个 `page_XXXX.parquet`（每页 5,000 行，末页不足） |
| `progress.csv` | 68 行 = 表头 + 1 条 `init` + 66 条 `ok`，字段 `page,rows,status,attempts,updated_at` |
| `failed.csv` | 仅表头 + 1 条 `init`，**失败 0 条** |
| `raw.parquet` | 3,477,676 B / 325,640 行 / 24,761 只 / `sha256 3d513e75…` |

续跑口径：`_read_done_pages()` 读 `progress.csv` 里 `status=ok` 的页号，`page in done and part.is_file()` 即跳过，中断后从断页续跑，符合 DG-7。

### 1.4 权威清单 L39 的改写依据

现文（`docs/数据权威路径清单.md:39`）尾句：

> 前 8 列由 `share_capital` 产出；`free_float_share_filled` / `free_float_source` 由 `tools/derive_free_float_fill.py` 派生…

**这句与代码实测不符**：`share_capital.py` 里 `free_float_share` 恒为 `None`（[share_capital.py:380](file:///e:/code/python/quant/src/data/local_store/share_capital.py#L380) 与 [:528](file:///e:/code/python/quant/src/data/local_store/share_capital.py#L528) 两处字面量 `'free_float_share': None`），`_normalize_frame` 再截断到 8 列；该列唯一真实产出方是「东财 F10 FREELIQCI 补数脚本」，此前只存在于仓库外一次性脚本（`tmp/from_www_tmp_20261109/fill_shares_20261105.py`），现已收拢为仓库内入口。

建议把该句替换为（清单文件本体交给文档那一路改，本任务不动）：

> 前 8 列中 `ts_code` / `effective_date` / `total_share` / `float_share` / `change_reason` / `source` / `fetched_at` 共 7 列由 `share_capital`（`assemble_shares` → `write_shares`）产出；**`free_float_share` 不由 `share_capital` 产出**（`share_capital.py:380`、`:528` 两处恒写 `None`），唯一产出方是 `tools/fill_free_float_shares.py`（东财 F10 `FREELIQCI_SHARES`，规则 `free>0 且 ≤ 补全后 float_share`，只填空值；离线默认复用 `E:\www\tmp\em_equity_all_*.parquet`，`--fetch` 才联网，单连接串行 + 0.25s 节流，断点在 `_staging/free_float_shares_em_<日期>/`）。`free_float_share_filled` / `free_float_source` 由 `tools/derive_free_float_fill.py` 派生（raw / ffill_250d / excluded 三态，窗口 250 交易日），**股本表重建后必须重跑该脚本**，否则退回 8 列。**固定执行顺序：`tools/fetch_stock_shares.py` → `tools/fill_free_float_shares.py --apply` → `tools/derive_free_float_fill.py`**，跳任一步都会退化成「8 列空 free」或「8 列」。原 `free_float_share` 保持源端原值不变，禁止就地覆盖。

---

## §2 D2：`filled ≤ float` 断言

| 项 | 值 |
|---|---|
| 文件 | [tools/derive_free_float_fill.py](file:///e:/code/python/quant/tools/derive_free_float_fill.py)（304 行 / 13,368 B / `sha256 2913c6c9…`） |
| 新增函数 | `filled_gt_float_rows(out)`：`filled` 非空 且 `float_share` 非空 且 `filled > float_share` |
| 断言位置 | `check(out, source)` 末尾：`violations.height > 0` 即 `AssertionError`，异常里带违例行数与 3 条最严重样例（含 `ts_code / effective_date / filled / float_share`） |
| 额度口径 | **固定 0，无放行参数**。首版曾加 `--allow-filled-gt-float` 作为显式门禁，用户批准截断口径后该参数**已彻底删除**——放宽断言的口子不留，符合 DG-26 |
| 配套截断 | `derive(frame, calendar, *, clip=True)` 内建截断（`filled = min(filled, float_share)`），`clip_filled_to_float()` 单独暴露供统计命中行数；违例不再靠人工放行，而是从派生阶段就不产生 |
| CLI | `main()` 只有 `--dry-run`；`stats` 增加 `filled_gt_float_rows` |

落盘后实测（默认额度、无任何放行）：

```text
.venv\Scripts\python.exe tools\derive_free_float_fill.py --dry-run
行数 166284（守恒）  三态 {'rows': 166284, 'raw_rows': 148600, 'ffill_250d_rows': 1134,
'excluded_rows': 16550, 'null_rows_total': 17684, 'filled_gt_float_rows': 0}
dry-run：未写盘
```

反例自测（`tests/test_free_float_fill.py`，181 行 / `sha256 7d929f3b…`，28 用例）：

| 用例 | 构造 | 结果 |
|---|---|---|
| `test_filled_gt_float_raises` | 前一条 `free=900 / float=1000`，后一条 `float=500 / free=空`；取 `derive(..., clip=False)` 还原未截断状态 | `AssertionError: free_float_share_filled > float_share`，`filled_gt_float_rows` 计数 = 1 |
| `test_derive_clips_by_default` | 同上，走默认 `derive()` | filled = `[900, 500]`，越界计数 0，`check()` 通过 |
| `test_no_violation_passes` | `free` 与 `float` 同步下降 | 计数 0，不误报 |
| `test_clip_filled_to_float_only_touches_derived_column` | 越界 1 行 | 原 8 列与三态逐值不变，只有 `_filled` 被截断 |
| `test_clip_is_noop_when_clean` | 无越界 | 计数 0 且 `clipped.equals(out)`（不误改） |
| `test_check_plan_rejects_broken_fallback` | unmatched 行写成 999 + fill 行写回 null（保持总空值数守恒） | 只有兜底断言拦住，报「兜底失效」→ 证明 DG-31 的断言不是空跑 |
| 其余 22 个 | 补全五分支 + 计划校验 | 全部通过 |

`pytest tests/test_free_float_fill.py tests/test_stock_shares.py -q` → **28 passed**；`ruff check` 三个文件 → `All checks passed!`。

---

## §3 D6：`stock_shares.meta.json` 的 `backup_file` 指向修复

| 项 | 值 |
|---|---|
| 旧值 | `data\local_store\_backup\stock_shares_20261110\stock_shares.parquet` —— **该路径不存在**（`Test-Path` False） |
| 新值 | `E:\code\python\quant\_backup\stock_shares_20261110\stock_shares.parquet` |
| 新值校验 | 文件存在，2,159,770 B，`sha256 3E9DC46E…C6CE`，与同文件 `backup_sha256` 字段**逐字符一致** |
| 改前快照 | `_backup/meta/stock_shares_meta_20261113/stock_shares.meta.json`（`copy2`，快照 SHA256 = 改前 SHA256） |
| 旧 SHA256 | `c09f419ef954beb8d9170981e03ea727750b927b046f2c59e75a3b95c7d239f9` |
| 新 SHA256 | `71d6414b19fc56b5407d51181e52adfd80d9725534aa8e96ef5d2a8f14d2572e` |
| 字节 / 行数 | 2,908 → 2,914 B（+6）；51 行 → 51 行（不变） |
| 改动范围 | 仅第 47 行一个 JSON 字符串；写入走 `write_text_atomic`（DG-9），改后 `json.loads` 通过 |

未动 `stock_shares.parquet`：改前改后均为 2,792,975 B / `sha256 2e721c87…b35`。

---

## §4 决策材料：549 行 `filled > float`（**用户已批口径 ①，执行结果见 §5**）

### 4.1 违例规模

| 维度 | 值 |
|---|---|
| 行数 / 只数 | **549 行 / 479 只** |
| 三态归属 | `ffill_250d` 549 行（`raw` 0、`excluded` 0） |
| 源端归属 | `eastmoney:stock_zh_a_gbjg_em` 549 行（巨潮 0 行） |
| 年份分布 | 2007 年 21 行、2011 年 55 行、2018 年 57 行、2022 年 90 行、2023 年 79 行、2024 年 54 行、2026 年 26 行，其余年份零散 |
| 越界幅度 | 中位 +0.39%，p75 +2.25%，p90 +14.61%，最大 **+857.14%**（`002563.SZ` 2014-04-01：继承 670,000,000 / 本期流通 70,000,000） |
| 分档 | ≤1% 362 行 / 1~5% 94 行 / 5~15% 38 行 / >15% 55 行 |
| 2026-11-11 最新可见记录仍违例 | **14 只**，仅 `600387.SH` 超 1%（+14.61%：3.1411 亿 vs 2.7408 亿），其余 13 只 ≤0.74%，其中 6 只 ≤0.05%（当日有成交的违例标的 9 只） |

### 4.2 成因分类（ffill 继承 vs 源端两接口不同步）

对 549 行逐行回溯「继承值来自哪条记录」（`join_asof` 取同票上一条非空 `free_float_share`，549/549 全部命中供体）：

| 类型 | 定义 | 行数 | 占比 |
|---|---|---|---|
| **A：ffill 继承 × 供体满格** | 供体行 `free_float_share == float_share`（当期源端把自由流通记为「全部流通」），本期流通股本下调 → 继承必然越界 | **435** | 79.2% |
| **B：ffill 继承 × 供体高比例** | 供体 `0.9 ≤ free/float < 1`，本期 `float` 跌幅超过剩余空间 | **69** | 12.6% |
| **C：ffill 继承 × 供体中比例** | 供体 `free/float < 0.9`，本期 `float` 大幅下降（最大 −89.6%） | **45** | 8.2% |
| **D：源端两接口不同步** | 同一行 `FREELIQCI_SHARES > LISTED_A_SHARES` | **0**（已在采集阶段拦下） | 0% |

三条硬证据：

1. **跨源 0 行**：549 行的 `source` 与其供体行 `source` 全部同为东财（`source != donor_source` 计数 0），不存在「东财 free × 巨潮 float」这类拼接。
2. **本期流通股本全部下降**：549/549 行 `float_share < donor_float`（中位 −0.49%，p25 −3.77%，p10 −21.1%），无一例上升；上升行按定义不可能越界。
3. **D 类真实存在但已被规则挡在表外**：源端同行 `free > float` 共 **772 行**，采集口径 `source_over_float` 直接跳过不填——它们不在 549 内，也不在表内。

> 结论：**549 行 100% 是 ffill 继承导致，0% 是源端两接口不同步**。触发事件集中在「高管股份变动」289 行、「股份性质变更」159 行、「定期报告」89 行——都是流通股本微调、而源端同期 `FREELIQCI_SHARES` 未披露的空档。

### 4.3 两种口径的影响面

#### 口径 ①：截断到 `float_share`（`filled = min(filled, float_share)`）

| 影响项 | 具体内容 |
|---|---|
| 改哪个文件 | `data/local_store/meta/factors/stock_shares.parquet`（只改 `free_float_share_filled` 一列） |
| 改哪些行 | **549 行 / 479 只**，`free_float_source` 保持 `ffill_250d` 不变；`free_float_share`（原始列）1 行不动 |
| 值变化 | 最大绝对下调 600,000,000 股（`002563.SZ`），中位下调 0.39% |
| meta 变更 | `stock_shares.meta.json` 的 `free_float_fill.counts` 需增记 `filled_gt_float_clipped=549` |
| 下游 `build_state_vector.py:236` | 情绪位 `sent_ff_turnover = Σamount / Σ(free_float_share_filled × close) × 100`：2026-11-11 由 **3.559900 → 3.559904**（+0.000004，相对 +0.000118%）；实测最坏历史日 2014-04-01 由 1.294174 → 1.296686（+0.194%）；2016-06-07 +0.020%、2023-11-29 +0.012%。`sent_ff_turnover_q` 是 1,250 日滚动分位，需重跑 `build_state_vector.py` 才能给出位移，按幅度预计 0~1 档 |
| 下游 `stock_turnover` | **零影响**。`src/data/local_store/turnover.py:902-904` 的本地派生只读 `float_share`，全仓无任何 `stock_turnover` 口径读 `free_float_share(_filled)` |
| 断言 | 截断后 `filled > float` 违例 0 行，`check()` 默认额度即可通过，不再需要 `--allow-filled-gt-float` |

#### 口径 ②：维持 ffill 继承（不改数值）

| 影响项 | 具体内容 |
|---|---|
| 改哪个文件 | 不改数据；`derive_free_float_fill.py` 每次落盘都必须显式 `--allow-filled-gt-float 549`，否则断言失败 |
| 改哪些行 | 0 行 |
| 下游 | 情绪位与 `stock_turnover` 均保持现值（3.559900 / 3.559900 不变） |
| 代价 | 「自由流通 ≤ 流通」不再是不变量，退化为「人工批准的例外额度」；`002563.SZ` 这类个股自由流通市值高估 857%，个股级自由流通换手/市值加权会失真；额度数字一旦随表重建漂移，断言的门禁值也要跟着手改 |

### 4.4 推荐：口径 ①（截断到 `float_share`）

理由：

1. **语义上界天然成立**——自由流通股本是流通股本的子集，截断后该不变量由代码默认断言守护，不再依赖人工额度（DG-26 的本意）。
2. **影响量级可证明可忽略**——全市场情绪位最坏日 +0.194%、最新日 +0.000118%，不改变任何结论方向；个股层面把最大高估从 +857% 收敛到 0。
3. **保留 ffill 的覆盖率收益**——549 行仍然是「有值」而不是退回 `excluded`，不损失样本；`free_float_source` 三态语义不变。
4. **与源端口径同构**——采集阶段对「同行 `free > float`」本来就是跳过不填（772 行），截断是同一规则在派生阶段的延伸，而不是新造口径。

反面提示：截断会让 `ffill_250d` 这一态的取值不再是「上一次真实披露值」，而是「上一次披露值与本期流通股本取小」。此语义需在 `stock_shares.meta.json` 的 `free_float_fill.rule` 里补一句说明（一句文本改动，不改数据）。

### 4.5 回滚方式

| 层级 | 回滚动作 |
|---|---|
| parquet | `--apply` 前先 `copy2` 到 `_backup/stock_shares_<日期>/stock_shares.parquet`；回滚即 `copy2` 回原位（快照 SHA256 可逐字节校验） |
| 派生两列 | `derive_free_float_fill.py` 本身完全可复现（片7 实测重跑 diff 0/0），从 8 列事实表重跑即可回到任一状态 |
| 状态向量 | 已有 `_backup/state_vector_20261112_003938/`、`_backup/derived_1993b_20261113/meta/state/` 等快照；重跑 `build_state_vector.py` 可完全重建 |
| meta | `_backup/meta/stock_shares_meta_20261113/stock_shares.meta.json` 为本轮改动前快照 |

### 4.6 待批的两件改数 → **已批准，已同批执行**

1. **549 行截断**（口径 ①）：影响 1 个 parquet 列（派生列 `free_float_share_filled`）+ meta 计数 + 状态向量待重建。
2. **13 行规则内漏填**：新入口用「补全后的 `float_share`」判定，改的是 `free_float_share` 原列本身（正式数据）。

用户 2026-11-13 批复「截断口径批准，13 行漏填同批一次 `--apply`」，两件由**同一次** `fill_free_float_shares.py --apply` 完成，逐项验收见 §5。

---

## §5 数据步实测：一次 `--apply` 同时完成 549 行截断 + 13 行补填

### 5.1 改前备份与新旧 SHA256（DG-22）

| 项 | 值 |
|---|---|
| 改前整体 `copy2` | `data/local_store/meta/factors/stock_shares.parquet` → `_backup/meta/stock_shares_20261113/stock_shares.parquet` |
| `--apply` 内部自动快照 | `_backup/stock_shares_20261113/stock_shares_002922.parquet`（`copy2` + SHA256 校验后才落盘） |
| 改前 | 2,792,975 B / 166,284 行 / `sha256 2e721c87e57e1da8aa47fbd3ee490f2044a87d3624c2c996cea9bd4a95ba9b35` |
| 改后 | 2,794,729 B / 166,284 行 / `sha256 599b9ea1540d1c33524450603dd587d16497263e1d1e58b79be05f75963eedaa` |
| 执行命令 | `.venv\Scripts\python.exe tools\fill_free_float_shares.py --apply --raw "E:\www\tmp\em_equity_all_20261105.parquet"` |

### 5.2 四项验收（逐项实测，命令见 §6）

**① 行数守恒 / 键集合**

| 指标 | 值 |
|---|---|
| 行数 | 166,284 → **166,284**（`inner join` 后 166,284 行） |
| 丢失键（旧有新无） | **0** |
| 新增键（新有旧无） | **0** |

**② 改动行数（两类不重叠）**

| 类 | 行数 | 定义 |
|---|---|---|
| 549 行截断 | **549** | 改前 `free_float_share_filled > float_share` 的行，改后 `_filled == float_share`（逐行 `all()` 为 True） |
| 13 行补填 | **13** | 改前 `free_float_share` 为空、改后非空（`null→value` 13、`value→null` 0、`value→value` 0） |
| 两类交集 | **0** | 549 行原本就非空，13 行原本为空，天然不重叠 |
| `_filled` 取值实际改变 | **558 = 549 + 9** | 13 行补填里有 4 行「新原值恰好等于原继承值」，取值不变但状态由 `ffill_250d`/`excluded` 变为 `raw` |

三态位移：`raw` 148,587 → **148,600**（+13）、`ffill_250d` 1,145 → **1,134**（−11）、`excluded` 16,552 → **16,550**（−2）；13 行补填的状态迁移实测为 `excluded → raw` 2 行、`ffill_250d → raw` 11 行。

**③ 既有非空值改动数（DG-30 / DG-31）**

| 列 | 结果 |
|---|---|
| `free_float_share`（原列，旧非空 148,587 行） | **改动 0 行**（null 感知比较 `ne_missing`，不是 `!=`——后者遇 null 会静默为假） |
| `free_float_share_filled`（派生列） | 改动 558 行（= 549 截断 + 9 补填实际改值） |
| `float_share` | 截断 549 行的 `float_share` **逐值不变**（`all()` True） |
| `free_float_source` | 截断 549 行的三态 **逐值不变**（`all()` True） |

截断下调量：最大 **858,749,539 股**、中位 **282,892 股**、合计 **7,222,927,183 股**。

**④ 空值数**

| 列 | 改前 | 改后 |
|---|---|---|
| `free_float_share` | 17,697 | **17,684**（−13，与补填行数一致；任务书预期值吻合） |
| `free_float_share_filled` | 16,552 | **16,550**（−2：13 行补填中 11 行原已有 ffill 继承值，只有 2 行从 `excluded` 转为有值） |

### 5.3 断言回默认额度 0

`derive_free_float_fill.py` 的 `--allow-filled-gt-float` **已从代码中删除**，不存在「保留 549 额度才能过」的状态：

```text
行数 166284（守恒）  三态 {'rows': 166284, 'raw_rows': 148600, 'ffill_250d_rows': 1134,
'excluded_rows': 16550, 'null_rows_total': 17684, 'filled_gt_float_rows': 0}
dry-run：未写盘
```

`--apply` 内部同样跑两次 `check()`（落盘前 + 读回后），两次都过；且 `check()` 的 `filled ≤ float` 违规计数实测 0。

### 5.4 可复现性：`derive()` 重放逐字节一致

```text
.venv\Scripts\python.exe -c "... 读正式表 8 列 → derive(base, calendar) → write_parquet_atomic(_staging/free_float_shares_em_20261113/replay_check.parquet)"
replay_sha 599b9ea1540d1c33524450603dd587d16497263e1d1e58b79be05f75963eedaa
formal_sha 599b9ea1540d1c33524450603dd587d16497263e1d1e58b79be05f75963eedaa
byte_identical True    rows_equal True
```

即：整张表可由「8 列事实 + 交易日历」在仓库内完全重建，两次运行字节级一致（DG-20 的可复现证据）。

### 5.5 DG-23：正式读路径端到端

用真实消费方 `tools/build_state_vector.load_shares()`（只读 `_SHARE_COLUMNS = ts_code / effective_date / free_float_share_filled / free_float_source`）：

| 断言 | 实测 |
|---|---|
| 读到行数 | **166,284**（= 新文件行数，证明读到的是新文件） |
| `free_float_share_filled` 空值 | **16,550**（与新文件一致；改前为 16,552） |
| `600387.SH` 最新记录 | `2026-06-30 / 274,075,882 / ffill_250d` —— 恰等于其 `float_share`，即 §4.1 中 +14.61% 违例已被截断 |
| `002563.SZ` 最新记录 | `2026-06-30 / 704,866,665 / raw` |
| `shares_as_of()` 8 列口径 | 原列 `free_float_share` 不变，派生列新增可见 |

### 5.6 manifest / meta / changelog 同步

| 文件 | 变更 | 校验 |
|---|---|---|
| `data/local_store/meta/factors/manifest.json` | `files[0]`：rows 166,284 / bytes 2,794,729 / sha256 `599b9ea1…` / date_range 重算 | `manifest_tool.py --check --only factors` → **PASS**（11 记录 / 11 一致 / 不一致 0 / 无法定位 0） |
| `data/local_store/meta/factors/stock_shares.meta.json` | `built_at`、`rows`、`columns.free_float_share_filled`（补截断语义）、`free_float_fill.rule`（补截断句）、`counts`（raw 148,600 / ffill 1,134 / excluded 16,550 / null_rows_total 17,684 / filled_gt_float_clipped 549）、`backup_file` / `backup_sha256`（→ `_backup/stock_shares_20261113/stock_shares_002922.parquet` / `2e721c87…`）、`output_sha256`（`599b9ea1…`）、`provenance.{filled_rows=13, kept_rows=148587, filled_gt_float_clipped=549, input/output_sha256, applied_at}` | `json.loads` 通过；字段与磁盘实测逐项一致 |
| `logs/data_changelog.csv` | 本任务 5 行（`parquet`×2、`meta.json`×2、`manifest.json`×1） | 见下表 |

全局 `manifest_tool.py --check`（不带 `--only`）当前 FAIL 6 条，均为他路在途数据（如 `universe_price_jumps`），**与 `meta/factors/` 无关**；本表 scoped 检查 PASS。

changelog 追加证据（`run_20261113_002859 / 002922 / 003030`）：

| 字节偏移 | 行长 | run | 路径 | bytes | sha256 → prev |
|---|---|---|---|---|---|
| 1,979,015 | 304 | `002859` | `…/stock_shares.parquet` | 2,794,729 | `599b9ea1` ← `2e721c87` |
| 1,979,320 | 304 | `002922` | `…/stock_shares.parquet` | 2,794,729 | `599b9ea1` ← `2e721c87` |
| 1,979,625 | 300 | `002922` | `…/stock_shares.meta.json` | 4,044 | `fe3a7284` ← `71d6414b` |
| 1,979,926 | 279 | `003030` | `…/stock_shares.meta.json` | 4,265 | `962ca70d` ← `fe3a7284` |
| 1,980,206 | 270 | `003030` | `…/factors/manifest.json` | 2,008 | `a04fa6ac` ← `c28854aa` |

前缀自证：以 `G:\git\python-quant\logs\data_changelog.csv`（1,979,926 B / 10,370 行）为基线，当前源文件 1,980,757 B / 10,373 行，`源.startswith(基线)` = **True**，差额 831 B / 3 行，**全部落在文件末尾**——说明本次是纯追加，既有字节一个未改。差额中 2 行为本任务 `003030` 的 meta / manifest 记账，另 1 行为另一路 `fix_st_manifest` 在 00:34:33 的追加（不属本任务）；本任务此前的 parquet / meta 三行已位于基线之内。

> 说明：`002859` 与 `002922` 两行是同一份输出的两次尝试——首次 `--apply` 落盘后内联复核因代码缺陷崩溃（`reread` 丢派生列），随后从 `_backup/meta/stock_shares_20261113/` 还原（校验 `2e721c87…`）并重跑；新一次输出与前次**字节完全相同**（`599b9ea1…`），故两行 sha256 一致、prev 均为改前值。缺陷已修（`derive_check(reread, base)`），见 §2。

---

## §6 复现命令

```powershell
# D1 逐值不变证明（只读，两份 raw 各验一次）
.venv\Scripts\python.exe tools\fill_free_float_shares.py --verify --raw "E:\www\tmp\em_equity_all_20261105.parquet"
.venv\Scripts\python.exe tools\fill_free_float_shares.py --verify

# D1 抓取（单连接串行 + 0.25s 节流，断点在 _staging/free_float_shares_em_<日期>/）
.venv\Scripts\python.exe tools\fill_free_float_shares.py --fetch

# 数据步：一次 --apply 完成 549 行截断 + 13 行补填（改前 copy2 备份 + 新旧 SHA256）
.venv\Scripts\python.exe tools\fill_free_float_shares.py --apply --raw "E:\www\tmp\em_equity_all_20261105.parquet"

# D2 断言（额度固定 0，无任何放行开关）
.venv\Scripts\python.exe tools\derive_free_float_fill.py --dry-run

# manifest 校验
.venv\Scripts\python.exe tools\manifest_tool.py --check --only factors

# 静态检查与单测
.venv\Scripts\python.exe -m ruff check tools\fill_free_float_shares.py tools\derive_free_float_fill.py tests\test_free_float_fill.py
.venv\Scripts\python.exe -m pytest tests\test_free_float_fill.py tests\test_stock_shares.py -q
```

## §7 文件指纹（DG-20）

| 文件 | 字节 | 行 | SHA256 |
|---|---|---|---|
| `tools/fill_free_float_shares.py`（工具入口） | 23,586 | 510 | `79f4db3de7cbb52ad8cbd48db815fd675299ccc2fc7cae2ad496b80fe04c6e54` |
| `tools/derive_free_float_fill.py` | 13,368 | 304 | `2913c6c906c997ae16160c824636a8cc2b8d46f4b1683b5d5263da72840c4617` |
| `tests/test_free_float_fill.py`（新增） | 7,928 | 181 | `7d929f3b915a31b1dee1e63fd26c8cf827500425b7a7bd486c305c43f742ed40` |
| `data/local_store/meta/factors/stock_shares.parquet`（**改后正式表**） | 2,794,729 | — | `599b9ea1540d1c33524450603dd587d16497263e1d1e58b79be05f75963eedaa` |
| `data/local_store/meta/factors/stock_shares.meta.json` | 4,265 | 71 | `962ca70ddb5747a160d7bf5574a129ffdc1c271752a94cf1fae23c371a0588b5` |
| `data/local_store/meta/factors/manifest.json` | 2,008 | 33 | `a04fa6ac2b5985d85eae9a45c4eaed5ad6800aa8ce502ab9b03c7ddeb36424d3` |
| `_backup/meta/stock_shares_20261113/stock_shares.parquet`（改前快照） | 2,792,975 | — | `2e721c87e57e1da8aa47fbd3ee490f2044a87d3624c2c996cea9bd4a95ba9b35` |
| `_backup/stock_shares_20261113/stock_shares_002922.parquet`（`--apply` 自动快照） | 2,792,975 | — | `2e721c87e57e1da8aa47fbd3ee490f2044a87d3624c2c996cea9bd4a95ba9b35` |
| `_backup/meta/stock_shares_meta_20261113/stock_shares.meta.json`（D6 改前快照） | 2,908 | 51 | `c09f419ef954beb8d9170981e03ea727750b927b046f2c59e75a3b95c7d239f9` |
| `_staging/free_float_shares_em_20261113/raw.parquet` | 3,477,676 | — | `3d513e75c95554b79195f7c31ce36156c22795ebd9793da0990d8508bc61f198` |

## §8 未做与归属

| 事项 | 归属 / 状态 |
|---|---|
| `docs/数据权威路径清单.md:39` 改写 | **已由文档那一路落库**（提交 `8125b4a`，逐列标注 7 列 + `free_float_share` 单独登记为 `tools/fill_free_float_shares.py`） |
| 549 行截断 / 13 行补填 | ✅ 已完成（§5） |
| **`sent_ff_turnover` 陈旧 → 派生层需重建** | ⏸ **本轮未重建，列为待办**（见 §8.1） |
| 片7 报告里 D3~D5 / D7~D9 的其余缺陷 | 不在本任务范围，仍在 `b1_slice7_check.md` §6 待办 |

### 8.1 待办：`sent_ff_turnover` 变化导致派生层陈旧

本次截断让 549 行的 `free_float_share_filled` 下调，`tools/build_state_vector.py` 的情绪位分子分母都依赖该列（`sent_ff_turnover = Σamount / Σ(free_float_share_filled × close) × 100`，[:236](file:///e:/code/python/quant/tools/build_state_vector.py#L236)），因此**现存 `state_vector_daily` 是用旧股本算出来的，已陈旧**。用户要求本轮**只登记不重建**，等派生层解冻后统一重跑。

受影响派生表清单（按消费关系实测，`grep free_float_share_filled` 全仓仅 6 个文件命中）：

| 派生表 / 产物 | 受影响字段 | 影响方式 | 重建入口 |
|---|---|---|---|
| `data/local_store/meta/state/state_vector_daily.parquet` | `sent_ff_turnover`、`sent_ff_turnover_q` | 直接依赖：549 行截断 → 分母 `ff_mv` 下降 → 比值上升。已实测单日幅度：2026-11-11 `3.559900 → 3.559904`（+0.000118%）、最坏历史日 2014-04-01 `1.294174 → 1.296686`（+0.194%）；`_q` 为 1,250 日滚动分位，位移量需重跑后实测 | `tools/build_state_vector.py` |
| `data/local_store/meta/state/manifest.json` + 同目录 `*.meta.json` | rows/bytes/sha256/计数 | 随上表重跑同步 | `tools/manifest_tool.py` / 脚本内置 |
| `stock_turnover`（`src/data/local_store/turnover.py`） | 无 | **零影响**：[:902-904](file:///e:/code/python/quant/src/data/local_store/turnover.py#L902-L904) 的本地派生只读 `float_share`，全仓无任何 `stock_turnover` 口径读 `free_float_share(_filled)` | 无需重建 |
| 其余派生表 | 无 | `sent_ff_turnover` 在仓库内只被 `tools/build_state_vector.py` 与 `tools/verify_state_vector.py` 读取（`grep sent_ff_turnover` 仅这 2 个源文件 + 1 个测试），无第三张表消费 | — |

重建时必须先记录改前快照（DG-11 → 仓库根 `_backup/`），再 `build_state_vector.py` 全量重跑，并按 DG-20 给出四项证据与 `sent_ff_turnover_q` 的实际位移行数。
