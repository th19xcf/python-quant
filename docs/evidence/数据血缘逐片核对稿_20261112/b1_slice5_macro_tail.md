# B1 片5 收尾：宏观族三项（10Y 补齐 / 信用曲线可疑段 / 拼接缝 unresolved）

- 执行时间：2026-11-12 23:56 ~ 2026-11-13 00:05（Asia/Shanghai）
- 范围：`meta/macro/cn_treasury_yield_10y.parquet` 补 1 日 + 信用曲线 manifest 增字段 + 缺口登记一条
- 未触碰：`docs/数据权威路径清单.md`（另一路在改）、任何派生层表、`data/` 下删除与搬迁、Git 历史
- 背景：`2026-11-12/data-lineage/outputs/b1_slice5_bond_fix.md`（三张债券表已补到 2026-11-12，10Y 表单期限表停在 11-11）

---

## 0. 结论摘要

| 项 | 结论 |
|---|---|
| 一、10Y 补到 2026-11-12 | 已补，**仅追加 1 行**（`2026-11-12 / 1.6032 / chinabond`）；行数 5,464 → 5,465，既有值改动 0、空值 0→0、与三表末日对齐 |
| 二、信用曲线 `suspect_segments` | 已写入 manifest（7 年 / 2008-01-02~2008-10-16 / 197 行 / 4.0 / `source_placeholder`），判据数字全部重算确认；parquet 未动 |
| 三、10Y 拼接缝 unresolved | 已登记为 **G-117**（G 类，待核实含 unresolved），两段存量保留、不择一覆盖 |
| 派生层 | **全程未触碰**；「派生层解冻后需跟进重建」列为待办（§4.1） |

---

## 1. 一、`cn_treasury_yield_10y.parquet` 补到 2026-11-12

### 1.1 走的是它自己的常规入口

```
.venv\Scripts\python.exe tools\treasury_yield.py collect --refresh-current
.venv\Scripts\python.exe tools\treasury_yield.py build
.venv\Scripts\python.exe tools\treasury_yield.py publish --yes --stamp 20261112_2345
```

**为什么必须加 `--refresh-current`（实测依据，不是习惯性操作）**：该入口的缺口判定取
「`000001.SH` 交易日 ∩ `>= SWITCH`」，而本地交易日历 `data/local_store/quotes/index/sh/000001.SH.parquet`
**末日只到 2026-11-11**。因此 2026 年分片被判 `ok` 并被 `[SKIP]`：

```
treasury_chinabond_2026 skip（已覆盖应有交易日 206 天）      ← 不带 --refresh-current
treasury_chinabond_2026 ok->fetched rows=213 2026-01-04..2026-11-12 still_missing=0   ← 带 --refresh-current
treasury_eastmoney fetched rows=5465 2005-01-04..2026-11-12
```

两源本次都已发布 2026-11-12（中债 213 行、东财 5,465 行），故 `post_eastmoney_fill=0`，
新增行来源为 `chinabond:bond_china_yield`。

### 1.2 改前备份与 SHA256

| 文件 | 改前 SHA256 | 改后 SHA256 | 字节（改前→改后） |
|---|---|---|---|
| `cn_treasury_yield_10y.parquet` | `fa16b05b2a6ebb4ff2ac667e848729c5bfe6304ba2b31107dad533405aef4e42` | `16dd083c563dbae922820ae7d6460efca56eb62566b524e29d41b3076dba9803` | 34,727 → 34,729 |
| `cn_treasury_yield_10y.csv` | `f2f67b65219cc5c5a1ad9a49584515b6b9c8c79f625857af0144406b0f4658bd` | `cf4a78635acbece69cd378d9e0c54efa1ea0252ab6fb6aec0bfe025a8b2336e9` | 244,423 → 244,468（5,466 行 = 表头 + 5,465） |
| `manifest.json` | `f6b54c352950c7e85b3cb3c8b98ee503772f7a93ffd513869120e8a7f4a31c4d` | `f050e29aebb13a22cbd13457d3ecda33b9dc7512321ddfeaaae41a9632974edf` | 27,098 → 27,252 |

两份改前完整副本（内容与 SHA 逐文件相等）：

- `_backup/meta/macro_20261112_pre_treasury_10y/`：任务要求的 `copy2` 备份（3 文件，parquet 34,727 B / `fa16b05b…`）
- `_backup/meta/macro_treasury_20261112_2345/`：入口 `publish` 内置的 DG-11 整体移动快照（同一份内容）
- 写入走 `write_parquet_atomic` / `write_text_atomic`（DG-9），落盘后 sha256 与 manifest `files` 段逐字段一致

### 1.3 四项验收（DG-20）

复算：`_staging/macro_tail_20261112/verify_10y.py`（只读，比对改前快照与当前正式文件）

| 验收项 | 结果 |
|---|---|
| **行数守恒** | 5,464 → **5,465**（+1）；新增键 `2026-11-12` 共 **1** 个，丢失键 **0** 个 ⇒ +1 = 1 日 × 1 期限（单期限表） |
| **既有值改动数 0** | 重叠 5,464 行，`y10` 改动 **0**、`source` 改动 **0** |
| **空值计数不变** | `date` / `y10` / `source` 三列空值 0 → 0；新增行本身空值 0/0/0 |
| **与三表日期段对齐** | 本表末日至 **2026-11-12**；`cn_bond_index` 18,651 行至 11-12、`cn_treasury_yield_curve` 41,416 行至 11-12、`cn_credit_yield_curve` 33,015 行至 11-12，四表末日一致 |

追加行取值与交叉对拍：

```
2026-11-12  y10 = 1.6032  source = chinabond:bond_china_yield
对拍 cn_treasury_yield_curve 同日期 tenor=10年  ytm_pct = 1.6032
重叠 1 日，最大绝对差 0.000000 pp
```

### 1.4 一个必须记账的边界：新行不在本地交易日历内

本地 `000001.SH` 日历止于 2026-11-11，因此 2026-11-12 这一行**超出本地日历覆盖**：

- `coverage_report` 报 `missing_vs_calendar=0`，是因为窗口右端取的是本表末日 11-12、而日历里没有 11-12，**不是**「11-12 已被日历验证」；
- 影响：任何按交易日历对齐的消费者本轮不会取到 11-12（日历先要更新），届时该行自动生效；
- 不自行扩日历：日历由行情链路发布，本任务不越界改。此项与 G-117 无关，记在此处备查。

---

## 2. 二、信用曲线 manifest 增 `suspect_segments`

### 2.1 写入内容

`data/local_store/meta/macro/cn_credit_yield_curve.manifest.json` 新增顶层键 `suspect_segments`（数组，1 条）：

| 字段 | 值 |
|---|---|
| `tenor_years` / `tenor` | `7.0` / `7年` |
| `start` / `end` | `2008-01-02` / `2008-10-16` |
| `rows` | `197`（该窗口 7Y 行**全部**是该常量，占比 100%，且连续无缺口） |
| `value` | `4.0` |
| `verdict` | `source_placeholder`（源端缺数填充，不是真实报价） |
| `evidence` | 同窗口 7Y 唯一值 **1**；其它期限唯一值 3月 186 / 6月 188 / 1年 153 / 3年 117 / 5年 118；**191/197** 个交易日 `5Y>7Y`；同批国债 7Y 在该窗口 **189/197** 个不同取值；源端 10Y / 30Y 同窗口均返空（各 0 行）作缺数标记对照；与国债 7Y 的负利差 **118** 行、最小 **−0.4044 pp**；2008-10-17 起恢复真实报价（首值 4.0351） |
| `consumer_action` | 派生层使用该段 7Y 时必须整段排除或显式标记；118 行负利差不是真实信用利差 |
| `note` | 不改已入库原始观测（DG-16 / DG-18）；定性见 `b1_slice5_bond_fix.md` §3 |

旧 `known_gaps` 保持原样（AA/AA+ 未采），新字段与它并列、不合并。

### 2.2 判据数字是重算的，不是从文档抄的

复算：`_staging/macro_tail_20261112/recheck_credit_7y.py`（只读）——逐项与 `b1_slice5_bond_fix.md` §3.1 相符：
7Y 全段 4,714 行、常量段 197 行、常量段连续、同窗口 3年/5年 极差 1.6250 / 1.7720 pp、
`5Y>7Y` 191/197、负利差 118 行（−0.4044 ~ −0.0004 pp，全部落在常量段）、窗口内 10Y/30Y 各 0 行。

### 2.3 为什么顺手改了生产者（否则字段会丢）

`tools/build_credit_yield_curve.py` 在落盘时**整体重建** manifest 字典（L237-292），
只手工改当前 JSON 会在下次重建时被覆盖掉。因此把该段提为模块常量 `SUSPECT_SEGMENTS`（L62-94），
并在 manifest 字典里 `'suspect_segments': SUSPECT_SEGMENTS`（L290），口径文字与数字均带注释说明。

- `cn_credit_yield_curve.parquet` **未改动**：rows 33,015 / sha256 `435e4655e7ed…` 与改前一致
- manifest 改前副本：`_backup/meta/macro_20261112_pre_credit_suspect/cn_credit_yield_curve.manifest.json`（1,751 B / `b24a49cc…`）
- manifest 改前 → 改后：`b24a49cc4e8b9082bedf8f3014b6f37ebb1b1e0a893c51a14104e3be9e40975a` → `6b6703d8f6fb8425e4c3661d83f7012ebbb4e55610191002adbc3a81db8f1e17`
- 静态检查：`ruff check` All checks passed / `mypy` Success

---

## 3. 三、10Y 拼接缝 unresolved 入册（G-117）

### 3.1 落库位置

`docs/数据缺口登记_正式表_2026-11-12.md` §3 正式登记表末尾追加 **G-117**（编号接 G-116，G 类：利率/信用/债券指数，核实标记「实测」，判定「待核实（含 unresolved）」）。

### 3.2 行内写了什么

现状（全部重算，复算入口 `_staging/macro_tail_20261112/recheck_10y_seam.py`）：

```
eastmoney:bond_zh_us_rate   288 行   2005-01-04 ~ 2006-02-28
chinabond:bond_china_yield  5,177 行 2006-03-01 ~ 2026-11-12
重叠交易日 0
mean|Δy10|  东财段 0.056509 / 中债全段 0.016857 / 中债 2006-2010 段 0.018344
倍数        3.352x（对全段）/ 3.081x（对同期段）
东财段周末行 9 / 288；中债段 0
东财段取值 2.7938 ~ 5.2221；中债段起点 2.9000
缝两侧 2006-02-28 东财 2.9266 → 2006-03-01 中债 2.9000，跨 1 个交易日差 0.0266 pp
```

- **异常信号**：零重叠（无法用重叠日证明拼接一致）+ 东财段波动为中债段 3.35 倍 + 含 9 个非交易日行
- **判定**：待核实（含 unresolved）、DG-18 保留两段存量、不择一覆盖
- **影响面**（比原建议更准确）：`src/` 下确实无读取方，但 `tools/build_state_vector.py:407` 把本表作为六维状态矢量
  **维度①（10Y 利率）** 的输入、`tools/verify_state_vector.py:332` 做校验——288 天的日间噪声会经状态矢量向后传播，
  而不是「零影响」。原文写的「`src/` 下无直接读取方」只覆盖 `src/`，已按实际读者补全。
- **不覆盖的四条理由**与**四个重开条件**（中债官方 2005-01~2006-02 月报对拍 / wind 第三源 / 取到后走 DG-11 / 确认不可用再定「改标口径」或「退役该段」）均已写入行内

### 3.3 同步更新的计数（避免本表自检失真）

| 位置 | 改动 |
|---|---|
| §0.4 归属任务简写 | 新增 `宏观债券-20261112` |
| §2 分类索引 | G 类 5 → **6**（加 G-117）；合计 111 → **112** |
| §5.1 | 有效条目 111 → **112**；编号范围 G-001~G-117；计数恒等式 117−4−1=**112**；四档 72/**25**/15/1 = **113** 行（追加「宏观债券收尾轮新增 G-117 一条未收口」） |
| §5.2 | 实测 51 → **52**，补 G-117 取证说明 |
| §5.5 | 未收口 24 → **25**（补 G-117）；自检 72+25+15 = **112** 有效 + 1 预留 = **113** 行 |

程序化自检（文档 §5.1 自带的复现命令 + 四档交叉核对）：

```
rows 113 / unique 113 / dup {}            # 表格行数与唯一编号
range 1..117 / missing []                 # 无缺号（G-040~043 为已登记退役号）
table ids 113 / tier union 112 / overlap [] ; ids not in tiers ['086']
已收口 72 + 未收口 25 + 无法收口 15 = 112 有效（+ 预留 G-086 = 113 行）
```

---

## 4. 边界与待办

### 4.1 必须跟进的待办（本任务明确不做）

1. **派生层解冻后需跟进重建**：`cn_treasury_yield_10y.parquet` 末日至 2026-11-12，
   以它为输入的 `meta/state/state_vector_daily.parquet`（维度① 利率）及依赖状态矢量的后续派生表，
   都需在派生层解冻后重建对齐；本轮**未触碰任何派生层表**。
2. **G-117 的重开条件已写进行内**，取得第三源前不得改这两段，也不得把东财段当作「已确认错误」处理。
3. **本地交易日历未到 2026-11-12**：日历更新后，11-12 行才会被按交易日历对齐的消费者取到（§1.4）。

### 4.2 边界确认

- 只写了 1 张基础层 parquet + 其 CSV 可读副本 + 1 份 `meta/macro/manifest.json` + 1 份 `cn_credit_yield_curve.manifest.json` + 1 份缺口登记文档
- 未删除 `data/` 任何内容；`_backup/meta/` 下三份改前快照齐全（§1.2 / §2.3）
- 未改 `docs/数据权威路径清单.md`；未改任何派生层表
- 采集单连接串行 + 内置 1.5 s 节流，未并发、未加压重试；全程未出现限流 / 封禁 / 返回空

### 4.3 复算入口

```
.venv\Scripts\python.exe _staging\macro_tail_20261112\verify_10y.py        # 10Y 四项验收 + 与三表对齐 + SHA
.venv\Scripts\python.exe _staging\macro_tail_20261112\recheck_credit_7y.py # suspect_segments 全部判据数字
.venv\Scripts\python.exe _staging\macro_tail_20261112\recheck_10y_seam.py  # G-117 分段/波动/缝两侧
.venv\Scripts\python.exe tools\treasury_yield.py publish                   # dry-run：当前应为 rows_to_add=0
```

断点与失败清单（DG-7，本任务 staging）：`_staging/macro_tail_20261112/progress.csv`、`_staging/macro_tail_20261112/failed.csv`（后者仅表头，无失败项）。
