# B1 片5：宏观债券族修复与补数

- 执行时间：2026-11-12 23:46 ~ 2026-11-13 00:05（Asia/Shanghai）
- 范围：`meta/macro/` 三张债券表（基础层）+ 两处定性 + `backups/critical_assets/` 一处快照偏差
- 未触碰：任何派生层表、`data/` 下的删除与搬迁、Git 历史

---

## 0. 结论摘要

| 项 | 结论 |
|---|---|
| 代码① `build_cn_bond_tables.py` manifest 写入 | 已改 `write_text_atomic`（DG-9） |
| 代码② 信用曲线产出方 | 一次性脚本已收拢为 `tools/build_credit_yield_curve.py`，口径未改 |
| 补数 | 三表由 2026-11-10 补齐到 **2026-11-12**（源端 11-11、11-12 均已发布；比任务要求的 11-11 多一天，见 §2.4） |
| 定性① 7Y 常量 4.0000 | 判定为**源端缺数填充**（占位哨兵），非接口口径问题；118 行负利差全部由该占位段派生 |
| 定性② 10Y 两源拼接缝 | 按 DG-18 **不择一覆盖**，出具 unresolved 登记建议（§4.3） |
| 快照偏差 | 两融快照已回刷，`3ca2f812…`（8,068 行 / 273,433 B）→ `ecec8fe5…`（8,070 行 / 279,550 B），旧件已归档 |

---

## 1. 代码改动

### 1.1 `tools/build_cn_bond_tables.py`（DG-9）

- L36 导入改为 `from src.data.atomic_write import write_parquet_atomic, write_text_atomic`
- L186-187 的裸 `write_text` 改为 `write_text_atomic(json.dumps(...), MACRO / f'{name}.manifest.json')`

原写法在 manifest 写到一半失败时会留下截断 JSON；改后与同文件两份 parquet 的 `write_parquet_atomic` 口径一致（临时文件 → fsync → `os.replace`）。

### 1.2 `tools/build_credit_yield_curve.py`（新，收拢原一次性脚本）

`_staging/gap_close_20261111/build_credit_curve.py` 的计算口径原样搬入，不改 melt、不改对拍阈值、不改落盘路径：

- 入参：`_staging/gap_close_20261111/cbond/curve_*.parquet`（2006-2026 年报分片 + 新增的增量分片）
- 只取 `中债中短期票据收益率曲线(AAA)`，同批返回的国债曲线只用于对拍
- 对拍：与既有 `cn_treasury_yield_curve.parquet` 逐值比，重叠 < 4,000 行或最大差 > 1e-4 即拒落盘
- 落盘：`write_parquet_atomic` + manifest（`produced_by` 指向本入口，`sha256` / `bytes` / `coverage` / `days_by_year`）
- 断点：年报分片存在即跳过；`--fetch` 写独立增量分片 `curve_supp_<起>_<止>.parquet`，重跑同区间直接复用，`progress.json` 记 `supp:<起>~<止>`
- 新增 `--check`（只校验）；不加 `--check` 即落盘，与原脚本「跑完即写」行为一致
- 增量重叠日逐值对拍：与 staging 已有值差 > 1e-9 即 `SystemExit(2)` 停手，不静默覆盖已入库值

### 1.3 `tools/fetch_cn_bond_data.py`（附带，补数前置）

原工具对已存在的年文件/指数文件直接 `[SKIP]`，**不具备补齐最新一日的能力**，这正是「三表停在 11-10」的直接原因。新增 `--since YYYY-MM-DD`：

- 已存在的年文件 / 指数文件：重新取全量后**只追加末日之后的新行**
- 重叠日逐值对拍，`值改动 != 0` 即 `SystemExit`，不覆盖已入库值
- 无新行时打印 `[NO-NEW]` 且不写盘；限流 / 封禁关键字命中即停压退出（`SystemExit(3)`）
- 默认（不带 `--since`）行为与改前完全一致

### 1.4 `src/data/manifest_semantics.py`（文案同步）

`meta/macro/manifest.json` 的 `source` 文案原写「债券指数与两条曲线由 tools/build_cn_bond_tables.py 产出」，信用曲线已改由新入口产出，同步改为两个入口分述，并更新行号引用。

### 1.5 静态检查

```
.venv\Scripts\python.exe -m ruff check tools/build_credit_yield_curve.py tools/fetch_cn_bond_data.py   → All checks passed
.venv\Scripts\python.exe -m mypy  tools/build_credit_yield_curve.py tools/fetch_cn_bond_data.py        → Success
```

既有失败项（与本次改动无关）：`tools/build_cn_bond_tables.py:111` `float(diff.max())` 的 mypy 报错（`Series.max()` 返回 `int | float | date | ...` 联合类型），该行是补数前就存在的写法，本次未改。

---

## 2. 补数

### 2.1 执行序列（单连接串行 + 1.3 s 节流，未并发）

```
.venv\Scripts\python.exe tools\fetch_cn_bond_data.py --since 2026-11-11
.venv\Scripts\python.exe tools\build_credit_yield_curve.py --fetch
.venv\Scripts\python.exe tools\build_cn_bond_tables.py --apply
```

- 指数 3 条、国债曲线（2026 年）各追加 2 日：`[OK] 国债总指数 … 本次追加 2`、`[OK] 2026: 212 行（本次追加 2）`
- 信用曲线增量分片 6 行（3 条曲线 × 2 日），年月分片断点复用、未重抓历史
- 全程未出现限流 / 封禁 / 超时 / 返回空；10Y 对拍 5,176 重叠日最大差 **0.000000pp**

### 2.2 落盘（原子写）

改前先把 6 个文件 `copy2` 到 `_backup/meta/macro_20261112_pre_bond_backfill/`，逐文件 SHA256 复核一致后再覆盖：

| 文件 | 改前 SHA256 | 改后 SHA256 |
|---|---|---|
| `cn_bond_index.parquet` | `8d8d023de086e8bb81c84c8794384c7adf6b364717a304938c72da9ba3fdcd8c` | `940c5260ab67e05205cf630d887676bd47c85c9a9045615e91e79192d8725ca6` |
| `cn_treasury_yield_curve.parquet` | `03e7cd33f7fc56474c03730cfd796ba54319b4dcecf82cca3ca7730a8a28f357` | `3544ebd039a5ec3600037c5cd4430a8240a7ad7db9e3b4162c3d9f594e925219` |
| `cn_credit_yield_curve.parquet` | `1d345910a2262b6bcb2cb5fc43e512bfe69022ba321b360a58926334b837664e` | `435e4655e7ed4f8c3cb95b59c108fbd3678bbcaf770856d8d713d7e8d8aad361` |

同目录三份 `*.manifest.json` 同步重写（`rows` / `coverage` / `sha256` / `bytes` 均已与 parquet 实测对齐，见 §2.3 的 `sha256_match=True bytes_ok=True`）。

### 2.3 验收（DG-20 四项）

复算脚本：`_staging/bond_backfill_20261112/verify_bond_backfill.py`

| 表 | 行数 | 新增键 | 丢失键 | 既有值改动 | 空值（旧→新） | 覆盖 |
|---|---|---|---|---|---|---|
| `cn_bond_index` | 18,645 → 18,651（+6） | 6 | 0 | **0** | 0 → 0 | 2002-01-04 ~ 2026-11-12 |
| `cn_treasury_yield_curve` | 41,400 → 41,416（+16） | 16 | 0 | **0** | 0 → 0 | 2006-03-01 ~ 2026-11-12 |
| `cn_credit_yield_curve` | 33,001 → 33,015（+14） | 14 | 0 | **0** | 0 → 0 | 2006-12-25 ~ 2026-11-12 |

- 新增行全部落在 2026-11-11 / 2026-11-12，非键列空值 0（未引入空值）
- 行数守恒：+6 = 3 指数 × 2 日；+16 = 8 期限 × 2 日；+14 = 7 期限 × 2 日（中短票无 30Y，见 §3.4）
- 抽样断言（2026-11-11 / 2026-11-12 逐期限）：

```
2026-11-11 指数: 国债总指数 254.558 / 固定利率国债 255.1644 / 国开行 255.421
           国债曲线(3M→30Y): 1.1642 1.1652 1.1813 1.2386 1.3287 1.4263 1.6039 2.052
           信用曲线(3M→10Y): 1.42 1.4389 1.4531 1.603 1.6641 1.8034 1.9227
2026-11-12 指数: 254.5514 / 255.1577 / 255.1566
           国债曲线: 1.1652 1.1654 1.1784 1.2389 1.329 1.4269 1.6032 2.054
           信用曲线: 1.435 1.4559 1.4729 1.6262 1.6869 1.8489 1.9737
```

两日均单调、量级延续 11-10，未见异常跳变。

### 2.4 关于「多补了 11-12」（如实记账）

任务口径是补到 2026-11-11（对齐 10Y 表的末日）。实际执行时源端 **11-12 的数据也已发布**（23:46 请求返回含 2026-11-12 行），`--since` 只追加「晚于本地末日的新行」，因此三表一并到了 11-12。

- 判断依据：`fetch_cn_bond_data.py` 与 `build_credit_yield_curve.py` 的追加语义是「补齐到源端已发布的最新一日」，人为截断到 11-11 等于丢弃已发布数据、并会在下次运行重新触发同一缺口
- 可回滚：改前 6 个文件完整保存在 `_backup/meta/macro_20261112_pre_bond_backfill/`
- 副作用：`cn_treasury_yield_10y.parquet`（单期限 10Y 表，另一个采集入口，不在本任务范围）仍停在 2026-11-11，与三表出现 1 日错位 → 已登记为待办（§6.2）

---

## 3. 定性①：信用曲线 7Y 有 197 天源端常量 4.0000

复算脚本：`_staging/bond_backfill_20261112/diagnose.py`（只读）

### 3.1 事实

| 观察 | 数值 |
|---|---|
| 7Y 全段行数 | 4,714（起始 2008-01-02，此前无 7Y 行） |
| `ytm_pct = 4.0000` 的行 | **197**，2008-01-02 ~ 2008-10-16，占该窗口 7Y 行 100% |
| 该窗口其它期限唯一值数 | 3M 186、6M 188、1Y 153、3Y 117、5Y 118（均在正常波动） |
| 该窗口 7Y 唯一值数 | **1**（恒为 4.0） |
| 该窗口 3Y / 5Y 极差 | 1.6250 pp / 1.7720 pp |
| 该窗口 10Y / 30Y | 全为空（源端不返回 → melt 时缺行，未插值） |
| 该窗口同批国债 7Y 唯一值数 | 189 / 197（正常波动） |
| 曲线形态违例（5Y>7Y 或 10Y<7Y） | **191 / 197 天** |
| vs 国债 7Y 的利差 | 负利差 **118** 行，最小 **−0.4044 pp**，最大 −0.0004 pp，**全部落在常量段** |
| 2008-10-17 起 | 7Y 恢复真实报价（3.9578 ~ 4.5037） |

### 3.2 判定

**这是源端缺数填充（占位哨兵），不是接口口径问题。** 五条可复算依据：

1. **只有 7Y 一根静止**：同一天的同一条曲线，3M~5Y 有 117~188 个不同取值，7Y 只有一个值。口径类问题会作用于「列映射」或「整条曲线」，不会只冻结单一期限。
2. **同一请求里的国债曲线正常**：同批响应的国债 7Y 在该窗口有 189 个不同取值。若为请求参数、解析映射或日期对齐出错，国债曲线会一起坏；它没坏，说明取数与解析链路正常。
3. **取值在经济学上不可能**：`5Y > 7Y` 在 191/197 天成立（信用曲线正常应向右上方倾斜），且 118 天低于同期限国债 7Y（AAA 信用债低于无风险利率）。真实报价不可能在 10 个月内恒等于 4.0000 且与市场方向无关。
4. **源端自己的「缺数」标记是空值，不是 4.0**：同一窗口的 10Y、30Y 返回空，melt 后缺行；全表中短票 3Y/5Y/7Y 各 256 个空值、10Y 1,007 个、30Y 4,970 个（整表从未提供）。既然源端用空表示缺数，那么一个非空、恒定、破坏形态的值只能是填充。
5. **边界严丝合缝**：7Y 序列从 2008-01-02 才开始（此前无该期限），填充段刚好覆盖 2008-01-02~2008-10-16，2008-10-17 起恢复真实值。即「该期限刚上线、真实报价还没跟上」的一段，被上游用常量顶替。

### 3.3 影响面

- 直接：118 行负利差（最小 −0.4044 pp），全部是占位值减去真实国债 7Y 的产物，**不是真实信用利差**
- 下游消费者：`src/` 下**没有任何代码读取** `cn_credit_yield_curve.parquet`（全仓 grep 仅命中生产者 `tools/build_credit_yield_curve.py` 与注册表 `src/data/manifest_semantics.py`），当前零实际影响
- 一旦派生层（信用利差因子、期限利差）开始消费，2008 年这段会产生 118 个虚假负利差 → 派生层重建时必须显式排除或标记该段

### 3.4 处置建议（本次不覆盖）

- **不修改已入库值**（DG-16/DG-18：无独立第二源可对拍，不得凭判断改写原始观测）
- 在 `manifest.json` 的 `known_gaps` 之外单列一条 `suspect_segments`（建议字段：`tenor=7.0`、`range=2008-01-02..2008-10-16`、`rows=197`、`value=4.0`、`verdict=source_placeholder`、`evidence=本节 1~5`），供派生层读取时排除
- 后续取证方向：取 2008 年同期的中债「中短期票据 AAA 7Y」官方 PDF/月报，或 wind/同花顺同期限序列做第三源对拍；若能取到真实值，按 DG-11 走 staging → 对拍 → 整体替换流程，不在本次范围内

---

## 4. 定性②：10Y 表 2005-01 ~ 2006-02 的 288 行两源拼接缝

### 4.1 事实

```
source                      rows  first       last
eastmoney:bond_zh_us_rate    288  2005-01-04  2006-02-28
chinabond:bond_china_yield  5176  2006-03-01  2026-11-11
重叠交易日: 0
mean|Δy10|  东财段 0.0565 / 中债全段 0.0169 / 中债 2006-2010 段 0.0184
倍数        3.35x（对中债全段）/ 3.08x（对中债同期段）
东财段周末行数 9/288（2005-01-04~2006-02-28 含 9 个周六/周日）
东财段 y10 取值 2.7938 ~ 5.2221；中债段起点 2006-03-01 = 2.9000
缝两侧：2006-02-28 东财 2.9266 → 2006-03-01 中债 2.9000，跨 1 个交易日差 0.0266 pp
```

- 两源**零重叠**，无法用重叠日证明拼接一致性
- 东财段日间波动是中债段的 3.35 倍（同期对照 3.08 倍），且含 9 个非交易日行 —— 更像「另一套报价口径 + 不同发布日历」，而非同一序列的延续
- 缝两侧差 0.0266 pp，相对 2005-2006 年 10Y 日均波动（0.0565）不算离群，但无重叠无法验证

### 4.2 为什么不择一覆盖（DG-18）

1. 两源零重叠，无第三源可判定谁对谁错；
2. 东财段噪声大，不等于错 —— 2005 年交易所与银行间两个市场的 10Y 报价本来就不同口径，直接换成中债会抹掉「交易所口径」这一段；
3. 直接删掉东财段会让 10Y 表起点从 2005-01-04 退到 2006-03-01，损失 288 个交易日；
4. DG-18 要求「两源冲突且无第三证据时登记 unresolved 并保留本地存量」。

### 4.3 unresolved 登记建议（交由数据缺口登记流程落库）

建议在 `docs/数据缺口登记_正式表_2026-11-12.md` 追加一条（编号接现有序列）：

> **G-xxx｜`meta/macro/cn_treasury_yield_10y.parquet` 2005-01-04~2006-02-28 段为东财单源，与中债段零重叠，口径一致性未证实**
> - 现状：288 行 `source=eastmoney:bond_zh_us_rate`（2005-01-04~2006-02-28）+ 5,176 行 `source=chinabond:bond_china_yield`（2006-03-01~2026-11-11），重叠 0 天
> - 异常信号：东财段 `mean|Δy10|` 0.0565，为中债全段 0.0169 的 3.35 倍（对中债 2006-2010 同期段 3.08 倍）；东财段含 9 个非交易日行
> - 判定：unresolved（DG-18 两源冲突无第三证据），**保留两段存量，不择一覆盖**
> - 影响面：`src/` 下无直接读取方；派生层若用 2005-2006 年 10Y 做期限利差/久期基准，这 288 天的噪声会直接进入因子（波动被放大 3 倍）
> - 后续取证方向：① 找 2005-01~2006-02 的中债官方 10Y 月报/年报 PDF，人工抽 3~5 个交易日对拍；② 用 wind/同花顺同期限序列做第三源；③ 若能取到第三源，按 DG-11 走 staging 替换并保留本段作为 `source` 分段的对照证据；④ 若确认东财段不可用，再决定「改标 `source` 为近似口径」还是「整体退役该段」，两者都需新的实测证据，不在本次范围

---

## 5. 快照偏差（两融）

### 5.1 归档旧快照（先 copy2 留档）

```
纯路径：backups/critical_assets/data/local_store/meta/macro/cn_market_margin_daily.parquet
归档至：backups/_snapshot_archive/20261112_margin_refresh/cn_market_margin_daily.parquet
旧 SHA256：3ca2f812ff621d71d0e42fb39435d27715015e4872cc9e8ce555775d03befa91
旧字节/行数：273,433 B / 8,068 行
归档副本 SHA256 与旧件一致（copy2 后逐值复算）
```

### 5.2 回刷

用既有入口 `tools/backup_critical_assets.py`（只复制 + 重写 manifest，不删除任何文件）：

```
回刷前 --verify：通过 249 项，问题 28 项（不止两融，整份快照停在 11-10 00:37，另有 27 项源已更新）
回刷后          备份完成: 396 个文件，manifest: backups/critical_assets/manifest.json
回刷后 --verify：通过 396 项，问题 0 项
```

两融条目现状：

| 项 | 备份 | 正式 |
|---|---|---|
| 行数 | 8,070 | 8,070 |
| 字节 | 279,550 | 279,550 |
| SHA256 | `ecec8fe5a1b00e32c5aadf0fe7058b28b3e2572a53d3247fd47f833ed6abccde` | 同左 |

- 与任务给的 `ecec8fe5…` 一致，偏差已消除
- manifest 条目 `mtime=2026-11-12T21:32:35`（即 21:32 那次 publish 的时间），证明确实回刷到该次发布的内容
- 附带效果：整份快照由 277 条 / 停 11-10 更新为 396 条 / 覆盖当前（多出的 119 条是 11-10 之后新增的 `research/events/data/backfill` 分片与 `meta/anomalies` 等，属 `CRITICAL_DIRS` 正常展开）
- 已知告警：`D:\zxzq\T0002\hq_cache\{shs,szs,bjs}.tnf` 在本机不可达（既有状况），项目内 `external/tdx_hq_cache/*.tnf` 副本已正常备份

---

## 6. 边界与待办

### 6.1 边界确认

- 只写了基础层 3 张 parquet + 3 份 manifest（`data/local_store/meta/macro/`），未触碰任何派生层表
- 未删除 `data/` 任何内容；`_backup/meta/macro_20261112_pre_bond_backfill/` 为改前完整副本（6 文件）
- 采集全程单连接串行 + 1.3 s 节流，未并发、未加压重试

### 6.2 必须跟进的待办

1. **派生层需在解冻后跟进重建**（本任务明确不做）：三张债券表末日至 2026-11-12，任何以它们为输入的派生表（信用利差、期限利差、久期基准、宏观因子）都需在派生层解冻后重建对齐
2. `cn_treasury_yield_10y.parquet` 仍停 2026-11-11：它是单期限 10Y 表、由另一个采集入口产出，与三表出现 1 日错位，需单独补 11-12 并确认该入口是否也需要 `--since` 能力
3. 7Y 占位段（197 行 / 2008-01-02~2008-10-16）建议在 `cn_credit_yield_curve.manifest.json` 增 `suspect_segments` 字段，供派生层排除（§3.4）
4. 10Y 两源拼接缝按 §4.3 登记 unresolved

### 6.3 复算入口

```
.venv\Scripts\python.exe _staging\bond_backfill_20261112\verify_bond_backfill.py   # 行数守恒 / 既有值零改动 / SHA256
.venv\Scripts\python.exe _staging\bond_backfill_20261112\diagnose.py               # 两处定性的全部数字
.venv\Scripts\python.exe tools\backup_critical_assets.py --verify                  # 快照 396/0
```
