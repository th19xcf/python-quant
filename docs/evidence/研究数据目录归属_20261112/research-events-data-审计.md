# research/events/data 审计报告

- 审计对象：`e:\code\python\quant\research\events\data\`
- 审计时间：2026-11-12
- 审计口径：只读。未修改、未删除任何文件，未动 Git 索引，未提交；`data/` 一个字节未动
- 环境：`.venv\Scripts\python.exe`（polars/pandas）、`git -C e:\code\python\quant`
- 证据脚本与中间结果：`2026-11-12\research-events-data-audit\work\`（`scan_files.py`、`stats.py`、`scan.json`、`stats.json`、`git_ls_files.txt`）

---

## 摘要（三问直答）

| 问题 | 结论 |
|---|---|
| 是不是数据文件 | 是。142 个文件 / 22,432,329 B（21.39 MiB），17 个顶层文件 + `backfill\`(84) + `backfill\daily\`(3) + `backfill_supplement\`(38)。内容不是单一类型，按实际列与内容可分为 6 类：原始采集、中间派生、结论报告、配置映射、元数据、断点进度 |
| 为什么放在这里 | 这是 `research/events/` 那次「公告事件 + 宏观事件」可行性试点的数据落盘目录。4 个研究脚本都把 `DATA_DIR` 硬编码成 `Path(__file__).resolve().parent / 'data'`，CLI 只暴露日期与关键词，不暴露输出目录；后来 `tools/backfill_events.py`、`tools/update_events.py` 又把它复用为补数/日更分片目录（默认值与硬编码等价，可用 `--shard-dir` 覆盖）。目录本身**没有任何 README**，定位只散落在 3 份 docs 里顺带提到 |
| 为什么有的进了 Git | 2026-11-03 `4b8fc1b` 一次性把整个试点（代码 + 报告 + 12 个小体积 CSV/JSON + 4 个 parquet 共 3.7 MB）提交入库；2026-11-09 `823a5b5` 又补入 36 个月度分片（665 KB）+ `_audit.json`。随后 `9ccdf5d`（11-03）先忽略 8.8 MB 的 `announcement_event_level_car.csv`，`232a1ae`（11-09 22:49）用 `git rm --cached` 取消跟踪 **40 个 parquet**（4 个顶层 + 36 个分片），并新增通用规则 `research/events/data/**/*.parquet`。现在 Git 里只剩 12 个 CSV/JSON（167,237 B），130 个 parquet 全部被 `.gitignore` 覆盖 |

---

## 1 盘点与性质判定

### 1.1 磁盘清单

```
Get-ChildItem research\events\data -Recurse -File | Measure-Object Length -Sum
→ 142 个文件 / 22,432,329 B

顶层 17 个：13,342,748 B
子目录 125 个：9,089,581 B
  backfill\            84（83 个月度分片 202001~202611 + _merged_for_import.parquet）
  backfill\daily\       3（20261111.parquet、_merged_20261111_20261111.parquet、progress.csv）
  backfill_supplement\ 38（36 个月度补数分片 + _merged_for_import.parquet + _audit.json）
```

mtime：顶层与原始采集件集中在 **2026-11-03 14:37~14:58**（试点当天一次性跑完）；`backfill\` 为 2026-11-04~11-09；`backfill\daily\` 为 **2026-11-11 14:21**（日更流程）。

### 1.2 逐文件判定（依据来自实际列名、行数、dtypes 与内容样例，不看文件名）

| 文件 | 大小 | 行数 | 关键列（实测） | 性质判定 | 依据 |
|---|---:|---:|---|---|---|
| `announcements_raw.parquet` | 822,191 | 26,115 | `keyword, art_code, stock_code, short_name, title, column_name, display_time, notice_date, url` | **原始采集** | 保留抓取关键词与原始披露时间/PDF 链接，`display_time` 96% 为 00:00:00（与报告一致）；`stock_code` 为 str 保留前导零 |
| `announcements_partial.parquet` | 677,167 | 29,739 | 同 raw | **中间派生（断点件）** | 由 `fetch_announcements.py:264-266` 每抓完一个关键词就整体覆盖写；行数 29,739 > raw 的 26,115，说明是去重前的累计中间态，不是最终结果 |
| `yjyg_raw.parquet` | 162,799 | 14,211 | `stock_code, short_name, forecast_type, change_pct, announce_date, report_date` | **原始采集** | 东财业绩预告原始字段，无事件类型归一、无关键词 |
| `announcement_events_normalized.parquet` | 623,561 | 22,101（唯一 id 21,807，重复 294 行） | `event_id, ts_code, stock_code, short_name, event_type, publish_time, title, url, source` | **中间派生** | 9 列，`source` 计数 `cninfo` 17,558 + `em_yjyg` 4,543；已按关键词映射成事件类型 |
| `announcement_event_level_car.parquet` | 2,095,922 | 20,220 | 在 normalized 9 列上增加 `trade_day, alpha, beta, n_est, regime, car0/car1/car5/car20, car_ma0/1/5/20` | **中间派生（事件级结果）** | 每行一次事件研究回归参数与多窗口 CAR；比 normalized 少 1,838 个 id（无可算 CAR） |
| `announcement_event_level_car.csv` | 8,805,001 | 20,220 | 同上 22 列 | **中间派生的 CSV 副本** | 与 parquet 同源同行数（20,220）。但 `stock_code` 落成 **int**（`000001` → `1`），是**有损副本** |
| `announcement_event_summary.csv` | 15,606 | 36 | `event_type, horizon, n, mean, mean_winsor, median, std, t, p, ..., n_dates, t_date, ..., n_bull/n_bear, significant, sample_flag` | **结论报告** | 9 类事件 × 4 个 horizon 的统计汇总（含按日期聚类推断） |
| `announcement_event_summary_ma.csv` | 15,371 | 36 | 同上 | **结论报告** | 同上，用 MA 口径（`car_ma*`）的汇总 |
| `macro_events.csv` | 7,480 | 45 | `event_id, event_date, trade_day, event_type, event_name, region, affected_sectors, polarity, source_url, note` | **配置映射（手写事件清单）** | 5 类 45 条宏观事件，带 `source_url`/`note`，属人工整理知识表，非抓取产物 |
| `event_sector_map.csv` | 6,267 | 54 | `event_type, sector_tag, market_direction, strength, etf_code, etf_name, source, status, tested, rationale, conclusion` | **配置映射（手写知识表）** | 18 类 → 板块映射 54 条，含 `rationale`/`conclusion`，是脚本输入 |
| `sector_etf_universe.csv` | 409 | 14 | `etf_code, ts_code, kind, priority` | **配置映射（标的池）** | 14 只 ETF 池，是脚本输入 |
| `macro_event_level_car.csv` | 61,789 | 181 | `event_id, event_type, event_name, event_date, sector_tag, etf_code, ..., car0/car1/car5/car20, hit0/hit1/hit5/hit20` | **中间派生（事件级结果）** | 45 事件 × 映射板块展开成 181 行 |
| `macro_event_summary.csv` | 39,825 | 88 | `event_type, sector_tag, etf_code, expected_direction, horizon, n, mean, t, ..., significant, sample_flag` | **结论报告** | 22 条映射 × 4 horizon 的统计汇总 |
| `macro_mapping_verdict.csv` | 2,349 | 22 | `event_type, sector_tag, etf_code, expected_direction, n, mean_car5, t_car5, win_rate, verdict` | **结论报告** | 22 条映射的「成立/不成立」判定，是报告结论的直接底稿 |
| `news_source_sample.csv` | 3,588 | 25 | `source, publish_time, title, has_link, link, content_len` | **元数据 / 一次性探针样本** | 3 个新闻源各取数条样例，`publish_time` 只有时分（`14:15:42`），用于证明「可直连且到秒」 |
| `news_source_probe.json` | 2,713 | — | 键：`cls_telegraph, eastmoney_fastnews, sina_7x24, cls_raw, sina_raw, eastmoney_research, elapsed_seconds` | **元数据** | 探针返回结构与耗时，非数据集 |
| `fetch_meta.json` | 710 | — | 键：`source, 减持/增持/股权激励/中标/重大合同/处罚/重组/分红（各自 total_hits/records）, range, announcement_rows, yjyg_rows, elapsed_seconds` | **元数据（采集台账）** | 记录抓取口径、逐关键词命中数、总行数与耗时 |
| `backfill\*.parquet`（83 个） | 合计 7,324,273 B | 单月 1,600~2,600 | `keyword, art_code, stock_code, short_name, title, column_name, display_time, notice_date, url`（与 raw 同构） | **原始采集（按月分片）** | `tools/backfill_events.py:170` 原子写；202001 实测 2,003 行，其中 1,198 条（art_code+stock_code）已在正式表、805 条被关键词口径过滤掉 |
| `backfill\_merged_for_import.parquet` | 419,209 | 21,076 | 同分片 | **中间派生（合并件）** | `backfill_events.py:221-222` 合并后交给 `import_file`，14,151 条已在正式表 |
| `backfill\daily\20261111.parquet` | 4,221 | — | 同分片 | **原始采集（日分片）** | `tools/update_events.py:171` 原子写 |
| `backfill\daily\_merged_20261111_20261111.parquet` | 4,221 | — | 同分片 | **中间派生（合并件）** | `update_events.py:239-240` |
| `backfill\daily\progress.csv` | 173 | 2 | 日期/轮次/行数类断点字段 | **元数据（断点进度）** | `update_events.py:182-214` 逐日追加 |
| `backfill_supplement\*.parquet`（36 个） | 共 665,308 B | 合计 15,686 | 同分片 | **原始采集（补数分片）** | `823a5b5` 提交说明：二分重抓补齐的 36 个月，用于复现 2020 起事件口径 |
| `backfill_supplement\_merged_for_import.parquet` | 419,209 | 15,686 | 同分片 | **中间派生（合并件）** | 36 分片合并；6,758 条已在正式表 |
| `backfill_supplement\_audit.json` | 11,278 | 38 条 | `month, keyword, total_hits_log, records_log, total_hits_now, shard_rows, refetch_rows, refetch_new, cumulative, window_residual, complete` | **元数据（补数审计）** | 逐月份/关键词的实测命中数与合并校验，`complete` 有 false 项 |

> 判定口径：出现「抓取关键词/原始 URL/未归一字段」→ 原始采集；出现「脚本回归参数 / CAR / 合并件」→ 中间派生；出现「mean/t/p/significant/verdict」→ 结论报告；「人工整理、带 rationale/source_url、被脚本当输入读」→ 配置映射；「行数/耗时/断点/审计字段」→ 元数据。

### 1.3 更正（2026-11-13 复核追加）

> 体例说明：本文原本没有修订历史节，本节按既有「编号小节 + 引用块」体例新增，只做追加更正，**不改动 §1.2 表内任何数字与结论、不重写该表**。§1.2 与 §6.3 原文一律保留原样，两处与本节冲突时以本节为准。

| 项 | 原文（保留不动） | 实测更正 |
|---|---:|---:|
| `backfill\_merged_for_import.parquet` 体积 | 419,209 B（§1.2 表第 60 行） | **660,377 B** |
| 该件行数 | 21,076 | 21,076（不变） |
| 该件 SHA256 | 原文未记 | `8fb98e3c1825f1483127111008a1274135129d2420bf9e2f5894a3c8bfe95613` |
| `backfill_supplement\_merged_for_import.parquet` | 419,209 B / 15,686 行 | 419,209 B / 15,686 行（正确，未变） |

**成因**：§1.2 表把 `backfill\_merged_for_import.parquet` 的体积与 `backfill_supplement\_merged_for_import.parquet` 抄串了——两件同名、同一次补数流程产出，`backfill_supplement` 那件恰为 419,209 B / 15,686 行，`backfill` 那件真值为 660,377 B / 21,076 行。按字节数算，两件不同行数的 parquet 不可能同为 419,209 B，这本身就是可自检的矛盾点，原表未做该校验。

**发现依据**：
- 输出：`e:\code\python\quant\2026-11-13\events-data-20-missing\outputs\research_events_data_缺失20件核查_2026-11-13.md` 第三节附注 1。
- 原始记录（本审计自己的机读结果，权威来源）：`e:\code\python\quant\2026-11-12\research-events-data-audit\work\scan.json` 中 `backfill/_merged_for_import.parquet` 条目为 `{"bytes":660377, "sha256":"8fb98e3c…5613", "rows":21076}`，`backfill_supplement/_merged_for_import.parquet` 条目为 `{"bytes":419209, "sha256":"f0555465…26c8", "rows":15686}`——即**原表的 419,209 与本文自己的 scan.json 不一致**，属转录笔误，不是两次测量差异。
- 落盘复核（2026-11-13，搬迁后新位置，两件均未被重写）：
  - `e:\code\python\quant\_staging\research_events_20261103\derived\backfill\_merged_for_import.parquet` → 660,377 B / SHA256 `8fb98e3c1825f1483127111008a1274135129d2420bf9e2f5894a3c8bfe95613`，与 scan.json 相等；
  - `e:\code\python\quant\_staging\research_events_20261103\derived\backfill_supplement\_merged_for_import.parquet` → 419,209 B / SHA256 `f05554657766bd51494b5fa98ec6fe51f96f65773c4fe6da59231bab0e3126c8`，与 scan.json 相等。
- 复算命令（仓库根，PowerShell）：
  ```powershell
  (Get-Item '_staging\research_events_20261103\derived\backfill\_merged_for_import.parquet').Length
  (Get-FileHash '_staging\research_events_20261103\derived\backfill\_merged_for_import.parquet' -Algorithm SHA256).Hash
  (Get-Content '2026-11-12\research-events-data-audit\work\scan.json' -Raw | ConvertFrom-Json).'backfill/_merged_for_import.parquet'.bytes
  ```
  注：这两件自 `232a1ae` / `5bd106e` 起已 `git rm --cached` 撤出跟踪（且被 `.gitignore` 覆盖），因此 `git cat-file -s 5de8618^:research/events/data/backfill/_merged_for_import.parquet` 返回 `does not exist in '5de8618^'`，Git 侧无法复算体积，证据链只能落在 `scan.json` 与落盘实测两处。

**连带结论作废**：§6.3 第 1 条中「另有 1 个内容不一致：`backfill\_merged_for_import.parquet` 备份 660,377 B vs 源 419,209 B」这一判断**不成立**——实际的源文件就是 660,377 B（SHA256 `8fb98e3c…`），备份侧 660,377 B 与之相符，**两侧一致，不存在内容不一致**。该条其余内容（源目录 142 件、备份 55 件、缺失 87 件及其构成）不受影响，仍然有效。§6.3 原文不改，本段为其唯一更正依据。

---

## 2 为什么在这里

### 2.1 写入这些路径的代码位置（全仓 Grep，排除 `.venv`）

| 写入方 | 位置 | 写什么 | 路径来源 |
|---|---|---|---|
| `research/events/fetch_announcements.py` | `:43` `DATA_DIR = Path(__file__).resolve().parent / 'data'` | — | **硬编码**（相对脚本位置） |
| 同上 | `:266` `partial.write_parquet(partial_path)` | `announcements_partial.parquet` | 硬编码（`:326` 传入） |
| 同上 | `:327` `announcements.write_parquet(DATA_DIR / 'announcements_raw.parquet')` | raw | 硬编码 |
| 同上 | `:333` `yjyg.write_parquet(DATA_DIR / 'yjyg_raw.parquet')` | yjyg | 硬编码 |
| 同上 | `:342` `(DATA_DIR / 'fetch_meta.json').write_text(...)` | fetch_meta | 硬编码 |
| `research/events/announcement_event_study.py` | `:44` 同款 `DATA_DIR`；`:202` 写 normalized；`:208` 写 level parquet；`:209` 写 level csv；`:213/:214` 写两个 summary | 5 个派生/结论文件 | **硬编码**；CLI 只有 `--start/--end`（`:190-191`），不能改输出目录 |
| `research/events/macro_event_study.py` | `:38` `DATA_DIR`；`:156-158` 写 level/summary/verdict | 3 个 CSV | 写路径硬编码；输入可用 `--events/--mapping/--universe` 覆盖（`:138-140`） |
| `research/events/fetch_news_sample.py` | `:27` `DATA_DIR`；`:156-157` 写 sample csv 与 probe json | 2 个探针文件 | 硬编码 |
| `tools/backfill_events.py` | `:46` `DEFAULT_SHARD_DIR = RESEARCH_DIR / 'data' / 'backfill'`；`:170` `write_parquet_atomic`；`:222` 写 merged | 83 个月度分片 + merged + progress | 默认硬编码，`--shard-dir`（`:298`）可覆盖 |
| `tools/update_events.py` | `:48-49` `DEFAULT_SHARD_DIR = RESEARCH_DIR / 'data' / 'backfill' / 'daily'`；`:171` 写日分片；`:240` 写 merged；`:182-214` 写 progress | 日分片 + merged + progress | 默认硬编码，`--shard-dir`（`:313`）可覆盖 |

写入时间线：
1. **2026-11-03 14:37~14:58**：`fetch_news_sample.py` → `macro_event_study.py` → `fetch_announcements.py` → `announcement_event_study.py` 依次跑完，产出 17 个顶层文件（与 mtime 完全吻合）。
2. **2026-11-04~11-09**：`tools/backfill_events.py` 按月补 2020-01~2026-11，产出 83 个分片。
3. **2026-11-09**：二分重抓补齐，产出 `backfill_supplement\` 36 个分片 + `_audit.json`。
4. **2026-11-11 14:21**：`tools/update_events.py` 日更，产出 `backfill\daily\` 3 个文件。

### 2.2 目录定位的文档说明

- `research\` 与 `research\events\` **都没有 README**（`Glob research/**/*.md` 无结果），目录定位没有就地说明。
- 间接说明（3 处，全部是「顺带提到」，不是本目录的定位文档）：
  - `docs\舆情与公告事件可行性验证报告.md:5`：「本文所有脚本与结果位于 `research/events/`，未修改 `src/` 下任何生产代码」——明确是**研究试点**，不接生产。
  - `docs\数据权威路径清单.md:95`：「`research/events/data/backfill/` 与 `_staging/events_backfill_2015_2019/shards/` 是采集分片（中间产物），同样不是正式路径，不得直接当事件表读取」——明确**不是正式数据路径**。
  - `docs\事件日更入口.md:42、:88`：`--shard-dir` 默认 `research/events/data/backfill/daily/`，且「已被 `.gitignore` 覆盖」「只读写 `meta/events/`、`research/events/data/backfill/daily/`、`_backup/events_daily/`」。
- 有 2 处**已过期**的描述，会误导读者：
  - `docs\主机迁移本地数据清单.md:217`：「`research/`（代码 + 数据）23 个文件 / 13,409,819 B …… 15 个数据文件……均已入库；本次新增忽略 2 个……`.gitignore:141-142`」。实测：目录 142 个文件 / 22,432,329 B，入库 12 个，忽略规则已迁到 `.gitignore:186-187`。该行写于 11-03，11-09 `232a1ae` 之后未更新。
  - `docs\数据权威路径清单.md:48` 记 `events.parquet` 为 185,651 行 / SHA `c9c35d86…`；实测 **185,653 行 / SHA `53f45eea…`**（11-11 之后又追加 2 行，文档未跟）。

---

## 3 Git 跟踪状态

### 3.1 `git ls-files research/events/data`（完整输出，12 条）

```text
research/events/data/announcement_event_summary.csv
research/events/data/announcement_event_summary_ma.csv
research/events/data/backfill_supplement/_audit.json
research/events/data/event_sector_map.csv
research/events/data/fetch_meta.json
research/events/data/macro_event_level_car.csv
research/events/data/macro_event_summary.csv
research/events/data/macro_events.csv
research/events/data/macro_mapping_verdict.csv
research/events/data/news_source_probe.json
research/events/data/news_source_sample.csv
research/events/data/sector_etf_universe.csv
```

合计 167,237 B；12 个文件**全部只有 1 次提交**，内容自首次入库后未再改动：
- 10 个首次入库 `4b8fc1b`（2026-11-03）
- `backfill_supplement/_audit.json` 首次入库 `823a5b5`（2026-11-09）
- 每个文件 `commits=1`

### 3.2 `.gitignore` 规则与覆盖（`git check-ignore -v` 聚合，142 个文件全覆盖判定）

| 规则行 | 原文 | 覆盖文件数 | 说明 |
|---:|---|---:|---|
| 174 | `research/events/data/backfill/` | 87 | `backfill\` 下 84 个 + `backfill\daily\` 3 个（目录级忽略，`-v` 命中 174） |
| 186 | `research/events/data/announcement_event_level_car.csv` | 1 | 8.8 MB，CSV 副本 |
| 187 | `research/events/data/announcements_partial.parquet` | 0（被 193 覆盖） | 现为**冗余规则**：`check-ignore` 实际报 193 |
| 190 | `research/events/data/*/_merged_for_import.parquet` | 0（被 174/193 覆盖） | 现为**冗余规则** |
| 193 | `research/events/data/**/*.parquet` | 42 | 顶层 5 个 parquet（raw/partial/normalized/level/yjyg）全部命中 + `backfill_supplement\` 37 个（36 分片 + merged） |
| — | 未命中 | 12 | 即上面 12 个跟踪文件 |

> 校验：42 + 1 + 87 + 12 = 142，与磁盘文件数一致。结论：**除 12 个跟踪文件外，其余 130 个文件全部被忽略**，且 187/190 两条精确规则已被 193 的通用规则吸收。

### 3.3 历史提交

| 提交 | 时间 | 动作 |
|---|---|---|
| `4b8fc1b` | 2026-11-03 15:05:34 | 首次入库：`research/events/` 代码 + 报告 + 12 个 CSV/JSON + **4 个 parquet**（`announcements_raw`、`announcement_events_normalized`、`announcement_event_level_car.parquet`、`yjyg_raw`，合计 3,704,473 B） |
| `9ccdf5d` | 2026-11-03 18:52:12 | 新增忽略：`announcement_event_level_car.csv`（8.8 MB）、`announcements_partial.parquet`。这两个**从未入库**（`git log --all --` 为空） |
| `823a5b5` | 2026-11-09 22:15:38 | 补入 36 个月度分片 + `_merged_for_import.parquet`(419,209 B) + `_audit.json` |
| `232a1ae` | 2026-11-09 22:49:43 | `git rm --cached` **40 个 parquet**（4 个顶层 + 36 个分片），新增 193 通用规则；只改索引，工作区文件保留 |

`232a1ae` 之后，4 个顶层 parquet 在 HEAD 中已不存在：

```text
git cat-file -e HEAD:research/events/data/announcements_raw.parquet
→ fatal: path ... exists on disk, but not in 'HEAD'
（normalized / level_car / yjyg 同）
```

### 3.4 Git 内体积

```text
git rev-list --objects --all | grep -F 'research/events/data/'
+ git cat-file -s / -t
```

| 分组 | 对象数 | 唯一 blob 体积 |
|---|---:|---:|
| 当前 12 个跟踪文件 | 12 | 167,237 B |
| 4 个顶层 parquet（历史） | 4 | 3,704,473 B |
| 36 个补数分片（历史） | 36 | 665,308 B |
| `backfill_supplement/_merged_for_import.parquet`（历史） | 1 | 419,209 B |
| tree 对象 | 3 | 3,195 B |
| **唯一对象合计（净占用）** | **56** | **4,959,422 B（4.73 MiB）** |

其中 parquet 唯一 blob 合计 **4,788,990 B（4.57 MiB）**。

含重复版本的流水体积（逐 commit 统计该目录 tree 内所有 blob 大小之和）：

```text
git rev-list --all                      → 430 commits
携带该目录的 commit                     → 319
累计快照体积                            → 354,269,779 B = 337.86 MiB
单次最大快照                            → 4,956,227 B
```

> 解读：Git 对相同内容只存一份，所以**净占用 4.73 MiB**；337.86 MiB 是 319 个 commit 反复携带同一批文件的「逻辑流水」，不代表仓库实际膨胀。

### 3.5 被忽略但历史上跟踪过的文件

**有，共 40 个**：4 个顶层 parquet（`announcements_raw`、`announcement_events_normalized`、`announcement_event_level_car.parquet`、`yjyg_raw`）+ 36 个 `backfill_supplement\*.parquet`。它们在 `4b8fc1b`/`823a5b5` 入库，`232a1ae` 取消跟踪；blob 仍留在历史里（即 3.4 的 4.57 MiB）。

全仓另有 1 个「已跟踪但被忽略」的文件，与本目录无关：

```text
git ls-files -ci --exclude-standard
→ data/local_store/meta/anomalies/manifest.json
```

---

## 4 重复与口径（DG-1 对拍）

正式路径（`docs\数据权威路径清单.md:48` 登记）：`data\local_store\meta\events\events.parquet`，全项目唯一公告事件表。

| 对拍项 | research 侧 | 正式路径 | 是否同一份 | 结论 |
|---|---|---|---|---|
| `announcement_events_normalized.parquet` | 22,101 行 / 唯一 id 21,807 / 2025-01-01~2025-12-31 / 9 列 / SHA `10244003…6fe6` | 185,653 行 / 唯一 id 185,653 / 2015-01-01~2026-11-11 / 6 列 / SHA `53f45eea…159b` | **否**（SHA 不同、列不同、范围不同） | 研究侧是 **2025 年子集快照 + 更宽字段**：21,362 个 id（97.96%）能在正式表找到，445 个（2.04%）找不到；research 侧还有 294 行重复 id |
| `announcement_event_level_car.parquet` | 20,220 行 / 22 列（含 `alpha,beta,n_est,regime,car*`） | 正式表无 CAR/回归参数 | **否，正式表无对应语义** | 不构成重复；这是研究独有的事件级结果 |
| `yjyg_raw.parquet` | 14,211 行 / 6 列原始预告 | 正式表 `source=em_yjyg` 4,543 行 | **否** | 规范化层（research 4,543 行）已被正式表承接：id 交集 3,955（87.1%），正式表多 588 个 id、research 多 294 个 id（重复行）。**原始层无正式对应路径** |
| `announcement_event_level_car.parquet` vs `.csv` | 均 20,220 行、同源 | — | 同源不同容器 | parquet SHA `fafa0121…0777`，CSV 8.8 MB 且 `stock_code` 为 int（`000001`→`1`）→ **CSV 是有损副本** |
| `backfill\` 分片 | 83 个月，单月 1,600~2,600 行 | 正式表 2015 起 | 部分 | 202001：2,003 行中 1,198 条已在正式表、805 条被关键词口径过滤；`_merged_for_import`(21,076) 中 14,151 条已入库 |
| `backfill_supplement\` 分片 | 36 个月 / 15,686 行 | 同上 | 部分 | `_merged_for_import`(15,686) 中 6,758 条已入库 |

**DG-1 判定**：
- 字面上未被违反——全项目只有 `meta/events/events.parquet` 一条**登记在册**的正式路径；research 侧既未登记、也被 `.gitignore` 覆盖、生产 `src/` 无任何代码读它。
- 但形式上存在「同一语义的第二份副本」，且口径已经漂移：research 快照停在 2025-12-31，正式表已到 2026-11-11（且比文档多 2 行）。风险在于**有人按文件名把 `announcement_events_normalized.parquet` 当事件表读**——这正是 `docs\数据权威路径清单.md:95` 专门警告、且 DG-2/DG-27 要防的情形。

---

## 5 消费者（逐文件）

### 5.1 真正读这个目录的代码

| 读的文件 | 读取位置 | 用途 |
|---|---|---|
| `announcements_raw.parquet` | `research\events\announcement_event_study.py:195` `pl.read_parquet(DATA_DIR / 'announcements_raw.parquet')` | 事件研究输入 |
| `yjyg_raw.parquet` | `research\events\announcement_event_study.py:196`（存在才读） | 业绩预告事件输入 |
| `macro_events.csv` | `research\events\macro_event_study.py:138`（默认路径）→ `read_csv` `:43` | 宏观事件清单 |
| `event_sector_map.csv` | `research\events\macro_event_study.py:139` → `:43` | 事件→板块映射 |
| `sector_etf_universe.csv` | `research\events\macro_event_study.py:140` → `:43` | ETF 标的池 |
| `backfill\*.parquet` | `tools\backfill_events.py:203`（`glob('*.parquet')`）、`:208`（`read_parquet` 合并）、`:153`（断点检查 `read_parquet(target).height`） | 合并导入 |
| `backfill\daily\*.parquet` | `tools\update_events.py:188`（断点检查）、`:229`（`read_parquet` 合并）、`:239-240`（merged 目标） | 日更导入 |
| `_merged_for_import.parquet` | `tools\backfill_events.py:232-233`、`tools\update_events.py:363/387` → `src.data.events.import_file` | 写入正式表 |

- **生产 `src\` 侧没有任何硬编码读取**：全仓 Grep 命中 `research/events/data` 的只有 `research\events\*.py`、`tools\backfill_events.py`、`tools\update_events.py`、`.gitignore`、docs。
- `src\data\events.py:331` 的 `import_file(path, ...)` 与 `:618` 的 `--path` 是**手工参数**，不强绑本目录；`docs\数据权威路径清单.md:191` 也写明「离线只能拷贝已有文件或 `research/events/data/*.parquet`」，属人工操作指引，不是代码依赖。

### 5.2 死产物（写完再没有任何代码读）

| 文件 | 写入位置 | 状态 |
|---|---|---|
| `announcement_events_normalized.parquet` | `announcement_event_study.py:202` | 无读；下游正式表已承载同语义，属研究留痕 |
| `announcement_event_level_car.parquet` | `:208` | 无读 |
| `announcement_event_level_car.csv` | `:209` | 无读，且与 parquet 重复、有损 |
| `announcement_event_summary.csv` / `_ma.csv` | `:213` / `:214` | 无读；结论已被 `docs\舆情与公告事件可行性验证报告.md` 引用 |
| `macro_event_level_car.csv` | `macro_event_study.py:156` | 无读 |
| `macro_event_summary.csv` | `:157` | 无读 |
| `macro_mapping_verdict.csv` | `:158` | 无读；报告结论底稿 |
| `news_source_sample.csv` | `fetch_news_sample.py:156` | 无读 |
| `news_source_probe.json` | `:157` | 无读 |
| `fetch_meta.json` | `fetch_announcements.py:342` | 无读（仅人工溯源） |
| `announcements_partial.parquet` | `fetch_announcements.py:266` | 只写不读；名为「断点件」但代码里没有对应续跑读取逻辑（`fetch_announcements` 每次从空列表重抓），属**伪断点** |
| `backfill_supplement\_merged_for_import.parquet` | 补数流程合并 | 补数已完成（`_audit.json` 在库），当前无读 |

> 注意：`_merged_for_import.parquet`（`backfill\`）不在此列——它是 `import_file` 的直接入参，属活产物。

---

## 6 处置建议

### 6.1 判断标准（先定尺子，再套文件）

| 维度 | 归「留在 Git」 | 归「不入 Git / 只留磁盘」 | 归「应迁 data/local_store」 |
|---|---|---|---|
| 可重建性 | 不可重建或重建成本高（手写知识、审计台账） | 脚本一次可重算 | 只有成为**唯一正式语义来源**时 |
| 体积 | 小（本项目实测 < 100 KB/文件） | 大（> 100 KB 或月度分片集合） | 不适用 |
| 消费者 | 被脚本当输入读（缺了脚本跑不起来） | 无代码读，仅供人工/报告引用 | 生产 `src/` 硬编码读取 |
| 语义唯一性 | 不与 `data/local_store` 正式路径重叠 | 与正式路径重叠或是其上游原料 | 尚无正式路径且被生产消费 |

### 6.2 逐类建议

**A. 建议继续跟踪（不动）——手写输入，缺了脚本不可复现**
- `macro_events.csv`、`event_sector_map.csv`、`sector_etf_universe.csv`（合计 14,156 B）
- 依据：`macro_event_study.py:138-140` 直接把默认路径指向它们；内容是人工整理的知识表（`source_url`/`rationale`/`conclusion`），无法从行情重建。

**B. 建议继续跟踪（不动）——小体积结论与审计台账**
- `announcement_event_summary.csv`、`announcement_event_summary_ma.csv`、`macro_event_summary.csv`、`macro_mapping_verdict.csv`（合计 73,151 B）
- `fetch_meta.json`、`news_source_sample.csv`、`news_source_probe.json`、`backfill_supplement\_audit.json`（合计 18,289 B）
- 依据：它们是报告结论（尤其 `macro_mapping_verdict.csv` 的 22 条判定）与补数过程的**唯一证据**，且都远小于 100 KB；删掉后只能重跑抓取才能复现。

**C. 建议 `git rm --cached`（只移索引、不删磁盘、不改写历史）——可重算的中间层**
- `macro_event_level_car.csv`（61,789 B）
- 依据：① 无任何代码读（5.2）；② 由 `macro_event_study.py` 每次重算（`:136-158`）；③ 与 11-09 已取消跟踪的 `announcement_event_level_car.*` 属同一层「事件级 CAR 中间件」，两边口径应一致——现在一边入库一边不入库，是标准不一致，不是有意区分。
- 若不做：Git 里长期保留一份会随脚本改动而漂移的中间件，且下一次重跑就会产生无意义 diff。

**D. 不建议迁入 `data/local_store\`（明确反对）**
- 三个重点文件的理由是分开的：
  - `announcement_events_normalized.parquet`：正式表已覆盖 97.96% 的 id 且多出 2015-2026 全区间；迁入即制造第二个「公告事件」正式路径，直接违反 DG-1/DG-2，并会让下游在两张表之间摇摆。
  - `yjyg_raw.parquet`：只是 2024-2025 预告原始件，规范化层已被正式表承接；`data/local_store` 没有「原始未归一公告」这一层，迁入会凭空新增一层语义。
  - `announcement_event_level_car.parquet`：CAR 是研究结论，不是生产事件表字段；若真要产品化，应另立 `research_inputs\` 派生表并登记，不能塞进 `meta\events\`。
- 结论：**本目录没有任何文件满足「应迁入 data/local_store」的判据**。

**E. 一次性研究产物，原地保留**
- 全部 parquet（130 个，13,459,943 B）、`announcement_event_level_car.csv`、`announcements_partial.parquet`
- 依据：都是 2025 样本或补数分片，已被 `.gitignore` 覆盖，是报告的可复现底稿；不占 Git，也不影响生产。
- 如果嫌目录脏，可整体挪到 `_staging\research_events_20261103\`（DG-5 口径），但这需要同步改 4 个脚本的 `DATA_DIR` 与 2 个 tools 的 `DEFAULT_SHARD_DIR`，收益低于改动风险，**本轮不建议动**。

**F. 死产物：保留 + 标注，不删**
- 5.2 表内 11 个文件都是死产物，但多数是报告证据（§6.2 B 已覆盖）。删除会把「结论可复现」变成「只能靠报告文字」，与项目「产物必须可复现」的取向相反。
- 建议只做一件事：在目录下加一行说明（例如 `research\events\data\README.md`，2~3 句写清「研究快照，非正式路径，正式表见 `meta/events/events.parquet`」）。这属于新建文档，需你点头后再做。

### 6.3 需要单独提请注意的两个问题（不在处置建议内，但审计发现）

1. **备份镜像实际没有覆盖这个目录的全部内容**：`232a1ae` 的提交说明写「数据安全改由 `G:\git\python-quant` 镜像 + `_backup` 快照保障」，但实测：

```text
源目录 142 个文件 / 22,432,329 B
备份   55 个文件 / 14,422,274 B
缺失   87 个：backfill\ 全部月度分片 + backfill\daily\ 3 个 + announcements_partial.parquet
另有 1 个内容不一致：backfill\_merged_for_import.parquet 备份 660,377 B vs 源 419,209 B
```

   原因：备份规则的四类范围（Git 跟踪文件 / `data/local_store` + `_backup` / `reports` / 外部输入）都不含 `research/events/data`，现有 55 个文件是历史手工拷贝的残留。**结论：取消跟踪后这批 parquet 实际上只有单盘一份**，与提交说明不符。要么把 `research/events/data` 纳入备份镜像，要么在文档里改成如实描述。

2. **`announcements_partial.parquet` 是伪断点**：`fetch_announcements.py:264-266` 每抓完一个关键词就覆盖写，但函数没有读取它续跑的分支（`:256-267` 每次都从 `all_records=[]` 开始）。它既不是可靠断点（按关键词整体覆盖，不是按页追加），也不在任何恢复路径里被读。

---

## 附：可复算命令

```powershell
# 清单
Get-ChildItem research\events\data -Recurse -File | Measure-Object Length -Sum
# 跟踪状态
git ls-files research/events/data
git ls-files -ci --exclude-standard
git check-ignore -v (Get-ChildItem research\events\data -Recurse -File).FullName
# 历史
git log --diff-filter=A --format='%h|%ad|%s' --date=short --name-only -- research/events/data
git show --name-status --format='%h|%ad|%s' --date=iso 232a1ae
# 体积
git rev-list --objects --all   # 过滤 research/events/data/ 后 git cat-file -s
# 对拍（脚本）
.venv\Scripts\python.exe 2026-11-12\research-events-data-audit\work\stats.py
```

审计期间未执行任何写操作（除在 `2026-11-12\research-events-data-audit\work\` 下生成证据文件）。
