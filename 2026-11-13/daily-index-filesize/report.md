# `meta/daily_index.parquet` 的 `file_size` 全库对账与修复

- 执行日期：2026-11-13
- 目标文件：`data/local_store/meta/daily_index.parquet`（9,787 行 × 11 列）
- 侧车：`data/local_store/meta/daily_index.manifest.json`
- 口径：`LocalQuoteStore.quote_path(data_type, ts_code)`，即 `quotes/<data_type>/<market_of(ts_code)>/<ts_code>.parquet`；`market_of` 取 ts_code 的交易所后缀（sh/sz/bj），非法后缀回落 sz。与 `coverage()` 同源。
- 只读阶段脚本：`_staging/daily_index_filesize_20261113/audit.py`（全表）与 `scan_stale.py`（rows/start/end 交叉核对）

## 1. 三类清单

### 1.1 `file_size` 不一致（252 行，已全部修复）

- 行数：**252**（247 fund / 5 stock / 0 index）
- 按 data_type/market：`fund/sh` 35、`fund/sz` 212、`stock/sh` 4、`stock/sz` 1
- delta 合计：**+39,335 B**；区间 **-2,685 ~ +611 B**（正向 239 行、负向 13 行）
- 与 `DG-33` 记载的「252 行 / −2,685 B ~ +611 B / 合计 +39,335 B」逐项一致，未触发「远超 252 且成因不明即停」的停手条件。
- 逐行明细：同目录 `mismatch_252.csv`（252 行，含改前值、改后值、delta）

| data_type | ts_code | 改前 file_size | 改后（磁盘实测） | delta |
|---|---|---:|---:|---:|
| fund | 168108.SZ | 25,375 | 22,690 | -2,685 |
| fund | 502020.SH | 54,245 | 53,296 | -949 |
| fund | 164702.SZ | 55,201 | 55,812 | +611 |
| fund | 150023.SZ | 77,855 | 78,416 | +561 |
| fund | 150130.SZ | 48,809 | 49,286 | +477 |
| fund | 166902.SZ | 50,639 | 51,098 | +459 |
| fund | 502040.SH | 55,821 | 56,267 | +446 |
| fund | 150290.SZ | 49,234 | 49,610 | +376 |
| fund | 163210.SZ | 56,464 | 56,834 | +370 |
| fund | 150016.SZ | 72,334 | 72,700 | +366 |

> 上表只列 |delta| 最大的 10 行，全量见 `mismatch_252.csv`。

### 1.2 磁盘文件缺失（0 行）

- 9,787 条登记全部能按 `quote_path` 落到真实文件，**缺失 0 行**。
- 因此不存在「填 0」「删行」「猜路径」三种处置，一行都没有动行集合。

### 1.3 路径形态可疑（0 行）

- 检查项：`market` 列 vs ts_code 后缀推导是否一致；ts_code 后缀是否属于 {SH, SZ, BJ} 且大写；同一磁盘路径是否被多个 `(data_type, ts_code)` 重复命中。
- 结果：**0 行可疑**。`market` 列与后缀推导 9,787/9,787 一致；后缀全部合法且大写；无重复命中（9,787 条登记对应 9,787 个互不相同的文件）。

## 2. 与 G-045「11 行」的重叠核实

- G-045 记的是 `rows` / `start` / `end` 三列陈旧，不是 `file_size`；用户要求核实的正是「与本次 252 行是否重叠」。
- 全表 9,787 行重新扫 `rows`（parquet 行数）/ `start`（date 列 min）/ `end`（date 列 max）与磁盘实况：**当前不符 0 行**。
- 与 252 行的重叠：**0 行**。
- 结论：G-045 的 11 行在 2026-11-13 03:20 那次 `flush_meta`/重建后已与磁盘对齐，本轮不存在「同一行既要改 file_size 又要改 rows/start/end」的情况；严格按照「只改 file_size、其余列逐值不动」执行，`rows`/`start`/`end`/`built_at` 一行未改。

## 3. 成因

- 252 份行情文件的 parquet 载体特征与一致行**完全相同**：13 列同一 schema、ZSTD 压缩、`created_by = Polars (python) version 2.1.0`；`date` 列 min/max 与行数与索引登记一致。
- 因此差异只来自「文件被重写过、字节变了」，而索引里的 `file_size` 没有随之刷新；不是行数或区间漂移。
- 时间线：252 份文件 mtime 落在 2026-11-10 14:35 ~ 2026-11-12 19:18；对应索引 `built_at` 为 2026-11-11 16:58 / 18:08 / 18:09 / 18:11（10 行）与 2026-11-13 00:40（242 行）。其中 10 行的 `built_at` **早于**行情文件 mtime，属「文件后改、索引未跟」。
- 归因边界：未把 252 行逐一定位到具体写入调用点（`write_daily` 会即时取 `stat().st_size`，`tencent.daily_fill.refresh_meta` 也按磁盘实况重建），**本条按「已定性、未逐笔归因」记录**，不在本轮扩围。

## 4. 修复动作与断言

- 快照：`_backup/daily_index_filesize_20261113/daily_index.parquet` 与同目录 `daily_index.manifest.json`（`copy2`，SHA256 与改前一致后继续）。
- 写回方式：`src/data/atomic_write.py::write_parquet_atomic`（临时文件 → fsync → `os.replace`），参数与 `LocalQuoteStore.flush_meta` 一致：`compression="zstd"`、`statistics=True`。
- 替换表达式带显式兜底：`when(file_size != disk_size).then(disk_size).otherwise(file_size)`，未命中行写回原值，不用无兜底的 `coalesce`。

断言（全部通过，脚本 `_staging/daily_index_filesize_20261113/fix.py` 内 `check()`）：

| # | 断言 | 结果 |
|---|---|---|
| 1 | 行数守恒 9,787 → 9,787 | 通过 |
| 2 | 列集合/顺序/ dtype 不变（11 列） | 通过 |
| 3 | 各列空值数改前=改后（`source_mtime_ns` 20 行空值两侧一致） | 通过 |
| 4 | `file_size` 改后空值数 = 0 | 通过 |
| 5 | 改动行集合恰等于不一致集合（252 / 252，多 0、缺 0） | 通过 |
| 6 | 未命中/写错行数 = 0 | 通过 |
| 7 | 其余 10 列逐值 `Series.equals(null_equal=True)` | 通过 |
| 8 | `file_size` 合计 879,485,981 → 879,525,316（+39,335） | 通过 |
| 9 | 落盘后回读与内存结果 `DataFrame.equals` | 通过 |

断言自测（证明断言能失败）：

- 注入 1 行 `file_size = null` → 被「空值数不变 / file_size 空值=0 / 改动集合不等」同时抓住，抛错。
- 把 `fund/150008.SZ` 的 `file_size` 再 +1（越界改动）→ 被「改动集合不等于目标集合 / 合计不符」抓住，抛错。

## 5. 修复前后 SHA256 与字节数

| 文件 | 改前字节 | 改前 SHA256 | 改后字节 | 改后 SHA256 |
|---|---:|---|---:|---|
| `meta/daily_index.parquet` | 143,687 | `6fec4e9bf2136256d5effb2b964c40a93272c4066de73b04ce99deb45a845151` | 143,746 | `e876f467df3aee1ab6d3ea05f6beab7c665de302759b0989a92469bfa577baaf` |
| `meta/daily_index.manifest.json` | 1,276 | `f9c95cd4876e5a13ca87a7d7645704a69728135d3848fede77885b7fe74f1057` | 1,862 | `a84759095f1ed314bd6dc90ca7ba6e9007a5d73f388da2dc155cdd159e6b680a` |

> 字节从 143,687 变为 143,746（+59 B）属 parquet 重编码，不代表内容口径变化；内容等价由第 4 节的逐列 `equals` 与空值计数证明，不靠 SHA 推断（DG-34）。

## 6. 侧车 manifest 回写

- 回写字段：`generated_at`（2026-11-13 本次执行时刻）、`rows`、`bytes`、`sha256`、`columns`、`coverage`；`notes` 追加一条本轮记录，原有 G-139 那条保留不动。
- 保留字段：`dataset` / `semantics` / `source` / `authority` / `path` / `produced_by` 原样。

## 7. `manifest_tool.py` 复核

| 命令 | 改前（基线） | 改后 |
|---|---|---|
| `--check --only meta` | PASS（36 份 / 124 条 / 不一致 0） | **PASS**（36 份 / 124 条 / 不一致 0） |
| `--check --only quotes` | PASS（10,786 份 / 21,569 条 / 不一致 0） | **PASS**（10,786 份 / 21,569 条 / 不一致 0） |

- 完整输出：`_staging/daily_index_filesize_20261113/check_meta.txt`、`check_quotes.txt`。
- 说明：`--only meta` 里 `meta/trading_calendar/manifest.json` 仍报「缺 dataset, source, authority, semantics」4 项语义未登记，属既有状态（基线同样存在），不计入不一致、不影响 PASS。

## 8. DG-33 一致性（索引与 manifest 同步到位）

- 本轮改的正是索引本身，不是绕过索引直接改行情；索引 `daily_index.parquet` 与其侧车 `daily_index.manifest.json` 在同一次执行内先后落盘，`manifest` 的 `bytes` / `sha256` 取的是新索引落盘后的实测值。
- 未触碰 `quotes/**` 任何文件，因此不存在「磁盘行情改了、索引还旧值」的反向形态；同时 `coverage()` 只读本索引、不 stat 行情，本轮之后其 `file_size` 与磁盘实测**逐行一致（0 行偏差）**。
- 无需回填 `meta/daily_index.parquet` 之外的索引；`meta/manifest.json` 已显式声明不并入 `daily_index.parquet`（见其 `note`），故不涉及。

## 9. 真实读路径验读

- 入口：`LocalQuoteStore(Path("data/local_store"), enabled=True)`（`src/data/local_store/store.py`），即 `coverage()` / `covers()` / `is_stale()` 实际使用的加载器 `_load_meta()`。
- 全表：加载 9,787 条，逐条与该标的行情文件 `stat().st_size` 比对，**不一致 0 条**。

| 标的 | 索引 file_size | 磁盘 | coverage.rows | coverage.start~end | read_daily 行数 |
|---|---:|---:|---:|---|---:|
| `fund/150008.SZ` | 76,110 | 76,110 | 2,468 | 2009-11-19 ~ 2020-08-25 | 2,468 |
| `fund/168108.SZ` | 22,690 | 22,690 | 509 | 2017-07-11 ~ 2021-07-05 | 509 |
| `fund/164702.SZ` | 55,812 | 55,812 | 2,128 | 2012-10-25 ~ 2024-10-23 | 2,128 |

- `read_daily` 实际读出行数与索引 `rows` 一致，说明新索引未破坏读路径；`store.stats()` 报 9,787 标的 / 25,759,095 行 / 10,784 份行情文件。

## 10. 建议登记内容（交主控串行写入缺口台账）

本轮**未修改** `docs/数据缺口登记_正式表_2026-11-12.md`（该文件另有任务在写）。建议按下列文本登记：

```text
| G-144 | A | `meta/daily_index.parquet` 252 行 `file_size` 与磁盘实际字节不符，`coverage()` 不 stat 行情故下游按陈旧值判断 | 覆盖度容量口径、文件大小估计 | 已修 | 9,787 行逐行按 `quotes/<data_type>/<market>/<ts_code>.parquet` 取 stat：不一致 252 行（fund 247 / stock 5），delta −2,685 ~ +611、合计 +39,335 B；仅改这 252 行 `file_size`，其余 9,735 行与其余 10 列逐值不动（`equals` 通过、各列空值数不变）；磁盘缺失 0 行、路径形态可疑 0 行；`file_size` 合计 879,485,981 → 879,525,316。索引 143,687 B/`6fec4e9b…` → 143,746 B/`e876f467…`；侧车 manifest 同步回写；`manifest_tool.py --check --only meta` 与 `--only quotes` 均 PASS。G-045 的 11 行（rows/start/end）现测 0 行不符、与本次 252 行重叠 0。报告：`2026-11-13/daily-index-filesize/report.md` | 对账-20261113 | 锚点+实测 |
```

## 11. 附：本轮判定口径与边界

- 未修改 `data/` 下任何行情文件；未删除、未新增任何索引行。
- 未触碰工作区其他任务的 `M` / `??`；未修改 `docs/数据缺口登记_正式表_2026-11-12.md`。
- 缺失文件按「单列、不填 0、不删行、不猜路径」处理；本轮实际为 0 行。
- 未放宽任何断言或阈值；断言自测见第 4 节。
