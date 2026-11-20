# `research/events/data/**` 少 20 件核查报告

- 核查对象：`e:\code\python\quant\research\events\data\`
- 核查时间：2026-11-13
- 核查口径：**只读取证**。未恢复、未移动、未删除任何文件；未改动 Git 索引、未提交
- 基线对比：2026-11-12 实测 142 件 / 22,432,329 B（`2026-11-12\research-events-data-audit\work\scan.json`，142 条逐件记录含 bytes/mtime/sha256/rows/columns）→ 今日 122 件 / 8,978,792 B
- 镜像侧对照：`G:\git\python-quant\research\events\data` 122 件 / 8,978,792 B，与源侧逐项一致（无镜像偏差），与背景描述相符

---

## 一、结论（先给答案）

**20 件全部为「① 已搬迁」，无一件丢失，无一件需要重建。**

| 结论 | 件数 | 体积 | 去向 |
|---|---:|---:|---|
| **① 已搬迁（内容逐字节一致）** | **20** | **13,453,537 B** | 11 件 → `docs/evidence/research_events_20261103/`；9 件 → `_staging/` 三个任务目录 |
| ② 有可恢复副本（丢失件） | 0 | 0 | — |
| ③ 确实丢失 | 0 | 0 | — |

搬迁动作有两次独立记录互相印证：

1. **Git 提交** `5de8618`（长哈希 `5de861896eea07c2afa08c3a1c1b97a4e0b7414d`，2026-11-12 22:10:45），标题「refactor(research): 研究事件数据按 DG-32 三分收口并改接补数落点」。`git show --stat 5de8618` 中 11 个 CSV/JSON 全部显示为**纯重命名（`| 0`，0 行增删）**——即 Git 层面就是同一 blob 换路径，内容不可能变化。
2. **数据变更台账** `e:\code\python\quant\logs\data_changelog.csv` 存在一行：
   `step2_research_events_split_20261112,2026-11-12T22:12:00,research_events_step2,move,research/events/data/ -> docs/evidence/research_events_20261103/ + _staging/,13453537,...`
   记录的 13,453,537 B = 106,117 B（11 件证据件）+ 13,347,420 B（9 件中转件），与实测逐项吻合。

数字闭环：142 − 20 = 122。当前 122 件的构成为：顶层 2（`announcements_raw.parquet`、`yjyg_raw.parquet`）+ `backfill\` 83 + `backfill\daily\` 1（`20261111.parquet`）+ `backfill_supplement\` 36；与 `scan.json` 做集合差得到的「缺失 20 件」正好就是下表 20 行，且**无任何新增（多余）文件**。

核查手法（可复算）：
- 清单还原：用 `2026-11-12` 审计的 `scan.json`（142 条，含每件 sha256）与当前磁盘做 `Compare-Object` 集合差，得到精确 20 条，0 条多余。
- 搬迁核对：9 件中转件逐一比 SHA256（与 `scan.json` 记录的搬迁前 SHA256 相等）；11 件证据件逐一比 Git blob（`git hash-object <新路径>` 与 `git rev-parse 5de8618^:<原路径>` 相等）。
- 二次副本核对：`_staging/` 9 件在 `G:\git\python-quant\_staging\` 下同样存在且 SHA256 相等；`docs/evidence/` 11 件在 `G:\git\python-quant\docs\evidence\research_events_20261103\` 下存在（11 件 / 106,117 B），且随 Git 入库，Git 历史另有完整 blob。

---

## 二、20 件逐件表

### A 组：11 件证据件（含 3 个手写知识表）→ `e:\code\python\quant\docs\evidence\research_events_20261103\`

| # | 原路径（`research/events/data/` 下） | 现状 | 现路径 | 证据（SHA256 / Git blob） |
|---:|---|---|---|---|
| 1 | `announcement_event_summary.csv` | ① 已搬迁 | `docs\evidence\research_events_20261103\announcement_event_summary.csv` | blob `98a5d0e8…` 与 `5de8618^` 原路径相等；SHA256 `451a1f9329688648d97b8a97fc9b767ff74080746d401d098740fca4d89c9f23`；15,606 B |
| 2 | `announcement_event_summary_ma.csv` | ① 已搬迁 | `docs\evidence\research_events_20261103\announcement_event_summary_ma.csv` | blob `171a244f…` 相等；SHA256 `b45f4b76f90752b8b57aff47f7c3019746d02cd4e63ee02c53addfdea5c39308`；15,371 B |
| 3 | `event_sector_map.csv` | ① 已搬迁 | `docs\evidence\research_events_20261103\event_sector_map.csv` | blob `0c8478a9…` 相等；SHA256 `2c73d09dad2e0362c7016b16066d170553c5c3ccd559d7c98f091a141c59ae21`；6,267 B |
| 4 | `fetch_meta.json` | ① 已搬迁 | `docs\evidence\research_events_20261103\fetch_meta.json` | blob `5e10601a…` 相等；SHA256 `ed30d3ced2e2f9e7d92299cb1d461907f426a094f9afcda5b282d8512e780418`；710 B |
| 5 | `macro_events.csv` | ① 已搬迁 | `docs\evidence\research_events_20261103\macro_events.csv` | blob `ba8e786f…` 相等；SHA256 `97abe8c323724ae179d7572fb318d9f4d56887eb844fc8eb5b2e87bb8d7d2774`；7,480 B |
| 6 | `macro_event_summary.csv` | ① 已搬迁 | `docs\evidence\research_events_20261103\macro_event_summary.csv` | blob `17623050…` 相等；SHA256 `8d60194ac8a65980e274bfd02ec2cd95e99251c45957f00cec2f548c36f56052`；39,825 B |
| 7 | `macro_mapping_verdict.csv` | ① 已搬迁 | `docs\evidence\research_events_20261103\macro_mapping_verdict.csv` | blob `67e9a6f1…` 相等；SHA256 `719a4ae92c938435f302b0369283e97797094721ff9c190d26f0afbf78a5a1c9`；2,349 B |
| 8 | `news_source_probe.json` | ① 已搬迁 | `docs\evidence\research_events_20261103\news_source_probe.json` | blob `daf8d41e…` 相等；SHA256 `3049ff591d2f7a02b4f7c78edd8f53275cb3eb22b26485bbda4f6f2a2e99d65d`；2,713 B |
| 9 | `news_source_sample.csv` | ① 已搬迁 | `docs\evidence\research_events_20261103\news_source_sample.csv` | blob `d10a94e3…` 相等；SHA256 `865f7e74869163f733c8f4f14ec40139b49103aef6ad0877748d33305644c908`；3,588 B |
| 10 | `sector_etf_universe.csv` | ① 已搬迁 | `docs\evidence\research_events_20261103\sector_etf_universe.csv` | blob `f2121230…` 相等；SHA256 `79dd64a94acfe7ee3b0a7aac23d37022cf7f5ca199d9d887546160a2b05f562b`；409 B |
| 11 | `backfill_supplement\_audit.json` | ① 已搬迁（**改了文件名**，去掉目录前缀） | `docs\evidence\research_events_20261103\_audit.json` | blob `7e12144c…` 相等；SHA256 `955ab04c6457b7bd390108bead264040d7208386c44e34f45cc73d0b6e2227ff`；11,799 B |

> 校验合计 106,117 B，与提交说明、changelog 记录的「11 件证据件 106,117 B」一致。
> 旧路径 `git cat-file -e HEAD:research/events/data/macro_events.csv` 返回 `fatal: ... does not exist in 'HEAD'`，说明旧路径在 HEAD 中已不存在，搬迁已入库。

### B 组：6 件可重算中间派生件 → `e:\code\python\quant\_staging\research_events_20261103\derived\`

`_staging` 是仓库根下的候选层（`src/data/staging.py` 定义的权威落点），不在 `data/` 之下，不进任何取数/回测/展示链路。

| # | 原路径 | 现状 | 现路径 | 证据（SHA256 与 11-12 审计记录相等） |
|---:|---|---|---|---|
| 12 | `announcement_events_normalized.parquet` | ① 已搬迁 | `_staging\research_events_20261103\derived\announcement_events_normalized.parquet` | `102440037997591a9b2ef04d97e29134ca6a963bce877f308e60dba1b7476fe6`；623,561 B；22,101 行 |
| 13 | `announcement_event_level_car.parquet` | ① 已搬迁 | `_staging\research_events_20261103\derived\announcement_event_level_car.parquet` | `fafa0121e3e20218100aa77e92bde6d79673f6af3faf28dfb4da21115dd10777`；2,095,922 B；20,220 行 |
| 14 | `announcement_event_level_car.csv` | ① 已搬迁 | `_staging\research_events_20261103\derived\announcement_event_level_car.csv` | `c74dc48ff2ca62131626f6ecd2483d78b5a15a1b6f6a1bc015551fd93c025660`；8,805,001 B；20,220 行（`stock_code` 为 int 的有损副本，原始即如此） |
| 15 | `macro_event_level_car.csv` | ① 已搬迁 | `_staging\research_events_20261103\derived\macro_event_level_car.csv` | `591cb40ae7801d4bf65781a11078d66a1efd694521353e5663442843d39886a5`；61,789 B；181 行 |
| 16 | `backfill\_merged_for_import.parquet` | ① 已搬迁（保持父子目录结构，未与 17 混放） | `_staging\research_events_20261103\derived\backfill\_merged_for_import.parquet` | `8fb98e3c1825f1483127111008a1274135129d2420bf9e2f5894a3c8bfe95613`；660,377 B；21,076 行 |
| 17 | `backfill_supplement\_merged_for_import.parquet` | ① 已搬迁 | `_staging\research_events_20261103\derived\backfill_supplement\_merged_for_import.parquet` | `f05554657766bd51494b5fa98ec6fe51f96f65773c4fe6da59231bab0e3126c8`；419,209 B；15,686 行 |

### C 组：1 件伪断点件 → `e:\code\python\quant\_staging\events_announcements\`

| # | 原路径 | 现状 | 现路径 | 证据 |
|---:|---|---|---|---|
| 18 | `announcements_partial.parquet` | ① 已搬迁（按 DG-5 续跑口径落在**不带运行日**的稳定目录） | `_staging\events_announcements\announcements_partial.parquet` | `2bce5ec21d391bfc06d213b08fce0580bbb7a72c7410b7032854ae7020d074fd`；677,167 B；29,739 行 |

### D 组：2 件日更断点/合并件 → `e:\code\python\quant\_staging\events_daily\`

| # | 原路径 | 现状 | 现路径 | 证据 |
|---:|---|---|---|---|
| 19 | `backfill\daily\_merged_20261111_20261111.parquet` | ① 已搬迁 | `_staging\events_daily\_merged_20261111_20261111.parquet` | `dc66fae46ff2507bf6a1b0722e748f9246c56c6d7d1ace3eca37dba1f5996259`；4,221 B；20 行 |
| 20 | `backfill\daily\progress.csv` | ① 已搬迁 | `_staging\events_daily\progress.csv` | `3af545bfceecfa22a6064a562c2ac30b12d6d69f7b4049da0949a21575732432`；173 B；2 行 |

> 第 19 件现 mtime 为 2026-11-12 22:03:30（原 mtime 2026-11-11 14:21:58），mtime 变了但 SHA256 与搬迁前完全相同——属「复制未保留时间戳」，内容未变（DG-34：mtime/SHA 单独都不能证明内容，须按内容比，本表已按 SHA256 比过）。

### 其它排查项的答复

| 排查项 | 结果 |
|---|---|
| 全仓同名文件搜索（候选落点 `data/local_store`、`_backup`、`reports`、`2026-11-12`、`2026-11-13`、`E:\www\tmp`） | 上述 6 个落点**均无同名副本**；唯一额外命中是 `E:\www\tmp\baseline\research\events\data\` 下 11 件**更旧的基线快照**（见下文附注 2），不是本次搬迁的目标副本 |
| `logs/data_changelog.csv` | 有 1 条 move 记录：`step2_research_events_split_20261112`，2026-11-12T22:12:00，13,453,537 B，与实测吻合；无 delete 记录 |
| `_backup/` 是否有对应快照 | 无（该子树从未进入 `_backup/`，本次也未产生快照——因为是移动不是替换，按 DG-11 无需快照） |
| `_staging/` 任务目录 `progress`/`failed` 清单 | `_staging\events_announcements\failed.csv`（48 B）、`_staging\events_daily\failed.csv`（21 B）为搬迁时按新口径补建的失败清单，内容不涉及这 20 件；未发现「这些文件抓取失败/丢弃」类记录 |
| 回收站 | **未访问**（按策略不得访问回收站）。此项不影响结论：20 件全部有现存副本且内容一致，不存在「被误删需从回收站取回」的情形 |
| `git log --all --diff-filter=D` | 只出现两次 `git rm --cached`（`232a1ae` 11-09 撤 40 个 parquet、`5bd106e` 11-09 撤 supplement 合并件、`f2e2a32` 11-12 撤 `macro_event_level_car.csv`），**均只改索引、未删磁盘**；`5de8618` 的 11 条 D 是同笔提交里的重命名（A+D 成对），实为移动 |
| `git rev-list --all --objects` | 11 件证据件的 blob 在历史中完整存在；4 个顶层 parquet 与 36 个分片的 blob 自 `232a1ae` 起仍在历史中（净占用 4.57 MiB），可随时 `git show` 取出 |

---

## 三、附注（本轮核查的额外发现）

1. **11-12 审计表有一处笔误，已被本次核对纠正**：`2026-11-12\research-events-data-audit\outputs\research-events-data-审计.md` §1.2 表把 `backfill\_merged_for_import.parquet` 记为「419,209 B / 21,076 行」，与该文件真实值（660,377 B）不符——419,209 B 实为 `backfill_supplement\_merged_for_import.parquet` 的体积，两件被抄串了。`scan.json` 的原始记录是对的（`bytes: 660377` / `sha256: 8fb98e3c…` / `rows: 21076`），本次搬迁后的 staging 副本与该 SHA 相等。因此审计 §6.3「备份 660,377 vs 源 419,209 内容不一致」这一条也属同一笔误导致的误判，实际两侧内容一致。
2. **`E:\www\tmp\baseline\research\events\data\` 有 11 件更旧的同名前身**：`macro_event_level_car.csv`、`macro_events.csv`、`macro_event_summary.csv`、`macro_mapping_verdict.csv`、`event_sector_map.csv`、`sector_etf_universe.csv`、`news_source_sample.csv`、`news_source_probe.json`（2,606 B，非 2,713 B）、`fetch_meta.json`（669 B，非 710 B）、`announcement_event_summary.csv`、`announcement_event_summary_ma.csv`。其中两个 json 的体积与最终版不同，是更早版本的基线快照，**不得当作搬迁副本或恢复源**。**更正补记（2026-11-13）：该目录 11 件是更旧的基线快照，体积已与最终版不同，任何情况下都不得当作恢复源**——恢复只能取自 Git 历史（`5de8618^` 的 blob）或 `G:\git\python-quant` 镜像。
3. **`_staging/` 侧确有第二份拷贝**：`G:\git\python-quant\_staging\research_events_20261103\`（7 件 / 12,668,715 B）、`...\events_announcements\`（3 件 / 678,779 B）、`...\events_daily\`（4 件 / 5,777 B）存在，且 9 件搬迁件在 E:/G: 两侧 SHA256 逐一相等。加上 `docs/evidence/` 随 Git 入库，**这 20 件每件都至少有 2 份物理副本**，不存在单盘风险。
4. **环境事件（如实记录，非本任务操作）**：本轮第一次 `git` 调用时，终端回执提示 `E:\code\python\quant\.git\index.lock` 被移入回收站（「Observed successful recycle events: 1」）。该删除不是我执行的，我的命令里没有任何删除动作；随后 `git` 全部命令正常，且当前 `.git\index.lock` 已不存在。按项目约定「遇到 `.git/index.lock` 不得自行删除，必须先报告」，此处如实上报，供你判断是否有并发提交被打断。

---

## 四、如果确实丢了、按什么口径重建（仅建议，本轮不动手）

本轮 20 件**全部健在**，本节仅为备用口径，来源是搬迁时同批写下的 `e:\code\python\quant\_staging\research_events_20261103\README.md`（加本报告核对结论）：

| 类别 | 文件 | 重建命令（仓库根执行，`.venv\Scripts\python.exe`） | 输入 | 判据 |
|---|---|---|---|---|
| 派生 3 件 | `announcement_events_normalized.parquet`、`announcement_event_level_car.parquet`、`announcement_event_level_car.csv` | `python research\events\announcement_event_study.py` | `research\events\data\announcements_raw.parquet`、`research\events\data\yjyg_raw.parquet`、`data\local_store\quotes\**` | 22,101 行 / 9 列；CAR 表 20,220 行。**按语义与行数判，不按 SHA**（README 明示重建后字节可能不同） |
| 派生 1 件 | `macro_event_level_car.csv` | `python research\events\macro_event_study.py` | `docs\evidence\research_events_20261103\` 下 `macro_events.csv`、`event_sector_map.csv`、`sector_etf_universe.csv` + 行业 ETF 本地行情 | 181 行 |
| 合并件 1 | `backfill\_merged_for_import.parquet` | `python tools\backfill_events.py --import-only --dry-run --start 2015-01-01 --end 2026-11-10` | `research\events\data\backfill\*.parquet`（83 分片） | 该件是 2026-11-03 冻结副本（660,377 B / 21,076 行）；同命令现在产出 190,698 行 / 5,349,542 B，字节差异属正常，落在 `_staging\events_backfill\_merged_backfill_for_import.parquet`，**不得覆盖本归档件** |
| 合并件 1 | `backfill_supplement\_merged_for_import.parquet` | `python tools\backfill_events.py --import-only --dry-run --shard-dir research\events\data\backfill_supplement --start 2020-03-01 --end 2025-07-31` | `research\events\data\backfill_supplement\*.parquet`（36 分片） | 15,686 行 / 419,209 B，可逐字节复现 |
| 断点 2 件 | `backfill\daily\progress.csv`、`backfill\daily\_merged_20261111_20261111.parquet` | 建议**不重建**：由 `tools\update_events.py` 下次日更自然重新生成，`progress.csv` 为逐日追加 | 日更分片 | 追补会制造假进度（与 `project_rules.md`「历史任务一律不追溯补」同源） |
| 断点 1 件 | `announcements_partial.parquet` | 不重建：`fetch_announcements.py:264-266` 每抓完一个关键词整覆盖写，且函数无续跑读取分支（11-12 审计已判「伪断点」） | 采集接口 | 重抓即产生新文件，语义等价但不保证字节一致 |
| 证据 11 件 | 3 个手写知识表（`macro_events.csv`、`event_sector_map.csv`、`sector_etf_universe.csv`）+ 2 个 summary + verdict + 2 个新闻探针 + `fetch_meta.json` + `_audit.json` | **不可重建**：手写知识表与采集台账无上游来源，summary/verdict 是报告结论底稿 | — | 恢复只能取自 Git 历史（`git show 5de8618^:research/events/data/<原名>`）或 `G:` 镜像；这也是把 11 件放进 `docs/evidence/` 随 Git 入库的原因 |

补充口径（若未来真要做恢复动作，这三条必须一起守）：
- 证据 11 件**唯一可靠恢复源是 Git**（`5de8618^` 的 blob），不要指望从 `E:\www\tmp\baseline\` 取（那里是更旧版本，两个 json 体积已不同）。
- 两份同名 `_merged_for_import.parquet` 必须分别落回 `derived\backfill\` 与 `derived\backfill_supplement\`，**不得合并到同一目录**（README 明确要求，合并即失去可辨别性）。
- 恢复后要做 DG-23 的下游一致性：至少一条真实读路径端到端跑通（如 `tools\backfill_events.py --import-only --dry-run` 的既有行受影响数须为 0），只验文件本身不算通过。

---

## 五、可复算命令

```powershell
# 1 当前件数
Get-ChildItem research\events\data -Recurse -File | Measure-Object Length -Sum
# 2 与 11-12 审计清单求差（得 20 条缺失、0 条多余）
$j = Get-Content '2026-11-12\research-events-data-audit\work\scan.json' -Raw | ConvertFrom-Json
$root='e:\code\python\quant\research\events\data'
$cur = Get-ChildItem $root -Recurse -File | ForEach-Object { $_.FullName.Substring($root.Length+1).Replace('\','/') }
Compare-Object $j.PSObject.Properties.Name $cur
# 3 证据 11 件：新老 blob 相等
git hash-object docs/evidence/research_events_20261103/macro_events.csv
git rev-parse 5de8618^:research/events/data/macro_events.csv
# 4 中转 9 件：SHA256 与审计记录相等
(Get-FileHash '_staging\events_announcements\announcements_partial.parquet' -Algorithm SHA256).Hash.ToLower()
# 5 搬迁记录
git show --stat --format='%H %ad %s' --date=iso 5de8618
Select-String -Path logs\data_changelog.csv -Pattern 'step2_research_events_split'
```
