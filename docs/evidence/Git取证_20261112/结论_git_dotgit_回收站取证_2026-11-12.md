# 两仓 `.git` 文件被反复移入回收站：只读取证结论（2026-11-12）

范围：源仓 `E:\code\python\quant`、备份仓 `G:\git\python-quant`。
性质：全程只读。未清理回收站、未还原任何条目、未写任何 `.git`、未提交、未推送、未动 `data/`。
产物：`work/`（脚本 4 个 + 原始导出 2 个 CSV），本文件为结论。

---

## 一、结论（按三档写明）

**第一档成立：能定到具体软件。**

- 执行「把文件移进回收站」这个动作的是 **TRAE CN（`D:\Program Files\Trae CN`，已安装版本 3.4.18，进程名 `Trae CN`）的 agent 命令执行环境**。它对被派发命令里发生的文件删除做了**「删除即入回收站」的安全兜底**，因此 git 每次正常写操作留下的旧副本都被搬进回收站，而不是物理消失。
- 「逻辑上发起删除」的是 **git 自己**（`git commit` / `git status` / 任何刷新索引与 ref 的命令），不是任何清理软件、杀毒软件或计划任务。

关键判据（三条互相独立）：

1. **工具层直接自报**。本会话执行 `git status` / `git log` 等只读 git 命令时，命令返回里附带平台生成的通知原文：
   `Original targets removed and moved to the recycle bin. Observed successful recycle events: 2 ... E:\code\python\quant\.git\index.lock`。
   即：一次只读 `git status` 就足以把 `.git\index.lock` 送进回收站。
2. **对照实验（同一进程、同一时刻、不同目录）**：
   | 探针路径 | 删除后是否进回收站 |
   |---|---|
   | `C:\Users\ADMINI~1\AppData\Local\Temp\_probe_c_tmp.txt` | **否**（C: 无新增 `$I`，工具层也未报 recycle 事件） |
   | `E:\code\python\quant\2026-11-12\git-dotgit-forensics\work\_probe_e_work.txt` | 是（工具层报 recycle events: 4） |
   | `E:\code\python\quant\tmp_probe_dir\_probe_e_root.txt` | 是 |
   | `E:\www\tmp\_probe_e_www.txt` | 是 |

   说明该行为**绑定在 agent 声明的可写工作区路径上，不是全机驱动级**：同一台机器同一秒在 C: 删同样的文件不会进回收站。这也同时排除了「全盘钩子/杀软驱动改写删除」这一类解释。
3. **与 git 事件逐条对齐（不是"时间相近"，是逐条命中）**：
   - `E:\code\python\quant`：`refs\heads\main` 进回收站 98 次，**98/98** 条与今日 reflog 事件相差 ≤3 秒；09:38:28 之后 reflog 共 99 条。
   - `G:\git\python-quant`：`refs\heads\main` 进回收站 81 次，**81/81** 条相差 ≤3 秒；09:38:28 之后 reflog 共 83 条。
   - `$R` 抽样内容与 git 语义吻合：`.git\index` 的 `$R` 头部是 `DIRC\0\0\0\2`（真实索引魔数），`refs\heads\main` 的 `$R` 头部是 40 位提交哈希（如 `c9c9e32b37c636cd…`、`2652cf5d99c17919…`）。
   - 典型批次：`2026-11-12 09:39:00–09:39:02` 正好对应备份仓 group C/D/E 三笔提交，同秒内还出现它们的消息临时文件 `.backup_msg_C_093900.txt`（245 B）/`.backup_msg_D_093900.txt`（142 B）/`.backup_msg_E_093901.txt`（128 B）。

**为什么**：`.git\index`、`refs\heads\main`、`index.lock`、`HEAD.lock`、`packed-refs.lock`、`AUTO_MERGE.lock`、`objects\maintenance.lock` 全部是 git 每笔提交/每次索引刷新会**重建的短期文件**。git 写入方式是「写临时文件 + 覆盖」，被覆盖的旧副本即成为一次删除；沙箱把它兜底搬进回收站。所以看起来是「`.git` 被反复删」，实际是**同几个文件的高频旧版本快照**，仓库本身没有内容丢失。

**用户此前"手工还原"的风险提示**：这些回收站条目是**过期的历史副本**（旧 index 指向旧 stat 缓存、旧 ref 指向旧提交）。把它们还原覆盖回 `.git` 会把仓库倒退到更早状态，属于有害操作。正确做法是不还原、直接清空（本次未据此做任何动作）。

---

## 二、`$I` 元数据统计（只读解析）

解析 3 卷回收站（`C:\$Recycle.Bin`、`E:\$Recycle.Bin`、`G:\$Recycle.Bin`，SID `S-1-5-21-…-500` = Administrator）：

| 指标 | 数值 |
|---|---|
| 解析到的 `$I` 总数 | 6,452（C 28 / E 3,943 / G 2,481） |
| 其中 2026-11-12 当天 | 6,422 |
| 路径含 `.git` 等关键字的 | **1,747**（E 925 / G 822） |
| 时间范围 | 2026-11-12 09:38:28 – 18:07:45 |
| 秒级批次 | 799 批，中位间隔 8 秒 |
| `$R` 缺失 | 2 条（均为 G: 18:07 的 0 字锁文件） |

按文件名聚合（全部 1,747 条）：

| 文件名 | 次数 | 说明 |
|---|---|---|
| `.git\index` | 373（E 196 / G 177） | 每次索引重写留一份旧副本 |
| `.git\index.lock` | 314 | git 临时锁 |
| `objects\maintenance.lock` | 182 | git maintenance 触发 |
| `.git\HEAD.lock` | 180 | HEAD 更新锁 |
| `.git\refs\heads\main` | 179（E 98 / G 81） | ref 更新旧值，`$R` 内容为 40 位 SHA |
| `.git\AUTO_MERGE.lock` | 179 | 合并锁 |
| `.git\packed-refs.lock` | 179 | ref 打包锁 |
| `refs\remotes\origin\main` | 76 | 仅 G:，推送/更新远端跟踪 ref 时产生 |
| `.git\trae_tmp_index_gap` | 42 | 本系列会话自建的隔离索引 |
| `.git\trae_tmp_index_finpit` | 8 | 同上 |
| `index.mtool*` / `index.taskf` / `index.basis` / `index.l375` / `tmp-turnover-index` 等 | 各 2–4 | 同上 |

同秒出现的非 `.git` 条目（共 103 条）也全部属于「工具/临时文件」而非业务数据，例如 `E:\www\tmp\idx1~idx3`、`.ruff_cache\0.17.3\…`、`2026-11-12\data-inventory\work\index.tmp`。

原始导出：
- `work/recyclebin_all.csv`（全部 6,452 条：卷 / SID / 删除时刻 / 原始路径 / 大小 / `$R` 是否存在）
- `work/recyclebin_git_hits.csv`（1,747 条 `.git` 相关）

---

## 三、已排除的方向（都有实测依据）

| 方向 | 结论 | 依据 |
|---|---|---|
| Windows Defender | 无关 | `WinDefend` 服务 **Stopped / Manual**；`Microsoft-Windows-Windows Defender/Operational` 最后一条 2026-11-12 **07:09:22**，近 3 天 **无 1116/1117** 记录 |
| 系统/应用事件 | 无对应活动 | 09:30–10:00 窗口 System 仅 1 条、Application 仅 2 条，且**无 Error/Warning** |
| 对象审计（Security 4663） | 不可用 | `auditpol` 返回 `A required privilege is not held by the client`；读 `Security` 日志同样被拒（1314）。**这条链路无法取证，不等于"没有活动"** |
| 第三方清理/优化软件 | 未安装 | 注册表卸载项里无 CCleaner / 360 / 腾讯电脑管家 / 金山 / Advanced SystemCare 等 |
| 同步/备份工具 | 已安装但未运行且任务无关 | `FileGee文件同步备份系统` 在列，**进程清单中无任何 FileGee 进程**；其唯一任务文件 `C:\ProgramData\FileGee\CHS\Personal\Data\Task` 内只有源 `I:\My documents.2026`，全文**不含** `E:\code`、`G:\git`、`quant` 字样 |
| 火绒安全软件 6.0.13 | 无参与证据，且被对照实验排除 | `HipsDaemon`/`HipsMain`/`HipsTray` 在运行，驱动 `sysdiag.sys`/`hrdevmon.sys`/`hrwfpdrv.sys`（北京火绒签名）；`C:\ProgramData\Huorong\Sysdiag\Quarantine` **0 个文件**（无隔离动作）。其 `log.db` 被服务独占、只读打不开，**未取得其内部日志**，故不作"日志无记录"的断言；排除依据是机制对照：同一会话在 C: 删除不进回收站、只在 E: 工作区进，与"驱动全盘接管删除"的表现不符 |
| 计划任务 | 无自定义项 | 命中"Clean/清理/优化/Storage/Maintenance"的**全部是微软内建任务**（`SilentCleanup`、`StorageSense`、`ScheduledDefrag` 等），无第三方注册 |
| 存储感知 / OneDrive / 文件历史 | 无关 | `File History (maintenance mode)` 为 Disabled；回收站内 6,422 条删除时刻与 reflog 一一对应，不是按天/按空间触发的批量清理 |
| 常驻扫描器触碰路径 | 未见 | 未 kill 任何进程；进程清单中只有 `TGitCache`（TortoiseGit 缓存，只读工作区）与 IDE 进程 |

---

## 四、两仓只读完整性自检（未修、未 reset、未 checkout）

| 检查项 | `E:\code\python\quant` | `G:\git\python-quant` |
|---|---|---|
| `git rev-parse HEAD` | `add33052b2b07184930bef531911f738fe08d13a` | `24c1570a9ed8b461b6c2ba186a69495cb8ed9b93` |
| `refs/heads/main` 内容 | `add3305…d13a`（与 HEAD 一致） | `24c1570…9b93`（与 HEAD 一致） |
| `git log -1` 时间 | 2026-11-12 18:30:38 +0800 | 2026-11-12 18:07:38 +0800 |
| `git fsck --no-progress` | 15 行，**全部是 dangling tree/commit/blob**，无 missing object、无 corrupt | 3 行 dangling，同样无缺失 |
| `git diff-index --cached HEAD` | 空（索引与 HEAD 树一致） | 空（索引与 HEAD 树一致） |
| `git status --short` | **0 行（clean）** | 6 行（`M .github/workflows/ci.yml`、`M tests/test_data_manager.py`、`M tests/test_volume_unit_repair.py`、`M tools/research/build_new_factors.py`、`?? docs/evidence/turnover_sign_fix_20261112/`、`?? tests/test_turnover_20_sign_guard.py`）——**属他路在途改动，本次未触碰** |
| refs vs packed-refs | 无 `packed-refs`，仅 loose `heads/main` | `packed-refs` 内 `origin/main = ced580e0…`，loose `remotes/origin/main = 24c1570a…`；loose 覆盖 packed，`for-each-ref` 输出 24c1570（**正常形态，不是错乱**） |
| 对象计数 | count 2,905 / in-pack 2,844 / packs 2 | count 2,540 / in-pack 5,754 / packs 2，`prune-packable 32` |

结论：**两仓 ref 与 HEAD 一致、索引与 HEAD 树一致、fsck 无对象缺失**，未发现指向错乱。故未触发"立刻停手"条件。

**一处需要知道的结构性隐患（不是当前故障）**：G: 的 `packed-refs` 里 `refs/remotes/origin/main` 仍是旧值 `ced580e0…`，当前靠 loose 文件 `24c1570a…` 覆盖。若这个 loose ref 再被移走并丢失，`origin/main` 会回落到旧值，表现为"远端跟踪分支倒退"。修法（未执行）：`git -C G:\git\python-quant pack-refs --all --prune` 或再执行一次 fetch/prune 让 packed 与 loose 同步。

---

## 五、复现命令（只读）

```powershell
.\.venv\Scripts\python.exe 2026-11-12\git-dotgit-forensics\work\parse_recyclebin.py      # 解析 $I → work/recyclebin_*.csv
.\.venv\Scripts\python.exe 2026-11-12\git-dotgit-forensics\work\analyze_recyclebin.py     # 卷/日期/文件名聚合
.\.venv\Scripts\python.exe 2026-11-12\git-dotgit-forensics\work\batch_timeline.py         # 秒级批次与间隔
.\.venv\Scripts\python.exe 2026-11-12\git-dotgit-forensics\work\correlate_reflog.py       # 与 reflog 逐条对齐
.\.venv\Scripts\python.exe 2026-11-12\git-dotgit-forensics\work\probe_scope.py            # 删除落点对照实验（会创建并删除自己的探针文件）
```

直接核验：
- 回收站条目：`Get-ChildItem 'E:\$Recycle.Bin' -Recurse -Force -Filter '$I*'`
- reflog：`git -C E:\code\python\quant reflog --date=iso | Select-Object -First 20`
- 完整性：`git -C <repo> fsck --no-progress; git -C <repo> diff-index --cached --stat HEAD`

---

## 六、补充样本取证（G-080 候选表、大体积 staging 产物、缓存）

重新扫描三卷（`$I` 总数由 6,452 增至 **7,332**），命中样本 **2,617** 条（C 卷 **0** 条，全在 E:/G:）。原始数据 `work/recyclebin_samples.csv`，脚本 `work/samples_timeline.py`。

### 6.1 时间线（按类聚合 + 关键逐条）

| 时间 | 路径 | 字节 | 推断动作者 | 证据类型 | 置信度 |
|---|---|---|---|---|---|
| 09:37:34–20:07:51（1,271 条） | 两仓 `.git\index`(439)、`index.lock`(394)、`refs\heads\main`(214)、`packed-refs.lock`(197) 等 | 0–83 KB | git 自身写操作 → 被 agent 执行环境回收 | `$I` + reflog 逐条对齐（98/98、81/81 ≤3s）+ 平台 recycle 事件 | **高** |
| 09:37:34–20:07:38（538 条，11.8 MB） | `__pycache__\*.pyc`、`__pycache__\tmp*` | 0–41 KB | python/pytest/mypy 等解释器进程 | `$I` 路径与 mtime 节奏一致 | 高 |
| 13:35:23–20:04:03（704 条，37.6 MB） | `.mypy_cache\**` | 数 KB–MB | mypy 进程自更新缓存 | `$I` | 高 |
| 11:43:01 / 11:43:04 | `_staging\ic_decay_20261111\`（含 `panel_ext.parquet` **2,876,466,698 B**）、`_staging\survivorship_ab_20261111\`（含 `panel_ab.parquet` **1,850,462,050 B**） | 4.7 GB | agent 会话删除整个 staging 目录（含目录节点 0 B 记录） | `$I` + 目录节点记录 | 高 |
| 19:43:26 | `_staging\g080_bench_20261112\bench.parquet`（88,689,051 B）+ 目录节点 | 88.7 MB | 同类目录清理 | `$I` | 高 |
| **19:53:38 / 19:55:10 / 19:56:08 / 20:00:13 / 20:01:28 / 20:02:37** | `_staging\g080_rewrite_20261112\valuation_daily_candidate.parquet` | **642,185,226 × 5 + 642,134,504 × 1** | `2026-11-12/techdebt/outputs/rewrite_valuation_g080.py` 每次重跑经 `write_parquet_atomic` 覆盖同一路径 → 旧副本被回收 | `$I`（6 条，`$R` 全部存在）+ 脚本第 207 行 `write_parquet_atomic(cand, CANDIDATE, …)` + `progress.json` 末次 20:02:45 | **高**（机制）／中高（具体进程） |
| 20:00:33 / 20:00:40 / 20:01:27 | `_staging\preclose_fix_20261112\880581.SH.candidate.parquet`、`399699.SZ.candidate.parquet` | 126 KB / 147 KB | 同一 preclose 修复脚本反复覆盖 | `$I` | 高 |
| 13:39–16:48（约 40 条） | `_staging\*\*.index`（`git_index`、`tmp.index`、`commit2.index`、`isolated.index` 等） | 81–84 KB | agent 用 `GIT_INDEX_FILE` 做隔离索引后清理临时索引 | `$I` | 高 |
| 16:39:33–16:39:49 / 16:40:08–16:40:10 | `_staging\hook_commit_20261112163933\`（index×3、msg.txt、目录节点）、`G:\…\_staging\s08_hook_backup_20261112164007\` | 82–83 KB | 两个会话各自建临时索引提交后清理 | `$I` | 高 |
| 18:57:09 / 18:57:56 | `_staging\event_proxy_events_20261112\{equity_issue,unlock}_proxy.parquet`、`manifest.json` | 200 KB / 695 KB / 5.8 KB | 该任务重跑覆盖 | `$I` | 高 |
| 13:34:41 / 13:34:56 | `_staging\ann_date_map_20261112\ann_date_map.parquet` | 4,179,968 B | 重跑覆盖 | `$I` | 高 |
| 2026-11-12（本次） | `work\probe_atomic\t.parquet`（探针） | 343,011 B | 我方的对照实验：连续两次 `write_parquet_atomic` | 平台 recycle 事件（1 条，显式给出该路径） | **高** |

**量化结论**：这 6 条 642 MB 记录合计占回收站 **3.85 GB**，加 11:43 两个 staging 大表 4.72 GB，本次样本在回收站里共占约 **8.6 GB**——用户感知的「642 MB 文件凭空消失」就是它。

### 6.2 动作者定位到哪一层

- **机制层（可定到软件）**：TRAE CN 的 agent 命令执行环境把「删除/覆盖旧文件」统一转成「移入回收站」。
- **规则层（可定到规则）**：触发条件是 `src/data/atomic_write.py` 的 `write_parquet_atomic`（临时文件 → `os.replace` 覆盖目标），**每条旧副本就是一次被覆盖的上一版**。同一路径重跑 N 次 → 回收站留 N 份完整旧表。这是"规则"，不是"谁在搞破坏"。
- **进程层（不可得，如实标注）**：具体 PID 定不到。原因：① `fsutil usn readjournal C:` → `Error 5: Access is denied`（`fsutil usn queryjournal C:` 可读，说明日志存在但读取需管理员）；② 无 4663 对象审计（`auditpol` 与 Security 日志均因权限被拒 1314）；③ 相关 python 进程已退出，无 4688 进程创建审计。补充：本批删除**全部发生在 E:/G: 卷**，即便拿到 `C:` 的 USN 也不含这些记录；且系统未开 4688/4663，回溯到进程号已不可行。

### 6.3 对「受控文件夹访问（CFA）是元凶」这一假设的证伪

| 待验项 | 实测 | 判定 |
|---|---|---|
| `Get-MpPreference` 的 `EnableControlledFolderAccess` / `ProtectedFolders` / `ExclusionPath` | **命令无任何输出**（Defender 服务 `WinDefend` 为 **Stopped/Manual**，取不到策略） | 不支持 |
| 注册表 `HKLM\SOFTWARE\Microsoft\Windows Defender\Windows Defender Exploit Guard\Controlled Folder Access` 及 Policies 同名键 | **键不存在**（两条查询均空） | 不支持 |
| Defender 事件 1116–1127 今日 | **0 条**（Operational 日志最后一条 07:09:22） | 不支持 |
| 机制对照 | CFA 的正常表现是**阻断写入并报错**（事件 1123/1124），不会静默把 642 MB 旧表移入回收站；而探针实验证明「一次 `os.replace` 覆盖」就产生一条 recycle 事件 | **证伪** |

结论：**不是 CFA，也不是 Defender**。给两仓加 Defender 排除、或把 `_staging`/`_backup` 移出 CFA 保护范围，都不会改变这一现象（详见第七节建议 4）。

### 6.4 计划任务 / 启动项 / 第三方工具

- 非微软计划任务仅 7 个：`AMDInstallLauncher`、`EsConfigDriverMonitor`、`LiteMonitor_AutoStart`、`StartCN`、`StartCNBM`、`StartDVR`、`DoubaoIme\ApplyUpdate`——**无清理类**。
- `Run`/`RunOnce` 非微软项仅 4 个：`sysdiag`（火绒）、`StartAUEP`（AMD）、`ESHOW_Sev`（加密狗监控）、`WorkBuddy`——**无清理/同步工具**。
- 第三方日志：火绒 `log.db`/`applog.db` 被服务独占无法只读打开（**不可得**，隔离区 0 文件）；FileGee 未运行且任务源为 `I:\My documents.2026`；Trae CN 自身不产出删除类日志。

## 八、「每次 Shell 调用结束时集中清理瞬时文件」假设的核验

### 8.1 可取到的时间戳（这是本次唯一硬证据源）

`%APPDATA%\Trae CN\ModularData\ai-agent\command-execution\<id>\execution.json` 逐调用记录：`createdAtMs` / `startedAtMs` / `finishedAtMs` / `cwd` / `exitCode` / `target:"sandbox"` / `sandboxImplType:"unified_sdk"` / `restrictedFiles:[]`。今日可解析 **1,010 次调用**；同目录下每次调用还带一个空的 `sandbox\` 子目录。回收站今日条目 **7,312** 条。脚本 `work/align_calls.py`。

### 8.2 对齐结果

| 判定项 | 数值 |
|---|---|
| 今日调用总数 | 1,010 |
| **结束时（0–2 s 内）产生删除的调用** | **2 次**（涉及事件 2/7,312 = 0.03%） |
| 结束前（即命令执行过程中）产生的删除 | 4,313（其中 **4,266 条属同一次 6 小时 15 分的长调用** `4e2de485`，13:39:50–19:55:24，exit=1） |
| 落在任何调用窗口之外（间隙/无覆盖） | 2,765 |
| **零删除事件的调用** | **994 / 1,010 = 98.4%** |
| 单次调用事件数众数 | `0`（994 次）；其次 1 次（7 次）、2 次（2 次） |
| `.git\index.lock`（399 条） | 执行中 265 / 结束后≤2 s 7 / 无覆盖 127 |
| `__pycache__`（542 条） | 执行中 421 / 结束后≤2 s 3 / 无覆盖 118 |

### 8.3 判定：**不成立**

依据：
1. **98.4% 的调用结束时一条删除记录都没有**。若存在"调用结束即清理"，每次调用至少应留 1 条。
2. 与"结束时刻"对齐的只有 **2 / 7,312 条**；而 `index.lock` 265 条、`__pycache__` 421 条发生在**命令执行过程中**——那是 git/python 自己写→删→再写的产物，不是事后清扫。
3. **2,765 条删除发生在没有任何工具调用运行的间隙**。清扫必须依附于"某次调用结束"这个时点，无法解释落在间隙里的删除；而"删除发生时同步改写为回收站"能解释。
4. 对照实验（本会话内）：一次只读调用（`Get-ChildItem`）→ **0 条**；一次 `write_parquet_atomic` 覆盖 → **恰好 1 条**，且正好是被覆盖的那个文件，同目录的其它瞬时文件一条没动。
5. 反例：`G:\git\python-quant\.git\packed-refs.lock` 这枚 0 字节陈旧锁从 18:07:38 存活至今近 2 小时、跨数十次调用，**从未被移走**（见 8.5）。若有"运行时清理瞬时文件"，它是最典型的清扫对象。

**修正后的机制表述**：不是"运行时定时清扫"，而是**「删除动作发生的那一刻，被沙箱执行层改写为移入回收站」**——同步、按事件触发、只作用于真正被删除/被覆盖的对象。

### 8.4 「IDE 运行时清理目录 / 日志 / 配置」：**证据不足 → 实际未观察到**

| 检查 | 结果 |
|---|---|
| 每次调用的 `…\command-execution\<id>\sandbox\` 目录 | **空目录**（无被清扫文件、无暂存副本） |
| `execution.json` 字段 | 只有沙箱结果与 `restrictedFiles:[]`/`traceEvents:[]`，**无任何清理相关字段** |
| `%APPDATA%\Trae CN\User\settings.json` | 无 `enableTrash` / `trash` / `sandbox` / `recycle` 任何键 |
| `%APPDATA%\Trae CN\logs\**` | 无文件名含 `sandbox` / `trash` / `recycle` 的日志 |
| `ModularData` 顶层 | 仅 `ai-agent`、`ttnet` 两个目录 |

结论：IDE 侧**没有**可佐证"运行时清理瞬时文件"的目录、日志或配置；能证实的只有"命令运行在沙箱化的执行层里"这一事实。因此该假设按**证据不足（且被 8.3 的反证推翻）**处理。

### 8.5 同族样本：`G:\git\python-quant\.git\packed-refs.lock`（0 字节，18:07:38）

实测：
- 文件仍在：`Length=0`，`CreationTime = LastWriteTime = 2026-11-12 18:07:38`，`Attributes=Archive`；两仓 `.git` 下现存的 `*.lock` **只有这一枚**。
- 回收站里同一路径、同一秒（**18:07:38**）有一条 `$I` 记录，**`$R` 缺失（R=N）**——全库仅有的 2 条 `R=N` 都落在这一秒的 G: 0 字节锁上。

判定：**与同一机制相关，但属于一次"未完成的回收"残留**——回收记账已写（`$I` 存在），载荷未落地（`$R` 缺失），原路径留下 0 字节文件；由于**此后没有任何进程再删除它**，而机制只在删除时触发、不做主动清扫，它就长期留存。

它为什么会引发 `File exists` 警告：git 更新 packed-refs 时要独占 `packed-refs.lock`，创建失败即打印 warning 并**跳过 packed-refs 更新**，但 loose ref 仍正常写入，所以 `reset`/`commit` 都"报一次警告且成功"。

**因此第七节建议 6 必须修正**：直接 `git pack-refs --all --prune` 会因这枚锁**同样报 `File exists` 而失败**，须先人工把该 0 字节锁移走（或改名）再执行。该锁已于 2026-11-12 晚按用户点名授权移除并成功执行 `pack-refs`，实测结果见 8.6。

### 8.6 处置实测：移除陈旧锁并 `pack-refs`（用户已点名授权，2026-11-12 晚补做）

用户点名了具体路径 `G:\git\python-quant\.git\packed-refs.lock`，授权只删这一个文件。执行结果与证据：

| 步骤 | 实测值 |
|---|---|
| 删前核验 | `Exists=True`，`Length=0`（=0 字节，符合放行条件），`CreationTime = LastWriteTime = 2026-11-12 18:07:38.280`，`SHA256=E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855`（空文件标准哈希） |
| 基线 | `show-ref` 3 行；`HEAD = refs/heads/main = refs/remotes/origin/main = 4650eb818caffb6cf304ba67d4c9accde2e76def`；`status --short` 10 行；`.git` 下 `*.lock` 共 1 枚（即本项目标） |
| 删除 | 只删该 1 个文件；删后 `still_exists=False`，`*.lock` 归 0 |
| `pack-refs --all --prune` | `EXIT=0`，**输出为空**——不再出现 `File exists`，与 8.5 的预判一致 |
| show-ref 前后 diff | **0 行差异** |
| 三处指向 | `4650eb8…` 三处全部与删前相同 |
| `status --short` 前后 diff | **0 行差异**（10 行基线未变） |
| `*.lock` 残留 | **0** |
| 打包生效 | `packed-refs` 169 B / 3 行（1 行头 `# pack-refs with: peeled fully-peeled sorted` + `refs/heads/main`、`refs/remotes/origin/main`）；loose ref 仅剩 1 个 `refs/remotes/origin/HEAD`（30 B 符号引用，`pack-refs` 按设计不打包符号 ref） |

未推送，`origin/main` 仍指 `4650eb8…`。

**现场再次复现同一机制**（可作为第 8.3 节结论的又一例证）：
- 第 2 步跑 `git status --short` 时，git 刷新并重写索引 → 工具层回收通知恰好 1 条，路径 `G:\git\python-quant\.git\index`；重写后的索引 84,184 B，`git diff-index --cached HEAD` 为 0 行（索引与 HEAD 树一致，无损坏）。
- 第 4 步 `pack-refs` 时回收通知 6 条：`packed-refs`、`packed-refs.lock`、`refs/heads/main`、`refs/heads/main.lock`、`refs/remotes/origin/main`、`refs/remotes/origin/main.lock` —— 正是 `--prune` 重写 packed-refs 并剪除两枚 loose ref 的正常动作，全部以"移入回收站"形式落地，与 8.3 的判定完全吻合。

## 七、建议（均未执行，供决策）

1. **不要还原这些回收站条目**，也不要为了"清理"去单独还原某个 `.git\index`：它们是旧副本，还原会覆盖当前正确状态。当前两仓已自检一致，可直接忽略。
2. **大文件改写改成「新名落盘 + 只切换一次」**（本条最能直接止损）：候选表先写 `valuation_daily_candidate_<轮次>.parquet`，校验通过后再 `os.replace` 成正式名。这样一次任务只产生 1 条回收站记录，而不是每重跑一次多 642 MB。**不需要管理员**。副作用：要改 [rewrite_valuation_g080.py](file:///e:/code/python/quant/2026-11-12/techdebt/outputs/rewrite_valuation_g080.py#L207) 的路径策略；轮次文件按 DG-5 留在 `_staging/` 并定期清理（清理动作本身也会进回收站，属预期）。
3. **旧的 642 MB 副本改由 `_backup/` 显式归档，不再依赖回收站兜底**：覆盖前把上一版移入 `_backup/g080_rewrite_<日期>/`（与 DG-11 同口径），回收站只留噪音。**不需要管理员**。副作用：占磁盘（每轮 642 MB），需定保留策略（建议只留最后一版）；`_backup/` 已在不进任何取数链路的边界内（DG-12）。
4. **不要**给两仓加 Defender 排除、也**不要**把 `_staging`/`_backup` 移出受控文件夹保护：CFA 未启用（注册表键不存在）、Defender 服务 `WinDefend` 已 Stopped，两者都不是本因（见 6.3），改了不会减少一条记录，反而削弱防护。**本应需要管理员——但结论是不必做**。副作用（若强行执行）：防护面变小，并留下"以为改过就安全"的错觉。
5. **清空回收站**：这些全是旧副本，可安全清空，能立刻回收约 8.6 GB。**不需要管理员**（当前用户自己的回收站）。副作用：不可恢复；**清空前必须先确认没有"误还原"**——把旧 `.git\index`／旧 ref／旧 642 MB 表还原回去才会造成真实损坏。
6. **补齐 G: 的 `packed-refs` 与 loose ref 同步**：须**先**移除 0 字节陈旧锁 `G:\git\python-quant\.git\packed-refs.lock`，再 `git -C G:\git\python-quant pack-refs --all --prune`；否则同一条 `File exists` 会让它继续失败（见 8.5）。**不需要管理员**。副作用：移除锁这一步属于删除动作、会被回收站留一条记录（属预期）；`pack-refs` 本身无副作用，只整理 ref 存储形态。
7. **想让下次能定到 PID** 才需要开审计：`auditpol /set /subcategory:"文件系统" /success:enable` 并启用 4688 进程创建审计。**需要管理员**（本会话无此权限）。副作用：Security 日志量激增（每天可达数百 MB）、需扩日志容量、有轻微性能开销；不开也不影响本次结论——回收站 `$I` 已覆盖全部删除事实，缺的只是发起进程号。
