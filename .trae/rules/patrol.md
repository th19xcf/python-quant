---
description: 巡检脚本 patrol_local.py 的挂起状态、判定门槛、恢复方式与模型侧读取时机；涉及巡检、长任务状态判定、patrol 相关任务前必读
---
# 巡检规则（按需，本地脚本执行，默认挂起）

- **默认不执行，已挂起**：巡检脚本 `tools/patrol_local.py` 已入库，但**当前挂起、不自动运行**，计划任务 `quant_patrol_10min` **未创建**。需要时人工执行一轮：`.venv\Scripts\python.exe tools\patrol_local.py`，结果照旧覆盖写 `logs/patrol_status.json`、异常时追加 `logs/patrol_alerts.log`。
- **挂起原因**：执行会话是非提权会话，无 Task Scheduler 写权限，注册计划任务被拒（`Access is denied`）；调度服务本身正常，纯属权限不足，不是服务故障。实测见判例 §六。
- **恢复方式两种，恢复前不要自行尝试**：① 管理员 PowerShell 注册计划任务——`schtasks /create /tn "quant_patrol_10min" /tr '"\<仓库\>\.venv\Scripts\python.exe" "\<仓库\>\tools\patrol_local.py" --quiet' /sc minute /mo 10 /f`；② 改成登录自启的常驻循环脚本（启动项 / `pythonw` 拉起，内部 `time.sleep(600)` 循环）。调度最小粒度 10 分钟，不要尝试更短间隔。两种方式都须由用户明确指示后才动，禁止再尝试绕过权限提权。
- **脚本成本与边界**：只读系统状态、进程 CPU 时间与文件元数据，不调用任何模型、不写 `data/`、不删除任何文件，单轮模型 token 成本为 **0**。
- **脚本产出两个文件**：`logs/patrol_status.json`（每轮覆盖写，含 `checked_at`、每个任务的判定与支撑数字）；`logs/patrol_alerts.log`（只在 `stalled` 时追加，正常时文件都不创建）。
- **判定是多信号、按可靠性排序的**，任一单条都不足以下结论：① `progress.json` / `manifest.json` 的计数变化（最强，DG-7 本就要求 `_staging/` 下有断点进度文件）；② 登记产物的 mtime 与大小；③ 任意子进程（`python` / `git` / `node` / `powershell` / `cmd` / `schtasks` 等，不限 python）的 CPU 时间增长；④ 会话最后更新时间（读得到就报，读不到标 `unavailable`，不伪造）。
- **判 `stalled` 的门槛**：只有信号 ①② 都可用、且三项硬信号连续两轮都不动才允许判 `stalled`；信号 ①② 缺登记或读不到时，即使进程状态异常也只能判 `unknown`。**不得**把「`python.exe` 进程不存在」单独当吊死判据——模型会话纯读文件、改文件、推理的时段本来就没有子进程，而这段时间往往最耗时。
- **任务登记**：`config/patrol_targets.json` 的 `tasks` 每项登记任务名、期望产物（路径或 glob，可为绝对路径）、`timeout_minutes`，可选 `process_match`（命令行子串）。清单缺失或为空时脚本退化为扫 `_staging/**` 与 `data/local_store/**` 的最近 mtime，并在结果里标 `degraded=true`。长任务结束后必须及时移除对应条目，否则进程消失且产物不再变动会在两轮后判 `stalled`。
- **模型侧按需读取，没有定时唤醒**：只在三处读 `logs/patrol_status.json`——① 用户发消息时；② 子会话回报时；③ 每日 16:10 那批任务里。每次只读这一个文件，读完整份再判定，不逐条回溯会话正文。**脚本挂起期间该文件不会自动刷新，读取时必须先看 `checked_at` 判断新鲜度，不得当作实时状态**；`checked_at` 距今超过 15 分钟即视为过期，只能作为背景参考。
- **有 alert 才处置，无 alert 就静默**：`logs/patrol_alerts.log` 非空时，按 `stalled` 条目先催（只问卡在哪步、有无外部阻塞、还要多久），确认卡死才另起接管，且必须从断点续跑、先声明文件归属；alert 为空则不打扰用户。
- **巡检不得越权**：脚本侧与模型侧都不得删除任何数据、不清空或覆盖备份目录、不重置 Git 历史、不批量清理工作区、不改任何代码、不自行开新任务、不替用户做决定。脚本侧另有静态用例守护（`tests/test_patrol_local.py::test_module_has_no_delete_calls`）。
- **可随时调整**：频率、开关由用户一句话决定；暂停或删除计划任务不等于终止在跑的子会话，不得表述为"已停止"。
