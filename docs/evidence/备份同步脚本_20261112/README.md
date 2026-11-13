# 备份同步脚本说明（2026-11-12 批次）

生成者：Agent（本会话不可写 `G:`，故改为生成脚本，由用户在**普通 PowerShell 窗口**执行）
脚本版本：`sync_to_backup.py v1.0.0`，生成时间 `2026-11-12T09:05:00+08:00`

## 一、为什么需要这个脚本

本会话进程在 `G:\` 上「新建文件/目录」被拒（`errno=13`、无 Win32 错误码），
但「覆写已有文件」正常；`C:`、`E:` 新建正常，卷健康、ACL 无 Deny、令牌为 Medium 完整性。
`git commit` 必须新建 `index.lock` 与 object，因此本会话无法完成备份仓库提交。
脚本把原定 1→4 全部动作固化下来，交给用户在普通 PowerShell 中执行。

## 二、执行前

```powershell
# 1) 先看清单（不写任何文件）
E:\code\python\quant\.venv\Scripts\python.exe `
  E:\code\python\quant\2026-11-12\backup-sync-script\outputs\sync_to_backup.py --dry-run

# 2) 确认无误后正式执行
E:\code\python\quant\.venv\Scripts\python.exe `
  E:\code\python\quant\2026-11-12\backup-sync-script\outputs\sync_to_backup.py
```

`--dry-run` 不探测 `G:`、不拷贝、不提交、不推送，只打印计划。

## 三、脚本做什么（四步）

| 步 | 动作 | 通过标准 |
|---|---|---|
| 1 | 镜像：`git ls-files` 跟踪文件 + `data/local_store/**` + `_backup/**` + `reports/**`；缺的补、大小不同或 SHA 不同的覆盖；源端已删的跟踪文件在对端对位删除 | 三棵子树「集合差 = 0」；逐文件 SHA256 不一致数 = 0 |
| 2 | 备份仓库 `git rm -r --cached reports` + `.gitignore` 追加 `/reports/` + 单独提交 | `git ls-files reports` 为空；磁盘 reports 文件数与执行前一致（实测 2,989）；`reports` 镜像集合差仍为 0 |
| 3 | A→B→C→D→E→F 六组备份提交（串行），逐组 `copy2` → `git add --` → 校验暂存集 → `git commit -F` → `git diff-tree` 自证 | 每组 HEAD 文件清单恰为该组文件，多出即停 |
| 4 | `git push origin main`（默认代理配置，仅一次） | 成功则打印成功行；失败只打印本地 HEAD / `origin/main` / 失败原文，不重试 |

末端打印「汇总（可直接粘贴）」，含三棵子树集合差与 SHA256 不一致数、各组提交哈希与文件数、推送结果、最终 HEAD。

## 四、可重复执行（幂等）

- 镜像：第二次运行待补文件为 0，不会重复拷贝。
- `reports` 取消跟踪：已无跟踪项且 `.gitignore` 已含 `/reports/` 时整步跳过。
- 六组提交：按 message 标题在 `git log --all --format=%s` 中查重，已提交的组跳过。
- 推送：`origin/main..HEAD` 为 0 时跳过推送。
  **预期：第二次运行应全部为空操作。**

## 五、断点续跑

脚本按「步骤 1 → 2 → 3(逐组) → 4」顺序推进，每步通过后才进入下一步，因此任何一步失败后
**直接重跑同一条命令** 即可：已完成的步骤会被识别为已完成并跳过。

## 六、失败停在哪、怎么回滚

| 失败点 | 现象 | 处理 |
|---|---|---|
| 预检 | `[STOP] G: 不可写` | 确认在用户自己的 PowerShell（非受限环境）里运行；未做任何写入，无需回滚 |
| 步骤 1 集合差 ≠ 0 或 SHA 不一致 | `[STOP] ... 集合差 N 不为 0` / `SHA256 不一致` | 仅发生了 `copy2`，`data/` 源侧从未被修改；重新执行一次即可；如需还原备份侧文件，从 `G:\git\python-quant` 的 git 历史取回（`git checkout -- <path>`） |
| 步骤 2 自检失败 | `[STOP] 取消跟踪后仍有 ...` / `磁盘 reports 文件数变化` | 该提交已生成。回滚：`git -C G:\git\python-quant reset --soft HEAD~1` 后手工检查；**不要** 用 `reset --hard`、`clean` |
| 步骤 3 某组暂存集/清单不一致 | `[STOP] 组 X 暂存集与预期不一致` | 未产生提交（暂存集检查）或已产生提交（清单检查）。后者回滚：`git -C G:\git\python-quant reset --soft HEAD~1`，再用 `git restore --staged` 清暂存 |
| 步骤 4 推送失败 | 打印本地 HEAD / origin/main / 原文 | 仓库与提交均完好，仅网络/代理问题；按事实处理，不改全局配置、不加 `--force` |

**回滚只允许 `git reset --soft`（保留工作区内容）**，脚本与说明均不使用 `--hard`。

## 七、红线（已写入脚本并按代码硬保护）

- 不引入、不调用 `shutil.rmtree`；脚本内无任何整目录删除。
- 不直接操作备份仓库 `.git` 内部文件（`guard_dst` 拦截所有落到 `.git` 下的写入）。
- 不使用 `--force` / `--force-with-lease` / `git reset --hard` / `git clean`。
- 不删除 `data/` 任何内容：删除动作只发生在「备份仓库跟踪、源端已不存在」的单文件对位删除，且目标必须在 `DST` 之下。
- 每一步写入目标都过 `guard_dst`，越界立即 `[STOP]`。

## 八、脚本内置的提交内容

| 组 | 源仓库提交 | 文件数 | 内容 |
|---|---|---|---|
| A | `9b6a24f` | 4 | B24 守卫（`research_candidates.py`、`build_new_factors.py`）+ 两份测试 |
| B | `accfebb` | 2 | `build_stock_basic.py` 三道守卫 + 其测试 |
| C | `f9a8147` | 2 | 研究面板差异判定、数据缺口登记 |
| D | `40aa8c7` | 2 | stock_basic manifest 核对、守卫重建执行记录 |
| E | `37c4bd2` | 1 | 只做多组合 7.9 节 |
| F | `660df03` | 1 | 备份范围口径（reports 仅镜像） |
| 先行 | — | 25 + 1 | `reports` 取消跟踪 + `.gitignore` |

备份仓库 message 全部为 `type(scope): YYYY-M-D，中文描述`、行间无空行、UTF-8 无 BOM
（`open(..., encoding='utf-8', newline='\n')` 写临时文件后 `git commit -F`，提交前自检首 3 字节）。

## 九、本次批次之外仍需注意

- 备份仓库现有两个待推提交 `83bf9a5`、`8159b7b`，本脚本推送时会一并推上。
- 脚本不检查 `data/` 源侧是否正在被其它任务写入；建议在无并发数据任务时执行。
