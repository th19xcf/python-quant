# 数据变更台账（data_changelog）

## 1. 用途与追加式约定

`logs/data_changelog.csv` 记录 `data/local_store/**` 下每个数据文件"何时、被哪个任务、从哪个哈希改成了哪个哈希"。

- **只追加，不改历史行**：已有行是事实记录，任何任务都不得回写、重排、去重或修正旧行；需要纠正时追加一条新记录并说明原因。
- 表头固定且顺序不变：`run_id,timestamp,task,action,path,bytes,sha256,prev_sha256,operator,reason`（`reason` 为 2026-11-10 新增末列，历史行该列为空）。
- `action` 仅取 `create|update|delete`；越界值写端直接抛错。`move` 不是落库取值：移动/改名用 `record_move`（CLI `--action move --dst-path ...`）拆成 `delete`（源）+ `create`（目标）两行，`create` 行的 `prev_sha256` 记源文件哈希。选择拆行而非新增 `dst_path` 列的理由是不改变 10 列结构、读侧无需为旧文件做列数分支，且每条记录仍只有一个可校验的 64 位哈希。
- `create` 的 `prev_sha256` 留空；`update` / `delete` 必须填覆盖前哈希。
- `bytes` 为数值字符串；**大小未知时写空字段，禁止写 `-1`**（哨兵值会污染 `sum()` / `mean()`）。
- `path` 为**仓库相对路径 + 正斜杠**（如 `data/local_store/...`、`reports/...`）；仓库内绝对路径会被相对化，反斜杠会被归一，仓库外路径直接抛错，不写半截。历史行保持原样不回写。
- `reason` 记录本次变更的成因，`delete` 必填（例如「一次性临时产物，pytest 残留」）。
- `timestamp` 取文件实际写入时间（脚本回填时用源文件 mtime，不要用当前时间冒充）。
- `task` 取任务目录名；确实无法归属时写 `unattributed`，不要臆造任务名。
- 行尾固定 **LF**、**UTF-8 无 BOM**；追加前校验文件末字节为 `\n`，残行存在时拒绝追加。

**唯一写入入口**：`src/data/changelog.py`。批量删除 `reports/**` 或 `data/local_store/**` 时，删之前调 `record_delete(path=..., task=..., reason=...)`（或用 CLI `python -m src.data.changelog --action delete --path ... --task ... --reason ...`），字段与续行由该模块统一生成，不要再手写 CSV 行。

**自动记账（2026-11-12 起）**：所有走 `src/data/atomic_write.py` 的写盘，只要目标落在 `data/local_store/**`，写成功后自动记一条——写前不存在记 `create`，存在记 `update`，`prev_sha256` 取写前哈希。`_staging/`、`_backup/`、`reports/`、`logs/`、`research/` 不记；`delete` / `move` 仍只能显式调用，不做推断。写台账自身用 `suspend_changelog_recording()` 显式关闭埋点。记账失败不反噬数据：失败写 `logs/data_changelog_failures.csv` 并向 stderr 告警，该文件非空时 `check_changelog.py` 判失败。

## 2. 任务收口时的追加要求

每个会写 `data/local_store/**` 的任务，在收口前必须追加一段台账记录：

1. 列出本次实际写入/删除的全部路径；
2. 标明动作（`create` / `update` / `delete`）；
3. `update` / `delete` 填覆盖前哈希，`sha256` 填写入后（或删除前最后一版）哈希；
4. 落盘后自检：行数增量与预期一致、哈希与文件实测一致、无空行、无 BOM；
5. 自动埋点已按**每一次写盘**记一条，不需要人工去重；手工补记时才按最终状态记一条，中间态写进任务日志而非台账。
6. 收口前执行 `python tools/check_changelog.py`：校验**末行完整**（以 `\n` 结尾、列数正确）、**只增不减**（基线是当前文件的字节前缀、行数不倒退）、以及与 `G:\git\python-quant\logs\data_changelog.csv` 的 **SHA256 / 字节数 / 行数**三项比对；退出码非 0 即判备份不完整或历史被改写，不得收口。

## 3. 与 `G:\git\python-quant` 备份的关系

- **台账记"变了什么"**：路径、动作、前后哈希、时间、任务。
- **备份记"当前是什么"**：完整文件内容，可用于恢复。
- 两者不能互相替代：只有台账没有备份，无法还原内容；只有备份没有台账，无法回答某次覆盖前是什么、由谁改的。
- 恢复时先按台账定位目标版本，再从备份取文件；恢复动作本身也要追加一条台账记录。

## 4. 重要提醒

`data/local_store` 已被 `.gitignore` 第 138 行整目录排除，git 不承担数据版本管理职责。**台账不替代备份，两者都要做**；台账本身 2026-11-12 起改为**不进 Git**（只增不减、每次提交都在历史里多存一份约 1.95 MB 快照），其唯一第二份副本是 `G:\git\python-quant\logs\data_changelog.csv`，收口时必须按 SHA256 / 字节数 / 行数三项核对。
