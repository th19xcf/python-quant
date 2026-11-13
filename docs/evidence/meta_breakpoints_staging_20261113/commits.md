# 提交报告：8 个放错层断点迁出正式层（2026-11-13）

## 1. Commit 哈希

| 类型 | 值 |
|---|---|
| **短哈希** | `5b7eb8e` |
| **长哈希** | `5b7eb8ef42b7e78667328dd0444cd2d981f6658a` |

注：本笔提交之后他路另落 `98f6d4d`（`.trae/rules/project_rules.md`），故当前 `HEAD` 已不是本笔；核对请用 `git show 5b7eb8e`。本笔提交未含他路文件，他路提交也未回退本笔内容（`git diff --stat 5b7eb8e^ HEAD` = 7 files changed, 78 insertions(+), 15 deletions(-)，即本笔 6 件 + 他路 1 件）。

## 2. 完整 Commit Message

```text
feat(data): 8 个放错层断点迁出正式层并补登 meta 权威路径与 manifest
- src/data/st_history.py: state_path 改到 _staging/st_history_fetch/fetch_state.json，写前显式建父目录，抓取续跑断点不再落 data/local_store/meta/st
- src/data/st_name_history.py: em_state_path / em_failures_path / candidate_state_path / candidate_failures_path 改到 _staging/st_em_names 与 _staging/st_candidate_state，状态与失败清单同域（DG-7）
- tools/fill_internal_gaps.py: PROGRESS / FAILED 改到 _staging/internal_gap_fix，任务名稳定不带运行日，跨天中断可续跑
- tools/verify_rights_issue.py: 配股核验续跑断点改到 _staging/rights_verify，直接以脚本运行时先把仓库根补进 sys.path
- docs/数据权威路径清单.md: 补登 meta/universe/internal_gap_inventory.csv 权威路径行（279 行 / 279 只 / 30 列，消费方 fill_internal_gaps 与 build_multi_asset_etf_pool）；补登 meta/state.json 非数据资产行（DG-32 判定 read_state 零调用点、无消费者）；已废弃路径表增 8 行断点迁移记录并写明代码改点
- docs/evidence/meta_breakpoints_staging_20261113/move_proof.md: 8 件迁移前后原件 / _backup 快照 / _staging 副本三方 SHA256 相等证明，合计 176691 B
- data/local_store/meta/clean/manifest.json 补建（此前该目录无 manifest）；data/local_store/meta/anomalies/manifest.json 由 4 件补全到实际 10 件数据文件（两份 manifest 均在 data/local_store 下，按仓库约定不入 Git，随 G: 镜像留存）
- 验证: 相关单测 65 项通过 0 失败、ruff 通过、manifest_tool --check --only meta 结论 PASS（不一致 0 / 无法定位 0 / 非 parquet 未登记 0）、迁移后断点读入口实测 PASS（路径解析、断点可解析、临时 store_root 不在 data 层生成断点）
```

## 3. 文件变更统计 + 清单

**统计**: `6 files changed, 74 insertions(+), 14 deletions(-)`

| 状态 | 文件 | 说明 |
|---|---|---|
| **A** | `docs/evidence/meta_breakpoints_staging_20261113/move_proof.md` | 8 件迁移三方 SHA256 相等证明 |
| **M** | `docs/数据权威路径清单.md` | 补登 2 行 + 已废弃路径 8 行 |
| **M** | `src/data/st_history.py` | `state_path` 改 `_staging/st_history_fetch/` |
| **M** | `src/data/st_name_history.py` | 4 个路径改 `_staging/st_em_names/`、`_staging/st_candidate_state/` |
| **M** | `tools/fill_internal_gaps.py` | `PROGRESS` / `FAILED` 改 `_staging/internal_gap_fix/` |
| **M** | `tools/verify_rights_issue.py` | 续跑断点改 `_staging/rights_verify/` |

不含 Git 跟踪、仅落盘并随 `G:` 镜像的两份 manifest：`data/local_store/meta/clean/manifest.json`（新建）、`data/local_store/meta/anomalies/manifest.json`（4 件 → 10 件）。

## 4. 工作区状态（提交后）

```text
 M src/data/quality/stale_status.py
 M tests/test_stale_status.py
 M tools/advice_stateful_backtest.py
?? docs/evidence/cn_bond_index_member_probe_20261112/
?? docs/evidence/cn_bond_yield_tenor_probe_20261112/
?? docs/evidence/fund_f10_listdate_probe_20261112/
```

以上 `M` / `??` 均非本任务产出（他路在途），未清理、未带入提交。

## 5. 备份同步确认

- ✅ 源仓 6 个提交文件 → `G:\git\python-quant\` 同相对路径，SHA256 全部相等：`src/data/st_history.py` 4e76694d…、`src/data/st_name_history.py` eecd0ba9…、`tools/fill_internal_gaps.py` f4f7a37b…、`tools/verify_rights_issue.py` ee0c80fc…、`docs/数据权威路径清单.md` 796060a3…、`move_proof.md` a8a5e08f…
- ✅ `data/local_store` 集合差 0 / 内容变更 2 件已同步（`meta/clean/manifest.json` 新增、`meta/anomalies/manifest.json` 补全）、源端已删 8 件已在镜像对位删除；复核 src 21724 / dst 21724，src_only 0 / dst_only 0
- ✅ `_backup` 新增 9 件快照（`meta/anomalies|universe|st_breakpoints_20261113/`）；复核 src 6932 / dst 6932，src_only 0 / dst_only 0
- ✅ `reports` 补齐 46 件非 pyc 缺口（余 7 件为 `__pycache__/*.pyc`，按规则跳过）
- ✅ 备份仓提交 `33df653`（6 files changed, 74 insertions(+), 14 deletions(-)）并推送成功：`e78d837..33df653 main -> main`
