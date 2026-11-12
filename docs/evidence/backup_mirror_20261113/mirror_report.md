# 备份镜像收口报告（2026-11-13）

口径：`e:\code\python\quant` → `G:\git\python-quant\`，单向 `copy2`，不删镜像侧、不 `rmtree`、不碰备份仓 `.git`；
比对与拷贝跳过 `__pycache__/`、`*.pyc`、`*.pyo`；一律按相对路径集合差 + 字节大小差 + 逐文件 SHA256 三项核对，
不按 mtime 或「今天」筛选。

## 一、十类备份范围核对结果

| # | 范围 | 源侧件数 / 字节 | 镜像侧件数 / 字节 | 集合差 src/dst | 大小差 | SHA256 差 |
|---|---|---|---|---|---|---|
| 1 | `2026-11-12/**` | 1,484 / 133,513,225 | 1,484 / 133,513,225 | 0 / 0 | 0 | 0 |
| 2 | `2026-11-13/**` | 2 / 31,992 | 2 / 31,992 | 0 / 0 | 0 | 0 |
| 3 | `data/local_store/**` | 21,743 / 2,043,738,867 | 21,743 / 2,043,738,867 | 0 / 0 | 0 | 0 |
| 4 | `_backup/**` | 6,650 / 20,252,382,813 | 6,650 / 20,252,382,813 | 0 / 0 | 0 | 0 |
| 5 | `reports/**` | 3,065 / 5,902,989,861 | 3,065 / 5,902,989,861 | 0 / 0 | 0 | 0 |
| 6 | Git 跟踪文件 | 922（HEAD 跟踪） | 937（HEAD 跟踪） | need_sync=0 | 0 | 0 |
| 7 | `research/events/data/**` | 122 / 8,978,792 | 122 / 8,978,792 | 0 / 0 | 0 | 0 |
| 8 | `backups/**` | 572 / 1,046,561,144 | 572 / 1,046,561,144 | 0 / 0 | 0 | 0 |
| 9 | `logs/**`（11 个快照目录） | 17,315 / 3,099,285,748 | 17,315 / 3,099,285,748 | 0 / 0 | 0 | 0 |
| 9b | `logs/data_changelog.csv` | 2,083,331 B / 10,727 行 | 2,083,331 B / 10,727 行 | — | 0 | 0 |
| 10 | `E:\www\tmp` 白名单 | 6,925 / 2,066,297,061 | 6,955 / 2,800,004,505 | 0 / 30（已知差异） | 0 | 0 |

## 二、基线 ① `research/events/data/**`

**新基线：122 件 / 8,978,792 B**（旧基线 142 件已作废，不得再按 142 比对）。

减少的 20 件来源：源仓提交 `5de8618`（2026-11-12）
`refactor(research): 研究事件数据按 DG-32 三分收口并改接补数落点`，按 DG-32 三分口径迁出：

- 11 件证据件（含 3 个手写知识表）→ `docs/evidence/research_events_20261103/`，合计 106,117 B
- 9 件中转件 → `_staging/`（`derived` 6 件、`events_announcements` 1 件断点件、`events_daily` 2 件），合计 13,347,420 B

逐件 SHA256 迁移前后相等，已核过、无一丢失。该笔提交正文自带实测口径句
「docs: 数据权威路径清单补第二步三分收口与 collect_assets 实测 122 件 / 8,978,792 B」，
与本次镜像实测的 122 件 / 8,978,792 B 完全一致。

## 三、基线 ② `E:\www\tmp` 白名单

**新基线：87 条目 / 6,925 件 / 2,066,297,061 B，unresolved 2 条，已知差异 30 件**
（旧基线 101 条目 / 6,932 件 / 2,066,789,995 B 已作废，不得再按旧数比对）。

生成口径（2026-11-13 收紧，脚本同为证据）：

- 生成器：`2026-11-12/backup-coverage-audit/work/scan_www_tmp.py`（引用扫描 + 截断片段剔除）、
  `build_whitelist.py`（只收录真实存在条目、按实际文件路径去重）
- 扫描命中 1,657 条 → 剔除片段 1,440 条
  （`bare_root` 1,389 / `brace_expansion` 5 / `range_tilde` 1 / `trailing_sep` 19 / `truncated_no_ext` 26）
  → 有效引用 404 条、不同路径 118 个 → 白名单条目 87 个（目录 15 / 文件 72 / 通配 0）
- 展开件数 6,925、字节 2,066,297,061 B（`build_whitelist.py` 打印值，已按实际路径去重）
- `unresolved` 由 59 条降至 **2 条**：`_probe_e_www.txt`、`dividend_reconcile_20261110\inspect.py`
- 已知差异 30 件 / 733,707,444 B，清单见
  `docs/evidence/www_tmp_whitelist_20261113/known_dst_only_baseline.csv`
  （20 件 `impact_amihud\*` = brace_expansion 剔除；3 件 `baostock_val_pre2018\progress_w*` = truncated_no_ext 剔除；
  7 件根级索引临时文件与 `pytest_out.txt` 仅被仓库内 `tmp/` 脚本副本引用，权威口径 `EXCLUDE_DIRS` 含 `tmp`）

## 四、已知差异与跳过清单

- **Git 跳过（工作区≠HEAD）**：`src/data/quality/stale_status.py`、`tests/test_stale_status.py`、
  `tools/advice_stateful_backtest.py`——源仓在途成果，按口径不动。
- **只在备份仓剔除**：`.github/workflows/ci.yml`（备份仓 GitHub Actions，源仓无此语义）。
- **保留的 15 条「只在备份仓」**：11 条 `docs/evidence/*` 未跟踪、2 条 `2026-11-13/*` 未跟踪、
  2 条 `_backup/docs/*` 被忽略；源仓工作区均存在，不属「源仓已删未同步」。
- **`www_tmp` 已知差异 30 件**：原样保留在镜像侧，不删、不计入差异失败。

## 五、复算方式

```powershell
# 集合差 + 大小差 + 逐文件 SHA256（逐棵）
.venv\Scripts\python.exe -c "..."   # 见各轮任务脚本；核心逻辑：os.walk 相对路径集合 + os.path.getsize + hashlib.sha256
# Git 跟踪文件
.venv\Scripts\python.exe mirror_git_tracked.py verify   # ls-tree HEAD 双侧比对 + diff-index 判脏
# www_tmp 白名单
.venv\Scripts\python.exe 2026-11-12\backup-coverage-audit\work\scan_www_tmp.py
.venv\Scripts\python.exe 2026-11-12\backup-coverage-audit\work\build_whitelist.py
```
