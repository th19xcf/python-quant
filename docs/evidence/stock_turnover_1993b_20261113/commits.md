# 提交记录（批 3b：G-124 复验 + G-125 修复）

- 任务：数据缺口登记 G-124 / G-125，目标 `data/local_store/factors/stock_turnover.parquet`
- 执行日：2026-11-13
- 本批共 3 笔提交：证据目录、缺口表两行、本文件（补记）
- 数据体（`data/local_store/**`、`_backup/**`）按规定不入 Git，只做备份仓镜像

---

## 一、第 1 笔：证据目录入库

# ✅ Commit 完成（`490825a`）

## 1. Commit 哈希

| 类型 | 值 |
|---|---|
| **短哈希** | `490825a` |
| **长哈希** | `490825a720cdd8cc5cf9d9e3cc930b29cab8102e` |

## 2. 完整 Commit Message

```text
docs(evidence): 2026-11-13，新增 stock_turnover 1993-05 位移块修复证据目录
docs(evidence): 记录 G-124 与缺口表原文的逐条核对结论，57 行缺陷已于 01:18 由 b_class_fix 闭合，本轮 calc/turn 逐行恒等 1.0、零改动
docs(evidence): 记录 G-125 实测，窗口 277 行 71 只中 derived 269 行里同日口径不符恰为 64 行 16 只，其中 32 行 turn/calc(T)≡0.2、32 行 turn==calc(T+1)
docs(evidence): README.md 含 ÷100 的单位与来源证据、冻结 TDX 侧证覆盖 2/16、64 行逐行明细、DG-20 四项完成证据、DG-31 断言自测与 DG-23/DG-30 端到端读数
docs(evidence): 附 changed_64_rows.csv、g125_rows64.csv、probe3_summary.json、apply_report.json、dg23_e2e.json、postfix_check.json
docs(evidence): 数据体与 _staging 脚本按规定不入 Git
```

## 3. 文件变更统计 + 清单

**统计**: `7 files changed, 718 insertions(+)`

| 状态 | 文件 | 变化 |
|---|---|---|
| **A** | `docs/evidence/stock_turnover_1993b_20261113/README.md` | +188 |
| **A** | `docs/evidence/stock_turnover_1993b_20261113/apply_report.json` | +101 |
| **A** | `docs/evidence/stock_turnover_1993b_20261113/changed_64_rows.csv` | +65 |
| **A** | `docs/evidence/stock_turnover_1993b_20261113/dg23_e2e.json` | +172 |
| **A** | `docs/evidence/stock_turnover_1993b_20261113/g125_rows64.csv` | +65 |
| **A** | `docs/evidence/stock_turnover_1993b_20261113/postfix_check.json` | +65 |
| **A** | `docs/evidence/stock_turnover_1993b_20261113/probe3_summary.json` | +62 |

## 4. 工作区状态（`git status --short`，提交后）

```text
 M src/data/quality/stale_status.py
 M tests/test_stale_status.py
 M tools/advice_stateful_backtest.py
?? docs/evidence/cn_bond_index_member_probe_20261112/
?? docs/evidence/cn_bond_yield_tenor_probe_20261112/
?? docs/evidence/fund_f10_listdate_probe_20261112/
```

> 以上 6 项均为他路在途改动，按任务边界未触碰、未提交。

## 5. 备份同步确认

✅ `copy2 → G:\git\python-quant\docs\evidence\stock_turnover_1993b_20261113\`（7 个文件）
✅ `copy2 → G:\git\python-quant\docs\数据缺口登记_正式表_2026-11-12.md`
✅ 逐文件 SHA256 **7/7 一致**，代表值：
- `README.md` = `9821A1CB99A61E876BBCE43749A3D14B734E8DFD6267A62021709B2E7C5ACB3C`
- `changed_64_rows.csv` / `g125_rows64.csv` / `apply_report.json` / `dg23_e2e.json` / `postfix_check.json` / `probe3_summary.json` 两侧同为源侧内容

---

## 二、第 2 笔：缺口表 G-124 / G-125 改判

# ✅ Commit 完成（`e98a590`）

## 1. Commit 哈希

| 类型 | 值 |
|---|---|
| **短哈希** | `e98a590` |
| **长哈希** | `e98a590012c12dcb6b1e61674166ab26ab1f3bbc` |

## 2. 完整 Commit Message

```text
docs(gap): 2026-11-13，缺口表 G-124 与 G-125 按本轮实测改判
docs(gap): G-124 判定改为已修复，57 行由 b_class_fix 于 2026-11-13 01:18 执行，本轮复验 calc/turn 恒等 1.0
docs(gap): G-124 依据补 corrections[3] 与 _backup/factors_turnover_bclass_20261113 留证，并写明与 G-125 的 64 行按键求交为 0
docs(gap): G-125 判定改为已修复，按同日 volume 重建 64 行、turn_new = calc(T)，16 只 1993-05-03~1993-05-06
docs(gap): G-125 依据替换为机理更正，32 行 0.2×calc(T) 与 32 行 calc(T+1) 是旧 7 行 TDX 日期轴的产物，原「错位一日」「÷100」措辞作废
docs(gap): 附行数守恒 18122973、null 计数守恒、目标外既有非空值改动 0 与证据路径 docs/evidence/stock_turnover_1993b_20261113/
```

## 3. 文件变更统计 + 清单

**统计**: `1 file changed, 2 insertions(+), 2 deletions(-)`

| 状态 | 文件 | 变化 |
|---|---|---|
| **M** | `docs/数据缺口登记_正式表_2026-11-12.md` | +2 / -2（L271 G-124、L272 G-125 各一行） |

## 4. 工作区状态（`git status --short`，提交后）

```text
 M src/data/quality/stale_status.py
 M tests/test_stale_status.py
 M tools/advice_stateful_backtest.py
?? docs/evidence/cn_bond_index_member_probe_20261112/
?? docs/evidence/cn_bond_yield_tenor_probe_20261112/
?? docs/evidence/fund_f10_listdate_probe_20261112/
```

## 5. 备份同步确认

✅ `copy2 → G:\git\python-quant\docs\数据缺口登记_正式表_2026-11-12.md`
✅ SHA256 源/备 **一致**：`79225EFBB080C1B2CA0605962AD852EEE6B3A75F284CB7D50CC8E1CE78CB808B`
✅ 数据体镜像（不入 Git，仅备份仓）：
- `data/local_store/factors/stock_turnover.parquet`（改后 `3188C23F…`，147,991,052 B）
- `data/local_store/factors/manifest.json`（改后 `6D5B0786…`，17,598 B）
- `_backup/factors_turnover_g125_20261113/stock_turnover.parquet`（回滚快照 `7D7E9EBA…`，147,991,117 B）
- `_backup/factors_turnover_g125_20261113/manifest.json`（回滚快照 `6AE7B339…`，16,052 B）

本轮备份仓增量同步合计 **12 个文件，SHA256 复核 12/12 一致**（集合差 0）。

---

## 三、索引回填自证

- 提交后另起命令执行 `git read-tree HEAD`，共享 `.git/index` 已重写。
- `git ls-files -s` blob 与 `git rev-parse HEAD:<path>` 逐条相等：
  - `docs/数据缺口登记_正式表_2026-11-12.md`：`251eb604a713af2a3079f21031c959b8570cee8d` = `251eb604…`
  - `docs/evidence/stock_turnover_1993b_20261113/README.md`：`58fe5711a06e75e105eb768fd9ec0c084be11e65` = `58fe5711…`
- `git status --short` 中不再出现本批任何 `MM`，仅剩他路 3 个 `M` 与 3 个未跟踪目录。

---

## 四、第 3 笔：本文件自身入库

补记文件 `commits.md` 单独一笔提交，message 与变更清单见下：

```text
docs(evidence): 2026-11-13，补记批 3b 两笔提交的 5 块报告与索引回填自证
```

| 状态 | 文件 | 变化 |
|---|---|---|
| **A** | `docs/evidence/stock_turnover_1993b_20261113/commits.md` | +本文件 |
