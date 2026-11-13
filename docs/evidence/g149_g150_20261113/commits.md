# G-149 / G-150 收口提交记录（2026-11-13）

## ✅ Commit 完成（`56dd383`）

### 1. Commit 哈希

| 类型 | 值 |
|---|---|
| **短哈希** | `56dd383` |
| **长哈希** | `56dd3831603a8ac3146b407c3b7221f139b69bd5` |

### 2. 完整 Commit Message

```text
docs(governance): 2026-11-13，G-149 补 valuation_daily 1 行、G-150 裁定为合法续尾
- G-149：补 000001.SZ 1993-05-06（close=55.30 取本地行情，total_mv=89,751,643×55.30=4,963,265,857.90，circ_mv=39,490,723×55.30=2,183,836,981.90，pe/pe_static/pb/ps/dv_ratio 按 DG-16 留空，source 用字段级语法），行数 17,969,104→17,969,105，既有行逐值改动 0
- G-149 验收：改前整体移入 _backup/meta/valuation_daily_g149_20261113/（移动前后 SHA256 相等），write_parquet_atomic 落盘，manifest 同轮回写 rows/bytes/sha256/date_range，manifest_tool --check --only meta PASS，端到端 store.read_table 与 attach_cost_inputs 实读通过
- G-150：判保留、合法续尾（9 只基金接缝收益 0.000% 且与 .day 重叠 26 行 0 差，399699.SZ 重叠 2,289 行 0 差，000004.SZ 已由 G-139 查清），该判定只解释现象，未改 is_mixed_source / freshness / strict 口径
- 文档计数：四档 87/41/17/1→89/39/17/1，§3 仍 146 行、编号仍 G-001~G-150，§5.3 valuation 行改 17,969,105（+11,126）
- 证据与断点：_staging/g149_g150_20261113/（progress.csv、failed.csv、fix_g149.py、verify_g150.py、report_apply.json、readback_g149.json）
```

### 3. 文件变更统计 + 清单

**统计**: `1 file changed, 9 insertions(+), 7 deletions(-)`

| 状态 | 文件 | 变化 |
|---|---|---|
| **M**（修改） | `docs/数据缺口登记_正式表_2026-11-12.md` | +9 / -7 |

### 4. 工作区状态（`git status --short`）

```text
 M src/data/quality/stale_status.py
 M tests/test_stale_status.py
 M tools/advice_stateful_backtest.py
?? docs/evidence/cn_bond_index_member_probe_20261112/
?? docs/evidence/cn_bond_yield_tenor_probe_20261112/
?? docs/evidence/fund_f10_listdate_probe_20261112/
```

均属他路在途，本轮未触碰、未带入提交。

### 5. 备份同步确认

✅ 19 个文件 `copy2` → `G:\git\python-quant\` 同相对路径，逐文件 SHA256 两侧一致（0 个 BAD）
- `data/local_store/meta/financials/valuation_daily.parquet` 642,203,839 B / `376d8c9e4c22…`
- `data/local_store/meta/financials/manifest.json` 12,904 B / `a7d43bcb1d26…`
- `_backup/meta/valuation_daily_g149_20261113/valuation_daily.parquet` 642,217,393 B / `f7da3e9de079…`
- `_backup/docs/数据缺口登记_正式表_2026-11-12_20261113_r10.md` 268,154 B / `546daf2cc1db…`
- `logs/data_changelog.csv` 2,239,703 B / `d7720827bad2…`
✅ 备份仓提交 `3234f82`（`1 file changed, 9 insertions(+), 7 deletions(-)`），已推送 `f90353d..3234f82 main -> main`

## 本轮数据验收（G-149）

| 项 | 值 |
|---|---|
| 行数 | 17,969,104 → **17,969,105** |
| 既有行改动 | **0**（10 个非键列逐列 0，null 安全比较） |
| 空值增量 | `pe`/`pe_static`/`pb`/`ps`/`dv_ratio` 各 +1，其余列 0 |
| 新增行 | `000001.SZ` 1993-05-06，`close=55.30`、`total_mv=4,963,265,857.90`、`circ_mv=2,183,836,981.90` |
| 改前 SHA256 | `f7da3e9de079b82624e8304fbb3fc002f6c2bd09f6d607c5e0d6d532dcbdd482` |
| 改后 SHA256 | `376d8c9e4c220eda1ff667e38246262510fbf071f45f8b8aafd459a896d50838` |
| 端到端 | `store.read_table('valuation')` 3 行齐；`attach_cost_inputs` 的 `total_mv_yi`（05-07）= 49.632659 |
| manifest 检查 | `tools/manifest_tool.py --check --only meta` PASS（4 条历史警告，无失败项） |

## G-150 只读核验（`_staging/g149_g150_20261113/verify_g150.py`）

| 标的组 | 续尾行数 | 接缝 | 与 `.day` 重叠 | 逐值最大绝对差 |
|---|---|---|---|---|
| 9 只场内基金 | 各 6 行（2025-11-24~2025-12-04） | 接缝收益 0.000% | 26 行 | 0.0 |
| `399699.SZ` | 1,317 行（2012-01-04~2017-06-08） | 三日 −0.145% / −1.856% / +1.489% | 2,289 行 | 0.0 |
| `000004.SZ` | 112 行（1991-01-14~1991-08-01） | 三日 +2.469% / −1.205% / +1.220% | 8,124 行 | 价格 0.0（volume 差异属 G-084 另一口径） |

## 与本改动无关的既有失败项

- `tests/test_lineage_writers.py::test_repo_sample_eleven_deviations` FAILED：`漏边：st_coverage`，用例期望 `('src/data/st_history.py', 405)`，仓库实际为 406 行；`git diff HEAD -- src/data/st_history.py` 为空，属 HEAD 既有的行号漂移，非本轮引入。
- `tests/test_financials_store.py` 全通过。
