# 批5-复权因子（G-118 / G-123）提交报告

主题：`fund_factor_fix_20261113`。本轮共两笔提交：① 数据侧落地 + 证据与缺口表；
② 本文件（5 块报告）。

---

## Commit 1

# ✅ Commit 完成（`3de2110`）

## 1. Commit 哈希
| 类型 | 值 |
|---|---|
| **短哈希** | `3de2110` |
| **长哈希** | `3de21109892c4ea238aa1e6145bcbab6c3b7dbcd` |

## 2. 完整 Commit Message

```text
feat(data): 2026-11-13，G-123 按当前事件表全量重算基金复权因子，G-118 判定为误报不改数据
feat(data): fund_adj_factor 2,979,381 行全量重算，行数守恒（only_old 0 / only_new 0），qfq/hfq 极差 3.844408 → 4.44e-16，不自洽标的 218 → 0
fix(data): 根因是按标的增量补齐致同一标的的行来自不同 build；改为全量重算，G-120 六只 2026-11-10/11 共 12 行按整只挂起口径剔出后行数与改前相等
fix(data): 同轮回写 quotes 内嵌复权列 241 只、daily_index 逐行 file_size、factors manifest、10,785 份侧车与 quotes 汇总 bytes，manifest_tool 三项 PASS 不一致 0
docs(gap): G-118 核实为停牌区间两事件同挂复牌行导致的误报，交付因子 0.33221925 与应有值逐位相同，按误报登记不改数据
docs(gap): G-123 转已修，四档 85/43/17/1 → 87/41/17/1，§5.3 漂移表如实记 +4,464 行未逐笔归因
docs(evidence): 记录 DG-20 四项、DG-31/34 空值与逐值对拍、DG-33 索引与侧车回写、DG-23/30 消费方真实读、G-120 口径隔离与回滚方式
```

## 3. 文件变更统计 + 清单

**统计**: ` docs/evidence/fund_factor_fix_20261113/report.md   | 145 +++++++++++++++++++++`
（整体：`2 files changed, 155 insertions(+), 8 deletions(-)`）

| 状态 | 文件 | 变化 |
|---|---|---|
| **A**（新增） | `docs/evidence/fund_factor_fix_20261113/report.md` | +145 |
| **M**（修改） | `docs/数据缺口登记_正式表_2026-11-12.md` | +10 / −8 |

## 4. 工作区状态（`git status --short`）

```text
 M src/data/quality/stale_status.py
 M tests/test_stale_status.py
 M tools/advice_stateful_backtest.py
?? docs/evidence/cn_bond_index_member_probe_20261112/
?? docs/evidence/cn_bond_yield_tenor_probe_20261112/
?? docs/evidence/fund_f10_listdate_probe_20261112/
```

上述 3 个 `M` 与 3 个 `??` 均为他路在途成果，本轮未触碰、未入库（DG-24）。
`_staging/fund_factor_fix_20261113/**` 按 DG-5 不入库；`data/local_store/**` 变更按 `G:` 镜像承担第二份。

## 5. 备份同步确认
✅ `mirror_backup.py → G:\git\python-quant\`（按源侧路径集合逐个 `copy2`）
✅ 本轮镜像 **793 个文件**：新拷贝 733 / 已一致 60 / 拷贝后 SHA256 不一致 **0** / 缺失 **0**；
`copied_bytes` = 14,596,344
✅ `logs/data_changelog.csv` 单独核对并补齐（2,236,269 B；`CD8EF945…398C5001`）
✅ 关键文件 SHA256 **全部一致**：
- `_backup/fund_adj_factor_20261113/fund_adj_factor.parquet`（改前 1,220,996 B / `89CA0CB2…372B17`，移动前后相等）
- `data/local_store/factors/fund_adj_factor.parquet`（1,421,338 B / `089C002D…D1FD9415`）
- `data/local_store/quotes/manifest.json`（10,755 B / `0E4B6C72…4E08FCA`）
- `data/local_store/meta/daily_index.parquet`（166,503 B / `8B2305B8…40FDA0E9`）
- `data/local_store/factors/manifest.json`（17,598 B / `71A731CE…67F58D8`）

---

## 数据侧自证（DG-20 四项，节选）

| 项 | 结果 |
|---|---|
| 行数守恒 | 2,979,381 → 2,979,381；`rows_only_old` 0 / `rows_only_new` 0；标的 2,620 → 2,620 |
| 逐值对拍（主键 join） | 改 `qfq` 209,024 行 / `hfq` 118,480 行 / 合计 243,731 行 / 241 只；既有非空值改动数即为上述值（原表该两列无空值），空值计数前后均为 **0** |
| 受影响文件 SHA256 | 改前 `89CA0CB2…372B17`（1,220,996 B）→ apply 后 `9F8BA1D8…FB1D7FCB` → lock_hold 后 `089C002D…D1FD9415`（1,421,338 B） |
| 抽样断言 | `150022.SZ` 2020-01-06 台阶 1.0372 与事件表 `share_ratio` 一致；`qfq/hfq` 极差 3.844408 → 4.44e-16，不自洽标的 218 → 0 |

下游：`verify_quote_factor_consistency(fund)` 2,620 文件 / 2,979,393 行 mismatch 0；
`verify_reversibility` 5 只 / 14,903 行最大相对误差 2.07e-16（容差 1e-12）；
`manifest_tool.py --check` 三项 PASS（quotes 10,786 份 / 不一致 0；factors 11 条 / 0；daily_index 2 条 / 0）；
单测 57 passed（`test_fund_factors` / `test_local_store_fund_factors_unit` / `test_fund_price_basis` / `test_factor_audit`）。
