# turnover_20 符号修正证据（2026-11-12）

## 改了什么

- `tools/research/build_new_factors.py` 的 `build_turnover` 不再对 `_turn20` 取负，
  `new_factors.turnover_20` 改为落盘**正原值**；方向由元数据 `direction=-1` 表达，
  与 `src/advice/fundamental_factors.py` 的 `ext_turnover_20` 同号。
- 重跑并原子替换 `data/local_store/meta/research_inputs/new_factors.parquet`（567,541 行 × 6 列）。
- 新增守护测试 `tests/test_turnover_20_sign_guard.py`；提交 `588bd62`。

## 四项验证结论（DG-20）

| 项 | 结论 |
|---|---|
| 行数守恒 | 567,541 → 567,541，列 6 未变，键集两向孤儿 0 |
| 逐值对拍 | 新 `turnover_20` = 旧值取负，max abs(新+旧) = **0.0**；`accruals`/`roe_stability`/`sue` 改动行 0 |
| 空值计数 | 2,449 / 15,354 / 19,120 / 72,039，四列前后逐列不变 |
| SHA256 | 旧 `f60866e2…`（7,999,280 B）→ 新 `9da7de40…`（8,000,137 B） |

补充：随机 200 抽样失败 0；新列负值行 0；与 `factors/stock_turnover.parquet` 的 20 日均值
ρ = +0.999990（旧表为 −0.999990）。

## 完整证据在哪

- 回滚快照：`_backup/meta/new_factors_signfix_20261112/new_factors.parquet`（SHA = 旧值 `f60866e2…`）
- 构建/对拍原件：`_staging/turnover_sign_fix_20261112/`（含两个 parquet，约 16 MB，按 DG-32 不入库）
- 本目录（纯文本）：`verify_dg20.json`（含 200 抽样明细）、`replace_record.json`（快照 + 原子替换）、
  `backup_sync.json`、`progress.json`、`failed.csv`
