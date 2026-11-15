# 派生总分红改读有效层（`601258.SH`）—— 执行与验收证据

- 执行日期：2026-11-11
- 变更对象：`tools/derive_total_dividend.py`（口径）、`data/local_store/factors/stock_dividend.parquet`（4 个单元格）
- 关联登记：`docs/数据缺口登记.md` §12「已执行 ③」
- 机器可读证据：`reports/dividend_derived_20261110/total_div_effective.json`

## 1. 口径变更

| 项 | 改前 | 改后 |
|---|---|---|
| 每股派现取值 | 只读原始列 `cash_div` | 有效层 `cash_div_fix.fill_null(cash_div)`，剔除 `no_ex_rights=True` 事件 |
| 取值实现 | 本工具自写 | 复用 `src/data/dividend_effective.py` 的 `apply_effective_columns` / `drop_non_ex_rights` / `effective_value`，无第二份 `fill_null` |
| 缺列行为 | 静默按原始列 | `apply_effective_columns` 抛 `DividendSchemaError` |
| 写入范围 | 全表覆盖 | 只覆盖「有效现金 ≠ 原始现金」的行，其余行**原样写回原值**（DG-31，显式 `when/otherwise`，无 `coalesce` 兜底） |

`no_ex_rights=True` 的事件不向原股东分配，现金派生列显式写 `0.0`（不是 `null`——`null` 表示「算不出」，此处是「已知为 0」）。全表 3 条此类事件的有效 `cash_div` 均为 0，金额列仍是 0.0，空值计数零变化。

## 2. 受影响文件

| 项 | 值 |
|---|---|
| 文件 | `data/local_store/factors/stock_dividend.parquet` |
| 行数 | 60,062 → 60,062（守恒） |
| 列数 | 33 → 33 |
| SHA256（改前 = 备份） | `176b4a5a2150e6bb5c3c4ecd573bdc35baf30aa59753e162e515fd343cc8f325` |
| SHA256（改后） | `1c365491d5679a1e8c6b8afb0f9210673d9c8cf2469bd13163122ecd267899bd` |
| 改前备份 | `data/local_store/_backup/stock_dividend.20261111_064425.parquet`（`copy2`，SHA256 与改前一致） |
| 写盘方式 | `src/data/atomic_write.write_parquet_atomic`（临时文件 → fsync → `os.replace`） |

## 3. 逐行改动清单（1 行 / 4 个单元格）

`601258.SH`，`ex_date=2019-12-30`，`record_date=2019-12-27`

| 字段 | 改前 | 改后 | 反算依据 |
|---|---|---|---|
| `cash_div`（原始） | 0.0 | 0.0（未动） | 上游原始行 |
| `cash_div_fix`（有效层） | 0.04 | 0.04（未动） | 除权参考价反算：`2019-12-27 close 1.46` → `2019-12-30 pre_close 1.42`，差 `0.04` |
| `total_div_est_all` | 0.0 | **409,089,002.8** | 0.04 × `est_total_share` 10,227,225,070 |
| `total_div_est_float` | 0.0 | **409,089,002.8** | 0.04 × `est_float_share` 10,227,225,070（当日总股本=流通股本） |
| `total_div_est_rec_all` | 0.0 | **261,539,136.08** | 0.04 × `est_rec_total_share` 6,538,478,402（`record_date` 口径） |
| `total_div_est_rec_float` | 0.0 | **261,539,136.08** | 0.04 × `est_rec_float_share` 6,538,478,402 |

改动原因：有效层 `cash_div_fix=0.04` 覆盖原始 `cash_div=0.0`，按有效值重算。

复权因子侧同日已按 0.04 生效，本次改完两边同口径：

| 日期 | close | pre_close | qfq_factor | hfq_factor |
|---|---|---|---|---|
| 2019-12-27 | 1.46 | 1.46 | 0.972603 | 5.022455 |
| 2019-12-30 | 1.45 | 1.42 | 1.0 | 5.163933 |

`5.163933 / 5.022455 = 1.028169 = 1.46 / 1.42`，即因子层只吸收 0.04 派现、无股本变动，与 `share_div_fix=0.0` 一致；改前派生列停在 0.0，是「同一张表两个口径」。

排除项（**必须不变，实测未变**）：`600025.SH` `report_period=2018-12-31`，`cash_div=0.14713`、`cash_div_fix=null`
→ `total_div_est_all` 2,648,340,000；`total_div_est_float` 1,313,576,640；两列 `_rec` 同值。

## 4. DG-20 四项

1. **行数守恒**：60,062 → 60,062；33 列集合完全一致（`after_columns == before_columns`）。
2. **逐值对拍**
   - 牵动 `cash_div` 有效差的行：**1 行**（`601258.SH` 2019-12-30）；改动单元格 **4**；其中既有非空值改动 **4**。
   - 未受影响行 60,061 行：四个金额派生列逐值不一致 **0** 行；`est_total_share` / `est_float_share` / `est_effective_date` / `est_rec_total_share` / `est_rec_float_share` 不一致 **0** 行。
   - 未受影响行唯一差异：`600306.SH` 的 `est_rec_effective_date`（存量 `2022-01-04`，按当前股本表重算为 `2022-06-30`）。这是既定「待决策 ④」，与本轮口径无关，**写入时按 DG-31 保留存量值**，全表仅此 1 个元数据单元格，金额列零影响。
   - 四个金额列合计变化：`sum_est_all` +409,089,002.80、`sum_est_float` +409,089,002.80、`sum_est_rec_all` +261,539,136.08、`sum_est_rec_float` +261,539,136.08（等于 1 行改动的算式和）。
3. **受影响文件 SHA256**：`176b4a5a…` → `1c365491…`（见 §2）。
4. **抽样断言**
   - `601258.SH 2019-12-30`：`total_div_est_all == 409,089,002.8`、`total_div_est_rec_all == 261,539,136.08`。
   - `600025.SH 2018-12-31`：四个值逐值等于 `2,648,340,000 / 1,313,576,640 / 2,648,340,000 / 1,313,576,640`。
   - 空值计数（改动前 → 改动后，10 个派生列全部相等）：`total_div_est_all` 656→656；`total_div_est_float` 702→702；`total_div_est_rec_all` 677→677；`total_div_est_rec_float` 724→724；`est_*` 六列同此为 656/702/656/677/724/677。

## 5. DG-31 空值防线

- 写回表达式为 `when(有效现金≠原始现金).then(新值).otherwise(存量值)`，未命中分支拿到的是自身存量值，不存在「变 null」路径。
- `assert_no_new_nulls()` 双保险：行数相等 + 逐列空值计数相等 + 逐列「既有非空→null」计数为 0，任一不满足即 `RuntimeError`，不落盘。
- 断言自测（`tests/test_total_div_est.py::test_assert_no_new_nulls_detects_null_regression`）：构造 1 行 `0 → null` 的坏帧必须抛错；构造短 1 行的帧必须抛错。
- 效果层自测（`test_identity_effective_columns_must_fail`）：把 `apply_effective_columns` 换成恒等函数（= 旧实现只读原始层），`601258.SH 2019-12-30` 得 0.0、断言必须失败；换回真实现后得 409,089,002.8 通过。

## 6. 下游一致性（DG-23）

- 真实读路径 `python tools/backfill_share_div.py --dry-run`：内部以生产者 `derive()` 按当前表全量重算并断言「派生列无变化」，通过（`est_changed=False`）；回补 0 行，校验前后 `split 9358 行 within_5pct 9292 / no_split 46449 行 over_1pct 13` 未变。
- `total_div_est_*` 与复权因子现在同口径：`601258.SH` 派生派现 0.04 × 10,227,225,070，因子层同日比例 1.028169。

## 7. 复现

```powershell
.venv\Scripts\python.exe tools\derive_total_dividend.py --dry-run   # 只算不写，输出 DG-20 证据
.venv\Scripts\python.exe -m pytest tests\test_total_div_est.py -q
```
