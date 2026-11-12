# G-098 日期快照族合并为单表 history：验收证据

> 日期：2026-11-13　执行范围：`meta/st_status/` 与 `meta/industry/` 两族共 14 个带日期正式路径
> 目标：每族只保留一条无日期路径（DG-1 / DG-4），数据体逐值不变，旧路径按 DG-13 移入仓库根 `_backup/`
> 工具：`tools/migrate_snapshot_history.py`（可重跑，命令见 §6）
> 机器可读回执：[assertions.json](file:///e:/code/python/quant/docs/evidence/g098_snapshot_history_20261113/assertions.json)；验收脚本：[verify_refactor.py](file:///e:/code/python/quant/docs/evidence/g098_snapshot_history_20261113/verify_refactor.py)
> 断点：`_staging/g098_snapshot_history_20261113/progress.json`（applied=true，4 步全记录）、`failed.csv`（空，仅表头）

## 结论

合并前 7 层 × 2 族 = 14 个日期文件，合并后 2 个单表，行数 40,401 = 7 层求和（两族相同），
14 个源文件逐层 `equals` 全等、既有非空值改动数 **0**、两侧空值计数逐层相等，
改前正式目录整体 `os.replace` 移入 `_backup/meta/<族>_20261113/`（移动前后 SHA256 / 字节 / inode 三项全等）。
13 处 freshness 配置项改前/改后返回值逐值相等。

## 一、DG-20 四项证据

### 1. 行数守恒（逐族可解释）

| `as_of` 层 | 源文件行数 | 合并表该层行数 | 差额 |
|---|---|---|---|
| 2026-11-03 | 5,654 | 5,654 | 0 |
| 2026-11-04 | 5,655 | 5,655 | 0 |
| 2026-11-05 | 5,812 | 5,812 | 0 |
| 2026-11-06 | 5,812 | 5,812 | 0 |
| 2026-11-09 | 5,818 | 5,818 | 0 |
| 2026-11-10 | 5,818 | 5,818 | 0 |
| 2026-11-12 | 5,832 | 5,832 | 0 |
| **合计** | **40,401** | **40,401** | **0** |

两族（`st_status` / `industry`）逐层数字完全相同。合并规则：按 `as_of` 纵向拼接，
不带「最新截面副本」——最新层的 5,832 行就是历史表的最后一段，不与任何行重复
（`(code, as_of)` 在全表唯一，工具内已断言，重复即中止）。

### 2. 逐值对拍（改动前后空值 + 既有非空值改动数）

按 `(code, as_of)` 与对应源文件逐一 join 后逐列比对：

| 族 | 逐层 `equals` | 空值计数（源 → 合并） | 既有非空值改动数 |
|---|---|---|---|
| `st_status` | 7/7 全等 | 232 / 232 / 233 / 233 / 233 / 233 / 233，逐层两侧相等 | **0** |
| `industry` | 7/7 全等 | 0 / 0 / 0 / 0 / 0 / 0 / 0，逐层两侧相等 | **0** |

### 3. 受影响文件 SHA256

**新建正式表**

| 文件 | 行数 | 字节 | SHA256 |
|---|---|---|---|
| `meta/st_status/st_status_history.parquet` | 40,401 | 109,210 | `388296f13facec3b6c75fa2de6d535d2c4d17cdb74ef00a2fda9cc2735493570` |
| `meta/industry/industry_history.parquet` | 40,401 | 71,901 | `c263de72829cfe63272833287079996aeeca5409f1e618a798190378cb7acad1` |
| `meta/st_status/manifest.json`（重写） | — | — | `63f110a8824a8d321dab2e398a1923b9eb081b1265a154b1dd526eae2b0e4b61` |
| `meta/industry/manifest.json`（重写） | — | — | `32362683c0d7c580775ac2e7fff007b149b0ea3d81869c67c122e2f740145d71` |

**14 个被退役源文件（改前即此值，移入 `_backup` 后仍为此值）**

| 源文件 | 行数 | 字节 | SHA256 |
|---|---|---|---|
| `st_status_20261103.parquet` | 5,654 | 49,683 | `6d10a8776520bbb87483b7ec652d49a353396ff94174f6921f5fac42629f7af4` |
| `st_status_20261104.parquet` | 5,655 | 49,705 | `9c87ddfde8d18eb0…`（全值见 assertions.json） |
| `st_status_20261105.parquet` | 5,812 | 51,034 | `7741effd248d5269…` |
| `st_status_20261106.parquet` | 5,812 | 51,036 | `d8180a1266d3c3a5…` |
| `st_status_20261109.parquet` | 5,818 | 50,902 | `0c690676bf5e43f9…` |
| `st_status_20261110.parquet` | 5,818 | 50,845 | `8e6d5466de69f9e8…` |
| `st_status_20261112.parquet` | 5,832 | 51,060 | `32dbc6ec4306aaf5…` |
| `industry_20261103.parquet` | 5,654 | 23,028 | `855db29b05b2ba348efeaa4a500a7d8edf3f41a9d64dd0db95ffd1765d52930c` |
| `industry_20261104.parquet` | 5,655 | 23,041 | `8b56207ab4ac5b64…` |
| `industry_20261105.parquet` | 5,812 | 23,649 | `9f95e1786a560b51…` |
| `industry_20261106.parquet` | 5,812 | 23,649 | `5b7649b6cbbd205a…` |
| `industry_20261109.parquet` | 5,818 | 23,492 | `78a11c5c3431f63d…` |
| `industry_20261110.parquet` | 5,818 | 23,492 | `ba4fa3c0a39ad0ae…` |
| `industry_20261112.parquet` | 5,832 | 23,544 | `97d38f67ce1568e1…`（全值见 `docs/数据权威路径清单.md` 已废弃路径表） |

### 4. 抽样断言

| 断言 | 结果 |
|---|---|
| 每份源文件 `as_of` 列只有一个值，且与文件名日期一致 | 14/14 通过 |
| 份内 `code` 唯一 | 14/14 通过 |
| `as_of` 无空值 | 14/14 通过 |
| 合并表 `(code, as_of)` 唯一 | 2 族均 0 重复 |
| 抽样行逐值相等（2026-11-03 与 2026-11-12 两层的 `000001.SZ`） | 4/4 `row_equals_source=true` |
| 迁移后按 `as_of` 逐层读回，与原文件 `equals` | 14/14 全等（见 §2 端到端最后 14 行） |

## 二、DG-23 真实读路径端到端（`verify_refactor.py` 实测输出）

```
risk_flags.load_snapshot(2026-11-10) -> 5818 行, day=2026-11-10
get_st_status_detail(000004.SZ, 2026-11-10) -> status=unknown, as_of=2026-11-10, source=stock_basic
risk_flags.snapshot_days -> ['2026-11-03', '2026-11-04', '2026-11-05', '2026-11-06', '2026-11-09', '2026-11-10', '2026-11-12']
st_history._snapshot_series -> 7 层, 首层 2026-11-03, 末层 2026-11-12
industry.load_snapshot(2026-11-10) -> 5818 行, day=2026-11-10
get_industry(600000.SH, 2026-11-10) -> TDX 金融/银行, as_of=2026-11-10, status=known
总体: PASS
```

- `get_st_status_detail` 走 `st_history` → `risk_flags.load_snapshot`，取到的是单表 11-10 层（5,818 行），不是任何日期文件；
- `get_industry` 走 `industry.load_snapshot`，取到 11-10 层的「TDX 金融/银行」；
- 以上两条读路径都在旧日期文件已移出正式目录之后执行，能读通即证明消费方读的是 history 表。

## 三、13 处 freshness 配置项改前 / 改后

改前值用 `_backup/meta/*_20261113/` 里被退役的日期文件复算（`latest_glob_distinct` 取 mtime 最新那份），
改后值用单表最新 `as_of` 层（`latest_as_of_distinct`）复算：

| 配置项 | 改前 | 改后 | 相等 |
|---|---|---|---|
| `quotes_stock.coverage.expected` | 5,832 | 5,832 | ✅ |
| `valuation_daily.coverage.expected` | 5,832 | 5,832 | ✅ |
| `financials_income.coverage.expected` | 5,832 | 5,832 | ✅ |
| `financials_balance.coverage.expected` | 5,832 | 5,832 | ✅ |
| `financials_cashflow.coverage.expected` | 5,832 | 5,832 | ✅ |
| `stock_turnover.coverage.expected` | 5,832 | 5,832 | ✅ |
| `stock_adj_factor.coverage.expected` | 5,832 | 5,832 | ✅ |
| `stock_dividend.coverage.expected` | 5,832 | 5,832 | ✅ |
| `stock_shares.coverage.expected` | 5,832 | 5,832 | ✅ |
| `st_status.coverage.count` | 5,832 | 5,832 | ✅ |
| `st_status.source` | 2026-11-12 | 2026-11-12 | ✅ |
| `industry.coverage.count` | 5,832 | 5,832 | ✅ |

`industry.source` 同步由 `latest_glob_max` 改为 `parquet_max(column=as_of)`，改前/改后同为 `2026-11-12`。

> 口径差别只在「退市标的增多后」显形：`parquet_distinct`（全历史并集）会把已退市代码算进分母使覆盖率虚高，
> `latest_as_of_distinct` 只数最新 `as_of` 层内的 code。单测
> `tests/test_data_freshness.py::test_latest_as_of_distinct_counts_only_the_latest_layer` 用
> 「旧层含 000003.SZ」的构造数据钉住：新 kind 得 2，退回 `parquet_distinct` 得 3（用例失败）。

## 四、`_backup` 移动证明（DG-11 / DG-13）

- 落点：`_backup/meta/st_status_20261113/`（8 件 = 7 日期文件 + 旧 `manifest.json`）、
  `_backup/meta/industry_20261113/`（8 件同构）；
- 方式：`os.replace` 整体移动，**未用** `os.remove`、未复制后删除；
- 逐件比对**移动前**（正式目录 stat）与**移动后**（`_backup` 内 stat）的 `sha256` / `bytes` / `inode`，
  14 个 parquet + 2 个 manifest 全部三项相等（明细见 `assertions.json` 各族的 `moved` 数组）。

## 五、边界：不在本次迁移内

`clean_set.py:1085` 钉在 `as_of=2026-11-03`、`single_window_diag.py` 默认 `--industry-as-of 2026-11-04`
是「陈旧截面」缺陷，本次**只做路径去日期化、行为逐行不变**，缺陷单独登记为 **G-148**（不改口径）。

## 六、复现命令

```powershell
# 1. dry-run（只写候选到 _staging，不动正式目录）
.venv\Scripts\python.exe tools\migrate_snapshot_history.py

# 2. 落盘（移旧入 _backup + 原子写正式 + 重写 manifest）
.venv\Scripts\python.exe tools\migrate_snapshot_history.py --apply

# 3. 13 处配置项改前/改后 + DG-23 端到端 + 14 层逐值对拍
.venv\Scripts\python.exe docs\evidence\g098_snapshot_history_20261113\verify_refactor.py
```
