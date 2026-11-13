# G-146 收口证据：`001914.SZ` 沿用旧码 `000043.SZ` 的 2006-10-09~2007-11-29 简称段

- 执行日期：2026-11-13
- 写入口：`tools/build_name_history.py --apply --offline`（沿用段由 `CARRIED_CODE_MAP` 驱动）
- 后处理：`tools/fix_name_history_end_dates.py --apply` → `tools/infer_post_g_segments.py --apply`
- 落盘文件：`data/local_store/meta/name_history.parquet`
- 机器回执：`_staging/g146_name_carry_20261113/verify_receipt.json`（同目录 `progress.json` / `failed.csv`）

## 1. 取证（同一主体代码变更）

| 证据 | 内容 |
|---|---|
| `meta/code_migration.parquet` | `000043.SZ → 001914.SZ`，`confidence=high`，`source=org_name_registry` |
| `meta/st/raw/em_org_basic.parquet` | 两码 `org_name` 同为「招商局积余产业运营服务股份有限公司」；`former_names` 链同含 `深南光A→G深南光→深南光A→中航地产→中航善达`；`listing_date` 同为 1994-09-28 |
| 行情重叠 | 两码重叠 **9,208** 个交易日，收盘价逐日相对偏差 **< 1e-6** |
| 交接口径 | 旧码「中航善达」止于 2019-12-13，新码「招商积余」起于 2019-12-16，日期连续 |

目标码自身的 `former_names` 链就已登记 `深南光A`（且出现两次，对应 G 段前后），是独立于旧码的第二重直证。
映射登记位置：`docs/数据权威路径清单.md`「名称历史的精度分层」；不新建映射表，理由同处写明（`meta/code_migration.parquet` 已是唯一正式来源，DG-1 / DG-2）。

## 2. 写入口与空洞约束

- 只处理 `CARRIED_CODE_MAP = {'001914.SZ': '000043.SZ'}` 这一对，不做通用「旧码事件自动沿用」。
- 只接受**整段落在新码两条既有段之间空洞内**的旧码段；重复段、跨边界段、开区间之后一律跳过并计数（`skipped_duplicate` / `skipped_outside_gap` / `skipped_open_gap`）。
- 本次统计：`segments=1`、`skipped_duplicate=3`、`skipped_outside_gap=1`（旧码末段止 2019-12-13，与新码 2019-12-15 错位）。
- 新增行：`001914.SZ / 深南光A / 2006-10-09 ~ 2007-11-29 / source=carried_from:000043.SZ / ann_date=null`。

## 3. 对拍（DG-34：不看 SHA，按业务主键逐值）

业务主键 `(ts_code, start_date, end_date)`，`end_date` 为 null 时用哨兵日 `9999-12-31` 参与 join（polars 中 null 不等于 null）。

| 项 | 改前（`_backup`） | 改后（正式） |
|---|---|---|
| 行数 | 14,034 | **14,035**（+1，删 0） |
| 只数 | 5,824 | 5,824 |
| `end_date` 空值 | 5,923 | **5,923**（不变） |
| `ann_date` 空值 | 95 | **96**（新增行本身无公告日） |
| `name` / `change_reason` / `source` 既有非空值改动数 | — | **0 / 0 / 0** |
| 既有非空值被写成 null | — | **0** |
| `fetched_at` | 运行时字段，全量重写 | 按 DG-34 不计入内容差异 |

- 行数守恒：14,035 − 14,034 = 1，等于新增段数。
- 断言自测：对改后表注入 1 个 `name=null`、注入 1 个 `name='篡改'`，检查器均如期报失败（DG-31）。
- 区间连续性：`001914.SZ` 6 段自 1994-09-28 首尾相接至开区间，`issues=[]`；`000043.SZ` 仍 5 段，`issues=[]`。

## 4. 文件与索引

| 项 | 值 |
|---|---|
| 改前快照 | `_backup/meta/name_history_g146_20261113/name_history.parquet`，194,354 B / SHA256 `256EFEF8F5AE2014968B2B2807100951BD5F26B2CF05C6E13596261AF2E2F847`；从正式路径整体 `shutil.move` 前后 SHA256 相等 |
| 改后正式 | 194,378 B / SHA256 `B434B9FE7365A1B3A4D0E7711A959CEBDDE1861910837F72093461E1A3DAD9B9` |
| `manifest.json` | `rows=14035`、`bytes=194378`、`sha256` 与磁盘逐项相等；`code_carry_map={"001914.SZ":"000043.SZ"}` |

说明：写入口在生成覆盖度时会读回正式表做对账，故快照移动完成后先以 `copy2` 从快照恢复一份逐字节相同的输入（SHA256 同上），再执行 `--apply`；快照本身保留不动。

## 5. 端到端真实读（`src/data/name_consistency.py::name_at`）

| 查询 | 结果 |
|---|---|
| `001914.SZ @ 2006-11-01` | `深南光A` / `carried_from:000043.SZ` / `公告直证（旧码事件按登记代码变更映射沿用）` |
| `000043.SZ @ 2006-11-01` | `深南光A` / `sz_change_name`（原值不变） |
| `001914.SZ @ 2006-10-08` | `G深南光`（边界前一天不变） |
| `001914.SZ @ 2007-11-30` | `中航地产`（边界后一天不变） |

## 6. 复算命令

```powershell
.venv\Scripts\python.exe tools\build_name_history.py --apply --offline
.venv\Scripts\python.exe tools\fix_name_history_end_dates.py --apply
.venv\Scripts\python.exe tools\infer_post_g_segments.py --apply
.venv\Scripts\python.exe _staging\g146_name_carry_20261113\verify.py
.venv\Scripts\python.exe -c "from src.data.name_consistency import name_at; print(name_at(r'data/local_store','001914.SZ','2006-11-01'))"
```
