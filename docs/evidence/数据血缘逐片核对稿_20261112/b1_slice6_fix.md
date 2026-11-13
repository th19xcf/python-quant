# B1 片6 收口 · ST 人工补充层修复报告

- 依据：`2026-11-12/data-lineage/outputs/b1_slice6_check.md`（mtime 2026-11-12 23:53:47）§1 / §5.3 / §8 / §11
- 基线代码：HEAD `03a3d00`（本轮改动尚未提交，行号以 HEAD 为基准标注，另给工作区新行号）
- 代码改动：`src/data/st_name_history.py`、`src/data/manifest_semantics.py`、`tests/test_st_name_history.py`
- 数据改动：`meta/st/supplement/` 三件 + `meta/st/st_coverage.parquet`（经 `apply` 重建）
- 结论：六项全部落地；第五项按指令只出正确表述、未改 `docs/数据权威路径清单.md`

---

## 0. 结论一览

| 项 | 结论 | 证据位置 |
|---|---|---|
| 补 ① 交集定案 | 两个读数**都不错，测的不是同一张表**：片6 比的是「补充层 vs 官方独有层」（交集 0），另一路比的是「补充层 vs 合并后 `st_coverage`」（后者本身就含补充层这批行，故 2,336/2,336 命中） | §1 |
| 一 写入口 | manifest / unresolved.csv / 两张 failures.csv 全部改 `write_text_atomic`；补 DG-10 事务声明；build / apply 写前各做一次 DG-11 快照 | §2 |
| 二 可复现性 | `fetched_at` 改证据文件 mtime、`built_at` 取其最大值、`today` 默认取行情末日；连续两次重跑 4 件产物 SHA256 全等 | §3 |
| 三 manifest | `files` 2 → 3 条（补 `unresolved.csv`）；新增 `quote_last_date` / `quote_codes` / `coverage_cutoff`；`manifest_tool --check` PASS | §4 |
| 四 负证据推进 | 2,336 → 2,337 行（旧行 0 丢失，+1 为 688827.SH 新进池）；`coverage_end` 改 2,324 行；null 前后均 0；2,325 行已到 2026-11-11 | §5 |
| 五 口径纠正 | 正确表述：**正式层优先、补充层只补缺，两者零交集**；现写「补充层覆盖正式层」与代码不符 | §6 |
| 六 弱证据冲突 | 15 只 / 17 段，整段落在负证据窗口内，`confidence` 全 `low`，交 B4 裁定，未改数据 | §7 |

---

## 1. 定案：ST 补充层与官方覆盖的交集（双向集合差）

### 1.1 四个数（当前磁盘，2026-11-13 00:16 取样）

| 数 | 值 | 口径 |
|---|---|---|
| 补充层标的数 | **2,337** | `meta/st/supplement/supplement_coverage.parquet` 行数 = 去重标的数（2,337 行 / 2,337 只，无重复键） |
| 官方 `st_coverage` 标的数 | **5,890** | `meta/st/st_coverage.parquet` 行数 = 去重标的数（5,890 行 / 5,890 只） |
| 两者交集 | **2,337** | 补充层 100% 在 `st_coverage` 内 |
| 双向差集 | 仅补充层独有 **0** 只；仅 `st_coverage` 独有 **3,553** 只 | 见 1.2 |

合并后净新增行数：**2,337 行**（5,890 − 3,553 官方独有）。其中 2,336 行是片6 之前就已并入的，**本轮 `apply` 净新增 1 行 = `688827.SH`**（2026-11-10 上市，此时才进池且有干净名称历史）。

### 1.2 为什么两个读数都对：三张表被混称了「官方」

| 层 | 定义 | 行数 | 与补充层交集 |
|---|---|---|---|
| 官方独有层 | `st_coverage` 中 `source` ∉ {`em_name_history_clean`, `cninfo_notice_dated`} | **3,553** | **0** |
| 补充层 | `meta/st/supplement/supplement_coverage.parquet` | **2,337** | — |
| 合并后 `st_coverage` | 官方独有层 + 补充层（`apply` 落库结果） | **5,890** = 3,553 + 2,337 | 2,337（含自身） |

- 片6 报「2,336 只与官方 `st_coverage` 零交集」，实际比的是 `official_coverage()` 过滤后的**官方独有层**（3,553 只）→ 交集 0，成立。
- 另一路报「2,336 只在官方 `st_coverage`（5,889 只）内」，比的是**合并后规范表**，而该表当时已包含这 2,336 行 → 交集 2,336，也成立。
- 差异根源是「官方」一词指代不一致，不是数据错。判定：**补充层与官方层零交集成立；合并表与补充层必然 100% 相交，这是合并的结果，不是独立证据。**

### 1.3 两侧取样时点与 mtime（不只看结论）

| 取样方 | 读的文件 | 行数 | 文件 mtime | SHA256(前 16) |
|---|---|---|---|---|
| 片6 体检 | `meta/st/supplement/supplement_coverage.parquet`（当时版本，现留存在快照里：`_backup/st_supplement_20261112/supplement_coverage.parquet`） | 2,336 | **2026-11-10 19:47:51** | `75f55307219b85c2` |
| 另一路 | `meta/st/st_coverage.parquet`（apply 前版本，现留存在 `_backup/st_build_store_20261112/st_coverage.parquet`） | 5,889 | **2026-11-12 00:33:23** | `dfb657e4c356d9eb` |
| 本轮（现在） | `meta/st/supplement/supplement_coverage.parquet` | 2,337 | 2026-11-13 00:00:20 | `d01cbcb6dd8c0796` |
| 本轮（现在） | `meta/st/st_coverage.parquet` | 5,890 | 2026-11-12 23:59:06 | `6869338e9de30d82` |

- 用**快照复算 apply 前**：补充 2,336 ∩ `st_coverage` 5,889 = **2,336**，仅补充独有 0，仅 `st_coverage` 独有 3,553；`688827.SH` 当时在补充层、不在 `st_coverage`（与另一路的读数逐项吻合）。
- `coverage_end` 推进发生在 2026-11-12 23:57 之后（`build`）与 23:59（`apply`），片6 的 2,336 行版本截至 2026-11-10 19:47 未再刷新，故其 `coverage_end` 仍停在 2026-11-09。**行数差 1 与 `coverage_end` 落后 2 个交易日，是同一批改动的两个侧面，不是两个独立缺陷。**

复算命令（只读）：

```powershell
.venv\Scripts\python.exe -c "
import polars as pl
s=pl.read_parquet('data/local_store/meta/st/supplement/supplement_coverage.parquet')
c=pl.read_parquet('data/local_store/meta/st/st_coverage.parquet')
off=c.filter(~pl.col('source').is_in(['em_name_history_clean','cninfo_notice_dated']))
S,C,O=set(s['ts_code']),set(c['ts_code']),set(off['ts_code'])
print('supplement',len(S),'st_coverage',len(C),'inter',len(S&C),'only_sup',len(S-C),'only_cov',len(C-S))
print('official_only',len(O),'inter_with_supplement',len(O&S))"
```

实测输出：`supplement 2337 st_coverage 5890 inter 2337 only_sup 0 only_cov 3553` / `official_only 3553 inter_with_supplement 0`。

---

## 2. 一、写入口合规（DG-9 / DG-10 / DG-11）

### 2.1 原子写

| 写点 | HEAD | 现在 |
|---|---|---|
| `manifest.json` | 裸 `write_text`（HEAD `:615` 附近） | `write_text_atomic`（经 `_write_text`，工作区 L202-204） |
| `unresolved.csv` | 裸 `frame.write_csv(path)`（HEAD `:615`） | `_write_csv()` → `write_text_atomic(frame.write_csv(), path)`（工作区 L207-209） |
| `em_failures.csv` | 裸 `write_csv` | `_write_csv()` |
| `candidate_failures.csv` | 裸 `write_csv` | `_write_csv()` |
| `em_state.json` / `candidate_state.json` / 验证报告 | 裸 `write_text` | 同一 `_write_text` → 原子写 |

`write_text_atomic` 由「已导入但 0 调用」变为 1 处导入、4 类写点共用。

### 2.2 DG-10 事务声明

写在模块 docstring「写盘事务」段，固定顺序 **快照 → 区间表 → 覆盖表 → unresolved.csv → manifest.json**，manifest 最后写、充当提交标记：

| 失败点 | 残留状态 | 检出方式 | 回滚 |
|---|---|---|---|
| 快照之前 | 正式层零改动 | — | 无需 |
| 快照后、区间表/覆盖表未换 | 正式层零改动 + 多一份快照目录 | 目录多出 `_backup/st_supplement_<日期>` | 删除该快照目录即可（本轮未删，留痕） |
| 区间表/覆盖表已换、csv 未换 | 新旧混合 | `manifest_tool --check` 报 sha256 不一致 | `copy2` 快照 4 件回正式目录 |
| csv 已换、manifest 未写 | manifest 记录数与磁盘行数不符 | `manifest_tool --check` + 行数比对 | 同上 |
| manifest 已写 | 事务完成 | `--check` 全 PASS | — |

单文件原子不等于多文件事务（DG-10 原文），上表即该笔多文件更新的显式顺序、残留与回滚。

### 2.3 DG-11 覆盖前快照

新增 `_snapshot_paths()`（工作区 L227-247）：`copy2` 到仓库根 `_backup/<label>_<日期>/`，已存在则追加序号，**绝不覆盖既有快照**。

| 触发点 | 快照内容 | 实测落点 |
|---|---|---|
| `build` | `supplement_intervals.parquet` / `supplement_coverage.parquet` / `unresolved.csv` / `manifest.json` | `_backup/st_supplement_20261112/`（旧版 2,336 行 / 16,041 B / mtime 2026-11-10 19:47:51）＋ `_1` `_2` `_3`（各为上一轮产物） |
| `apply` | `st_intervals.parquet` / `st_coverage.parquet` / `coverage.json` | `_backup/st_build_store_20261112/`（apply 前 5,889 行版本） |

**用 `copy2` 而非 `move`**：`move` 会让构建失败时正式目录缺位，而快照已能提供逐字节回滚（同 `local_store/build.py::_snapshot_quotes_dir` 先例）。快照与原文 SHA256 相等由 §1.3 表给出（`75f55307…` = 片6 当时版本）。

---

## 3. 二、可复现性

### 3.1 改法

| 字段 | 原 | 现 | 依据 |
|---|---|---|---|
| `fetched_at`（负证据 2,335 行） | `datetime.now()` | `em_org_basic.parquet` 的 mtime（`2026-11-05T00:43:16`） | 负证据的取证动作就是读这份登记册 |
| `fetched_at`（定日区间 2 行） | `datetime.now()` | 该标的公告缓存分片的 mtime | 同上 |
| `built_at` | `datetime.now()` | 上述时刻的最大值，并写 `built_at_basis` 说明 | manifest 需逐字节可复现 |
| `today`（窗口上界） | `date.today()` | **行情末日**（2026-11-11） | 覆盖窗口上界本来就是 `min(个股行情末日, 构建日)` |
| `_intervals_from_segments` 退市判断 | `coverage_end < date.today()` | `coverage_end < 行情末日` | 隔天重跑不会改变区间 |

### 3.2 实测：连续两次重跑逐字节相同

```
run1 {'supplement_intervals.parquet': 'bb60b0967cc60c72d45c21e3c2847309e3d194d2e3a8cb9ce71522e802d5a6e6',
      'supplement_coverage.parquet':  'd01cbcb6dd8c0796836db8330d34cb6ecb9d0c948edf72ded80fb04db16fc69d',
      'unresolved.csv':               'ef4977642e2077089066048eda6e07349b939b5890061b1f00a615a724a1cc16',
      'manifest.json':                'a4c7c83e1c82dedf649dfd86f18a15340b193418db8d14091cd96c62e9708c76'}
run2 与 run1 逐项相同（4/4）
```

（`manifest.json` 的 sha 在最后一次修 `manifest_semantics.py` 注释行号后刷新为 `a4c7c83e…`；前三件全程未变。）

复现：`.venv\Scripts\python.exe tools\sync_st_name_history.py build` 连跑两次后比对 4 件 SHA256。

---

## 4. 三、manifest 补齐（含代码侧 vs HEAD 对照）

### 4.1 代码侧

| 项 | HEAD（已提交） | 工作区（本轮） |
|---|---|---|
| `files` 段 | 2 条，仅两个 parquet | **3 条**：两 parquet + `_csv_entry(unresolved.csv)`（工作区 `:754`；HEAD 对应位置 `:527`/`:579` 无此条） |
| `quote_last_date` | 无 | `"2026-11-11"` |
| `quote_codes` | 无 | `5832` |
| `coverage_cutoff` | 无 | `"2026-11-11"` |
| `built_at` | `datetime.now()` | 证据 mtime 最大值 + `built_at_basis` |
| `unresolved.csv` 写入口 | HEAD `:615` 裸写 | `_write_csv` → `write_text_atomic`（工作区 `:731`） |

### 4.2 实测：重跑产物与磁盘当前内容一致

除 §3.2 的四件全等外，逐字段核对 manifest：

| 字段 | 值 |
|---|---|
| `quote_last_date` / `coverage_cutoff` | 2026-11-11 / 2026-11-11 |
| `pool` / `quote_codes` / `uncovered_before` | 5,832 / 5,832 / 2,346 |
| `clean_negative_coverage` / `dated_candidate_coverage` | 2,335 / 2 |
| `unresolved` | 9（全部 `st_name_without_dated_event`） |
| `supplement_interval_rows` / `supplement_coverage_rows` | 2 / 2,337 |
| `files[0]` intervals | 2 行 / 7 列 / 2,781 B / `bb60b096…` / 区间 2019-07-16~2025-04-30 |
| `files[1]` coverage | 2,337 行 / 5 列 / 11,520 B / `d01cbcb6…` / 2026-11-11 最多 |
| `files[2]` unresolved.csv | 9 行 / 4 列 / 1,132 B / `ef497764…` |

`manifest_tool --check --only meta/st/supplement` →

```
manifest 1 份 / 文件记录 3 条 / 不一致 0 条 / sha256 格式告警 0 条 / 无法定位 0 条 / 语义未登记 0 项
结论: PASS
```

说明：csv 的 `rows` 在 `manifest_tool` 里归 `rows_unverifiable`（只对 parquet 核行数），`bytes` 与 `sha256` 照常核对通过。

---

## 5. 四、负证据推进到行情末日

### 5.1 三项断言（DG-20 / DG-31）

对比对象：改前快照 `_backup/st_supplement_20261112/supplement_coverage.parquet`（2,336 行，`75f55307…`）vs 改后正式表（2,337 行，`d01cbcb6…`）。脚本：`2026-11-12/data-lineage/work/b1s6_fix_assert.py`。

```
rows_before 2336   rows_after 2337
lost_keys 0        added_keys 1        added_codes ["688827.SH"]
changed_rows {coverage_start 0, coverage_end 2324, source 0, fetched_at 2336}
null_counts  {coverage_start (0,0), coverage_end (0,0), source (0,0), fetched_at (0,0)}
reversed_windows 0
end_distribution_after  2026-11-11:2325 / 2026-11-09:1 / 2026-09-14:2 / 2026-01-20:1 / 2025-12-31:1
selftest_rejects_null_injection true
```

- **行数守恒**：2,336 旧行逐键 100% 保留（`lost_keys=0`）；+1 行是池扩容新增可覆盖标的 `688827.SH`（2026-11-10 上市，此前不在池内），即「旧行不丢 + 新增可解释」，不是凭空多行。
- **改动行数**：`coverage_end` 2,324 行（2,325 行从 11-09 推到 11-11，其中 1 行本就终于 11-09 未动 → 2,324）；`coverage_start` / `source` 0 改动，证明只动了窗口上界。
- **null 计数**：四列改前改后均 0；断言写法为「改后 ≤ 改前」且窗口两端必须为 0，并对 `coverage_end` 注入 1 个 null 的副本做反例自测——脚本确实抛 `AssertionError`，即该断言能对「空值 0 变 N」失败（DG-31）。
- `fetched_at` 2,336 行全改属预期：时间戳基准从运行时刻改成证据 mtime（§3.1），与窗口推进是两件事，故分列统计。

### 5.2 `apply` 预演（避免跨批次无意改动）

先写 `_staging/st_supplement_20261112/rebuild/` 比对，再落正式层：

| 表 | 行数 | 仅正式独有 | 仅候选独有 | 逐列改动 |
|---|---|---|---|---|
| `st_intervals` | 2,304 → 2,304 | 0 | 0 | `precision` 0 / `evidence` 0 / `raw_fetched_at` 0（**逐值零差异**，B4 内容未被改动） |
| `st_coverage` | 5,889 → 5,890 | 0 | 1 | `coverage_start` 0 / `coverage_end` 2,324 / `source` 0 / `fetched_at` 5,889（null 全 0） |

正式层 `apply` 后复算与预演一致（`st_coverage` 2,7126 B / `6869338e…`，`st_intervals` 33,908 B / `2804275…`）。

### 5.3 下游一致性（DG-23）

真实读路径 `st_history.status_at`，样本 = 补充层 `em_name_history_clean` 排序后每 120 取 1（40 个键 × 2 天）：

| 阶段 | 2026-11-10 / 2026-11-11 |
|---|---|
| `apply` 前 | 40/40 `unknown` |
| `apply` 后 | 40/40 `non_st`（转换计数 `unknown -> non_st: 40`） |

证明新文件确实被读侧消费，而不是只把文件写完。

---

## 6. 五、口径纠正（供权威清单/下一路直接采用）

`docs/数据权威路径清单.md` 本次未改；该路已在 `ec901e2` 里把 ST 补充层一条改写为「官方优先、intervals 五元组并集」，与本报告口径一致，可直接沿用。代码侧尚未同步下面这句：

> 补充层只补官方层未覆盖的标的与区间：覆盖表按 `concat` 后 `unique(ts_code, keep='first')` 合并，官方行在前 → **正式层优先**；区间表按 (ts_code, start_date, end_date, flag, source) 去重后取**并集**。当前补充层 2,337 只与官方独有层 3,553 只**零交集**，「补充层覆盖正式层」这一语义从未被执行。

同一处还有两件同源问题，一并给出口径（不在本次改动范围）：

| 位置 | 现写 | 应为 |
|---|---|---|
| `manifest.json` 的 `authority`（源：`src/data/manifest_semantics.py` L90） | 「清单 **L65**：…合并顺序为补充层覆盖正式层」 | 行号 **L67**；口径同上句 |
| `manifest.json` 的 `semantics`（源：同文件 L93） | 「合并优先级为「补充层覆盖正式层」」 | 「正式层优先，补充层只补缺（零交集）」 |

`src/data/manifest_semantics.py` 本轮只改了两个会随构建失真的字符串（`dataset` 里写死的「2,336 行覆盖」已改成就地指向 `files` 段、`source` 里的行号已按新代码刷新），`authority` / `semantics` 的覆盖口径**未动**，留给统一改文档的那一路按上表替换，避免与它的 11 条改动撞车。

---

## 7. 六、价格反推疑似 ST × 负证据冲突清单（只登记，不改数据）

口径：`meta/st/st_price_inferred.parquet` 中 `flag=suspected_st` 且 `confidence=low` 的段，整段落在补充层负证据（`source=em_name_history_clean`）窗口内。共 **15 只 / 17 段**，17/17 整段落在窗口内。

反推依据（`evidence` JSON 同源字段）：疑似段内日收益长期贴住 ST 的 ±5% 限制带，`breach_inside=0`（段内无一次越过 5% 限制的证据），故判为「疑 ST」但 `confidence=low`——除权、停牌、涨跌停制度变更都能制造同类形态。下表只给裁判需要的三个量：段内交易日数、段内最大绝对日收益、段内日均成交额。

| # | 标的 | 疑似段 | 交易日 | max\|日收益\| | 日均成交额(万元) | 冲突的负证据窗口 |
|---|---|---|---|---|---|---|
| 1 | `600063.SH` | 2011-02-10 ~ 2011-07-29 | 117 | 0.0559 | 13,929.3 | 1997-05-28 ~ 2026-11-11 |
| 2 | `600133.SH` | 2006-12-25 ~ 2007-07-24 | 133 | 0.0505 | 3,923.2 | 1998-02-12 ~ 2026-11-11 |
| 3 | `600135.SH` | 2006-12-21 ~ 2008-02-13 | 257 | 0.0509 | 4,140.5 | 1998-01-22 ~ 2026-11-11 |
| 4 | `600211.SH` | 2006-12-21 ~ 2007-08-02 | 128 | 0.0503 | 2,754.0 | 1999-07-21 ~ 2026-11-11 |
| 5 | `600283.SH` | 2004-11-04 ~ 2005-05-26 | 133 | 0.0512 | 86.2 | 2000-10-18 ~ 2026-11-11 |
| 6 | `600379.SH` | 2006-12-25 ~ 2007-10-12 | 175 | 0.0504 | 2,672.1 | 2002-01-16 ~ 2026-11-11 |
| 7 | `600517.SH` | 2009-09-24 ~ 2010-03-09 | 107 | 0.0550 | 10,905.8 | 2003-10-10 ~ 2026-11-11 |
| 8 | `600643.SH` | 2007-01-05 ~ 2007-12-19 | 224 | 0.0504 | 17,708.2 | 1993-04-26 ~ 2026-11-11 |
| 9 | `600664.SH` | 2007-01-08 ~ 2008-08-14 | 317 | 0.0505 | 3,674.0 | 1993-06-29 ~ 2026-11-11 |
| 10 | `600682.SH` | 2006-12-22 ~ 2008-03-07 | 286 | 0.0503 | 3,320.3 | 1993-10-18 ~ 2026-11-11 |
| 11 | `600688.SH` | 2006-10-25 ~ 2009-12-30 | 742 | 0.0508 | 10,874.4 | 1993-11-08 ~ 2026-11-11 |
| 12 | `600688.SH` | 2010-10-26 ~ 2011-05-18 | 137 | 0.0505 | 9,914.7 | 1993-11-08 ~ 2026-11-11 |
| 13 | `600688.SH` | 2012-12-04 ~ 2013-06-27 | 120 | 0.0509 | 3,431.6 | 1993-11-08 ~ 2026-11-11 |
| 14 | `600865.SH` | 2007-01-12 ~ 2008-01-25 | 244 | 0.0504 | 1,860.6 | 1994-08-09 ~ 2026-11-11 |
| 15 | `601567.SH` | 2011-06-27 ~ 2012-01-16 | 136 | 0.0558 | 3,105.2 | 2011-06-15 ~ 2026-11-11 |
| 16 | `601908.SH` | 2012-09-14 ~ 2013-03-29 | 128 | 0.0556 | 2,507.7 | 2011-09-08 ~ 2026-11-11 |
| 17 | `603085.SH` | 2025-06-05 ~ 2026-06-09 | 246 | 0.0560 | 8,693.3 | 2015-06-30 ~ 2026-11-11 |

- 批量复核入口：`2026-11-12/data-lineage/work/b1s6_price_conflicts.py`（只读，输出 `b1s6_price_conflicts.json`，17 行明细）。
- 最需优先裁定：`603085.SH`（2025-06-05 ~ 2026-06-09），段尾距今不到半年，若成立即负证据的**近期假阴性**，会直接影响近端回测的 ST 判定。
- `600688.SH` 一只 3 段（2006/2010/2012 起），若其中任一段成立，说明该标的名称登记册缺多条历史曾用名，负证据不能按「单只一次确认」处理。
- 本片不裁决、未改任何数据；`st_price_inferred` 与 `supplement_coverage` 均保持原状。

---

## 8. 影响面与复现

### 8.1 代码

| 文件 | 变更 | 校验 |
|---|---|---|
| `src/data/st_name_history.py` | +181 / −34（原子写、快照、确定性时刻、manifest 三件登记、apply 快照） | `ruff` All checks passed；`mypy` Success；`pytest tests/test_st_name_history.py` 9 passed |
| `src/data/manifest_semantics.py` | 2 行字符串（`dataset` 去掉写死行数、`source` 行号刷新） | 同上 |
| `tests/test_st_name_history.py` | 1 条用例改按新签名传 `fetched_at` / `quote_last` | 9 passed |

### 8.2 数据（附 SHA256）

| 文件 | 行数 | bytes | SHA256 |
|---|---|---|---|
| `meta/st/supplement/supplement_intervals.parquet` | 2 | 2,781 | `bb60b0967cc60c72d45c21e3c2847309e3d194d2e3a8cb9ce71522e802d5a6e6` |
| `meta/st/supplement/supplement_coverage.parquet` | 2,337 | 11,520 | `d01cbcb6dd8c0796836db8330d34cb6ecb9d0c948edf72ded80fb04db16fc69d` |
| `meta/st/supplement/unresolved.csv` | 9 | 1,132 | `ef4977642e2077089066048eda6e07349b939b5890061b1f00a615a724a1cc16` |
| `meta/st/supplement/manifest.json` | — | 2,989 | `a4c7c83e1c82dedf649dfd86f18a15340b193418db8d14091cd96c62e9708c76` |
| `meta/st/st_intervals.parquet` | 2,304 | 33,908 | `2804275…`（逐值与改前快照 0 差异，仅重编码） |
| `meta/st/st_coverage.parquet` | 5,890 | 27,126 | `6869338e9de30d8275a4857f2c3fcb020d7045c0b2ccc00674847e8b57ca3353` |

回滚：`copy2 _backup/st_supplement_20261112/*` 回 `meta/st/supplement/`；`copy2 _backup/st_build_store_20261112/*` 回 `meta/st/`（含 `coverage.json`）。

### 8.3 复现命令

```powershell
$env:QUANT_CHANGELOG_TASK='b1_slice6_fix'
.venv\Scripts\python.exe tools\sync_st_name_history.py build      # 产物确定性，连跑两次 SHA256 相同
.venv\Scripts\python.exe tools\sync_st_name_history.py apply      # 重建 st_intervals / st_coverage
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s6_fix_assert.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s6_apply_preflight.py
.venv\Scripts\python.exe 2026-11-12\data-lineage\work\b1s6_downstream.py after --compare
.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta/st/supplement
```

---

## 9. 遗留与交接

| # | 事项 | 去向 |
|---|---|---|
| 1 | 代码已提交：源仓 `2a33b60`（3 文件），备份仓 `f3160fc` 已推送 `th19xcf/python-quant`；同批他路改动（`src/data/events.py`、`src/data/factors_manifest.py`、`tests/test_data_events.py`）由他路 `239eaf8` 单独入库，未混入本笔 | 完成 |
| 2 | `manifest_semantics.py` 的 `authority` / `semantics` 仍写「补充层覆盖正式层」且引 L65（应为 L67）；文档清单已在 `ec901e2` 更正，代码侧与本报告 §6 仍未同步 | 由统一改清单的那一路按 §6 替换 |
| 3 | 15 只 / 17 段弱证据冲突（含 `603085.SH` 近端假阴性风险） | B4 裁定 |
| 4 | `plan_supplement` 仍无 `start <= end` 断言；本轮把 `today` 默认改为行情末日后该反序形态已不可达，但缺显式守卫 | 与 #5 同批的代码任务 |
| 5 | `_backup/` 下同日多次 build 会留下同内容序号快照（`_1` `_2` `_3`，各 18 KB） | 保留（DG-11 只增不覆盖）；如需清理须点名路径 |
| 6 | `tmp/cov_sandbox/` 残留副本（片6 §11 #7） | 未动，需点名后处理 |
