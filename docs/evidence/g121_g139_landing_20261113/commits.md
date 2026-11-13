# G-121 / G-139 落地轮：提交报告（2026-11-13）

> 执行范围：`data/local_store/quotes/fund/sz/160136.SZ.parquet` 删 `2019-09-05`、`2019-09-17` 两行（用户点名放行）+ `meta/daily_index` 的 `source_start` 回填 + `meta/anomalies` 两项派生登记退役 + 缺口表 G-121/G-139 转「已修」。
> 执行脚本：`_staging/g139_g121_20261113/pipeline.py`（build / verify / apply 三段，均带断言）。

## 1. Commit 哈希

| 类型 | 值 |
|---|---|
| **短哈希** | `c17007f` |
| **长哈希** | `c17007f71c91ffd7baadbad0343a1c5a8b57b223` |

## 2. 完整 Commit Message

```text
feat(data): 2026-11-13，G-121 退役 160136.SZ 两行桩 bar 并回填 daily_index source_start
feat(data): 160136.SZ 行情 86 → 84 行，删 2019-09-05 / 2019-09-17 两行 OHLC 全 1.0、量额 0 的桩 bar，与 fund_adj_factor 因子 84 行对齐
feat(data): daily_index 只改该行 rows/start/file_size/built_at，其余 11 列逐值不等 0 格、空值数逐列相等，同轮回填 source_start 9,767 行取 .day 首日、20 行留空
fix(data): placeholder_quote_codes.csv 476 → 475 行、fund_pseudo_registry 删 160136.SZ|2019-09-05|2019-09-17，属派生登记未随行情退役的同源缺陷
test(data): test_fund_price_basis 全库 span 合计 25,770 → 25,768，同口径替换未放宽
docs(gap): G-121、G-139 转已修，四档 83/45/17/1 → 85/43/17/1，§7 补第 40 条与 r9 快照 SHA256
docs(gap): 8 件改前快照移入 _backup/ 四个目录，移动前后逐件 SHA256 相等
docs(gap): manifest_tool 三项 PASS 不一致 0，真实读 84 行 / 13 列空值 0，因子侧 84/84 对齐孤儿 0
```

## 3. 文件变更统计 + 清单

**统计**: `2 files changed, 21 insertions(+), 15 deletions(-)`

| 状态 | 文件 | 变化 |
|---|---|---|
| **M**（修改） | `docs/数据缺口登记_正式表_2026-11-12.md` | +21 / −15（G-121、G-139 判定改已修；§2 计数 note；§5.1 四档 83/45→85/43；§5.5 自检 145/146；§7 第 40 条） |
| **M**（修改） | `tests/test_fund_price_basis.py` | 全库 span 合计断言 25,770 → 25,768（同口径，未放宽） |

## 4. 工作区状态（提交后）

```text
 M src/data/quality/stale_status.py        ← 他路在途，未碰
 M tests/test_stale_status.py              ← 他路在途，未碰
 M tools/advice_stateful_backtest.py       ← 他路在途，未碰
?? docs/evidence/cn_bond_index_member_probe_20261112/   ← 他路在途，未碰
?? docs/evidence/cn_bond_yield_tenor_probe_20261112/    ← 他路在途，未碰
?? docs/evidence/fund_f10_listdate_probe_20261112/      ← 他路在途，未碰
```

隔离索引提交后已在**新的一条命令**里执行 `git read-tree HEAD` 回填共享索引，`git ls-files -s` 的 blob 与 `git rev-parse HEAD:<path>` 逐一相等（`369e7765…` / `3979ed99…`），`git status --short` 无假 `MM`。

## 5. 8 件正式数据落盘 SHA256（改后）

| 文件 | 字节 | SHA256 |
|---|---|---|
| `quotes/fund/sz/160136.SZ.parquet` | 8,676 | `6ab80e40be12de09ac2b4317a4ec9976bfcab3552c1e016f348b294d66ec1ebd` |
| `quotes/fund/sz/160136.SZ.manifest.json` | 1,670 | `b0aa6cb0b6f098df3fec9a4fe0eec1d6bd3542ea2c96a0421bc9448106343857` |
| `quotes/manifest.json` | 11,136 | `284de7f51a699c9f4fb02013281aec0f03b3a9c43dd849caad8323f813ff3d01` |
| `meta/daily_index.parquet` | 166,567 | `9118111960ed5751a004c9b35a2223e1dde4be76ad23d05c88b6dff854c2836f` |
| `meta/daily_index.manifest.json` | 2,269 | `7d51e351a64951d0aafc241b6e4fd801c3575ddb3e85fea54e2417fc1c7968d8` |
| `meta/anomalies/placeholder_quote_codes.csv` | 21,381 | `30e2017eda28b95f9c1a475fbfbe90268f5ff1553ffdf0cf62bc6f16315bf73c` |
| `meta/anomalies/fund_pseudo_registry.json` | 26,071 | `b408abf4072367287a63a0d6b0077970dfe11ca78d21a19d08e0dfa5ab3999cb` |
| `meta/anomalies/manifest.json` | 9,292 | `adadafdbb737e5594ed7479e8853baef90f49c9d8b7ec87cfe16efc31571fe3e` |

改前 8 件已按 DG-11/DG-13 移入仓库根 `_backup/{quotes_fund_sz_160136_20261113,quotes_manifest_g121_20261113,meta_daily_index_20261113,meta_anomalies_g121_20261113}/`，`backup_sha.json` 记录移动前后逐件 SHA256 相等（`BK_OK` × 8）。

## 6. 验收实测

| 项 | 结果 |
|---|---|
| 行情行数 | 86 → **84 行**，区间 `2020-12-11~2021-04-16`；删的两行 OHLC 全 `1.0`、`volume=amount=0` |
| 逐值对拍 | 84 行按 `date` left join 逐列 `eq_missing` **全等**；anti-join 显示消失行恰为 `{2019-09-05, 2019-09-17}` |
| 索引 | 9,787 行 / 12 列不变；`rows` / `start` / `file_size` / `built_at` 各恰 1 行变动，其余列逐值不等 **0** 格；改动行恰 `('fund','160136.SZ')`；各列空值数与改前相等 |
| `source_start` | 9,767 行取 `.day` 首行日期、**20 行留 null**；`is_mixed_source` 命中 **11 只**（`000004.SZ` + 9 只基金 + `399699.SZ`） |
| 断言自测 | 向 `000001.SZ`（stock）注入 `rows`/`start` 后变动计数 1 → 2；全 null 注入时 `null_count != 20` 被抓住（DG-31） |
| 派生登记 | CSV 476 → **475 行**，与 `_build_placeholder_quote` 重算的 256 段 inner join 逐值全等，`256 + 219 = 475` |
| `manifest_tool --check` | `quotes` **PASS**（10,788 份 / 21,575 条 / 不一致 0）、`meta/daily_index` **PASS**（2 条 / 不一致 0）、`meta/anomalies` **PASS**（4 条中 3 条核对 / 不一致 0） |
| 真实读（DG-23/DG-30） | `read_daily('fund','160136.SZ')` **84 行**、`2020-12-11~2021-04-16`、13 列空值 **0**；`coverage()` rows=84、`is_mixed_source()=False`、`source_start=2019-09-05`、`full=True`；`000004.SZ` 侧 `is_mixed_source()=True`、`start=1991-01-14` / `source_start=1991-08-02` / 8,251 行 |
| 因子侧 | `fund_adj_factor` 84/84 日期对齐、孤儿 **0**、缺口 **0** |
| 单测 / 静态 | `pytest tests/test_fund_price_basis.py` **13 passed**；`ruff check` 该文件 **All checks passed** |

**既有失败项（与本改动无关，如实列出）**：`mypy tests/test_fund_price_basis.py` 报 `tests\test_fund_price_basis.py:55: error: The return type of a generator function should be "Generator" or one of its supertypes [misc]`——`_reset_cache()` fixture 的类型标注问题，出自 2026-11-12 引入该文件时，本轮未改该行，归属既有问题。

## 7. 备份同步

`_staging/g139_g121_20261113/backup_sync.py` 增量 `copy2` → `G:\git\python-quant\`，**19 个文件**（2 个 git 变更文件 + 8 件正式数据 + `_backup/` 4 个目录 8 件 + `_backup/docs/…_r9.md`），逐文件 SHA256 断言相等，未删任何目录、未碰备份仓 `.git`。
