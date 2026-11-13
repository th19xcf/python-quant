# G-067 第二段：跑批窗口内输入改动时间线

- 基线 run 起点：`2026-11-11 19:42:20`（`reports/advice/stateful/20190131_20260803_h20` 落盘）
- 此后被改写的 `quotes/stock/**` 分片：**78** 个；其中在 2019-01-01 之后存在逐值差异的：**11** 个
- 「来源」取 `_backup/**` 中同名旧版所在目录；无旧版的标「来源未定」

## 1. 按时间聚类

| 改动时间 | 文件数 | 改动来源 | 触及 2019+ 行 |
|---|---:|---|---:|
| 2026-11-11 21:46 | 8 | _backup/quotes_stock_20261111 | 8 |
| 2026-11-12 00:08 | 1 | _backup/quotes_stock_302132SZ_20261112 | 0 |
| 2026-11-12 13:09 | 1 | _backup/quotes_stock_basis_drop_20261112 | 0 |
| 2026-11-12 13:10 | 6 | _backup/quotes_stock_basis_drop_20261112 | 0 |
| 2026-11-12 16:26 | 25 | _backup/factor_fix_20261111、_backup/quotes_adj_recalc_20261110_002529、来源未定 | 1 |
| 2026-11-12 16:27 | 19 | _backup/factor_fix_20261111、_backup/quotes_adj_recalc_20261110_002529、来源未定 | 1 |
| 2026-11-12 20:28 | 17 | _backup/quotes_stock_sz_1992_shift_20261113、_backup/quotes_sz_1993b_20261113 | 0 |
| 2026-11-12 20:39 | 1 | _backup/quotes_sz_1993b_20261113 | 1 |

## 2. 其它输入层（meta / factors）

| 改动时间 | 文件 | 说明 |
|---|---|---|
| 2026-11-12 20:39:43 | `data\local_store\meta\daily_index.parquet` | 指数日线（派生层重建一并写入） |
| 2026-11-12 20:39:43 | `data\local_store\factors\stock_adj_factor.parquet` | 复权因子（派生层重建） |
| 2026-11-12 00:22:07 | `data\local_store\factors\stock_turnover.parquet` | 换手率因子（派生层重建） |
| 2026-11-12 20:39:43 | `data\local_store\factors\meta.json` | factors 层元信息 |
| 2026-11-12 20:39:44 | `data\local_store\factors\manifest.json` | factors 层清单 |

## 3. 逐文件明细

| 分片 | 改动时间 | 来源 | 行数 旧→新 | 窗口内差异列 |
|---|---|---|---|---|
| sh/600010.SH.parquet | 2026-11-12 13:10:26 | _backup/quotes_stock_basis_drop_20261112 | 6111→6111 | 无 |
| sh/600157.SH.parquet | 2026-11-12 13:10:26 | _backup/quotes_stock_basis_drop_20261112 | 6348→6348 | 无 |
| sh/600188.SH.parquet | 2026-11-11 21:46:06 | _backup/quotes_stock_20261111 | 6800→6800 | change(4); hfq_factor(1905); pct_chg(4); pre_close(4); qfq_factor(1100) |
| sh/600221.SH.parquet | 2026-11-12 13:09:52 | _backup/quotes_stock_basis_drop_20261112 | 6265→6265 | 无 |
| sh/600270.SH.parquet | 2026-11-12 16:27:00 | _backup/factor_fix_20261111 | 4243→4243 | 无 |
| sh/600305.SH.parquet | 2026-11-12 16:27:00 | _backup/quotes_adj_recalc_20261110_002529 | 6186→6188 | 无 |
| sh/600401.SH.parquet | 2026-11-12 16:27:01 | 来源未定 | —→— | — |
| sh/600432.SH.parquet | 2026-11-12 16:27:01 | 来源未定 | —→— | — |
| sh/600519.SH.parquet | 2026-11-11 21:46:06 | _backup/quotes_stock_20261111 | 6040→6040 | change(4); hfq_factor(1905); pct_chg(4); pre_close(4); qfq_factor(1206) |
| sh/600640.SH.parquet | 2026-11-12 16:27:02 | _backup/quotes_adj_recalc_20261110_002529 | 7973→7975 | 无 |
| sh/600656.SH.parquet | 2026-11-12 16:27:02 | 来源未定 | —→— | — |
| sh/600680.SH.parquet | 2026-11-12 16:27:02 | 来源未定 | —→— | — |
| sh/600719.SH.parquet | 2026-11-11 21:46:06 | _backup/quotes_stock_20261111 | 7081→7081 | hfq_factor(1885) |
| sh/600806.SH.parquet | 2026-11-12 16:27:03 | 来源未定 | —→— | — |
| sh/600872.SH.parquet | 2026-11-12 16:27:03 | _backup/quotes_adj_recalc_20261110_002529 | 7570→7572 | 无 |
| sh/600938.SH.parquet | 2026-11-11 21:46:06 | _backup/quotes_stock_20261111 | 1105→1105 | change(2); hfq_factor(1048); pct_chg(2); pre_close(2); qfq_factor(57) |
| sh/600989.SH.parquet | 2026-11-12 16:27:03 | _backup/quotes_adj_recalc_20261110_002529 | 1816→1818 | change(1); pct_chg(1); pre_close(1) |
| sh/601268.SH.parquet | 2026-11-12 16:27:04 | 来源未定 | —→— | — |
| sh/601288.SH.parquet | 2026-11-12 13:10:26 | _backup/quotes_stock_basis_drop_20261112 | 3960→3960 | 无 |
| sh/601801.SH.parquet | 2026-11-12 16:27:04 | 来源未定 | —→— | — |
| sh/601811.SH.parquet | 2026-11-12 16:27:04 | 来源未定 | —→— | — |
| sh/601868.SH.parquet | 2026-11-12 13:10:26 | _backup/quotes_stock_basis_drop_20261112 | 1239→1239 | 无 |
| sh/601869.SH.parquet | 2026-11-12 16:27:05 | 来源未定 | —→— | — |
| sh/601988.SH.parquet | 2026-11-12 13:10:26 | _backup/quotes_stock_basis_drop_20261112 | 4931→4931 | 无 |
| sh/603279.SH.parquet | 2026-11-12 16:27:06 | 来源未定 | —→— | — |
| sh/603309.SH.parquet | 2026-11-12 16:27:06 | 来源未定 | —→— | — |
| sh/603596.SH.parquet | 2026-11-12 16:27:07 | 来源未定 | —→— | — |
| sh/605033.SH.parquet | 2026-11-12 16:27:08 | 来源未定 | —→— | — |
| sh/605338.SH.parquet | 2026-11-12 16:27:08 | 来源未定 | —→— | — |
| sz/000001.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 8511→8511 | 无 |
| sz/000002.SZ.parquet | 2026-11-12 20:28:31 | _backup/quotes_stock_sz_1992_shift_20261113 | 8515→8515 | 无 |
| sz/000006.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 8194→8192 | 无 |
| sz/000007.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 7690→7688 | 无 |
| sz/000008.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 7996→7994 | 无 |
| sz/000011.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 8200→8198 | 无 |
| sz/000012.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 8338→8336 | 无 |
| sz/000014.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 8213→8211 | 无 |
| sz/000016.SZ.parquet | 2026-11-12 20:39:43 | _backup/quotes_sz_1993b_20261113 | 8191→8189 | hfq_factor(1838) |
| sz/000018.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 6473→6471 | 无 |
| sz/000019.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 7900→7898 | 无 |
| sz/000020.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 7865→7863 | 无 |
| sz/000024.SZ.parquet | 2026-11-12 16:26:41 | _backup/factor_fix_20261111 | 5211→5211 | 无 |
| sz/000033.SZ.parquet | 2026-11-12 16:26:41 | 来源未定 | —→— | — |
| sz/000409.SZ.parquet | 2026-11-11 21:46:06 | _backup/quotes_stock_20261111 | 6582→6582 | hfq_factor(1901) |
| sz/000501.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 8091→8089 | 无 |
| sz/000502.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 6734→6732 | 无 |
| sz/000503.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 7910→7908 | 无 |
| sz/000504.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 7633→7631 | 无 |
| sz/000506.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 7209→7207 | 无 |
| sz/000507.SZ.parquet | 2026-11-12 20:28:05 | _backup/quotes_sz_1993b_20261113 | 8061→8059 | 无 |
| sz/000511.SZ.parquet | 2026-11-12 16:26:42 | 来源未定 | —→— | — |
| sz/000550.SZ.parquet | 2026-11-11 21:46:06 | _backup/quotes_stock_20261111 | 7910→7910 | hfq_factor(1905) |
| sz/000562.SZ.parquet | 2026-11-12 16:26:42 | _backup/factor_fix_20261111 | 4777→4777 | 无 |
| sz/000594.SZ.parquet | 2026-11-12 16:26:42 | 来源未定 | —→— | — |
| sz/000686.SZ.parquet | 2026-11-12 16:26:43 | 来源未定 | —→— | — |
| sz/000693.SZ.parquet | 2026-11-12 16:26:43 | 来源未定 | —→— | — |
| sz/000725.SZ.parquet | 2026-11-12 13:10:26 | _backup/quotes_stock_basis_drop_20261112 | 6134→6134 | 无 |
| sz/000777.SZ.parquet | 2026-11-12 16:26:43 | 来源未定 | —→— | — |
| sz/000916.SZ.parquet | 2026-11-12 16:26:44 | _backup/factor_fix_20261111 | 4084→4084 | 无 |
| sz/000953.SZ.parquet | 2026-11-12 16:26:44 | _backup/quotes_adj_recalc_20261110_002529 | 6156→6158 | hfq_factor(1888) |
| sz/000979.SZ.parquet | 2026-11-12 16:26:44 | 来源未定 | —→— | — |
| sz/002070.SZ.parquet | 2026-11-12 16:26:45 | 来源未定 | —→— | — |
| sz/002152.SZ.parquet | 2026-11-12 16:26:46 | 来源未定 | —→— | — |
| sz/002275.SZ.parquet | 2026-11-12 16:26:47 | 来源未定 | —→— | — |
| sz/002326.SZ.parquet | 2026-11-11 21:46:06 | _backup/quotes_stock_20261111 | 3979→3979 | change(1); hfq_factor(1900); pct_chg(1); pre_close(1); qfq_factor(182) |
| sz/002352.SZ.parquet | 2026-11-11 21:46:06 | _backup/quotes_stock_20261111 | 4011→4011 | change(2); hfq_factor(925); pct_chg(2); pre_close(2); qfq_factor(1414) |
| sz/002864.SZ.parquet | 2026-11-12 16:26:50 | 来源未定 | —→— | — |
| sz/300186.SZ.parquet | 2026-11-12 16:26:52 | _backup/factor_fix_20261111 | 1030→1030 | 无 |
| sz/300259.SZ.parquet | 2026-11-12 16:26:52 | 来源未定 | —→— | — |
| sz/300372.SZ.parquet | 2026-11-12 16:26:53 | 来源未定 | —→— | — |
| sz/300741.SZ.parquet | 2026-11-12 16:26:55 | 来源未定 | —→— | — |
| sz/301206.SZ.parquet | 2026-11-12 16:26:57 | 来源未定 | —→— | — |
| sz/301215.SZ.parquet | 2026-11-12 16:26:57 | 来源未定 | —→— | — |
| sz/301300.SZ.parquet | 2026-11-12 16:26:57 | 来源未定 | —→— | — |
| sz/301315.SZ.parquet | 2026-11-12 16:26:57 | 来源未定 | —→— | — |
| sz/301393.SZ.parquet | 2026-11-12 16:26:58 | 来源未定 | —→— | — |
| sz/301529.SZ.parquet | 2026-11-12 16:26:58 | 来源未定 | —→— | — |
| sz/302132.SZ.parquet | 2026-11-12 00:08:14 | _backup/quotes_stock_302132SZ_20261112 | 3840→3844 | 无 |

## 4. 结论

- 时间线只说明「输入在窗口内被动过」，不直接等于「候选数变化由它引起」；候选数变化必须在同一份数据快照上重跑后才能对账。
- 有旧版可比的分片，逐列出了 2019-01-01 之后的逐值差异；无旧版的标「来源未定」，不得推断。
