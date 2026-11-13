# G-067 第二段：provisional 状态说明（2026-11-12 20:45）

## 1. 状态：provisional，不作结论

本次跑批（2019–2023 五片）发生在另一条「派生层重建」任务整体搬移
`data/local_store/factors/` 与改写 `quotes/stock/**` 的窗口内，实测读到半搬移状态，
因此**全部标记 provisional**：文件与断点保留留档，不作为 G-067 的对账结论。

- run 目录：`reports/advice/stateful/20190131_20260803_g067rechunk/`
- 断点：`_staging/g067_rechunk_20261112/progress.csv`、`failed.csv`
- 逐片日志：`_staging/g067_rechunk_20261112/logs/chunk_<label>.log`

## 2. provisional 已产出的分片

| 片 | 候选数（provisional） | 基线 h20 | 差 | 跑批耗时 |
|---|---:|---:|---:|---:|
| 2019 | 83 | 83 | 0 | 100.4s |
| 2020 | 23 | 22 | +1 | 97.5s |
| 2021 | 19 | 19 | 0 | 125.8s |
| 2022 | 70 | 71 | −1 | 102.4s |
| 2023 | 72 | 71 | +1 | 108.6s |
| 2024 | — | 202 | — | 20:39:00 失败 |
| 2025 | 未跑 | 189 | — | — |
| 2026 | 未跑 | 74 | — | — |

2024 失败原因：`FileNotFoundError: data/local_store/factors/stock_adj_factor.parquet`
（20:39:00 读到的瞬间该文件不存在，20:39:19 再次存在；20:39:43 该文件与
`meta/daily_index.parquet`、`factors/meta.json`、`factors/manifest.json` 又被整体重写）。

## 3. 已排除的因素（旧/新逐项相同）

| 项 | 2019 | 2020 | 2021 | 2022 | 2023 |
|---|---|---|---|---|---|
| `dates` 交易日列表 | 223=223 | 243=243 | 243=243 | 242=242 | 242=242 |
| `panel_dates` 面板日 | 667=667 | 684=684 | 682=682 | 682=682 | 682=682 |
| `universe.kept` | 2993=2993 | 同 | 同 | 同 | 同 |
| `universe.rule_counts` | 全同 | 全同 | 全同 | 全同 | 全同 |

结论：日历与选池都没变，候选数差异只能来自因子面板取值（复权因子 / 换手率 / 行情修正）。

## 4. 下一步（等主控「派生层已收口」信号后执行）

1. 全 8 片重跑到**新目录**（如 `20190131_20260803_g067rechunk2`），与 provisional 目录并存对照；
2. 重跑前后各取一次 `data/local_store` 全树指纹（`work/fingerprint.py`），必须零差异；
3. 重跑后同样复核 `dates` / `panel_dates` / `universe.kept` / `rule_counts` 四组新旧相同；
4. 再出三组对账（731 / 实际落盘 / 575 缺口）与逐码清单。
