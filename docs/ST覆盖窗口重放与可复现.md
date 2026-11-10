# ST 覆盖窗口：口径、重放层与可复现重建

生成时间：2026-11-10　　关联：`reports/st_g1_conflicts_20261110/coverage_drift64.md`

## 一、为什么要有重放层

`meta/st/st_coverage.parquet` 的 `coverage_start` 决定读侧「哪段历史算已知非 ST」：
窗口外的日期按 unknown 处理（fail-closed），窗口内未被 ST 段覆盖的日期才算 non_st。
`coverage_drift64.md` 记了 64 行漂移（53 行 1990 兜底、7 行 2005-01-01 占位、4 行快照前移）。

按「`cov_start` 取最早裁定 ST 段起点、无 ST 段则退回常规构建证据起点」修完这 64 行后，
问题变成：**正式表的内容无法由 `build_store` 复现**。`build_store` 对深市仍按上市日/
行情首行起算、对沪市/北交所按 `max(上市日, 2005-01-01)` 起算，任何人重跑一次，
这 64 行就会静默退回旧口径，且没有任何校验会报警。

因此把人工裁定结论落成**固定输入 + 构建收口步骤**，而不是只手改一张表：

| 组成 | 位置 | 作用 |
|---|---|---|
| 裁定输入 | `meta/st/raw/reviewed_coverage.parquet` | 逐只给出 `coverage_start` / `coverage_end` / `source` / `evidence` |
| 收口步骤 | `st_history.apply_reviewed_coverage` | 在快照合并与证据下延之后套用，**只命中表内标的** |
| 开关 | `build_store(..., apply_coverage_replay=True)` | 默认开；关闭后行为与旧版完全一致 |
| 确定性时间戳 | `st_history._input_timestamp` | `fetched_at` 取输入缓存最新 mtime（截到秒），不再用 `datetime.now()` |

## 二、口径（不变的部分）

- 深市默认口径**没有改**：未列入 `reviewed_coverage` 的标的仍按上市日/行情首行起算。
- 沪市/北交所默认口径没有改：仍按 `max(上市日, 2005-01-01)` 起算，再按 `_extend_coverage_to_evidence`
  用最早 ST 段起点下延。
- 本轮 64 行里 50 行落在「最早裁定 ST 段起点」，14 行在该标的上没有任何 ST 段，
  退回常规构建证据起点（行情首行/上市日/最早名称变更）。全部 64 行都是窗口**收窄**，
  没有一行把 unknown 变成已知非 ST。

## 三、验收数字（2026-11-10）

| 项 | 值 |
|---|---|
| 重建行数 | 5,888（含覆盖层补齐的 920305.BJ） |
| 空 `coverage_start` / 空 `coverage_end` / 起点晚于终点 | 0 / 0 / 0 |
| 连跑两遍 SHA256 | `105c9904810c17a6add32f76563bc9fc3845c1ce081adc5e139b94af912fc9e2`（两遍一致） |
| 正式表 SHA256 | 同上（`== rebuild_a == rebuild_b`） |
| 覆盖层命中 | 64 行（`coverage_replay_applied=64`，`appended=1`） |
| 64 行结论与上一轮一致 | 64/64 命中，0 处变动 |
| `st_intervals.parquet` | 未写入（SHA `83bc6aea…b8e9fca` 不变） |
| 逐值对拍（vs 上一轮备份） | 起点仅这 64 行不同；3,461 行 `coverage_end` 由 2026-11-08 刷新为 2026-11-10 |

复现命令（同输入 + 同 `as_of` 逐字节一致）：

```bash
.venv/Scripts/python.exe -c "from datetime import date; from src.data import st_history as s; \
s.build_store('data/local_store', output_dir=r'E:/www/tmp/rebuild', as_of=date(2026,11,10))"
```

## 四、上一轮修复里的一个缺陷（本轮一并修掉）

上一轮把 64 行写回正式表时，用了 `pl.coalesce([...when/then...])` 且**没有兜底分支**，
未命中的 5,824 行 `coverage_start` 被写成 null；同时「改动行数恰等于 64」的断言用
`!=` 比较，null 比较结果为 null 被过滤掉，断言因此空过。
null 起点在读侧按 `date.min` 处理（等于「上市至今全算已知非 ST」），属于放大确定性。

本轮重建已修复：正式表 5,888 行全部非空，起点不晚于终点；`reviewed_coverage` 只接管
这 64 行，其余 5,824 行与上一轮备份逐值一致（`coverage_end` 按 `as_of` 正常刷新）。

## 五、改动清单

| 文件 | 说明 |
|---|---|
| `src/data/st_history.py` | 新增 `reviewed_coverage_path` / `_input_timestamp` / `apply_reviewed_coverage` / `COVERAGE_REPLAY_NOTE`；`build_store` 增加 `as_of` 与 `apply_coverage_replay` 参数、`fetched_at` 改确定性时间戳、summary 增加重放统计 |
| `tests/test_st_coverage_replay.py` | 4 项：只命中裁定标的、可关闭、缺失标的补齐、同输入逐字节一致 |
| `data/local_store/meta/st/raw/reviewed_coverage.parquet` | 64 行裁定输入（数据侧，gitignore） |
| `data/local_store/meta/st/st_coverage.parquet` | 重建落盘（备份 `_backup/st_cov_replay_20261110/`） |

## 六、解锁条件与注意事项

- 新增裁定一行：往 `raw/reviewed_coverage.parquet` 追加 `ts_code/coverage_start/coverage_end/source/evidence/decided_at`，
  重跑 `build_store` 即生效；**不要只手改 `st_coverage.parquet`**。
- `coverage_end` 由 `as_of` 决定，跨日重建会变；对拍时必须固定 `as_of`。
- 覆盖层只解决「窗口起点」，不解决 ST 段本身；段仍以 `st_intervals.parquet` 为唯一来源。
