# 实盘配置 · 样本外前向记录

自 2026-12 起只记录、不优化。冻结参数见 [spec_live_config_2026-11-22.json](spec_live_config_2026-11-22.json)；原低估值选股口径已于 2026-11-22 关闭，存档在 [frozen_spec_closed_2026-11-22.json](frozen_spec_closed_2026-11-22.json)（关闭依据：裁决提交 `b78f88a`）。规则见 `.trae/rules/research-discipline.md` RD-10。

## 记录对象

- **etf_base**：510300.SH / 511010.SH / 518880.SH，各 1/3；月末收盘决策，T+1 开盘调回等权；成本 16.5bp 往返（含冲击）。
- **dividend**：510880.SH 独立 sleeve，按月单独一行；仓位由 000015.SH PB 滚动 60 月分位决定（≤50% 满仓、>50% 半仓，空余现金年化 1.5%），不与底仓合并或重复计算。

## 字段

| 字段 | 含义 |
|---|---|
| `date` | 信号日（月末最后一个交易日） |
| `ts_code` | 标的代码（底仓三只 ETF 或 510880） |
| `weight` | 目标权重（底仓 1/3；红利 sleeve 为 0.5 或 1.0） |
| `actual_return` | 下月实际收益（T+1 开盘买入、持有一个月，扣 16.5bp 往返） |
| `benchmark_return` | 510300.SH 同窗口收益 |
| `diff` | `actual_return - benchmark_return` |
| `sleeve` | `etf_base` / `dividend`（2026-11-22 新增，旧六列语义不变） |

## 月度流程

1. 月末收盘后：向 `records.csv` 追加当期行（底仓 3 行 + 红利 1 行），收益三列留空。
2. 次月末：按 T+1 开盘口径回填 `actual_return` / `benchmark_return` / `diff`。
3. 只允许回填收益列；不得修改已记录的 `date` / `ts_code` / `weight` / `sleeve`，不得删除历史行。
4. 记录期内不得依据记录结果调参、换池或改频率。

脚本：`python tools/oos_forward_monthly.py --month YYYY-MM --mode auto|signal|settle`（`--dry-run` 只预览）。

## 解冻门槛

满足任一才允许解冻重估：

- 连续 12 个月 `diff` 月均值同号，且 Newey-West（lag=3）t 值 `|t| >= 2.0`（持续同向偏离且统计显著）；且累计记录不少于 24 个月。
- 发现数据或代码 bug：可修 bug 后重跑，但须保留旧记录并在 `README.md` 追加变更说明。

解冻后只允许重估一次；重估即重新冻结、前向记录清零重启。无上述证据时，冻结参数不得改动。
