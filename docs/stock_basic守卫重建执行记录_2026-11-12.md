# stock_basic 守卫重建执行记录（2026-11-12）

> 范围：仅 `data/local_store/meta/stock_basic.parquet` 与其 manifest，外加构建器守卫与单测。
> 未改 `src/`、未提交 Git。复现与证据：`_staging/stock_basic_manifest_20261112/`。

## 0. 一句话结果

三道守卫全部落地并通过单测；干跑与真实重建的影响面**恰好是 4 个 name + 1 个 delist_reason**；
行数 5,916 不变，5 个单元格被修改，空值计数逐列不变；manifest 已同步重算并追加重建事件。

## 1. 三道守卫的实现位置

均在 [build_stock_basic.py](file:///e:/code/python/quant/tools/build_stock_basic.py) 内，仅在合并阶段生效（不改候选推导逻辑）：

| 守卫 | 实现 | 行为 |
|---|---|---|
| 一：截断名不覆盖 | [`name_guard_reason`](file:///e:/code/python/quant/tools/build_stock_basic.py#L138-L158) | 候选名更短且是现有名前缀 → 保留现有值，`action=guard_keep_existing`，原因写明「判为截断」 |
| 二：XD/DR/N 前缀与异常名 | [`clean_name`](file:///e:/code/python/quant/tools/build_stock_basic.py#L127-L135) + `name_guard_reason` | `MARKET_PREFIXES=('XD','XR','DR','N','C')` 剥离；清洗后为空 / 只剩标记 / 长度 < 2 → 不覆盖、记跳过 |
| 三：官方摘牌日不前移 | [`pins_official_delist`](file:///e:/code/python/quant/tools/build_stock_basic.py#L166-L174) | 现有 `delist_date` 来自 `official_delist` 且晚于候选 `quote_last_bar` → `list_date_basis`/`delist_date`/`delist_date_basis`/`date_precision` 4 列全部保持原值，只记「末根 bar 更早」 |

接入点：[`merge_with_existing`](file:///e:/code/python/quant/tools/build_stock_basic.py#L542-L641) 新增 `guards=True` 形参；
[`_merge_into_existing`](file:///e:/code/python/quant/tools/build_stock_basic.py#L644-L667) 透传；
`--no-guards` 仅作诊断开关（默认关闭守卫供反证单测使用）。

**未改 `names.json` 数据本身**：它是通达信快照这一登记来源，4 字截断无法从该源内补全全名，
因此由守卫一阻止其覆盖正式表；前缀问题由 `clean_name` 处理。改源会破坏「今天是叫什么」的来源语义。

## 2. 干跑（写盘前）

命令：`.venv\Scripts\python.exe tools\build_stock_basic.py --no-write`
日志：`_staging/stock_basic_manifest_20261112/dry_run.log`

会被改写的单元格 **5 个**：

| ts_code | 字段 | 旧值 | 新值 |
|---|---|---|---|
| 001381.SZ | name | 皇冠 | 皇冠新材 |
| 600251.SH | name | 冠农股份 | 新疆冠农 |
| 603112.SH | name | 华翔股 | 华翔股份 |
| 603680.SH | name | 今创集 | 今创集团 |
| 601268.SH | delist_reason | 退市 | 旧码迁移 |

被守卫拦下 **18 个**：13 只日期链（`delist_date`）+ 5 只截断名（601801/601811/601869/603309/605033）。
与用户给定的预期完全一致，未超范围。

## 3. 真实重建

| 项 | 值 |
|---|---|
| 候选 | `_staging/stock_basic_20261112_071351/stock_basic.parquet` |
| 改前快照 | `_backup/meta/stock_basic_rebuild_20261112_071355/stock_basic.parquet` |
| 改前 SHA256 | `d3d90abd27628b8b43131e2f661d3a4d7a3fa01e761b51e770ebbc8e0071b9fe` |
| 改后 SHA256 | `048335ccd9d81da7880dcd170d851e8b522eabb4d6bf5242019c7efdd31717b8` |
| 写入方式 | `src.data.atomic_write.write_parquet_atomic`（临时文件 → fsync → `os.replace`） |
| 闸门 | `promote_rebuild.py` 先断言改动集合恰为预期 5 格、行数与代码集不变、空值计数不变，任一不符即不写盘 |

## 4. DG-20 四项验收（`verify_evidence.json`）

| 项 | 实测 |
|---|---|
| 行数守恒 | 5,916 → 5,916；代码集合完全一致 |
| 逐值对拍 | 改动单元格 **5** 个：`name` 4、`delist_reason` 1；其余 11 列改动数为 0；既有非空值改动数 = 5（与预期一致），无 null→值、无值→null |
| 空值计数 | 13 列改前改后全部相同（`name` 2、`delist_date`/`delist_reason`/`delist_date_basis` 各 5,580、其余 0） |
| 受影响文件 SHA256 | parquet 前 `d3d90abd27628b8b43131e2f661d3a4d7a3fa01e761b51e770ebbc8e0071b9fe` → 后 `048335ccd9d81da7880dcd170d851e8b522eabb4d6bf5242019c7efdd31717b8`；manifest `86b6c75bbd7b67c6e329e9e319921b6d05c30136a6c437a84397dfb43fd687e9` |
| 抽样断言 | 5 行新旧逐字段对照写入 `verify_evidence.json` 的 `samples` |

下游真实读路径：`expectation.listing_windows()` 返回 5,916 个窗口；`601268.SH` 退市日仍为官方
`2015-05-21`（守卫三生效），`688827.SH` 为 `2026-11-10 / None`。

## 5. manifest 同步

- 重算后 `source_parquet_sha256` = `048335ccd9d81da7880dcd170d851e8b522eabb4d6bf5242019c7efdd31717b8`，行数仍 5,916。
- `row_count_history.events` 现为 4 条：`build`（旧 manifest 5,914）→ `increment`（补 688827.SH，5,915→5,916，manifest 未同步）
  → `manifest_rebuild`（只重算 manifest，parquet 未动）→ **`table_rebuild`**（本次守卫重建，写明「行数不变，非增量补行」，
  并记录改前/改后 parquet SHA256）。
- `previous_manifest` 指向本轮第一版 v2 manifest（SHA `41a8a5d7…a3e0`）；`unresolved` 仍保留 688827.SH 名称冲突。

## 6. 断言能失败（反证单测）

`tests/test_stock_basic_guards.py`，7 条全绿：

- `test_truncated_name_keeps_existing`：候选 `皖新传` 不得覆盖 `皖新传媒` —— **守卫一失效即红**。
- `test_truncated_name_would_override_without_guard`：`guards=False` 时确实覆盖成功，证明上条不是恒真。
- `test_official_delist_not_moved_earlier`：官方 `2019-05-23` 不得前移到 `2018-04-27` —— 守卫三失效即红。
- `test_clean_name_strips_xd_dr_n`：XD / DR / N 剥离；仅剩标记时判异常。
- `test_longer_name_overrides` / `test_same_length_rename_overrides`：守卫不误伤 4 处真修正。
- `test_guard_does_not_pin_quoted_delist`：守卫三不误伤 bar 口径的退市日更新。

运行：`.venv\Scripts\python.exe -m pytest tests/test_stock_basic_guards.py -q`（7 passed）。

## 7. 复现命令

```powershell
# 干跑（列出将被改写 / 被守卫拦下的单元格）
.venv\Scripts\python.exe tools\build_stock_basic.py --no-write
# 生成候选
.venv\Scripts\python.exe tools\build_stock_basic.py
# 闸门校验 + 快照 + 原子替换
.venv\Scripts\python.exe _staging\stock_basic_manifest_20261112\promote_rebuild.py
# DG-20 四项验收
.venv\Scripts\python.exe _staging\stock_basic_manifest_20261112\verify_rebuild.py
# manifest 重算（已执行，含 table_rebuild 事件）
.venv\Scripts\python.exe _staging\stock_basic_manifest_20261112\rebuild_manifest.py --write `
  --event-kind table_rebuild --event-note "守卫重建：名称 4 处修正，601268.SH 改为旧码迁移；行数不变"
```

## 8. 回滚

1. `copy2 _backup/meta/stock_basic_rebuild_20261112_071355/stock_basic.parquet → data/local_store/meta/stock_basic.parquet`
2. 用 `_backup/meta/stock_basic_manifest_20261112_070513/stock_basic.manifest.json` 覆盖 manifest（回到 5,916 行、改前 SHA 的 v1 状态）；
   或以当前 manifest 为基础删掉最后一条 `table_rebuild` 事件后重写。
