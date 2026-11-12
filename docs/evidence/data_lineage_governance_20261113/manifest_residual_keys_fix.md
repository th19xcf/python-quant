# manifest 残留键清理 + 两个 _backup 快照按 DG-13 归位（2026-11-13）

## 0. 结论

- 三份 manifest 里「仍按旧正式路径登记已迁出文件」的键已逐个摘掉：`meta/anomalies` 摘 2 个文件项、`meta/multi_asset` 摘 `summary.json`、`meta/universe/layers` 摘顶层 `st_snapshot_vs_interval`。
- `tools/reconcile_p2_metadata.py` 对 `placeholder_codes.parquet` 的写盘入口已删除，`--commit` 现在只写 `factors/fund_meta.json`；`src/data/manifest_semantics.py` 的三处描述同步改。
- 两个 `_backup/anomalies_*_20261112/` 目录按 DG-13 移到 `_backup/meta/` 下，移动前后 3 个文件 SHA256 逐一相等。
- `manifest_tool.py --check` 三个目录：**PASS / 文件记录 26 条 / 不一致 0 / 无法定位 0**（改前分别是 3 条 `file_missing`：`no_ex_rights_events.csv`、`placeholder_codes.parquet`、`summary.json`）。
- `no_ex_rights_events.csv` 已在 `docs/evidence/` 且被 Git 跟踪，**未**另做 `_backup` 快照（见 §6）。

## 1. 改前实测现状（与任务给的清单逐项对表）

| 核查点 | 实测结果 | 判定 |
|---|---|---|
| `data/local_store/meta/anomalies/manifest.json` 的 `files` 段 | 7 项，其中 `no_ex_rights_events.csv`（1,945 B / 7 行）、`placeholder_codes.parquet`（3,331 B / 83 行）两项对应的文件在目录内**不存在** | 残留键 2 个，确认 |
| `data/local_store/meta/multi_asset/manifest.json` 的 `files` 段 | 5 项，含 `summary.json`（`rows=71` / `sha256=bb110e5c…`），目录内**无该文件**；`dataset` 文本仍写「统计 summary.json」 | 残留键 1 个，确认 |
| `data/local_store/meta/universe/layers/manifest.json` | 顶层有 `st_snapshot_vs_interval`（18 个键的统计块，内含 `snapshot_file: st_status_20261110.parquet`），目录内**无该 json**；`dataset` 文本仍写「与 st_snapshot_vs_interval.json」 | 残留键 1 个，确认 |
| `tools/reconcile_p2_metadata.py` | L180-182：`anomaly_dir.mkdir(...)` + `write_parquet_atomic(placeholders, anomaly_dir/'placeholder_codes.parquet')` | 真写盘，确认 |
| `src/data/manifest_semantics.py` | L15 把 `placeholder_codes.parquet` 当正式台账写进 `dataset`；L14 `authority` 引「L69（占位代码清单）」 | 描述仍按旧正式产物，确认 |

四个文件的实际落点（逐个实测，非照抄）：

| 已迁出文件 | 实际落点 | 字节 / SHA256 | Git 跟踪 |
|---|---|---|---|
| `no_ex_rights_events.csv` | `docs/evidence/no_ex_rights_events/no_ex_rights_events.csv` | 1,945 B / `4ac34477b74d7f6d67e9d4eeaa9d9f34021be25c40e1f5ce8610e4815fc93e2f` | 是（`git ls-files` 命中） |
| `placeholder_codes.parquet` | `_backup/anomalies_placeholder_codes_20261112/`（本次已按 DG-13 移到 `_backup/meta/…`） | 3,331 B / `4f2fdf7ddab4a1188106d7d80291587041bac98ac76c5c93005f01c8aa3af7ca` | 否（`_backup/` 不入库） |
| `summary.json` | `docs/evidence/summary/summary.json` | 1,777 B / `bb110e5c17e7f99c59c8ec92073d9a8bbb4a8b62cf413c6c93072a28fa5d7ec9` | 是 |
| `st_snapshot_vs_interval.json` | `docs/evidence/st_snapshot_vs_interval/st_snapshot_vs_interval.json` | 3,315 B / `d9f3a3ae16053858cdfc2420fc0fb29980089f42dfde5cfeed6b812701dfba2e` | 是 |

改前 `manifest_tool.py --check --only meta/anomalies --only meta/multi_asset --only meta/universe` 的结论是 3 条 `file_missing`（上表前三个文件），与任务描述一致。

## 2. 改前快照与 SHA256（copy2）

快照目录：`_backup/meta/manifest_residual_20261113/`

| manifest | 快照文件 | 改前字节 | 改前 SHA256 |
|---|---|---|---|
| `meta/anomalies/manifest.json` | `manifest__anomalies.json` | 14,374 | `0e565a17719a7b0beda87ae26152ff9d8eb6155343ea23e2d1a4e49d6ee3b6cd` |
| `meta/multi_asset/manifest.json` | `manifest__multi_asset.json` | 3,368 | `7459e679f723c98d77316beade72c69570c303ebd88a6b0fb59ce1b9fd648baf` |
| `meta/universe/layers/manifest.json` | `manifest__layers.json` | 25,944 | `e7e0a18f138606417f368607e5f3119210952501d3d9015a20bbefdb65e99ffa` |

每份快照写入后立即回读校验 `copy2` 前后 SHA256 相等（脚本内断言，三份全等）。

改后（写入走 `src/data/atomic_write.write_text_atomic`，DG-9）：

| manifest | 改后字节 | 改后 SHA256 | 结构变化 |
|---|---|---|---|
| `meta/anomalies/manifest.json` | 11,479 | `3379e38a996b3392514051cadcc96d342681a91e19454e52a6daeb145a9ffe09` | `files` 7→5；`authority` / `dataset` / `source` / `semantics` 四键改文；新增 `retired_files` |
| `meta/multi_asset/manifest.json` | 3,252 | `66e7970145485f090219bc432596a8242b00ab00a048f8d74db783448fe8d51e` | `files` 5→4（摘 `summary.json`）；`dataset` 改文；新增 `retired_files` |
| `meta/universe/layers/manifest.json` | 19,063 | `530ce3bce2dc8ebef7c37fda6ca7c2ac35c006d7088e20bb07d66b31b177fedc` | 顶层摘 `st_snapshot_vs_interval`；`dataset` 改文；新增 `retired_files` |

改动脚本：`2026-11-12/data-lineage/work/apply_manifest_residual_fix.py`（可复算）。

逐键比对新旧（脚本内 `walk` 递归比对）确认：除上述摘除项与四处/两处/一处文本外，**其余键值逐值未变**；`files` 内保留项顺序不变（列表推导保序）。`retired_files` 是新增的**纯字符串说明数组**，`manifest_tool.collect_records` 只把「值含 `/` 或 `\` 的字符串」当路径且只认 `path` / `source_parquet` 字段，该数组不参与登记与核对（改后 `--check` 记录数仍为 5 / 4 / 6，未新增记录）。

## 3. 代码侧处置

| 文件 | 改动 | 依据 |
|---|---|---|
| `tools/reconcile_p2_metadata.py` | 删除 `anomaly_dir` 两行与 `write_parquet_atomic(placeholders, …/placeholder_codes.parquet)`；`write_parquet_atomic` 从 import 中移除；docstring 第 2/3 条与 `--commit` 帮助改为「只进报告 / 只写 fund_meta.json」；提交分支留 3 行注释说明退役与去向 | 该 parquet 零读侧、已按 DG-32 判为不该入正式层、已按 DG-13 移入 `_backup/meta/` |
| `src/data/manifest_semantics.py` | `meta/anomalies` 的 `authority`（L69→L92）、`dataset`（`placeholder_codes.parquet`→`placeholder_quote_codes.csv`）、`source`、`semantics`；`meta/multi_asset` 与 `meta/universe/layers` 的 `dataset` 各删「summary.json」「st_snapshot_vs_interval.json」 | 语义键的单一文本源，须与磁盘事实一致，否则下次重建 manifest 会把旧描述写回 |
| `tools/build_universe_layers.py` | `_manifest` 里删除 `'st_snapshot_vs_interval': snapshot,`，原位置留 2 行注释指向 `docs/evidence/` | 该脚本是 layers manifest 的唯一生产方；不删则下次 `--apply` 重建会把刚摘掉的键写回，属同类枚举残留 |
| `tools/anomalies/build_no_ex_rights_events.py` | 模块 docstring 第 1 行的产物路径 `data/local_store/meta/anomalies/…` 改为 `docs/evidence/no_ex_rights_events/…` | 同类残留：常量 `LEDGER`（L55）早已指向 `docs/evidence/`，只有注释还宣称写旧正式路径 |
| `tests/test_tools_changelog_wiring.py` | 在既有 `coverage_gaps.parquet` 退役断言后，加同形态断言：`--commit` 不得在台账里出现 `meta/anomalies/placeholder_codes.parquet` | 与既有退役表防复活用例同构，守护本次删除的写盘入口 |

`tools/reconcile_p2_metadata.py` 内同类枚举核查结果：`_coverage_gaps()` 的计算保留（只进 `report['coverage_gaps']`），但 `coverage_gaps.parquet` 的写盘早已在 2026-11-12 移除（本次复核确认 L180-186 只剩 fund_meta 一条写入），故无需再动；`placeholders` DataFrame 仍被报告使用（`placeholder_funds` / `placeholder_sample`），未删计算。

## 4. DG-13 归位（移动，不是删除）

| 原路径 | 新路径 | 文件 | 字节 | SHA256（移动前 = 移动后） |
|---|---|---|---|---|
| `_backup/anomalies_placeholder_codes_20261112/` | `_backup/meta/anomalies_placeholder_codes_20261112/` | `placeholder_codes.parquet` | 3,331 | `4f2fdf7ddab4a1188106d7d80291587041bac98ac76c5c93005f01c8aa3af7ca` |
| `_backup/anomalies_coverage_gaps_20261112/` | `_backup/meta/anomalies_coverage_gaps_20261112/` | `coverage_gaps.parquet` | 5,336 | `2ef245b60089d1c7863aa69c0b185a9a63db11c560e2b849ba267475b0a73ef6` |
| 同上 | 同上 | `coverage_gaps_revived_20261112T2234.parquet` | 4,188 | `6fc9a659ae2dfa5b4073b81739aa732f5369a3f91ff4db796358b29c0e2a0bfd` |

证明方式：`shutil.move` 前对源目录逐个文件算 `(字节, SHA256)`，移动后对新目录同样计算，两组字典**完全相等**（`sha_equal: true`），且源路径 `exists()` 均为 `False`。移动前已断言目标目录不存在，未覆盖任何既有快照。

## 5. 验证

### 5.1 manifest 核对（对应目录）

```
.venv\Scripts\python.exe tools\manifest_tool.py --check --only meta/anomalies --only meta/multi_asset --only meta/universe
```

```
manifest 份数 : 6    文件记录 : 26（无法定位 0）
不一致条目 : 0       sha256 格式告警 : 0
无 manifest 的 parquet : 0    有 manifest 但未定位 : 0
  meta/anomalies/manifest.json              记录 5   一致 1  不一致 0  无法定位 0  不可核对 4
  meta/multi_asset/etf_fee.manifest.json    记录 4   一致 4  不一致 0  无法定位 0  不可核对 0
  meta/multi_asset/futures_manifest.json    记录 3   一致 3  不一致 0  无法定位 0  不可核对 0
  meta/multi_asset/manifest.json            记录 4   一致 1  不一致 0  无法定位 0  不可核对 3
  meta/universe/layers/manifest.json        记录 6   一致 6  不一致 0  无法定位 0  不可核对 0
  meta/universe/manifest.json               记录 4   一致 4  不一致 0  无法定位 0  不可核对 0
结论: PASS
```

`--only meta/multi_asset` 与 `--only meta/universe` 会连带匹配其子 manifest，故实跑 6 份而不是 3 份；6 份全部 0 不一致，其中受影响的三份均为 `无法定位 0`（改前这三份各有 1~2 条 `file_missing`）。「不可核对 4 / 3」是 CSV 类记录（无 parquet 行数可比），与改前同类，不是新增缺口。

### 5.2 全仓路径字面量逐个定性

`placeholder_codes.parquet`（运行期代码命中 = **0 写盘、0 读盘**）：

| 命中位置 | 性质 |
|---|---|
| `tools/reconcile_p2_metadata.py:181` | 注释（本次新增，说明已退役与去向） |
| `tests/test_tools_changelog_wiring.py:73-77` | 用例断言（禁止复活） |
| `docs/evidence/derived_1993b_20261113/验收报告.md:65` | 文档（11-13 验收报告叙述「跳过 anomaly 刷新」） |
| `docs/数据可用性审计_行情_2026-11-09.md:186` | 文档（09 旧建议，已过期，未改） |
| `docs/数据权威路径清单.md:71` | 文档（正式登记行，待文档负责人改，见 §7） |
| `docs/数据权威路径清单.md:92` | 文档（«占位行情代码清单» 行里的区分说明，待改） |
| `docs/数据资产说明_2026-11-12.md:151` | 文档（仍按 84 行旧版本登记，待改） |
| `2026-11-12/**`（audit / inventory / retired-table-conflict 等） | 历史审计工作脚本与产物（一次性，非运行期），其中 `retired-table-conflict/work/reconcile_head.py:182` 是旧版脚本的副本 |

`no_ex_rights_events`（仅 1 个运行期文件命中，无写正式层）：

| 命中位置 | 性质 |
|---|---|
| `tools/anomalies/build_no_ex_rights_events.py:1` | docstring（本次已改为 `docs/evidence/`） |
| `…:39,40` | docstring 用法示例 |
| `…:55` | 常量 `LEDGER = docs/evidence/no_ex_rights_events/…`（真写盘，落 `docs/evidence/`，Git 跟踪） |

`st_snapshot_vs_interval`：

| 命中位置 | 性质 |
|---|---|
| `tools/build_universe_layers.py:22-23` | docstring（早已写明落 `docs/evidence/`） |
| `…:447` | 注释（本次新增） |
| `…:497-500` | 真写盘 → `docs/evidence/st_snapshot_vs_interval/` |
| `…:631` | 真写盘 → `_staging/` 中间件 |

`summary.json`（multi_asset 相关）：`tools/build_multi_asset_etf_pool.py:10` docstring、`:548` 写 `_staging/`、`:570` 写 `docs/evidence/summary/summary.json`；**无**写 `meta/multi_asset/` 的入口（生产者早在 2026-11-12 已排除该文件，manifest 是没跟上）。

### 5.3 静态检查与单测

- `ruff check` 四个改动 python 文件：`build_universe_layers.py` 报 6 项（UP017 ×2、B905 ×3、C416 ×1），全部位于 L108/167/314/421/478/608，**与本次改动无关**；用 `git show HEAD:tools/build_universe_layers.py | ruff check --stdin-filename …` 复核，改前同样是这 6 项 → 既有告警，未新增。其余三个文件 0 告警。
- `pytest tests/test_tools_changelog_wiring.py tests/test_manifest_coverage_gaps.py tests/test_manifest_sha_case.py`：**11 passed / 0 failed**。
- `py_compile`：四个改动文件 + 本次作业脚本全部通过。

## 6. 明确不做的事

- **不给 `no_ex_rights_events.csv` 补 `_backup` 快照**：它已在 `docs/evidence/no_ex_rights_events/` 且被 Git 跟踪，Git 历史即第二份；再复制一份进 `_backup/` 只会变成 `_backup` 里的第三份同内容文件，违反 DG-12（`_backup` 只承担回滚快照职责）与「不留多份并存」。
- **不动 `docs/数据权威路径清单.md`**：四处需同步改的内容只列出，交该文档负责人处理（§7）。
- **不改 `docs/数据资产说明_2026-11-12.md` 与 09 年审计报告**：两份都是带日期的历史审计产物，改它等于改写当日结论；只在本报告照明。
- **不为 `2026-11-12/**` 下的历史一次性脚本补删**：它们不是运行期代码，删改会破坏审计可复算性。

## 7. 需由 `docs/数据权威路径清单.md` 负责人同步的四处

| 行 | 现状 | 建议改法 |
|---|---|---|
| L19（L3 风险剔除行） | 「快照版差异证据见同目录 `st_snapshot_vs_interval.json`」 | 改为 `docs/evidence/st_snapshot_vs_interval/st_snapshot_vs_interval.json`（已迁出，不在同目录） |
| L71（占位代码清单·审计汇总 parquet 行） | 仍作为正式路径登记，且消费者列写「仅产出方 `tools/reconcile_p2_metadata.py:182` 写入」 | 整行移入「已废弃路径」表：原路径 `meta/anomalies/placeholder_codes.parquet`（83 行 / 3,331 B）→ `_backup/meta/anomalies_placeholder_codes_20261112/`，SHA256 `4f2fdf7d…`，废弃原因「零读侧、写盘入口已删、DG-32 第①问不成立」 |
| L92（占位行情代码清单行） | 「**与 `meta/anomalies/placeholder_codes.parquet`（83 行、零读侧）分属两件**」 | 保留「分属两件」的区分，但把后者的路径改成 `_backup/meta/anomalies_placeholder_codes_20261112/placeholder_codes.parquet`（已退役） |
| L172（已废弃路径表·coverage_gaps 行） | 移入位置写 `_backup/anomalies_coverage_gaps_20261112/coverage_gaps.parquet` | 改为 `_backup/meta/anomalies_coverage_gaps_20261112/coverage_gaps.parquet`（DG-13 归位后路径变更，SHA256 不变 `2ef245b6…`） |

另：`docs/数据资产说明_2026-11-12.md:151`（84 行版本）与 `docs/数据可用性审计_行情_2026-11-09.md:186`（建议把清单扩到 117 只）同样引用了已退役 parquet，属历史文档，是否修订由文档负责人定。

## 8. 作业过程说明

- 期间两次出现残留 `.git/index.lock`（他路会话提交留下的锁文件），阻塞 `git` 只读命令；由运行环境自动移入回收站后继续，未手工删除任何仓库文件。本任务提交使用隔离索引，并在提交前后实时取 HEAD 基准。
- 工作区另有他路在途改动（`tools/derive_free_float_fill.py`、`tests/test_free_float_fill.py`、`tools/fill_free_float_shares.py`、`docs/evidence/*_probe_20261112/`），**未纳入本次提交**。

## 9. 备份同步（`G:\git\python-quant`）

同步范围按「备份同步规则」四类逐一落到备份仓，逐文件 `copy2` 后回读 SHA256 比对相等：

| 类别 | 文件 | SHA256（源 = 备） |
|---|---|---|
| ① Git 跟踪（本次提交） | `src/data/manifest_semantics.py` | `939d70e5d60879edb17c9ef30fb8cd824761668c7ac253d466539037c16187a8` |
| ① | `tests/test_tools_changelog_wiring.py` | `da4d4fd993286117e879c5e7e83a310572da7e146be9ae73efaf864039a833ca` |
| ① | `tools/anomalies/build_no_ex_rights_events.py` | `c5e240366d8e8954c6e9923e4282e4a96796634b8ecfd7a65ca46be6df22d162` |
| ① | `tools/build_universe_layers.py` | `a12d2cb103fa948522f300df106b008b148d1667a336b32db5fed8544437c143` |
| ① | `tools/reconcile_p2_metadata.py` | `06d407c3fafd696f34583fa4e1d4b5843879325d16707b3739d3e7ffff6ca5d1` |
| ② `data/local_store/**` | `meta/anomalies/manifest.json` | `3379e38a…a9ffe09` |
| ② | `meta/multi_asset/manifest.json` | `66e79701…48fe8d51e` |
| ② | `meta/universe/layers/manifest.json` | `530ce3bc…1b177fedc` |
| ③ `_backup/**` | `meta/anomalies_placeholder_codes_20261112/placeholder_codes.parquet` | `4f2fdf7d…aa3af7ca` |
| ③ | `meta/anomalies_coverage_gaps_20261112/`（2 个 parquet） | `2ef245b6…` / `6fc9a659…` |
| ③ | `meta/manifest_residual_20261113/`（3 份改前快照） | 与 §2 改前 SHA256 一致 |
| ④ 任务交付目录 | `2026-11-12/data-lineage/outputs/manifest_residual_keys_fix.md`、`work/apply_manifest_residual_fix.py`、`work/commit_msg_manifest_residual.txt` | 同步后回读一致 |

备份侧旧路径 `_backup/anomalies_placeholder_codes_20261112/` 与 `_backup/anomalies_coverage_gaps_20261112/` 在**逐文件核对新位置同内容 SHA256 之后**对位删除并移除空目录，属「源端已移动 → 备份侧对位删除」，不是备份侧单方面删数据；备份仓 `.git` 未触碰，未 `rmtree` 任何目录。

备份仓提交 `5e2ea39`（5 个跟踪文件，`git rev-parse HEAD:<path>` 与源仓 blob 逐一相等），推送 `56425dd..5e2ea39 main -> main`；本报告与两个作业脚本在备份仓只镜像不入库（`2026-11-12/` 整体忽略）。
