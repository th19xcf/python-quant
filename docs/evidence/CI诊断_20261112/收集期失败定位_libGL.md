# CI「Test with coverage gate」退出码 2 —— 收集期失败定位（实测）

- 日期：2026-11-12
- 被测代码：源仓 HEAD `c1f462859f9967331454b0e96b263918212e95a1`（用 `git archive HEAD` 导出，等价于 CI 的 `actions/checkout` 内容，不含 `data/`、`.venv`）
- 环境：Windows 11 宿主上新建的 WSL2 发行版（`wsl --import` 导入），Ubuntu 24.04.4 LTS，内核 6.6.114.1-microsoft-standard-WSL2
- 结论级别：**实测复现**，不是静态推断

## 一、结论（一句话）

`pytest` 收集期失败的唯一根因是 **`libGL.so.1` 缺失**：`src/utils/exception_handler.py:15` 在模块级 `from PySide6.QtWidgets import QApplication, QMessageBox`，PySide6 加载 `libGL.so.1` 失败抛 `ImportError`，导致 **68 个测试模块** 全部 `ERROR collecting`，pytest 以 `Interrupted: 68 errors during collection` 结束，退出码 **2**。

CI 的 `Install Qt runtime libraries` 步只装了 `libegl1 libxkbcommon-x11-0 libdbus-1-3 libglib2.0-0`，其中没有任何一个提供 `libGL.so.1`（该 so 由 `libgl1` / `libglx-mesa0` 提供）。

## 二、报错原文（一次运行的完整首块）

```text
==================================== ERRORS ====================================
______________ ERROR collecting tests/test_advice_calibration.py _______________
ImportError while importing test module '/root/quant/tests/test_advice_calibration.py'.
Hint: make sure your test modules/packages have valid Python names.
Traceback:
/usr/lib/python3.14/importlib/__init__.py:88: in import_module
    return _bootstrap._gcd_import(name[level:], package, level)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
tests/test_advice_calibration.py:20: in <module>
    _SPEC.loader.exec_module(_MODULE)
scripts/calibrate_advice.py:30: in <module>
    from src.advice.config import (
src/advice/__init__.py:24: in <module>
    from src.advice.pipeline import AdviceEngine
src/advice/pipeline.py:32: in <module>
    from src.advice.ic_monitor import ic_gate_decision, monitor_note, run_ic_monitor
src/advice/ic_monitor.py:27: in <module>
    from src.advice.factor_eval import MIN_PAIRS, _rank_ic, ic_monitor
src/advice/factor_eval.py:35: in <module>
    from src.advice.fundamental_factors import FUNDAMENTAL_CANDIDATES
src/advice/fundamental_factors.py:31: in <module>
    from src.data.local_store.share_capital import read_shares
src/data/local_store/share_capital.py:103: in <module>
    from src.utils.exception_handler import handle_exception_with_retry
src/utils/exception_handler.py:15: in <module>
    from PySide6.QtWidgets import QApplication, QMessageBox
E   ImportError: libGL.so.1: cannot open shared object file: No such file or directory
```

汇总行：

```text
!!!!!!!!!!!!!!!!!!! Interrupted: 68 errors during collection !!!!!!!!!!!!!!!!!!!
2 skipped, 3 deselected, 2 warnings, 68 errors in 5.33s
```

- `E   ImportError: libGL.so.1: cannot open shared object file: No such file or directory` 在整份日志中出现 **68 次**，且是**唯一**一种 `E` 行（`grep '^E ' | sort -u` 只有这一条）。
- 失败模块入口各不相同（`test_advice_*` / `test_ui_*` / `test_local_store*` / `test_turnover_*` 等），但都在 `src/utils/exception_handler.py:15` 汇聚到同一条导入链。

## 三、A/B 实测（同一环境，唯一变量 = libgl1）

用 `ci.yml` 里**完全一致**的那条命令：`pytest --cov=src --cov-report=term-missing`
（`pyproject.toml` 的 `addopts = -q -m 'not perf'` 自动生效，与 CI 相同）

| 条件 | 命令 | 退出码 | 收集期结果 | 耗时 |
|---|---|---|---|---|
| 无 `libgl1`（CI 现状同构） | `pytest --cov=src --cov-report=term-missing` | **2** | `68 errors during collection` | 5.33 s |
| 有 `libgl1` | 同上 | 1 | 0 errors，正常执行完 | 65.67 s |

有 `libgl1` 那次的完整汇总（用于证明"收集期已过关"，其失败与本次定位无关）：

```text
8 failed, 2587 passed, 27 skipped, 3 deselected, 1 xfailed, 75 warnings, 4 errors in 65.67s
Required test coverage of 63.0% reached. Total coverage: 67.84%
```

那 8 failed / 4 errors 全部是本隔离环境与 CI 的**数据/服务差异**，不是收集期问题：
- `tests/test_data_manager.py`（4 errors + 1 failed）：`sqlalchemy.exc.OperationalError`，本环境没有 MySQL；
- `tests/test_volume_unit_repair.py`（6 failed）：需要 `data/local_store` 真实快照，而 CI checkout 与本环境都只有 Git 跟踪文件；
- `tests/test_ui_smoke.py`（1 failed）：离屏 Qt 渲染断言差异。

## 四、触发条件（复现三要素，缺一不可）

1. **Linux**：PySide6 在 Linux 上才会 dlopen `libGL.so.1`；Windows 上该库随 wheel 自带，所以本机 `.venv` 永远复现不出来。
2. **系统未装 `libgl1`**：`libegl1` 只提供 `libEGL.so.1`，不提供 `libGL.so.1`；`libglib2.0-0`/`libdbus-1-3`/`libxkbcommon-x11-0` 也都无关。
3. **有测试在模块级（收集期）走到 `src/utils/exception_handler.py`**：该文件在 import 时就 `from PySide6.QtWidgets import ...`，不是延迟导入，因此失败发生在收集期而非运行期——这正是退出码 2（`ExitCode.INTERRUPTED`）而不是 1 的原因。

## 五、环境构建记录（可复算）

宿主侧（全部只读仓库之外，`E:\www\tmp\ci_wsl\`）：

1. Docker / Podman / VBoxManage / qemu / multipass 均不存在；WSL 2.7.3.0 有内核与 `wslservice`，但**无已注册发行版**。
2. Microsoft Store 下载通道 `wsldownload.azureedge.net` **DNS 解析失败**，故 `wsl --install -d Ubuntu --no-launch` 卡在 `%LOCALAPPDATA%\Temp\Ubuntu.wsl` 417,923,072 B（398.6 MiB，16:56:03 后不再增长）——该路径已放弃。
3. 改用 `https://cdimage.ubuntu.com/ubuntu-base/releases/24.04/release/ubuntu-base-24.04.4-base-amd64.tar.gz`（29,989,394 B）→ `wsl --import UbuntuCI E:\www\tmp\ci_wsl\UbuntuCI <tar>` 成功，`wsl -l -v` 显示 `UbuntuCI Stopped 2`。

发行版内部：

```text
apt-get install -y ca-certificates curl gnupg software-properties-common \
                   libegl1 libxkbcommon-x11-0 libdbus-1-3 libglib2.0-0
add-apt-repository -y ppa:deadsnakes/ppa
apt-get install -y python3.14 python3.14-venv python3.14-dev      # → Python 3.14.10
python3.14 -m venv /root/venv
pip install poetry==2.5.1 poetry-plugin-export
poetry export --without-hashes --with dev,baostock -f requirements.txt -o /root/requirements-ci.txt
pip install -r /root/requirements-ci.txt                          # PIP_INSTALL_RC=0，pytest 9.2.0
```

- Qt 运行库**按 ci.yml 原样**安装（`libegl1 libxkbcommon-x11-0 libdbus-1-3 libglib2.0-0`），四种之外的 `libgl1` 在 A/B 的 B 组才补装。
- 组参数与 CI 一致：只装 `main + dev + baostock`，不装 `gpu/analytics/learning/tdx`。
- 唯一偏离：包源改为清华镜像（`pypi.org` 在本机 WSL 内只有 ~230 kB/s，200+ MB 依赖按原速需数小时）；版本全部由 `poetry.lock` 钉死，镜像只影响下载速度，不影响装到的版本。
- 环境变量 `QT_QPA_PLATFORM=offscreen`，与 ci.yml 的 `env` 一致。

## 六、与 CI 那条红线的对应关系，以及取证的边界

对齐的部分（实测）：
- 退出码 **2**，且 `Interrupted: N errors during collection` 的签名一致（CI 该步 19 s 结束，本机 5.33 s；差值来自 runner 冷启动与 `--cov` 首次解析）；
- 依赖集合、Python 3.14、`QT_QPA_PLATFORM=offscreen`、`poetry install --no-root --with dev,baostock` 全部对齐。

失败那次提交的代码已核对：CI 跑在备份仓 `G:\git\python-quant`，失败 sha `98b05d4`（2026-11-12）里的 `src/utils/exception_handler.py` 第 15 行同样是模块级 `from PySide6.QtWidgets import QApplication, QMessageBox`；`git diff --stat 98b05d4 HEAD -- src/utils/exception_handler.py src/advice/fundamental_factors.py src/data/local_store/share_capital.py` 为空，即失败提交与本次被测代码在导入链上逐字节一致。

没取到证的部分（不得当成结论）：
- GitHub 托管的 `ubuntu-24.04` runner **镜像本身是否预装 `libgl1`** 无法从本机确认：`/actions/runs/{id}/logs` 与 `/actions/jobs/{id}/logs` 匿名 API 返回 **403 `Must have admin rights to Repository.`**，annotations 只有通用 `Process completed with exit code 2.`。
- 因此严格表述是：**「在 Ubuntu 24.04 + ci.yml 那组 apt 包的最小环境里，收集期唯一失败原因就是 libGL.so.1，退出码 2」已被实测坐实**；「GitHub runner 上同样缺 libgl1」是按同一组 apt 命令推得的等价条件，需一条 job 日志或一次对照运行才能最终闭环。

## 七、修复方向（本次只读，未改任何跟踪文件）

`ci.yml` 的 `Install Qt runtime libraries` 步补一个包即可让收集期过关：

```yaml
sudo apt-get install -y libegl1 libxkbcommon-x11-0 libdbus-1-3 libglib2.0-0 libgl1
```

装 `libgl1` 后本环境实测：收集期 0 错误，`--cov` 跑到 `67.84%`（> `fail_under=63`），覆盖率门不构成红因；剩下的红点是 8 failed / 4 errors 这类数据与服务差异，需在 CI 侧另外处置。

## 八、日志清单

| 文件 | 内容 |
|---|---|
| `work/wsl_pytest_exact_ci_cmd_no_libgl1.log` | ci.yml 原命令 + 无 libgl1 → 退出码 2、68 errors（核心证据） |
| `work/wsl_collect_only_no_libgl1.log` | `--collect-only` 无 libgl1，含 68 个模块的完整 traceback |
| `work/wsl_collect_only_after_libgl1.log` | `--collect-only` 补装 libgl1 后，0 error |
| `work/wsl_pytest_full_after_libgl1.log` | ci.yml 原命令 + 有 libgl1 → 退出码 1，8 failed/2587 passed，覆盖率 67.84% |
| `work/wsl_env_install.log` | venv/poetry/依赖安装全过程（含 `poetry export` 与 pip 输出） |
| `work/wsl_apt_setup.log` | apt 与 deadsnakes 安装 Python 3.14 的完整日志 |

## 九、边界与可回滚

- 全程未修改任何 Git 跟踪文件（`ci.yml`、`pyproject.toml`、`tests/**`、`src/**` 均未动），未 `git add`、未 commit。
- 未移动/删除 `data/` 任何内容；未向 `.venv` 安装任何包。
- 所有产物只在两处：仓库内 `2026-11-12/ci-status/work|outputs`（未跟踪），以及宿主 `E:\www\tmp\ci_wsl\` + WSL 发行版 `UbuntuCI`（可用 `wsl --unregister UbuntuCI` 整体丢弃）。

## 十、runner 上的 A/B 闭环：libgl1 在 24.04 有效，但在 runner 上不足（2026-11-12 追加）

第三节的 A/B 是**自建 24.04 环境**的实测。真实 runner 上的对照如下（数据取自匿名 API 的 step 结论与 `::error::` 注解，原文留档 `work/runner_401_annotations.txt`）：

| 运行 | 提交 | 该轮唯一变量 | `Install Qt runtime libraries` | `Test with coverage gate` |
|---|---|---|---|---|
| #399 | `dd4d757` | 补 `libgl1` | success（09:58:50 → 09:58:56） | **failure**，10:00:05 → 10:00:22（**17 s**），注解 `Process completed with exit code 2.` |
| #401 | `24c1570` | 加 `pythonpath = ["."]`（源仓 `2adc693` 的 blob）+ 钉 `runs-on: ubuntu-24.04` | success | failure，但**已越过收集期**：`8 failed, 2586 passed, 26 skipped, 3 deselected, 1 xfailed, 4 errors in 105.69s`，exit 1 |

判读：`17 s` 与 `105.69 s` 是两种签名——前者是收集期中断（与本机无 libgl1 时的 5.33 s 同型，只差 runner 冷启动），后者是完整跑完。

因此结论要分两层写，不能合成一句：

1. **libgl1 确实是自建 24.04 环境的第一层缺件**：同一组 apt 包下 68 个模块收集期全灭、退出码 2；补上后 0 错误（第三节 A/B）。
2. **libgl1 在 GitHub runner 上不是充分条件**：#399 装了 libgl1 之后 Test 步仍 exit 2、时长仍是 17 s，说明 runner 侧另有一层。该层由临时诊断步（`19d9f6f` 加入、`24c1570` 换成 Failure probe）以 `::error::` 注解取到原文——`E   ModuleNotFoundError: No module named 'src'` 外加大量 `ERROR tests/*`。
   根因：CI 用 `poetry run pytest`（console script）启动，cwd 不会进 `sys.path[0]`；本机惯用 `python -m pytest` 会把 cwd 塞进 `sys.path[0]`，因而在本机永远复现不出来。修法是 `[tool.pytest.ini_options] pythonpath = ["."]`（源仓 `2adc693`）。
3. **`runs-on` 口径**：钉 `ubuntu-24.04`。理由：`ubuntu-latest` 正在向 26.04 迁移（runner-images#14748），依赖面会随标签漂移；本轮全部实测证据（68 errors / 0 errors / 8 failed 清单）都建立在 24.04 上，钉死才能保证「本地复现」与「runner 表现」可对照。代价是 24.04 上 `Install dependencies` 本轮耗时约 13 分钟（`24c1570` 那次），仍成功。

## 十一、runner #401 的 8 failed / 4 errors 逐条归因（2026-11-12 追加）

原文出处：`work/runner_401_annotations.txt`（`nodeids`、`uniq-errors`、`summary` 三条注解；`/actions/jobs/{id}/logs` 匿名 403，故以注解为准）。

现有 marker / skip 条件：`pyproject.toml` 的 `markers` 只注册了 `perf`，`addopts = "-q -m 'not perf'"` 因此排出 3 条 perf 用例（与本次红点无关）；**下列三个测试文件在本轮之前都没有任何 skip 条件**，所以环境缺失时必然记成失败。

| # | nodeid | 报错原文首行 | 归因 |
|---|---|---|---|
| 1 | `tests/test_data_manager.py::test_data_manager_initialization` | `sqlalchemy.exc.OperationalError: (pymysql.err.OperationalError) (2003, "Can't connect to MySQL server on 'localhost' ([Errno 111] Connection refused)")` | 缺 MySQL 连接 |
| 2 | `tests/test_data_manager.py::test_data_fetcher` | `ERROR ... OperationalError (2003, ... Connection refused)`（module fixture 建连时抛） | 缺 MySQL 连接 |
| 3 | `tests/test_data_manager.py::test_data_processor` | 同上 | 缺 MySQL 连接 |
| 4 | `tests/test_data_manager.py::test_data_updater` | 同上 | 缺 MySQL 连接 |
| 5 | `tests/test_data_manager.py::test_basic_data_methods` | 同上 | 缺 MySQL 连接 |
| 6 | `tests/test_volume_unit_repair.py::test_evidence_marks_exactly_19_rows_in_7_codes` | `FileNotFoundError: No such file or directory (os error 2): .../data/local_store/quotes/stock/sh/600221.SH.parquet` | 缺 `data/local_store` 快照 |
| 7 | `tests/test_volume_unit_repair.py::test_repaired_rows_satisfy_round_lot_residual` | 同上 | 缺 `data/local_store` 快照 |
| 8 | `tests/test_volume_unit_repair.py::test_repaired_rows_satisfy_neighbor_scale` | 同上 | 缺 `data/local_store` 快照 |
| 9 | `tests/test_volume_unit_repair.py::test_round_lot_criterion_rejects_wrong_values` | 同上 | 缺 `data/local_store` 快照 |
| 10 | `tests/test_volume_unit_repair.py::test_neighbor_scale_criterion_rejects_wrong_values` | 同上 | 缺 `data/local_store` 快照 |
| 11 | `tests/test_volume_unit_repair.py::test_amount_close_inverse_invalid_on_extreme_days` | `FileNotFoundError ... /data/local_store/quotes/stock/sz/301487.SZ.parquet` | 缺 `data/local_store` 快照 |
| 12 | `tests/test_ui_smoke.py::test_stock_name_label_updates_and_elides` | `AssertionError: assert '中华人民共和国超长股票名称测试股份有限公司' not in '中华人民共和国超长股票...份有限公司(60003…'` | 平台差异：缺 CJK 字体 → Qt 无法度量中文宽度，`elidedText` 不截断 |

逐条摊开后是三类，不是一句「环境差异」：

**A. 缺 MySQL（#1~#5，1 failed + 4 errors）—— 本该跳过却没跳过**
- 现有 skip：无。`tests/test_data_manager.py` 的 module fixture 直接 `db_manager.connect()`，用例本身也没有探测。
- 最小改法：模块级 `pytestmark = pytest.mark.skipif(not _mysql_reachable(), ...)`，`_mysql_reachable()` 读 `Config().database.host/port` 做 1 s socket 探测（源仓 `add3305`）。未改任何断言、未删用例。

**B. 缺 `data/local_store` 快照（#6~#11，6 failed）—— 本该跳过却没跳过**
- 现有 skip：无。模块内 6 条用例都直接 `pl.read_parquet(...)`，没有路径存在性前置。
- 最小改法：模块级 `pytestmark = pytest.mark.skipif(not QUOTE_DIR.is_dir(), ...)`（源仓 `add3305`）。CI 的 `actions/checkout` 天然不含 `data/`（本地列存不入库），这不是被测代码的缺陷。

**C. 缺 CJK 字体（#12，1 failed）—— 平台差异，补环境而非 skip**
- 现有 skip：无；该断言依赖 `QFontMetrics(QFont('Microsoft YaHei'))` 对中文的度量，Linux 上无该字体、且无任何 CJK 回退字体时测宽偏窄 → 不省略。
- 处置：**不改测试**，在 ci.yml 的 Qt 运行库步补装 `fonts-wqy-zenhei`（备份仓 `ee3e1b0`）。理由：这是本仓对 `elidedText` 逻辑的唯一覆盖，skip 掉等于失去覆盖；而补字体后本机 WSL 实测 RC=0，属环境缺件。

**D. 单列（真缺陷，已修，见第十三节）**
- `tests/test_local_store.py::test_factor_mtime_ttl_fallback_sees_cross_process_rebuild`：本机全量顺序跑曾报 `assert [0.5] == [0.8]`，单跑 3/3 通过；runner #401 未失败。根因不是「顺序」，而是该用例依赖快照文件的 `st_mtime_ns` 是否推进（详见第十三节）。


## 十二、修复落地后的 runner 结果（2026-11-12 追加）

| 运行 | 提交 | 内容 | 结论 |
|---|---|---|---|
| #402 | `ee3e1b0` | 补 `fonts-wqy-zenhei` + 两处 skipif（A/B 类） | **success**：`2587 passed, 37 skipped, 3 deselected, 1 xfailed, 75 warnings in 89.90s`；`Test with coverage gate` success（10:36:12 → 10:37:33），覆盖率门通过 |
| #403 | `53862f8` | 撤除临时 Failure probe 诊断步，其余不变 | **success**：步骤清单只剩 8 步（`1 Set up job` → `8 Test with coverage gate`），无诊断步；`Test with coverage gate` success（10:40:05 → 10:41:55）。撤除后无注解产出，符合预期 |

#402 的账能对上：对比 #401 的 `2586 passed / 26 skipped / 8 failed / 4 errors`，12 个红点里 **11 个转为 skipped**（MySQL 5 例 + 缺 data 分片 6 例）、**1 个转为 passed**（`test_ui_smoke.py::test_stock_name_label_updates_and_elides`，装 CJK 字体后 elide 正常）；passed 由 2586 → 2587、skipped 由 26 → 37，`3 deselected`（`-m 'not perf'`）与 `1 xfailed` 未变，无新增失败。

原文留档：`work/runner_402_annotations.txt`（`summary` 注解；该步已撤除，此后不再产生注解）。

## 十三、D 类真缺陷：根因、修法、验收（2026-11-12 追加）

**结论先说**：这条不属于「前序模块留下跨进程 TTL 缓存 / 临时目录 / 环境变量」，那个假设被实测排除；它与执行顺序无关，取决于一次时间戳竞态。

### 13.1 排除「顺序 / 残留状态」的证据

| 排查项 | 结果 |
|---|---|
| 是否有模块级改写 `_FACTOR_MTIME_TTL` | 全仓仅该用例自身 `monkeypatch.setattr`，无其它赋值 |
| 是否 `importlib.reload('src.data.local_store.store')` | 无（全仓 reload 目标只有 advice 校准模块） |
| 是否泄漏的时钟补丁 | `time.monotonic` 的补丁全部走 `monkeypatch`（自动恢复）；全量两次均绿 |
| 是否共享 tmp 目录 | 该用例用 `tmp_path`，每次独立 |
| 全量实跑 | 修复前后共 **4 次** `-m "not perf"` 全量，均 EXIT=0，该 nodeid 全 passed |
| 历史 junit | 6 份报告（2328 / 2333 / 2222 / 2642×3 条）该 nodeid 全 passed |
| WSL 全量 | 8 个红点里没有它 |

即「全量顺序跑必红」不成立，原先的红是一次罕见竞态。

### 13.2 真正的机制

`src/data/local_store/store.py` 的跨进程重建失效信号是快照文件的 `st_mtime_ns`：
`_cached_mtime()` 在 TTL 内直接返回缓存值，`_factor_frame()` 只有 `_factor_file_mtime != 新 mtime` 时才清缓存重扫。
用例用「同一个路径再写一次 parquet」模拟跨进程重建；若这次写入没有推进 `st_mtime_ns`（落在同一时间戳刻度内，或写入环节保留了原时间戳），缓存按设计不失效 → 第二次读仍是 `0.5` → `assert [0.5] == [0.8]`。

强制复现（可复算，两种形态，脚本留档 `work/repro_mtime_scope.py`）：
```powershell
cd e:\code\python\quant
$env:PYTHONPATH='.'
.venv\Scripts\python.exe 2026-11-12\ci-status\work\repro_mtime_scope.py
# mode=A first=[0.5] second=[0.5] assert_pass=False mtime_advanced=False   ← 把 mtime 复位
# mode=B first=[0.5] second=[0.8] assert_pass=True  mtime_advanced=True    ← mtime 推进
```
pytest 级对照（脚本留档 `work/test_mtime_pair.py`）：`.venv\Scripts\python.exe -m pytest 2026-11-12\ci-status\work\test_mtime_pair.py -q` 实跑 `1 failed, 1 passed`，失败行正是 `assert [0.5] == [0.8]`。

### 13.3 修法（只动测试隔离）

`tests/test_local_store.py`：
- `_write_factor_snapshot()` 改为返回写入路径（原返回 `None`，调用方都不取返回值）；
- 用例记录首个 `st_mtime_ns`，第二次写入后 `os.utime(path, ns=首个值 + 2 s)`，把「重建推进了 mtime」这一前提显式化（+2 s 覆盖 FAT 的 2 s 粒度）；
- 断言原样未改、用例未删、阈值未调；`src/` 一行未动。

生产侧语义未变：跨进程重建仍只以 mtime 为信号（`_cached_mtime` docstring 已写明这是兜底设计，同进程重建走 `invalidate()`）。若将来需要更强信号（size / 内容哈希），另立任务。

### 13.4 验收

| 项 | 结果 |
|---|---|
| 修前形态必红 | `2026-11-12/ci-status/work/test_mtime_pair.py::test_before_fix_without_mtime_bump` → `assert [0.5] == [0.8]`（同文件另一条修复形态用例 passed） |
| 修后全量 ×2 | EXIT=0 ×2，junit `tests=2646 failures=0 errors=0 skipped=6`，该 nodeid 两次 `passed`（0.021 s） |
| 单跑 ×3 | EXIT=0 ×3 |
| 静态检查 | `ruff check tests/test_local_store.py` All checks passed |
| 提交 | 源仓 `e7653dd`（1 file，+19/−5）；备份仓 `45d7562`（按 HEAD blob 写盘，未带他路在途改动） |
| runner 验证 | 备份仓 `45d7562` 推送后 **run #408 success**：8 步全绿，`Test with coverage gate` 11:21:51 → 11:23:36 |

