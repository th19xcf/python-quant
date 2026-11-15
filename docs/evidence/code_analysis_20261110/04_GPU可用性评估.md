## 0. 结论速览

**本机有 GPU，但没有任何一块能被 cupy 使用。** 机器上同时存在 AMD Radeon RX 5700 XT（PCI `VEN_1002&DEV_731F`，RDNA1 / gfx1010，8 GB）与 Intel(R) Graphics 核显（PCI `VEN_8086&DEV_7D67`）；`nvidia-smi` 不在 PATH、`System32`、`NVSMI` 三处任一路径，**未检测到任何 NVIDIA 设备**。结论属「有卡但非 NVIDIA，所以 cupy 用不了」这一类，而非「没有卡」。AMD 的 gfx1010 已被移出 ROCm 官方支持范围，且 ROCm 在 Windows 上不受支持；`numba.cuda.is_available()` 与 `pl.GPUEngine` 均运行时失败。

**六个候选热点中，没有任何一项值得为它实现 GPU 路径。** 原因不是"GPU 没装"，而是这六处的形态与 GPU 的收益前提不匹配：主循环是 Python 级标量控制流（GPU 无法参与），批量算子的 CPU 实现已经在 1~1.3 秒量级（GPU 的传输与启动开销就吃掉大半），矩阵类计算虽然存在但自由度只有 K ≈ 25（连一个 CUDA warp 都填不满）。

**因此评估口径应从"迁移哪些到 GPU"改为"如何把探测与回退做成一层基础设施"。** 当前项目已经具备这个骨架（`gpu_acceleration.py` 的 try-import 降级 + `pyproject.toml` 的可选组），但存在一个必须先修的正确性缺陷：装 cupy 会让 `GPU_AVAILABLE` 变 `True`，从而把计算导向一条**不加速、且绕过 `global_indicator_manager`** 的旁路。

| 判断项 | 结论 | 标识 |
|---|---|---|
| 本机有无可被 cupy 使用的 GPU 算力 | 无（有 AMD 独显 + Intel 核显，但均非 NVIDIA；未检测到 NVIDIA 设备） | [测试直接证据] |
| 六项热点中值得做 GPU 的 | 0 项 | [测试直接证据] + [静态分析结果] |
| 现有 GPU 代码路径是否真在 GPU 上算 | 否，四个 `calculate_*_gpu` 全部委托 Polars/Numba CPU 实现 | [代码直接证据] |
| 装 cupy 会不会带来风险 | 会，启用无收益旁路并改变指标计算口径 | [代码直接证据] + [工程推论] |
| 第一步该做什么 | 修 `GPU_AVAILABLE` 判定 + 配置化逐项开关，暂不实现任何 kernel | [工程推论] |

---

## 1. 探测口径

| 项目 | 内容 |
|---|---|
| 执行解释器 | `e:\code\python\quant\.venv\Scripts\python.exe`（Python 3.14.9） |
| 探测脚本 | `tmp/probe_gpu_20261110.py`（只读：仅 `pl.read_parquet` 读取 `data/local_store` 下 parquet，不写业务文件、不改源码） |
| 脚本用途 | ① 枚举计算后端；② 测大面板读取/join/rolling 的真实耗时与 RSS；③ 测 bootstrap 与相关系数矩阵耗时；④ 测 Python 逐 bar 循环吞吐用于外推回测引擎 |
| 附加证据 | `pytest tests/performance_test.py -m perf`（回测逐 bar 开销门禁，2 passed）、注册表 `HardwareInformation.qwMemorySize`（显存真实容量） |
| 只读声明 | 未 `pip install` 任何包；未改源码；未改 `data/`；未 `git commit`；未 kill 任何进程 |

---

## 2. 硬件与环境实测

### 2.1 显卡与算力

| 设备 | 识别方式 | 结果 | 标识 |
|---|---|---|---|
| 独显 | `Win32_VideoController` | AMD Radeon RX 5700 XT，`PNPDeviceID=PCI\VEN_1002&DEV_731F&SUBSYS_E4111DA2&REV_C1`，驱动 `32.0.21045.16010`，Status=OK | [测试直接证据] |
| 独显显存 | 注册表 `HardwareInformation.qwMemorySize` | **7.98 GB**（WMI 的 `AdapterRAM` 报 `4293918720` ≈ 4.0 GiB，是 32 位截断伪值，不可信） | [测试直接证据] |
| 核显 | `Win32_VideoController` | Intel(R) Graphics（Core Ultra 9 285K 集成），`PNPDeviceID=PCI\VEN_8086&DEV_7D67`，驱动 `32.0.101.6629`，Status=OK | [测试直接证据] |
| 虚拟显示设备 | `Win32_VideoController` | "Sharing Monitor"（`PNPDeviceID=ROOT\DISPLAY\0000`，远程/串流虚拟设备，无算力） | [测试直接证据] |
| NVIDIA 设备 | `nvidia-smi` 四点位探测 | **未检测到**：不在 PATH（`Get-Command nvidia-smi` 无结果）、`C:\Windows\System32\nvidia-smi.exe` 与 `C:\Program Files\NVIDIA Corporation\NVSMI\nvidia-smi.exe` 均 `Test-Path=False` | [测试直接证据] |
| CUDA 驱动 | `numba.cuda.is_available()` | `False`，报 `CudaSupportError: CUDA driver library cannot be found` | [测试直接证据] |
| ROCm | `Test-Path "C:\Program Files\AMD\ROCm"` | `False` | [测试直接证据] |
| WSL（Linux 侧算力） | `wsl -l -v` | 未安装任何发行版 | [测试直接证据] |
| OpenCL 运行时 | `Test-Path OpenCL.dll` | `True`（但 venv 内 `pyopencl` 缺失，无 Python 绑定） | [测试直接证据] |
| 算力等级 | RX 5700 XT = Navi 10 / gfx1010 / RDNA1 | ROCm 自 5.x 起将 gfx1010 移出官方支持列表；Windows 平台 ROCm 官方不支持 | [工程推论] |

#### 2.1.1 `nvidia-smi` 原样输出

```
PS> nvidia-smi
nvidia-smi : 无法将“nvidia-smi”项识别为 cmdlet、函数、脚本文件或可运行程序的名称。
请检查名称的拼写，如果包括路径，请确保路径正确，然后再试一次。
    + CategoryInfo          : ObjectNotFound: (nvidia-smi:String) [], CommandNotFoundException

PS> (Get-Command nvidia-smi -ErrorAction SilentlyContinue)
（无输出）
```

即：**不存在可执行的 `nvidia-smi`**，因此无法取得型号、显存、驱动版本号与 CUDA Runtime 版本四项信息。这不等于"机器上没有 GPU"——GPU 是有的，只是没有 NVIDIA 的 GPU。[测试直接证据]

#### 2.1.2 非 NVIDIA 设备清单（`wmic` 在 Windows 11 24H2 起已移除，改用 `Get-CimInstance` 等价命令）

```powershell
Get-CimInstance Win32_VideoController | Select Name, AdapterRAM, DriverVersion, PNPDeviceID
```

| Name | AdapterRAM | DriverVersion | PNPDeviceID | 性质 |
|---|---|---|---|---|
| Sharing Monitor | （空） | 11.28.49.440 | `ROOT\DISPLAY\0000` | 虚拟显示设备，无算力 |
| Intel(R) Graphics | 2,147,479,552 | 32.0.101.6629 | `PCI\VEN_8086&DEV_7D67` | Intel 核显（Arrow Lake-S，Xe-LPG） |
| AMD Radeon RX 5700 XT | 4,293,918,720（截断值） | 32.0.21045.16010 | `PCI\VEN_1002&DEV_731F` | AMD 独显，RDNA1 / Navi 10 / gfx1010 |

#### 2.1.3 `.venv` 内 cupy 状态

```
PS> python -c "import cupy; print(cupy.__version__); cupy.show_config()"
cupy NOT INSTALLED -> No module named 'cupy'
```

**cupy 未安装，无版本号，`cupy.show_config()` 无法执行**（该命令要求 cupy 已安装且能加载 CUDA 运行时）。[测试直接证据]

#### 2.1.4 结论属于哪一类

| 候选情形 | 是否成立 | 判定依据 |
|---|---|---|
| ① 没有卡 | ❌ **不成立** | 有 AMD 独显 + Intel 核显，`Status=OK` |
| ② 有卡但驱动 / CUDA 版本不匹配 | ❌ 不成立 | 机器上根本没有 NVIDIA 设备与 CUDA 驱动，谈不上"版本不匹配" |
| ③ **有卡但是非 NVIDIA，所以 cupy 用不了** | ✅ **成立** | cupy 只支持 NVIDIA CUDA；AMD 侧需要 ROCm（本机无、Windows 不支持、gfx1010 亦被移出支持列表）；Intel 核显需 oneAPI/SYCL，venv 内无绑定 |
| ④ 有 NVIDIA 卡但未装 cupy | ❌ 不成立 | 未检测到 NVIDIA 设备 |

**正确表述：本机「有 GPU，但没有可被 cupy 使用的 GPU」。** 不能写成"没有显卡"或"GPU 不存在"。[测试直接证据]

**结论：本机不存在可被 Python 调用的 GPU 计算后端。** AMD 卡的 OpenCL 运行时虽然存在，但项目没有 `pyopencl` 绑定，且 `gpu_acceleration.py` 中 OpenCL 分支只做初始化和上下文创建、**不参与任何指标计算**。[代码直接证据]

### 2.2 CPU / 内存

| 项目 | 值 |
|---|---|
| CPU | Intel Core Ultra 9 285K，24 物理核 / 24 逻辑核，3.7 GHz |
| 内存 | 48 GB |
| 说明 | 24 核 + 48 GB 是本机最强算力，也是所有"CPU 回退"路径的实际承载者 |

---

## 3. 软件栈现状

### 3.1 GPU 相关包（venv 实测）

| 包 | 状态 | 说明 | 标识 |
|---|---|---|---|
| `cupy` / `cupyx` | 未安装 | `ModuleNotFoundError` | [测试直接证据] |
| `cudf` / `cudf_polars` | 未安装 | 同上 | [测试直接证据] |
| `torch` | 未安装 | 同上 | [测试直接证据] |
| `numba` | 0.68.1 **已装** | 但 `numba.cuda` 子模块无可用设备 | [测试直接证据] |
| `pyopencl` / `pycuda` | 未安装 | 同上 | [测试直接证据] |
| `polars` | 2.1.0（`polars-runtime-32` 2.1.0） | `hasattr(pl, "GPUEngine")` → `True`，但实跑 `collect(engine=pl.GPUEngine(...))` 抛 `ModuleNotFoundError: cudf_polars` | [测试直接证据] |
| `numpy` | 2.5.4 | 标准 MKL/OpenBLAS 版，无 CUDA 链接 | [静态分析结果] |
| `scipy` | 1.18.1 | CPU 版 | [静态分析结果] |

**Polars GPU engine 的实况**：`polars` 已支持 `GPUEngine` 入口，但后端必须额外安装 `cudf-polars`（按 CUDA 主版本分发的独立包），且本项目当前版本在缺失时是**抛异常**而不是静默降级——`raise_on_fail=False` 才是回退路径，而项目代码里没有任何地方使用 `GPUEngine`。[代码直接证据]

### 3.2 `pyproject.toml` 的 `cupy = "*"` 来历与现状

| 问题 | 事实 | 标识 |
|---|---|---|
| 何时引入 | 提交 `c0bdf91`（2026-10-29，「引入 poetry.lock 锁定依赖并修正构建层约束」），此后未再改动 | [代码直接证据] |
| 为何不锁版本 | PyPI 的 `cupy` 是**源码分发**（需本机 nvcc + CUDA Toolkit 编译），真正可用的是 `cupy-cuda11x` / `cupy-cuda12x` 这类按 CUDA 主版本分发的 wheel。锁任何一个具体版本都无法同时适配不同机器的 CUDA，留 `*` 等于"声明意图、不做承诺" | [工程推论] |
| 谁会用到 | 仅 `src/tech_analysis/gpu_acceleration.py`；间接消费者为 `streaming_processor.py`（`_calculate_initial`）与 `analyzer/parallel_ops.py`（`_try_gpu_indicator`），另有 3 个测试文件 | [代码直接证据] |
| 现在是否真在 GPU 上算 | **否**。四个 `calculate_*_gpu` 已全部改造为 CPU 实现：MA → `common_calculations.calculate_moving_average`（Polars）、MACD/RSI → `indicators/formulas.py` 的 Polars 表达式、SAR → `indicators/trend.py::_sar_core`（Numba JIT）。文件内注释原文：「MA 是 O(n) 滚动均值，GPU 无收益」「SAR 是状态机式顺序递推…GPU 无收益」 | [代码直接证据] |
| `cp` 还剩什么用途 | 只有 `to_gpu` / `to_cpu` 两个方法；全仓 `grep "\.to_gpu\("` **零调用者**，`to_cpu` 只在 `calculate_macd_gpu` / `calculate_rsi_gpu` 内对自己返回的 numpy 值做恒等转换 | [代码直接证据] |

**这是本报告最重要的单条发现**：`GPU_AVAILABLE` 的判定是 `try: import cupy`。一旦在有 NVIDIA 卡的机器上执行 `poetry install --with gpu`，`is_gpu_available()` 立即为 `True`，`calculate_with_gpu()` 变为可达，`streaming_processor._calculate_initial` 与 `parallel_ops._try_gpu_indicator` 会优先走这条路——而它执行的仍是 CPU 计算，代价是多一次 numpy→Polars→numpy 往返，**并且绕过了 `global_indicator_manager.calculate_indicator`**（后者负责列名/别名与 dtype 归一）。也就是说：装了 cupy 之后，部分指标的计算入口发生改变，但没有任何加速。[代码直接证据]

---

## 4. 六个候选热点的逐项判断

### 4.0 收益判断的共同基准

GPU 要正收益，必须同时满足三条：① 单次 kernel 处理的数据量足够大（经验阈值：≥10⁶ 个 float64 元素，否则 kernel 启动 + 传输开销超过计算本身）；② 数据能常驻显存、不需要每步回传；③ 逻辑是可并行的算术/归约，不是串行状态机或分支密集的控制流。

本机实测的对照基线：

| 基准项 | 实测值 | 标识 |
|---|---|---|
| Polars 大规模 join：17,957,979 行 × 18,070,885 行，键 `(ts_code, trade_date)` | **1.201 s**，RSS 2.9 GB → 8.2 GB，输出 17,957,979 行 | [测试直接证据] |
| Polars rolling/rank：18,044,637 行 × 3 个表达式（252 日 `rolling_rank` + `rolling_mean` + 截面 `rank`） | **1.291 s**，折合 13.97 M 行·算子/秒 | [测试直接证据] |
| 读取 `valuation_daily.parquet`（17,957,979 行 × 12 列） | 0.164 s | [测试直接证据] |
| 读取 `stock_adj_factor.parquet`（18,070,885 行 × 4 列） | 0.032 s | [测试直接证据] |
| `np.corrcoef`：5,000 只 × 2,700 日 | 0.413 s（输出 25 M 元素矩阵） | [测试直接证据] |
| bootstrap 2,000 次 × 500 / 5,000 / 50,000 样本（均值 CI + 中位数 CI 各一次） | 0.021 s / 0.130 s / 1.585 s | [测试直接证据] |
| 回测逐 bar 开销 b | 单独运行 4.24~4.95 µs/bar；250k bar 时 8.33 µs/bar；门禁上界 15 µs/bar | [测试直接证据]（`tests/performance_test.py` 阈值注释 + 本次 `-m perf` 通过） |

**含义**：项目目前所有"大规模"算子的 CPU 耗时都在 1.3 秒以内。GPU 的 PCIe 传输成本按 12 GB/s 估，125 MB（15.7 M × float64）单程约 10 ms，一次 kernel 启动 5~10 µs，PCIe 同步往返 20~50 µs——即使这些算子被完美移植，收益上限也就是"1.2 秒变 0.4 秒"，端到端占比个位数百分比。而代价是引入两个按 CUDA 版本分发的重依赖。[工程推论]

> 下文 4.1~4.6 的三段式表格中，**①** 行为收益判断，标识已随行标注；**②③** 行为设计建议，除特别说明外统一为 **[工程推论]**（尚未实现、无可执行代码可验证）。

---

### 4.1 回测引擎（`src/backtest/engine/execution_core.py`，1926 行）

**数据形态实测**

| 维度 | 事实 | 标识 |
|---|---|---|
| 主循环结构 | `_run_trading_loop` 是双层 Python `for`：外层 `for day_index, trade_day in enumerate(sorted_dates)`，内层 `for code, pos in zip(codes_today, positions_today, strict=True)` | [代码直接证据] |
| 单 bar 工作量 | 11 个列引用元组解包 + 8 次 `_finite_float()`（`float()` 装箱 + `math.isfinite`）+ 2~3 次 dict 查找 + ST 状态二分 + 可能的 `Bar` NamedTuple 构造（仅当日有挂单时） | [代码直接证据] |
| 单 bar 数据量 | **标量**。每个 bar 处理 1 只票 1 天，数组长度 = 1 | [代码直接证据] |
| 分支密度 | `_execute_pending` → `_dispatch_order` → `_check_order_executable` → `_execute_buy/_execute_sell` 链上含涨跌停判定、板块档位、一字板、停牌、整手、ADV 分档、风控闸门，每单 10+ 个条件分支 | [代码直接证据] |
| 全市场规模 | 约 5,800 只 × 2,700 交易日 ≈ 15.7 M bar（按本地 `stock_turnover` 18.04 M 行、`valuation_daily` 17.96 M 行同源估） | [静态分析结果] |
| 实测总耗时 | 15.7 M × 4.5 µs ≈ **71 s**；按 250k bar 时的 8.33 µs/bar 则是 **131 s** | [测试直接证据] + [工程推论] |

**三段式判断**

| 段落 | 结论 |
|---|---|
| ① 是否值得做 GPU 路径 | **不值得，且技术上不可行。** 每个"工作单元"是 1 个标量而非数组，GPU 的 kernel 启动开销（5~10 µs）已经超过单 bar 全部 CPU 工作量（4.5 µs）。要凑够 GPU 的经济规模，必须先把逐 bar 循环改写成"时间主序宽表 `(T, N)` + 布尔掩码批量推进"，那是一次**数据结构重写**，改写后的正确形态是 CPU 向量化（NumPy/Polars），不是 GPU kernel——因为改完仍是每 bar 一个 5,800 长度的向量，仍是小 kernel。[代码直接证据] + [测试直接证据] |
| ② CPU 回退怎么做 | 无需专门做 GPU 回退；需要的是**向量化改写后的 CPU/向量化双路径**，用同一份 golden 数据集逐位比对。当前已有的保护是 `tests/performance_test.py` 的 `T(n)=a+b·n` 门禁（上界 15 µs/bar），改写前后必须都通过。 |
| ③ 配置怎么表达 | 不需要 GPU 开关。若将来做向量化改写，用**配置文件**（`BacktestSettings.vectorized: bool`）而非环境变量——它是结果口径相关开关，需要随代码一起版本化、可审计。 |

**建议**：不做 GPU。真正的性能项是把 `_finite_float` 的每 bar 8 次标量调用改为预计算列级 NaN 掩码，以及把 `sorted_dates` 的逐日推进改为 numpy 索引切片；这属于既有 CPU 优化，与本报告主题无关，不展开。

---

### 4.2 因子批量计算（`src/advice/factor_panel.py` → `src/alpha/calculator/alpha_calculator.py`）

| 维度 | 事实 | 标识 |
|---|---|---|
| 实现形态 | `compute_factor_panel` 调 `calculate_factors_cross_sectional`，模块文档明写「全表一次性求值，分区由 Polars 的 `.over(code_col)` 在引擎内部完成，因子层与指标公式层不做逐组 Python 循环」 | [代码直接证据] |
| 规模 | 推荐面板 24 个打分列 + 6 类扩展因子；全市场约 15.7 M 行 × 约 79 列候选 | [代码直接证据] |
| 实测吞吐 | 18.04 M 行 × 3 个 rolling/rank 表达式 = 1.291 s。按同比例外推 79 列 ≈ **34 s** | [测试直接证据] + [工程推论] |
| 算子类型 | `rolling_mean` / `rolling_std` / `rolling_rank` / `rank('average')` / `shift` —— 全部是可并行的窗口与排序算子 | [代码直接证据] |

**三段式判断**

| 段落 | 结论 |
|---|---|
| ① 是否值得做 GPU 路径 | **不值得（当前规模），但它是六项里唯一"形态上真正适合 GPU"的一项。** 理由：① 算子是纯可并行窗口/排序，无串行依赖；② 数据量 15.7 M 行远超 GPU 经济阈值；③ 但 CPU 侧 Polars 已跑到 14 M 行·算子/秒，34 s 的总量里 GPU 即便 5× 加速也只省 27 s，而 `cudf-polars` 的 `.over()` 分组滚动算子支持是**部分支持**，分组数 = 5,800，属于其支持较弱的高基数分组场景。[测试直接证据] + [工程推论] |
| ② CPU 回退怎么做 | 天然回退：`cudf-polars` 缺失或算子不支持时，`pl.LazyFrame.collect(engine=pl.GPUEngine(raise_on_fail=False))` 会**整体回落 CPU 引擎**（不是逐算子回落）。因此回退是"要么整条 lazy plan 在 GPU 上跑，要么全在 CPU"，中途不会产生混合口径。这一点必须在代码注释里写死，否则后人会误以为可以逐算子混跑。 |
| ③ 配置怎么表达 | **自动探测 + 配置文件覆盖**。理由：默认值必须能让无卡机器零配置启动，所以 `auto`（探测 `cudf_polars` 可导入且 `cuDF` 能枚举到设备才启用）；同时保留 `DataSettings.accel_factors: 'auto' \| 'on' \| 'off'` 供排障时强制单点。不用纯环境变量，因为项目已在 `src/utils/config.py` 用 `pydantic-settings` + `.env`，新增字段即可复用现有热加载（`ConfigManager._get_env_file_mtime`），不需要另造一套。 |

**建议**：不做。若将来因子列数从 79 涨到 300+ 或日频升到分钟频，再评估——届时的正确做法仍是先压 Polars 表达式、再看 GPU。

---

### 4.3 相关/协方差矩阵、组合优化

**这条候选在项目里存在，但规模小到不构成加速对象。**

| 检查项 | 结果 | 标识 |
|---|---|---|
| `np.corrcoef` 出现位置 | `src/alpha/calculator/alpha_calculator.py:171`（因子间相关矩阵）、`:218`（单因子与收益的 IC）、`src/quant/learning/evaluator.py:40`（Rank IC） | [代码直接证据] |
| `np.linalg` 出现位置 | 仅 `src/quant/learning/weights.py:214-216`：Ridge 闭式解 `np.linalg.solve(gram + ridge, rhs)`，失败回落 `np.linalg.lstsq` | [代码直接证据] |
| `scipy.optimize` 出现次数 | **0 次**，项目无二次规划/凸优化求解器 | [静态分析结果] |
| 相关矩阵的**输出**规模 | `np.corrcoef(factor_data)` 中 `factor_data` 是因子列表，长度 = 因子数 K（α 因子 ≤ 25，`factor_eval.py` 的 `correlation_matrix` 同样 K×K）→ 输出 **25×25 = 625 元素** | [代码直接证据] |
| 相关矩阵的**计算**规模 | 成本 ≈ `K² × N`（N = 行数）。K=25、N=15.7 M 时 ≈ 9.8×10⁹ 次乘加；按本机实测 `np.corrcoef` 的 3.3×10¹¹ FLOP/s 折算 ≈ **0.03 s** | [工程推论]（由 §4.0 实测的 5,000×2,700 反推吞吐） |
| Ridge 回归规模 | `gram = matrix.T @ matrix` → **K×K = 25×25**；`np.linalg.solve` 解 25 元线性方程组，微秒级。训练样本数（`analysis_monthly` 实测 567,541 行）只影响 `gram` 的累加，仍是 `K² × N` 量级 | [代码直接证据] + [测试直接证据] |
| 组合优化的实际实现 | `src/advice/portfolio.py::allocate` 是**迭代归一 + 上限裁剪**的确定性权重分配，纯 Python 循环（`for _ in range(normalize_iterations)`），不含任何线性代数、不含二次规划 | [代码直接证据] |
| 组合规模 | `max_positions` 量级（通常 ≤ 数十只），循环上限 `normalize_iterations`（个位数~数十） | [代码直接证据] |

| 段落 | 结论 |
|---|---|
| ① 是否值得做 GPU 路径 | **不值得。** 判据不是"有没有矩阵"，而是"矩阵多大"。这里所有矩阵的**自由度都只有 K ≈ 25**：相关矩阵 25×25、Ridge 的 Gram 矩阵 25×25、线性方程组 25 元。GPU 的收益来自"把大矩阵拆成很多小块并行"，而 25×25 的矩阵连一个 CUDA warp（32 线程）都填不满，`np.linalg.solve` 在 CPU 上就是微秒级。作为对照：本机 `np.corrcoef` 算 5,000×2,700（输出 25 M 元素）只要 0.413 s，而项目实际只算 25×25。组合优化干脆没有矩阵运算，GPU 无从下手。[代码直接证据] + [静态分析结果] |
| ② CPU 回退怎么做 | 不适用——没有 GPU 路径需要回退。 |
| ③ 配置怎么表达 | 不适用。 |

**建议**：不做，也不要在报告之外的场合把它列为"待优化项"。真正相关的问题在别的报告里：`portfolio.py` 的分配是启发式而非最优化，若将来要上均值-方差/风险平价，届时 N≈数十，仍应留在 CPU（`scipy.optimize` 或 `cvxpy`），GPU 只有在 N 上千时才可能相关。

---

### 4.4 相似态回溯检索（`tools/research/analog_lookup.py`，925 行）

这是用户点名"可能真受益"的一项，需要拆开看它到底把时间花在哪。

| 阶段 | 实现 | 实测/估算 | 标识 |
|---|---|---|---|
| 数据加载 + 指标计算 | `_load_indicator_panel`：按 `batch=400` 分批 `load_panel` + `compute_factor_panel(extended=True)` | **主导成本**，本质同 §4.2；全市场 5,800 只 × 11 年需 15 批 | [代码直接证据] |
| 相似度度量 | `_add_mode_metric`：Polars `rolling_rank` / `rolling_mean` / `rolling_std` `.over('ts_code')` | 同 §4.2，18 M 行 × 3 表达式 1.291 s | [测试直接证据] |
| 前向收益 + 基准 | `_add_forward_returns` / `_add_benchmark`：`shift(-h).over('ts_code')`、`mean().over('date')` | Polars 表达式，毫秒级 | [代码直接证据] |
| 去重叠 | `_dedupe_overlap`：**纯 Python `for pos in indices` 循环 + dict 查表** | 随命中数线性增长；命中 10⁴ 行时是 10⁴ 次 Python 迭代 | [代码直接证据] |
| bootstrap CI | `_bootstrap_ci`：`rng.integers(0, n, size=(2000, n))` 一次性生成索引矩阵，再 `mean/median(axis=1)` | **2000×500 = 0.021 s；×5,000 = 0.130 s；×50,000 = 1.585 s**；每次调用峰值内存 `2000×n×8` 字节（n=50,000 时 800 MB） | [测试直接证据] |
| 调用次数 | 每列 × 每 horizon × {均值 CI, 中位数 CI} = 4 horizon × 2 = **8 次** | 最坏（n=50,000）≈ 12.7 s | [代码直接证据] |
| Mann-Whitney U | `scipy.stats.mannwhitneyu`，基线 > 50,000 时抽 50,000 | 百毫秒级 | [代码直接证据] |

**三段式判断**

| 段落 | 结论 |
|---|---|
| ① 是否值得做 GPU 路径 | **不值得，尽管它是六项中"最像 GPU 任务"的一项。** bootstrap 的形态确实是理想的 GPU 场景（独立重采样、无分支、纯归约）。但实测绝对量级把这条路否掉了：n=500 时 0.021 s、n=5,000 时 0.130 s，即使在最极端的 n=50,000 也只有 1.585 s/次。端到端一次全市场检索的耗时由"15 批 `load_panel` + `compute_factor_panel`"决定（分钟级），bootstrap 占比 < 1%。把 < 1% 的部分做到 0 也换不来可感知收益。**另外 `_dedupe_overlap` 那个 Python 循环反而是真正可优化点，但它不可 GPU 化，只能改成 Polars `shift` + 掩码。**[测试直接证据] |
| ② CPU 回退怎么做 | 若将来真做（比如把 bootstrap 从 2,000 提到 200,000 次做更细的分位估计），回退方式是**两条独立函数**而非同一函数内 if：`_bootstrap_ci_numpy` / `_bootstrap_ci_cupy`，由配置在调用点选择。绝不写成"函数内 try GPU except CPU"，那样无法测试 GPU 分支。等价性判据：固定 `seed` 后，GPU 版与 CPU 版的索引矩阵生成器不同（PCG64 在两者上不可复现），因此**不比数值，比统计性质**——同一真值分布下 CI 区间端点差异 ≤ 半宽 × 1e-3，且点估计（均值/中位数）用 `rtol=1e-12` 比对（点估计不依赖重采样索引）。 |
| ③ 配置怎么表达 | **配置文件**（`ResearchSettings.analog_bootstrap_backend: 'cpu' \| 'gpu'`）。理由：这是研究工具、非生产主链，误开 GPU 的后果只是慢或 OOM，不影响业务数值；而环境变量在批处理脚本里容易残留、难追溯。 |

**建议**：不做。若要提速这条工具，优先做两件事：`_dedupe_overlap` 的 Polars 化、`batch` 从 400 调大（减少 15 批的重复 `load_panel` 固定开销）——都是纯 CPU。

---

### 4.5 大规模面板 join / 重采样（`stock_adj_factor` 1,807 万行、`valuation_daily` 1,796 万行）

| 维度 | 实测 | 标识 |
|---|---|---|
| 两表规模 | `valuation_daily` 17,957,979 行 × 12 列（550 MB parquet）；`stock_adj_factor` 18,070,885 行 × 4 列（12.3 MB parquet） | [测试直接证据] |
| 读取耗时 | 0.164 s / 0.032 s | [测试直接证据] |
| join 耗时 | `left.join(right, on=['ts_code','trade_date'], how='left')` → **1.201 s**，输出 17,957,979 行 | [测试直接证据] |
| 内存 | 读 valuation 后 RSS 96 → 2,222 MB；join 后 → **8,229 MB** | [测试直接证据] |
| 说明 | 18.07 M 行的 `stock_adj_factor` parquet 只有 12.3 MB，是因为 `(ts_code, trade_date, qfq_factor, hfq_factor)` 高度可压缩；解压后驻留内存远大于文件体积 | [工程推论] |

**三段式判断**

| 段落 | 结论 |
|---|---|
| ① 是否值得做 GPU 路径 | **不值得。** ① CPU 侧 1.2 s 已经不是任何流程的瓶颈；② GPU 要求**两侧数据全部驻留显存**，仅 valuation 侧解压后就占 ~2.1 GB RSS，两表 + 输出接近 8.2 GB——8 GB 卡（即便有 NVIDIA）会直接 OOM 或被迫分块，而分块恰恰抵消了 GPU 的吞吐优势；③ `cudf-polars` 的 join 是支持的，但它的价值在"数据已经端到端在 GPU 上"的流水线中；本项目的数据全部落在 parquet / 本地列存，每次都要付一次 H2D。[测试直接证据] + [工程推论] |
| ② CPU 回退怎么做 | 不适用（不建议做 GPU 路径）。作为对照，若强行做，`pl.GPUEngine(raise_on_fail=False)` 在显存不足时会整条 plan 回落 CPU，但**回落发生在 OOM 之后而不是之前**，意味着必须处理 `MemoryError`/`cudf` 异常，复杂度高于收益。 |
| ③ 配置怎么表达 | 若将来做（256 GB 内存 + 48 GB 显存的机器上确有意义），用**自动探测**：按「显存 ≥ 输入两侧解压后大小 × 2」判定，不足则 CPU。这个阈值必须由探测算出来，不能写死常量。 |

**建议**：不做。真正该做的是减少 join 次数与列裁剪（当前 join 携带了 `source` / `fetched_at` 等审计列），属于 CPU 侧 IO 优化。

---

### 4.6 数据采集（baostock / 通达信 / akshare）

| 检查项 | 结果 | 标识 |
|---|---|---|
| 采集链路实现 | `baostock_handler.py`(55) / `akshare_handler.py`(55) / `tdx_handler.py`(656) / `tdx_day_parser.py`(267，二进制 `struct` 解包) / `financials/collector.py`(881) / `managers/data_fetcher.py`(1478) | [代码直接证据] |
| `src/data` 下 polars/numpy 的用法 | 15 个文件有 `import polars` / `import numpy`，但用途是 **parquet 读写 + 去重 + 区间裁剪**（如 `collector.py` 的 `pl.read_parquet` / `unique` / `filter(pl.col('trade_date') >= since)`），以及 `adj_factor_calculator.py` 的 `np.searchsorted` / `np.cumprod`——后者作用在**单只票的除权事件数组**上（长度通常 <100），不是跨票批量运算 | [代码直接证据] |
| 瓶颈类型 | 网络 RTT + 源端限速 + 单连接串行；`tdx_day_parser` 是 `struct.unpack` 逐记录解析 | [代码直接证据] |

| 段落 | 结论 |
|---|---|
| ① 是否值得做 GPU 路径 | **不值得，零收益。** 采集链路的时间全部花在 socket/HTTP 往返与逐记录 `struct.unpack` 解包上；链路上涉及的 polars/numpy 只用于 parquet 落盘与单票除权事件的小数组（长度 <100），GPU 连"凑够 10⁶ 元素"的门槛都达不到。[代码直接证据] + [静态分析结果] |
| ② CPU 回退怎么做 | 不适用。 |
| ③ 配置怎么表达 | 不适用。 |

**建议**：不做。若要提速采集，方向是并发连接数（受源端限流约束）与增量断点续传，与本报告无关。

---

## 5. 硬性设计约束（必须写进任何后续实现）

以下五条是"GPU 有则用、无则回退"能否安全落地的前提。当前项目满足其中 3 条、违反 2 条。

| # | 约束 | 当前状态 | 差距 | 标识 |
|---|---|---|---|---|
| 1 | **不做硬依赖**：`cupy` / `cudf` 只能留在 `pyproject.toml` 可选组；`import` 失败必须降级 | ✅ 满足。`gpu_acceleration.py` 用 `try/except ImportError` 且 `cp = np` 兜底；`pyproject.toml` 的 `[tool.poetry.group.gpu]` 是 `optional = true` | 无 | [代码直接证据] |
| 2 | **默认 CPU 且不刷屏** | ⚠️ 部分违反。`gpu_acceleration.py` 在 **模块 import 时**就打 4 条 `logger.info`（「CuPy不可用，将使用CPU计算」「OpenCL不可用」），每次 `import src.tech_analysis` 都会打一遍 | 应降为 `logger.debug`，或仅在显式开启探测时输出 | [代码直接证据] |
| 3 | **结果等价性 + 明确容差** | ❌ 未定义。仓库内无 GPU/CPU 双路比对测试 | 见下方容差表 | [静态分析结果] |
| 4 | **每一处可单独关闭** | ❌ 违反。只有全局 `GPU_AVAILABLE` 一个布尔，无法只关"因子"或只关"bootstrap" | 需要逐项开关（见 §7 配置设计） | [代码直接证据] |
| 5 | **不引入锁版本冲突** | ⚠️ 风险。`cupy = "*"` 指向源码分发，装它需要本机 nvcc；CUDA 版本不匹配时 import 成功但运行时抛 `CUDARuntimeError` | 见下方版本对应表 | [工程推论] |

### 5.1 等价性容差（必须写具体值，不能写"误差可接受"）

| 比对对象 | 判据 | 理由 |
|---|---|---|
| 指标列逐元素（float64，如 MA/RSI/MACD/ATR） | `np.allclose(gpu, cpu, rtol=1e-9, atol=1e-12)` | float64 不同归约顺序的相对误差上界在 1e-12 量级，1e-9 留 3 个数量级余量；`atol=1e-12` 覆盖接近 0 的读数（如收益率差分） |
| 排序 / 分位（`rank`、`rolling_rank`、分桶） | **逐位相等**（整数秩，`==` 比对） | 浮点容差会掩盖秩的换位；秩不同即分位不同，属于口径分歧而非精度差异 |
| 前向收益 `fwd_h`、超额 `exc_h` | `rtol=1e-9, atol=1e-12` | 同第一行 |
| 回测成交判定（涨跌停 / 停牌 / 一字板 / 整手 / ADV 分档） | **布尔 exact** | 这些是离散判定，任何浮点漂移都可能翻转成交与否，容差在这里是错误的做法 |
| 回测 equity 曲线终值与逐日值 | `rtol=1e-9` | 由成交金额累加而来，成交判定 exact 后金额误差只剩浮点归约 |
| **反向红线** | 若某项必须放宽到 `1e-6` 以上才能通过，判定为**两路口径不同**，必须拒绝上线，不得调大容差 | 防止用放宽阈值掩盖实现分歧 |

### 5.2 cupy / CUDA 版本对应与失配行为

| 安装方式 | 对应 CUDA | 体积（wheel） | 失配时的行为 |
|---|---|---|---|
| `cupy-cuda12x` | CUDA 12.x | 约 400~500 MB | `import cupy` **成功**，首次 `cp.cuda.runtime.getDeviceCount()` 抛 `CUDARuntimeError: cudaErrorInsufficientDriver` 或 `cudaErrorNoDevice` |
| `cupy-cuda11x` | CUDA 11.2~11.x | 约 400~500 MB | 同上 |
| `cupy`（PyPI 名，源码分发） | 由本机 CUDA Toolkit 决定 | 需本地编译 | Windows 无 nvcc 时**安装阶段即失败**；这正是 `pyproject.toml` 写 `cupy = "*"` 的真正后果 |
| `cudf-polars-cu12` | CUDA 12.x | 约 1.5~3 GB | 缺失时 `pl.GPUEngine` 抛 `ModuleNotFoundError`（本次已实测）；版本与 polars 不匹配时抛 `ImportError` 或 `RuntimeError` |

**因此探测逻辑必须分两层**：`import` 成功 ≠ 可用。正确的判定是
`import cupy 成功` **且** `cupy.cuda.runtime.getDeviceCount() > 0` **且**（可选）`cupy.cuda.Device(0).mem_info[0]` 足够大。当前项目只做了第一层。[代码直接证据]

### 5.3 若必须安装验证（仅建议，本次未执行）

| 包 | 装哪 | 体积 | 风险 |
|---|---|---|---|
| `cupy-cuda12x` | **独立 venv 或 conda 环境**，不进主 `.venv` | ~450 MB | 与 numpy 2.5.4 / Python 3.14 的 wheel 兼容性**未验证**；本机无 NVIDIA 卡，装了也**无法验证功能**，只能验证"import 后正确降级" |
| `cudf-polars-cu12` | 同上，且必须与 polars 2.1.0 严格配套 | 1.5~3 GB | 体积大、版本矩阵窄；本机装完仍无法运行 |
| 结论 | **本机不建议安装。** 要在有 NVIDIA 卡的机器上验证，需在该机器上装，并把验证结果作为回归基线 | — | — |

---

## 6. 部署矩阵

### 6.1 路径决策矩阵

路径代号：`CPU` = 纯 CPU，不探测；`GPU?` = 形态适合 GPU 但收益不足，**不实现**；`N/A` = 规模不构成对象（矩阵自由度仅 25）。

| 候选热点 | A. NVIDIA 卡 CUDA 12.x 显存 ≥16 GB | B. NVIDIA 卡但显存 ≤8 GB / 老算力 | C. 无独显（仅核显） | D. AMD 独显（本机 RX 5700 XT） | E. 低配（≤16 GB 内存 / ≤8 核） |
|---|---|---|---|---|---|
| 1. 回测引擎 | `CPU`（唯一可行） | `CPU` | `CPU` | `CPU` | `CPU` |
| 2. 因子批量计算 | `GPU?`（唯一形态合格项；实测不建议） | `CPU` | `CPU` | `CPU` | `CPU` |
| 3. 协方差 / 组合优化 | `N/A`（矩阵仅 25×25） | `N/A` | `N/A` | `N/A` | `N/A` |
| 4. 相似态 bootstrap | `GPU?`（端到端占比 <1%） | `CPU` | `CPU` | `CPU` | `CPU` |
| 5. 面板 join / 重采样 | `GPU?`（需 ≥32 GB 显存才不 OOM） | `CPU`（8 GB 必 OOM） | `CPU` | `CPU` | `CPU` |
| 6. 数据采集 | `CPU`（IO 密集） | `CPU` | `CPU` | `CPU` | `CPU` |

**读法**：整张表里没有一个 `GPU`（无问号）的格子。这是本报告的核心结论。

### 6.2 预期收益与失效表现

| 候选热点 | 预期收益（A 类机器上，若强行实现） | 不实现 / 失效时的表现 | 标识 |
|---|---|---|---|
| 1. 回测引擎 | 0（不可实现，形态不匹配） | 现状不变：15.7 M bar 约 71~131 s；行为完全确定 | [测试直接证据] |
| 2. 因子批量计算 | 34 s → 估 7~15 s（假设 5,000 万行·算子/秒的有效吞吐，含传输） | `cudf_polars` 缺失 → `GPUEngine` 抛错；若用 `raise_on_fail=False` 则整条 plan 回落 CPU，**数值完全一致** | [工程推论] |
| 3. 协方差 / 组合优化 | 0（矩阵仅 25×25，CPU 微秒级；组合优化无矩阵运算） | 现状不变 | [静态分析结果] |
| 4. 相似态 bootstrap | <1% 端到端（0.02~1.6 s 中的一部分） | CPU 路径不变，`seed=42` 下结果可复现 | [测试直接证据] |
| 5. 面板 join | 1.2 s → 估 0.4 s | 显存不足 → `cudf` OOM 异常；`raise_on_fail=False` 时整 plan 回落 CPU，结果一致但白付一次 H2D | [测试直接证据] + [工程推论] |
| 6. 数据采集 | 0 | 现状不变 | [静态分析结果] |

### 6.3 各机型的实际动作

| 机器画像 | 该做什么 | 不该做什么 |
|---|---|---|
| **A. 有 NVIDIA 卡（CUDA 12.x，≥16 GB）** | 装 `poetry install --with gpu` 前必须先修 §7 的判定逻辑；修完后默认 `gpu_enabled='off'`，仅在做 golden 对比时开 | 不要因为"有卡"就默认开启；当前开启等于走上无收益旁路 |
| **B. 有 NVIDIA 卡但低显存** | 保持默认 CPU；`gpu_enabled='off'` | 不要尝试面板 join（必 OOM） |
| **C. 无独显 / 仅核显** | 默认 CPU，零配置；把 `gpu_acceleration.py` 的 import 期 `logger.info` 降为 debug | 不要探测 OpenCL 后误以为有加速 |
| **D. AMD 独显（本机）** | 默认 CPU；**明确记录"AMD 不可用"**，避免后续反复评估 | 不要尝试 ROCm / OpenCL 桥接（gfx1010 无 ROCm 支持，Windows 无 ROCm） |
| **E. 低配** | 默认 CPU；优先做内存与 IO 优化（分块读取、列裁剪） | 不要增加任何探测开销（探测本身要 import 大包，低配机器上 import 失败更慢） |

---

## 7. 结论与第一步建议

### 7.1 明确结论

| 问题 | 回答 |
|---|---|
| 这台机器上 GPU 值不值得用？ | **不值得，也不可用。** 机器有 GPU（AMD RX 5700 XT 独显 + Intel 核显），但**未检测到 NVIDIA 设备**，cupy 无法使用；AMD 侧 gfx1010 不在 ROCm 支持范围且 Windows 不支持 ROCm；Intel 核显无对应 Python 绑定。 |
| 用在哪？ | **没有任何一处。** 六项候选中：回测引擎、数据采集、相似态去重叠逻辑在**形态上**就与 GPU 不匹配（Python 标量控制流 / 网络 IO / 哈希表循环）；相关矩阵与 Ridge 在**规模上**不够（自由度只有 K ≈ 25）；剩余两项（因子批量计算、bootstrap 与面板 join）形态匹配，但 CPU 已在 1.3 s 内完成，GPU 的传输与启动开销吃掉全部收益。 |
| 跨机器部署时该怎么设计？ | 把 GPU 做成**一层可关闭的探测 + 白名单**，而不是"每一处都写 GPU kernel"。具体：单点探测（§5.2 的两层判定）+ 逐项配置开关（§7.2）+ 双路径 golden 对比测试（§5.1 容差表）。**默认全部 CPU。** |
| 第一步该做什么？ | **先修一个正确性缺陷，而不是先上性能。** 见 §7.3。 |

### 7.2 配置表达方案（三选一的推荐）

| 方案 | 优点 | 缺点 | 结论 |
|---|---|---|---|
| 纯环境变量 | 部署最简单，容器友好 | 无法表达"逐项关闭"的列表语义；批处理脚本里易残留、难追溯 | ❌ |
| 纯配置文件（硬编码） | 可版本化、可审计 | 同一份代码部署到不同机器需改文件；容器里改文件比改 env 麻烦 | ❌ |
| **自动探测 + 配置文件覆盖** | 默认零配置（无卡机器自动 CPU）；有卡机器可通过 `.env` 强制开关；逐项列表用 JSON/pydantic 字段自然表达；复用项目现有 `pydantic-settings` + `.env` 热加载 | 需要写一个探测函数并保证它本身不抛错 | ✅ **推荐** |

落地形态（沿用 `src/utils/config.py` 已有约定，不新增机制）：

| 字段 | 类型 | 默认 | 语义 |
|---|---|---|---|
| `accel_gpu_enabled` | `'auto' \| 'on' \| 'off'` | `'auto'` | `auto` = 两层探测（import 成功 **且** deviceCount > 0）；`off` = 完全不 import cupy/cudf；`on` = 探测失败则启动期直接报错（用于 CI 验证） |
| `accel_gpu_disable` | `list[str]` | `[]` | 逐项关闭，取值如 `['factors', 'analog_bootstrap', 'panel_join']`；空列表 = 全部按探测结果 |
| `accel_gpu_min_free_mb` | `int` | `2048` | 低于该剩余显存判为不可用，避免"能探测到但一跑就 OOM" |

### 7.3 第一步建议（按优先级，均为可回滚）

| 顺序 | 动作 | 为什么是第一步 | 标识 |
|---|---|---|---|
| **1** | 把 `GPU_AVAILABLE` 的判定从「`import cupy` 成功」改为「`import cupy` 成功 **且** `cp.cuda.runtime.getDeviceCount() > 0`」 | 这是**正确性缺陷**，不是性能问题。当前在有卡机器上装 cupy 会静默启用一条不加速且绕过 `global_indicator_manager` 的旁路，导致指标计算入口与 dtype 归一行为改变。修它不需要 GPU 硬件，本机即可用 monkeypatch 测试 | [代码直接证据] |
| **2** | 把 `gpu_acceleration.py` 模块 import 期的 4 条 `logger.info` 降为 `logger.debug` | 违反"默认 CPU 不刷屏"约束（§5 第 2 条），且每进程 import 都打一遍 | [代码直接证据] |
| **3** | 为 `calculate_with_gpu` 与 `global_indicator_manager.calculate_indicator` 增加一组**逐元素比对测试**（复用 `tests/test_indicator_consistency.py` 已有的 golden 思路），按 §5.1 的 `rtol=1e-9 / atol=1e-12` 与秩逐位相等判定 | 没有等价性测试就无法安全地"有卡开启"，也无法证明"无卡降级不改变结果" | [静态分析结果] |
| **4** | 新增 `accel_gpu_*` 三个配置字段（§7.2），默认全 CPU | 满足"可单独关闭"约束，为将来真要上 GPU 时留出开关，但不做任何 kernel | [工程推论] |
| **5** | 在 `pyproject.toml` 把 `cupy = "*"` 改为注释说明 `cupy-cuda12x`/`cupy-cuda11x` 二选一，或直接删除该组并在文档中给安装命令 | 避免后人执行 `poetry install --with gpu` 时触发源码编译失败 | [工程推论] |

**明确不做的**：不在任何候选热点上实现 GPU kernel；不安装 `cupy` / `cudf` / `torch`；不改回测引擎的数据结构。若将来出现**新需求**（如百万次蒙特卡洛、分钟级全市场面板、神经网络因子），再按 §5 的五条约束重新评估——届时的判断依据仍然是"单次 kernel 的数据量是否 ≥10⁶ 元素且常驻显存"。

### 7.4 结论标识汇总

| 结论 | 标识 |
|---|---|
| 本机有 GPU（AMD + Intel）但未检测到 NVIDIA 设备；cupy 不可用属于「有卡但非 NVIDIA」 | [测试直接证据] |
| `cupy` / `cudf` / `torch` / `pyopencl` 均未安装，`pl.GPUEngine` 抛 `cudf_polars` 缺失 | [测试直接证据] |
| 四个 `calculate_*_gpu` 已全部为 CPU 实现，`to_gpu` 无调用者 | [代码直接证据] |
| `cupy = "*"` 由 `c0bdf91` 引入且从未改动，是源码分发、默认装不上 | [代码直接证据] + [工程推论] |
| 回测引擎是 Python 级标量双层循环，实测 4.24~8.33 µs/bar，全市场 71~131 s | [代码直接证据] + [测试直接证据] |
| 因子/rolling 实测 13.97 M 行·算子/秒；join 实测 1.2 s / 8.2 GB | [测试直接证据] |
| bootstrap 2000×50,000 实测 1.585 s，端到端占比 <1% | [测试直接证据] |
| 相关矩阵与 Ridge 的自由度只有 K ≈ 25（`np.corrcoef` / `np.linalg.solve` 各 3 处），矩阵填不满一个 CUDA warp | [静态分析结果] |
| 采集链路 `src/data` 无跨票批量数组运算，仅 parquet 读写与单票小数组（<100） | [静态分析结果] |
| 各热点"若上 GPU"的具体加速倍数 | [未验证]（无硬件可测，仅按传输带宽与 kernel 开销做量级推断） |
| RX 5700 XT 的实际 FP32 算力与 ROCm 支持边界 | [工程推论]（基于架构代号 gfx1010 与公开支持列表，本机无法实测） |

---

## 8. 第一步修复的执行记录（2026-11-10，已改未提交）

状态：**代码已改、测试已过，按交付纪律暂不 `git add` / `git commit` / 不做备份同步**，等待与另一条在跑会话（ST 历史接线）统一提交。

### 8.1 改动清单

| 文件 | 变化 | 内容 |
|---|---|---|
| `src/tech_analysis/gpu_acceleration.py` | +169 / -68 | ① 新增 `detect_gpu() -> (available, reason)`，原因码 `available` / `cupy_missing` / `no_device` / `driver_mismatch`；② 新增 `detect_opencl()`，同口径（`import` 成功不算可用）；③ 四个 `calculate_*_gpu` 统一经 `global_indicator_manager` 计算并做同一 Float64 归一，消除旁路；④ 模块级探测日志由 `logger.info` 降为 `logger.debug`，无卡机器不再刷屏；⑤ 删除不再使用的直接公式导入（`calculate_moving_average` / `macd_exprs` / `rsi_expr` / `_sar_core`） |
| `tests/test_gpu_acceleration_fallback.py` | 新增 171 行 | 15 个用例：4 个探测原因码 + 1 个「仅可 import 不算可用」反例守卫 + 8 个回退逐位等价（4 指标 × 2 场景）+ 2 个分支可达性 |

```
 src/tech_analysis/gpu_acceleration.py | 237 ++++++++++++++++++++++++----------
 1 file changed, 169 insertions(+), 68 deletions(-)
```

### 8.2 关键实现：判定分两层

```python
def detect_gpu() -> tuple[bool, str]:
    cupy = _import_cupy()                      # 第一层：库是否存在
    if cupy is None:
        return False, GPU_REASON_CUPY_MISSING
    try:
        device_count = cupy.cuda.runtime.getDeviceCount()   # 第二层：是否真有设备
    except Exception:
        return False, GPU_REASON_DRIVER_MISMATCH
    if device_count <= 0:
        return False, GPU_REASON_NO_DEVICE
    return True, GPU_AVAILABLE_CODE
```

回退等价性的做法是**取消两条实现**，而不是让两条实现尽量对齐：四个 `calculate_*_gpu` 无论 GPU 是否可用，都经 `_calculate_via_manager()` 调 `global_indicator_manager.calculate_indicator()`，并执行与 `streaming_processor` 相同的 Float64 归一。因为只有一条计算实现，"降级后结果不一致"在结构上不可能发生；将来接入真实 kernel 时只替换该方法内部的计算部分，入口与 dtype 归一不变。

### 8.3 测试实测

| 命令 | 结果 |
|---|---|
| `pytest tests/test_gpu_acceleration_fallback.py -q` | **15 passed** |
| `pytest tests/test_indicator_consistency.py tests/test_optimization.py -q` | **22 passed**（既有断言一条未改） |
| `ruff check src/tech_analysis/gpu_acceleration.py tests/test_gpu_acceleration_fallback.py` | **All checks passed** |

等价性断言用 `np.testing.assert_array_equal`（**逐位相等**，非 `allclose`），覆盖 `cupy_missing` 与 `no_device` 两种不可用状态下的 MA / MACD / RSI / SAR 四指标。[测试直接证据]

### 8.4 「import 成功即可用」判据的排查结果

| 位置 | 判据 | 是否有同类缺陷 | 处置 |
|---|---|---|---|
| `gpu_acceleration.py:GPU_AVAILABLE` | `import cupy` 成功 | ✅ **有** | 已改为 `detect_gpu()` 两层判定 |
| `gpu_acceleration.py:OPENCL_AVAILABLE` | `import pyopencl` 成功 | ✅ **有**（且模块级 `cl` 在无设备时仍非 None） | 已改为 `detect_opencl()`，要求实际枚举到 GPU 设备 |
| `numba.cuda` 使用点 | 全仓无 `from numba import cuda` / `@cuda.jit`，仅用 `@njit` 做 CPU JIT | ➖ 无对象 | 不涉及 |
| `torch` / `cudf` / `pycuda` | `src/` 内无 `import`（`grep` 零命中） | ➖ 无对象 | 不涉及 |
| `src/quant/reports/quantstats_report.py:_QUANTSTATS_AVAILABLE` | `import quantstats` 成功即先置标志 | ❌ 无缺陷（纯 Python 报表库，无硬件依赖，import 成功即可用） | 不需要改 |
| `src/alpha/evaluation/alphalens_adapter.py:_ALPHALENS_AVAILABLE` | 同上 | ❌ 无缺陷（同上） | 不需要改 |
| `polars` GPU engine | 未使用 `pl.GPUEngine`；真实调用会抛 `ModuleNotFoundError: cudf_polars` | ➖ 目前无包装层 | 将来若接入，必须用 `raise_on_fail=False` 或先探测 |
| `src/data/managers/data_fetcher.py:source.is_available()` | 数据源健康检查（网络可达性） | ➖ 非 GPU 判据 | 不涉及 |

**排查结论**：`import 成功即认为可用`这类错误判据在仓库里只有 GPU 相关的两处（`cupy`、`pyopencl`），本次已一并统一；其余两处库可用性标志（quantstats / alphalens）不涉及硬件，不存在同类问题。[静态分析结果]

### 8.5 尚未做的事（下一步）

| 项 | 状态 | 说明 |
|---|---|---|
| `accel_gpu_enabled` / `accel_gpu_disable` / `accel_gpu_min_free_mb` 三个配置字段 | 未做 | §7.2 已给设计，需改 `src/utils/config.py`；本次只修正确性缺陷，不引入新配置面 |
| `pyproject.toml` 的 `cupy = "*"` 改为注释或删除 | 未做 | 属依赖声明调整，建议与配置字段同批做 |
| 提交与备份同步 | 未做 | 按交付纪律，等两条会话都完成后由主控统一安排 |

