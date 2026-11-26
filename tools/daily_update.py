#!/usr/bin/env python3

"""日更 / 补数单入口：一条命令跑完当日全部数据维护，一行汇总收尾。

步骤顺序（与 ``STEP_PLAN`` 一致，dry-run 会原样打印）::

     1. 证券码表        tools/build_names_json.py → tools/build_name_history.py → tools/build_stock_basic.py
     2. 行业快照        python -m src.data.industry refresh
     3. 行情采集入库    src.data.tencent.daily_fill.fill_daily_gap(workers=1) + LocalQuoteStore.flush_meta()
     4. 本地行情构建    python -m src.data.local_store build（增量 + 换手派生 + 异常清单）
                       收尾断言：meta/daily_index 末日 == 交易日历最近已完成交易日，不等即判失败
                       本步带 --skip-factor-verify：因子副本一致性断言依赖第 7 步结果，见下
     5. 基金行情刷新    python -m src.data.local_store build --data-type fund（增量，末日已最新则跳过）
     6. 期货行情增量    tools/fetch_futures_daily.py（按交易日增量追加，已覆盖日跳过；
                       每日 1 次请求，不再整只跳过已存在合约）→
                       tools/build_futures_continuous.py --apply（重建连续/换月/合约三表）
                       实测（2026-11-22）无新交易日 ≈20 秒（采集 9 + 重建 11），每新增
                       交易日 +≈1.5 秒；某日失败即退出且不写该日进度，下次重跑从该日续；
                       非致命，失败只告警
     7. 复权因子刷新    factor_rebuild / fund_factors 仅重算「行情末日 > 因子末日」的标的（已最新则跳过）
                       刷新后复验行情内嵌因子副本与快照逐行一致（个股/指数/基金）
     8. 月度分红全量    tools/refresh_dividend_monthly.py（季度/半年度除权除息 + 月度分红全量 +
                       因子重建，5 阶段串行；非致命，失败只告警）
     9. 分红日频增量    tools/refresh_dividend_daily.py（基金年度批量 + 个股近两报告期，
                       只追加/补缺并重建受影响标的因子；非致命，失败只告警）
      9.1 公告事件库     tools/update_events.py --date <目标日> --allow-empty（逐自然日抓取 +
                       稳定化收敛 → 追加入库；稀疏关键词结构正常的 0 条按「当日无公告」
                       接受，结构异常仍判失败；非致命，失败只告警）
    10. 申万行业指数    tools/build_sw_industry_index.py --re-pull（801xxx 段 31 只，幂等只追加；
                       源端 OHLC 只到 2026-11-09，不追 T 日、不伪造，尾缺口记侧车 tail_gap；
                       非致命，失败只告警）
    11. ST 快照与区间表  src.data.risk_flags refresh + src.data.st_history build
    12. 名录每日检查    src.main --update-fund / --update-index（有变更才落盘）
    13. 估值增量        tools/collect_financials.py --only valuation --since auto
     13.1 两融明细      tools/fetch_margin_detail.py --end <目标日>（T+1 发布：只补
                       已开的往日交易日，源端 0 行记 [SKIP] 跳过而不失败，下一轮重试；
                       非致命，失败只告警）
    14. 指数月末估值    tools/collect_index_valuation.py --apply（红利择时信号，月末执行）
    15. 因子面板增量    tools/factor_panel.py --apply --mode incremental --date <目标日>
                       （只算当日截面，按 signal_date 分区 upsert；非致命，失败只告警）
    16. B 候选名单      tools/b_topn_picks.py --date <目标日>（仅目标日为月末调仓日
                       时产出：申万一级行业内前 2 → 合并前 50 等权；只写 reports/）
    17. 市场宽度增量    src.data.services.breadth_service.refresh
    18. ST 逐日面板     tools/build_st_daily.py --since <窗口月首> --incremental
                       只重写最近 N（默认 5）个交易日所在月分区；月末当天即整月重建，
                       窗口跨月时上月一并重写；历史 ST 输入指纹变了或月分区有空洞
                       则转全量（见 ``_derived_window``）
    19. 涨跌停逐日      tools/build_limit_board.py --since <同上> --incremental
                       依赖第 18 步的当日 ST 结果：第 18 步失败或面板末日 < 行情末日
                       时本步直接阻断（不使用旧 ST 面板静默生成）
    20. 候选池周更      tools/build_multi_asset_etf_pool.py --apply（未到期不跑）
    21. 回写与校验      manifest 回写 → 逐件 rows/bytes/sha256 对账 → 预期差集为 0
    22. 洁净集快照      python -m src.data.quality.clean_set build
    23. 保鲜体检        src.data.quality.freshness --summary
    24. 版本记录        python -m src.data.data_version（失败只告警，不改数据）
    25. 备份            tools/backup_critical_assets.py（必须在所有写 data/ 的步骤之后）
    26. 门禁            tools/daily_data_gate.py（必须在备份之后）
    27. 数据集体检      tools/dataset_health_check.py（只读收尾闸门：异常数非 0 时
                       日更整体判失败并逐条打印 ERROR/WARNING，不回滚已入库步骤；
                       一页摘要写入 ``_staging/<批次>/dataset_health_summary.md``）

口径与边界::

    - 步骤顺序不是随意的：第 4 步 build 的收尾门禁「行情内嵌因子副本 == 因子
      快照」需要基金行情与基金因子都已刷新到目标日，而这两件事分别在第 5、6
      步。放在第 4 步等于让断言依赖后面步骤的结果——基金滞后时第 4 步必然失败，
      修复窗口（第 6 步）却排在其后，形成死锁（2026-11-17 两次补跑均卡在此处）。
      因此第 4 步只做「本地构建 + 末日断言」，因子副本一致性断言统一挪到第 6 步
      末尾：刷新完成后再校验，覆盖本次运行刷新的全部标的，也仍能拦住真正的滞后。
    - 每步独立记录成败，某步失败只记录并继续后续步骤；最后一行汇总给出
      「N 步 ok / 失败 M（致命 K）」与总耗时，失败步逐条列出明细。
    - 致命步（行情采集、本地构建、基金行情刷新、复权因子刷新、回写校验、
      备份、门禁、数据集体检）任一失败，退出码为 1；其余步骤失败只记录，
      不影响退出码。备份/门禁的先后顺序是硬约束，新增步骤只能插在它们之前；
      唯一的例外是只读的数据集体检，它必须排在门禁之后做最后一道闸门，且
      只改退出码、不回滚也不阻断前面已完成的入库步骤。
    - ST 逐日/涨跌停逐日固定排在「市场宽度」之后（第 16、17 步）：两者都只吃
      行情 + 名称/ST 区间，涨跌停还要读当天的 ST 面板，故必须先后相邻；第 16 步
      失败或面板末日 < 行情末日时，第 17 步直接判失败，不落旧 ST 结果。
      增量窗口 = 最近 5 个交易日所在月分区（月末当天即整月重写，跨月含上月），
      历史 ST 输入（名称段/官方区间/可信覆盖的起始月）逐月指纹变化、月分区出现
      中间空洞或 ``--full-derived`` 时转全量；``--skip-st-daily`` 连带跳过第 17 步。
      实测（2026-11-22，单月分区窗口）：ST 逐日 2.5s、涨跌停逐日 4.2s，日更总
      时长净增约 7s；涨跌停增量跳过需读全历史行情的 breadth 对拍（沿用上一份
      comparison.json / reconcile_daily.parquet）。保鲜体检已把两张表按「每交易日」
      登记（st_daily / limit_board_daily，lag≥1 告警、≥3 严重），增量只重写最新
      月分区，门禁据此判定「最近交易日是否更新」。
    - 采集固定 ``workers=1``（单连接串行），限流参数沿用 TencentClient 默认
      ``min_interval=0.05`` / ``max_retries=3``，本入口不暴露也不修改。
    - 补数先按代码清单把缺口落到 ``_staging/<批次>/``（``gaps.parquet`` +
      ``input_set.json``，后者带输入集合哈希），再一次 apply；同一批次名
      不允许换一份输入集合复用，防止「补一批 → 复跑 → 再补」。
    - 校验不抽样、不跑全库 ``manifest_tool --check``：只核本次改动件。
    - ``quotes/manifest.json`` 是人工维护的全市场总账（仓库内无产者，含
      futures 口径），本入口只做只读对账（universe 分市场求和 == universe
      合计），不代写数值；该件与磁盘的偏差按 WARN 打印实测差额。
    - 候选池是筛选结果（视图层），不承担数据完整性；被排除标的的行情/分红
      由保鲜体检的全集判据负责。
    - 申万 801xxx 是「源端只更新到此为止」的已知滞后家族：本步只追加、不追
      T 日；侧车 ``tail_gap`` 每次刷新（无新增也刷新），保鲜体检凭它判「已知
      源端滞后」而不计断裂，源端补齐后自动恢复为正常判据。
    - 因子面板增量排在估值增量之后：面板以 ``valuation_daily`` 为驱动表，估值
      当日未落盘时算不出当日截面；放前面会让面板长期落后一个交易日。该步只算
      目标日截面并按 ``signal_date`` 分区 upsert，失败只告警、不阻断后续步骤。
    - 默认不打印各步过程日志，失败时才回显该步末尾输出；``--verbose`` 打开
      INFO/DEBUG 与子进程原样输出。

用法::

    # dry-run（默认，不联网、不落盘；打印完整步骤清单与顺序）
    python tools/daily_update.py
    # 日更 apply（一条命令跑完全部步骤，目标交易日自动探测）
    python tools/daily_update.py --apply --allow-remote
    # 脱离父终端拉起 apply（拉起方退出/被硬杀不影响子进程；输出仍落 logs/ 控制台日志）
    python tools/daily_update.py --detach --apply --allow-remote
    # 日更 apply 后顺带出 v3 目标持仓清单（只出清单与台账，不下单）
    python tools/daily_update.py --apply --allow-remote --target-holdings --capital 1000000000
    # 补数（按代码清单；缺口先落 _staging/<批次>/ 再一次 apply）
    python tools/daily_update.py --codes 600000.SH,000001.SZ --end 2026-11-13 \
        --batch gap_20261113 --apply --allow-remote
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from typing import Any, Callable, Iterable, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import polars as pl  # noqa: E402
from loguru import logger  # noqa: E402

from src.data.local_store.paths import default_store_root  # noqa: E402
from src.data.local_store.store import (  # noqa: E402
    LocalFactorStore,
    LocalQuoteStore,
    verify_quote_factor_consistency,
)
from src.data.hash_cache import (  # noqa: E402
    ENV_CACHE_PATH,
    FactsCache,
    full_recompute_requested,
)
from src.data.manifest_refresh import file_facts, refresh_manifest_files  # noqa: E402

DATA_SUFFIXES = ('.parquet', '.csv', '.tsv', '.txt')
STAT_KEYS = ('rows', 'bytes', 'sha256')
STORE_PREFIX = 'data/local_store'
#: manifest 哈希事实缓存（``work/`` 已被 gitignore；可用 ``QUANT_MANIFEST_HASH_CACHE`` 覆盖）。
MANIFEST_FACTS_CACHE = Path('work') / 'cache' / 'manifest_facts.json'
#: 日线收盘判定口径（与 ``src.data.tencent.daily_fill`` 一致）：15:05 前当日 K
#: 线未完成，最近「已完成」交易日取前一交易日。
CLOSE_CUTOFF = dtime(15, 5)
#: 末根距目标交易日超过该天数视为长期停更/退市，不进待补清单（与 fill_daily_gap 默认同口径）。
DEFAULT_MAX_GAP_DAYS = 90
#: build 前的「通达信日结已落地」守卫：盘后数据实测 16:23:48 才落地（曾晚于日更启动），
#: 链路本身无停等，故 build 前按每 5 分钟重试、最多等 40 分钟；超时判该步失败。
TDX_LANDING_POLL_SECONDS = 300
TDX_LANDING_MAX_WAIT_SECONDS = 2400
#: 探测落地的优先样本（覆盖沪指/深成指/主板个股），命中即返回，避免每次全目录扫。
TDX_LANDING_PROBES = (('sh', '000001'), ('sz', '399001'), ('sh', '600000'), ('sz', '000001'))
#: 采集写盘后由两条既有链路产出的非行情件（构建收尾 / 派生 / 台账）。
SIDE_OUTPUTS = (
    'meta/daily_index.parquet',
    'meta/daily_index.manifest.json',
    'meta/state.json',
    'meta/anomalies/manifest.json',
    'factors/stock_turnover.parquet',
    'factors/manifest.json',
    'factors/meta.json',
    'factors/covered_codes.txt',
    # 目录级登记：本次写盘的 meta 件按磁盘实测回填（见 _refresh）
    'meta/manifest.json',
    # 受控写入的自动登记 ledger：每次 write_dataset 都会追加，属恒变动件
    'meta/registry/auto_register.json',
    # 同目录第二个 ledger：受控写入以 --allow-standalone 放行时另记一份 JSONL。
    # 日更本身不写它，但同批次若跑过补采/单表工具就会追加；不登记会漏成预期外改动。
    'meta/registry/standalone_writes.jsonl',
)
#: 构建收尾会整目录重写的台账（内容随行情变动，逐件白名单不现实）。
SIDE_PREFIXES = ('meta/anomalies/', 'factors/rebuild/',
                 'meta/financials/parts/valuation/', 'meta/industry/',
                 # 派生表（市场宽度 / 状态向量）按目录整体重写：表 + 目录 manifest
                 'meta/breadth/', 'meta/state/',
                 # ST 逐日面板与涨跌停逐日由整目录产出（月分区 + manifest +
                 # comparison + reconcile + 增量状态），逐个枚举不如按目录放行。
                 'meta/st/daily/', 'meta/limit/',
                 # 两融明细按月分区整体重写（月分片 + completeness + manifest）
                 'meta/margin_detail/',
                 # 停复牌日表全量重算（表 + coverage + 目录 manifest）
                 'meta/suspension/')
#: ST 区间表日更（risk_flags refresh + st_name_history build + st_history build）会写的件。
#: supplement 由 ``st_name_history build`` 重建，覆盖窗口随行情末日推进；不重建会
#: 让沪市/北交所 2,324 只的 ``coverage_end`` 停在最后一次手工构建日。
AUX_ST_OUTPUTS = (
    'meta/st/st_intervals.parquet',
    'meta/st/st_coverage.parquet',
    'meta/st/coverage.json',
    'meta/st/manifest.json',
    'meta/st/supplement/supplement_intervals.parquet',
    'meta/st/supplement/supplement_coverage.parquet',
    'meta/st/supplement/unresolved.csv',
    'meta/st/supplement/manifest.json',
    'meta/st_status/st_status_history.parquet',
    'meta/st_status/manifest.json',
)
#: 估值日更会写的件（最终表 + 断点；分片按 ``meta/financials/parts/valuation/`` 前缀计入）。
#: ``write_dataset`` 补丁数据文件时必定同写父目录 manifest，登记表文件就必须一并登记它，
#: 否则 11-25 那轮把 ``meta/financials/manifest.json`` 报成预期外改动。
AUX_VALUATION_OUTPUTS = (
    'meta/financials/valuation_daily.parquet',
    'meta/financials/manifest.json',
    'meta/financials/parts/progress.json',
)
#: 因子面板增量会写的件（当日截面按 signal_date 分区 upsert）。
AUX_FACTOR_PANEL_OUTPUTS = (
    'factors/factor_panel.parquet',
    'factors/factor_panel.manifest.json',
)
#: 复权因子刷新会写的件（个股 + 基金快照、sidecar 与目录级 manifest；
#: 分块按 ``factors/rebuild/`` 前缀计入）。行情副本由同一步一并重写。
AUX_FACTOR_OUTPUTS = (
    'factors/stock_adj_factor.parquet',
    'factors/fund_adj_factor.parquet',
    'factors/fund_covered_codes.txt',
    'factors/fund_meta.json',
    'factors/rebuild/manifest.json',
)
#: 分红日频增量会写的件（两张事件表 + 状态台账；受影响标的的复权因子与
#: 行情副本由同一步重建，按 ``AUX_FACTOR_OUTPUTS`` 与行情路径另行登记）。
AUX_DIVIDEND_OUTPUTS = (
    'factors/stock_dividend.parquet',
    'factors/fund_dividend.parquet',
    'meta/dividend_status/daily_refresh_state.json',
)
#: 公告事件库日更会写的件（正式表 + 覆盖声明；分片落 research/events 不在列存根下）。
AUX_EVENTS_OUTPUTS = (
    'meta/events/events.parquet',
    'meta/events/coverage.json',
    # 同上：事件表走 write_dataset，父目录 manifest 由同一次写入改写
    'meta/events/manifest.json',
)
#: 证券码表日更会写的件（通达信当日码表 → 名称历史 → 名录）。
AUX_CODE_TABLE_OUTPUTS = (
    'meta/names.json',
    'meta/name_history.parquet',
    'meta/name_history.manifest.json',
    'meta/name_history_coverage.json',
    'meta/stock_basic.parquet',
    'meta/stock_basic.manifest.json',
)
#: 名录周更会写的件（首件用于判断距上次刷新是否超过阈值）。
AUX_BASIC_OUTPUTS = {
    'fund': ('meta/fund_basic.parquet', 'meta/fund_basic.manifest.json'),
    'index': ('meta/index_basic.parquet', 'meta/index_basic.manifest.json'),
}
#: 名录默认检查周期（自然日）：每天检查一次，有变更才落盘；体检按最后检查日期判
DEFAULT_BASIC_REFRESH_DAYS = 1
#: 候选池周更会写的件（首件用于判断距上次重建是否超过阈值）。
AUX_POOL_OUTPUTS = (
    'meta/multi_asset/etf_pool.parquet',
    'meta/multi_asset/etf_pool.csv',
    'meta/multi_asset/etf_gap_detail.csv',
    'meta/multi_asset/etf_event_gaps.csv',
    'meta/multi_asset/etf_pool_excluded.json',
    'meta/multi_asset/manifest.json',
)
#: 候选池默认重建周期（自然日）
DEFAULT_POOL_REFRESH_DAYS = 7
#: 月度分红全量的到期天数与硬超时（2026-11-24）：基金逐只取证 1,696 只 × 3~5 秒，
#: 实测 89~91 分钟（11-20 / 11-23），原 1800s 超时每天必然截断。改为距上次完成
#: ≥ 该天数才跑，超时放宽到 3 小时；未完成的断点（state.json status≠done）不受到期限制，下轮续跑。
DEFAULT_DIVIDEND_MONTHLY_DAYS = 28
DIVIDEND_MONTHLY_TIMEOUT_S = 3 * 3600
#: ST 逐日 / 涨跌停逐日增量窗口：最近 N 个交易日所在月分区（跨月自然含上月）
DERIVED_LOOKBACK_SESSIONS = 5
#: 期货日更会写的件（三张元数据表 + 侧车；单合约日线按 ``quotes/futures/`` 前缀另行登记）。
AUX_FUTURES_OUTPUTS = (
    'meta/multi_asset/futures_contracts.parquet',
    'meta/multi_asset/futures_continuous.parquet',
    'meta/multi_asset/futures_roll_events.parquet',
    'meta/multi_asset/futures_manifest.json',
)
#: 宏观两融日更（margin_daily publish）会写的件：正式表 + CSV 派生物 + 目录 manifest。
AUX_MACRO_OUTPUTS = (
    'meta/macro/cn_market_margin_daily.parquet',
    'meta/macro/cn_market_margin_daily.csv',
    'meta/macro/manifest.json',
)

#: 市场宽度日更会写的件（增量补齐，行情末根未前进时不落盘）。
AUX_BREADTH_OUTPUTS = (
    'meta/breadth/breadth_daily.parquet',
    'meta/breadth/coverage.json',
    'meta/breadth/manifest.json',
)
#: 指数月末估值会写的件（红利择时信号 ``index_pe_pct`` 的唯一数据源）。
AUX_INDEX_VALUATION_OUTPUTS = (
    'meta/index_valuation/index_valuation.parquet',
    'meta/index_valuation/manifest.json',
)
#: 增量清点跳过 ``backtest_runs``：回测运行记录是结果归档（json/csv/log 与索引），
#: 不是数据表，日更既不产出也不消费它，计入会让「本次新增文件」混入非数据资产。
SKIP_DIRS = {'_backup', '__pycache__', 'backtest_runs'}
#: 单入口统一日志（JSONL 追加：run_start / step_start / step_end / run_end）。
DAILY_LOG = ROOT / 'work' / 'logs' / 'daily_update.log'
#: 控制台落盘目录：daily_YYYYMMDD_console.log（父进程与各子进程的 stdout/stderr）。
CONSOLE_LOG_DIR = ROOT / 'logs'
#: 入口锁：同一时间只允许一个写数据进程；持有超过该小时数即判陈旧并归档告警。
LOCK_PATH = ROOT / '_locks' / 'daily_update.lock'
LOCK_MAX_AGE_HOURS = float(os.environ.get('QUANT_DAILY_LOCK_MAX_HOURS', '12'))
#: 脱离父终端拉起：``--detach`` 经一个「拉起即退出」的中继进程两跳启动，
#: 最终进程的父进程已消失，父终端的 taskkill /T 与作业对象关闭都追不到它。
#: 不需要管理员权限（不注册计划任务、不装服务）。
DETACH_RELAY_FLAG = '--_detach-relay'
DETACH_ENV = 'QUANT_DAILY_DETACHED'
#: ``--detach`` 启动自检窗口（秒）：拿到 pid 后须在此窗口内看到子进程的 run_start，
#: 且窗口结束时子进程仍存活；否则判启动失败并留 detach_failed 事件。
DETACH_START_CHECK_SECONDS = 10.0
_SELF = Path(__file__).resolve()
_CREATE_NEW_PROCESS_GROUP = 0x00000200
_DETACHED_PROCESS = 0x00000008
_CREATE_BREAKAWAY_FROM_JOB = 0x01000000

#: 单入口步骤顺序（dry-run 与 apply 共用）：(键, 名称, 是否致命)。
#: 硬约束：备份在所有写 ``data/`` 的步骤之后，门禁在备份之后。
#: 顺序理由：``build``（第 4 步）跑的是 ``--skip-factor-verify``，因为它自带的
#: 「行情内嵌因子副本 == 因子快照」断言依赖第 5 步（基金行情刷新）与第 7 步
#: （复权因子刷新）。断言若留在第 4 步，基金滞后时第 4 步必红、而能修复它的
#: 第 7 步排在后面，形成死锁；故断言收窄到第 7 步刷新完成后执行。
#: 期货（第 6 步）插在行情族（3~5 步）之后、派生层（7 步起）之前：只依赖自己的
#: 采集与构建，不被任何股票派生步骤消费。
STEP_PLAN: tuple[tuple[str, str, bool], ...] = (
    ('code_table', '证券码表（names.json / name_history / stock_basic）', False),
    ('industry', '行业快照', False),
    ('fill', '行情采集入库', True),
    ('build', '本地行情构建 + 末日断言', True),
    ('fund_quote', '基金行情刷新（通达信本地增量）', True),
    ('futures', '期货行情增量（按交易日追加 + 连续合约重建）', False),
    ('factor', '复权因子刷新（个股 + 基金）', True),
    ('dividend_monthly', '月度分红全量（季度/半年度除权除息 + 月度分红 + 因子重建）', False),
    ('dividend', '分红日频增量（基金年度批量 + 个股近两报告期）', False),
    ('events', '公告事件库日更（逐自然日抓取 + 稳定化收敛）', False),
    ('sw_index', '申万行业指数增量（801xxx 只追加）', False),
    ('st', 'ST 快照与区间表', False),
    ('basic', '名录每日检查（fund / index）', False),
    ('valuation', '全市场估值增量', False),
    ('margin', '融资融券明细日更（T+1 发布，未发布按跳过）', False),
    ('index_valuation', '指数月末估值（红利择时信号，月末执行）', False),
    ('factor_panel', '因子面板增量（当日截面按 signal_date 分区 upsert）', False),
    ('b_topn_picks', 'B 股东盈余比候选名单（月末调仓日：行业内前 2 → 前 50 等权）', False),
    ('breadth', '市场宽度增量', False),
    ('breadth_ma', '均线占比宽度（breadth_ma_daily，依赖 build）', False),
    ('state_vector', '市场状态向量（state_vector_daily，依赖 breadth_ma）', False),
    ('macro', '宏观两融日更（源端缺口补齐，需 --allow-remote）', False),
    ('st_daily', 'ST 逐日面板增量（最近 N 个交易日所在月分区）', False),
    ('limit_daily', '涨跌停逐日增量（依赖 ST 逐日当日结果）', False),
    ('pool', '候选池周更', False),
    # 2026-11-25：停复牌日表原只能手动跑 extended_builders，曾落后行情 8 个交易日；
    # 只读本地行情派生，放在所有行情/因子写入之后、manifest 之前，非致命。
    ('suspension', '停复牌日表（交易日历 − 实际 bar 日，全量重算）', False),
    ('manifest', 'manifest 回写与校验', True),
    ('clean_set', '洁净集快照', False),
    ('freshness', '保鲜体检', False),
    ('version', '数据版本记录', False),
    ('backup', '不可重建资产备份', True),
    ('gate', '每日数据门禁', True),
    # 只读收尾闸门：放在门禁之后，异常只改退出码，不回滚已完成的入库步骤。
    ('dataset_health', '数据集体检（一页摘要入日更日志）', True),
)


# ----------------------------------------------------------------------
# 小工具
# ----------------------------------------------------------------------
def _parse_date(text: str | None) -> date | None:
    if not text:
        return None
    raw = str(text).strip().replace('-', '')
    if len(raw) != 8 or not raw.isdigit():
        raise argparse.ArgumentTypeError(f'日期需为 YYYY-MM-DD 或 YYYYMMDD：{text}')
    return date(int(raw[:4]), int(raw[4:6]), int(raw[6:]))


def _sha256_text(items: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for item in items:
        digest.update(item.encode('utf-8'))
        digest.update(b'\n')
    return digest.hexdigest()


def _coverage(root: Path) -> dict[tuple[str, str], dict[str, Any]]:
    """读 ``meta/daily_index.parquet`` 作为覆盖度（口径以该表为准）。"""
    import polars as pl  # noqa: PLC0415

    path = root / 'meta' / 'daily_index.parquet'
    if not path.is_file():
        return {}
    frame = pl.read_parquet(path, columns=['ts_code', 'data_type', 'rows', 'start', 'end'])
    return {(row['data_type'], row['ts_code']): row for row in frame.to_dicts()}


def _to_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _daily_index_end(root: Path, kinds: Sequence[str] | None = None) -> date | None:
    """``meta/daily_index.parquet`` 的末日（可按 data_type 过滤）；只读。"""
    path = root / 'meta' / 'daily_index.parquet'
    if not path.is_file():
        return None
    try:
        frame = pl.read_parquet(path, columns=['data_type', 'end'])
    except (OSError, pl.exceptions.PolarsError):
        return None
    if kinds is not None:
        frame = frame.filter(pl.col('data_type').is_in(list(kinds)))
    if frame.is_empty():
        return None
    return _to_date(frame['end'].max())


def _check_daily_index_end(root: Path, expected: date) -> tuple[bool, str]:
    """断言行情末日 == 最近交易日；返回 (是否通过, 说明)。

    只读 ``meta/daily_index.parquet``，不做任何写入，负向用例可直接构造
    一份末日不符的 daily_index 证明它会红。
    """
    actual = _daily_index_end(root)
    if actual is None:
        return False, 'meta/daily_index.parquet 缺失或无 end'
    if actual != expected:
        return False, f'daily_index 末日 {actual} != 最近交易日 {expected}'
    return True, f'daily_index 末日 {actual} = 最近交易日 {expected}'


def _recent_completed_trade_day(root: Path, now: datetime | None = None) -> date:
    """交易日历里最近一个「已完成」交易日（15:05 前当日 K 线未完成，取前一交易日）。"""
    from src.data.trading_calendar import get_calendar  # noqa: PLC0415

    moment = now or datetime.now()
    calendar = get_calendar(root, as_of=moment.date())
    day = moment.date()
    if calendar.is_trading_day(day) and moment.time() < CLOSE_CUTOFF:
        previous = calendar.previous(day, inclusive=False)
    else:
        previous = calendar.previous(day, inclusive=True)
    if previous is None:
        raise RuntimeError(f'交易日历在 {day} 之前没有可用交易日')
    return previous


def _tdx_lday_end(tdx_path: Path, target: date | None = None) -> date | None:
    """通达信 lday 的末根交易日（只读首尾各 32 字节，复用既有 .day 解析）。

    先探 ``TDX_LANDING_PROBES`` 里的代表性文件，命中 ``target`` 立即返回；
    未命中再全目录扫描取最大末日。全部缺失/解析失败时返回 None。
    """
    from src.data.tdx_day_parser import read_first_last_trade_date  # noqa: PLC0415

    latest: date | None = None
    probes = [tdx_path / market / 'lday' / f'{market}{code}.day'
              for market, code in TDX_LANDING_PROBES]
    for path in probes:
        _, last = read_first_last_trade_date(path)
        if last is not None and (latest is None or last > latest):
            latest = last
        if target is not None and latest is not None and latest >= target:
            return latest

    for market in ('sh', 'sz', 'bj'):
        lday = tdx_path / market / 'lday'
        if not lday.is_dir():
            continue
        for path in lday.glob(f'{market}*.day'):
            _, last = read_first_last_trade_date(path)
            if last is None:
                continue
            if latest is None or last > latest:
                latest = last
            if target is not None and latest >= target:
                return latest
    return latest


def _await_tdx_landing(plan: _Plan, args: argparse.Namespace,
                       target: date) -> tuple[bool, str]:
    """build 前置守卫：等通达信日结落地到 ``target``。

    不适用场景（守卫关闭 / 通达信路径不存在 / 末根已到目标）只做一次只读探测即返回，
    不 sleep、不写盘。超时仍落后则判失败并给出「通达信数据未落地」原因；不放宽
    ``daily_index`` 末日断言，也不写任何数据。
    """
    if not getattr(args, 'tdx_landing_guard', True):
        return True, ''
    config = getattr(plan, 'config', None)
    raw = str(getattr(getattr(config, 'data', None), 'tdx_data_path', '') or '').strip()
    if not raw:
        return True, ''
    tdx_path = Path(raw)
    if not tdx_path.is_dir():
        logger.warning('[daily] 通达信路径不可用（{}），跳过落地守卫', raw or '-')
        return True, ''
    max_wait = float(getattr(args, 'tdx_landing_max_wait', TDX_LANDING_MAX_WAIT_SECONDS)
                     or 0)
    poll = float(TDX_LANDING_POLL_SECONDS)
    started = time.monotonic()
    deadline = started + max_wait
    attempts = 0
    while True:
        attempts += 1
        last = _tdx_lday_end(tdx_path, target=target)
        if last is not None and last >= target:
            waited = time.monotonic() - started
            if attempts == 1:
                return True, ''
            return True, f'通达信末根 {last} 已落地（等待 {waited:.0f}s / {attempts} 次）'
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False, (f'通达信数据未落地：lday 末根 {last or "缺失"} < 目标交易日 {target}，'
                           f'已等 {max_wait:.0f}s（{attempts} 次）仍不到；不写任何数据')
        logger.info('[daily] 等待通达信日结落地：末根 {} < {}，{:.0f}s 后重试',
                    last or '缺失', target, min(poll, remaining))
        time.sleep(min(poll, remaining))


def _quote_end_by_code(root: Path, kind: str) -> dict[str, date]:
    ends: dict[str, date] = {}
    for (data_type, code), row in _coverage(root).items():
        if data_type != kind:
            continue
        value = _to_date(row.get('end'))
        if value is not None:
            ends[code] = value
    return ends


def _factor_end_by_code(path: Path) -> dict[str, date]:
    if not path.is_file():
        return {}
    try:
        frame = (pl.scan_parquet(str(path)).select('ts_code', 'trade_date')
                 .group_by('ts_code').agg(pl.col('trade_date').max()).collect())
    except (OSError, pl.exceptions.PolarsError):
        return {}
    ends: dict[str, date] = {}
    for row in frame.iter_rows(named=True):
        value = _to_date(row['trade_date'])
        if value is not None:
            ends[str(row['ts_code'])] = value
    return ends


def _stale_factor_codes(root: Path, kind: str) -> list[str]:
    """因子末日落后于该标的行情末日的代码（= 本次需增量重算的集合）。"""
    quote_ends = _quote_end_by_code(root, kind)
    if not quote_ends:
        return []
    name = 'stock_adj_factor.parquet' if kind == 'stock' else 'fund_adj_factor.parquet'
    factor_ends = _factor_end_by_code(root / 'factors' / name)
    return sorted(code for code, quote_end in quote_ends.items()
                  if factor_ends.get(code) is None or factor_ends[code] < quote_end)


def _register_quote_paths(plan: _Plan, kind: str, codes: Iterable[str]) -> None:
    """把本步会重写的行情件登记进 manifest 校验白名单。"""
    for code in codes:
        rel = plan.store.quote_path(kind, code).relative_to(plan.root).as_posix()
        plan.expected.add(rel)
        plan.expected.add(Path(rel).with_suffix('.manifest.json').as_posix())
        plan.plan_quote.add(rel)


def _load_codes(args: argparse.Namespace) -> set[str] | None:
    codes: set[str] = set()
    if args.codes:
        codes.update(part.strip().upper() for part in args.codes.split(',') if part.strip())
    if args.code_file:
        path = Path(args.code_file)
        if not path.is_file():
            raise SystemExit(f'代码清单不存在：{path}')
        codes.update(
            line.strip().upper() for line in path.read_text(encoding='utf-8').splitlines()
            if line.strip() and not line.strip().startswith('#'))
    return codes or None


def _manifest_facts_cache(force_full: bool = False) -> FactsCache:
    """manifest 事实缓存：日更默认走增量复用，``QUANT_MANIFEST_FULL_HASH=1`` 强制全量重算。"""
    raw = os.environ.get(ENV_CACHE_PATH, '').strip()
    path = Path(raw) if raw else Path(__file__).resolve().parents[1] / MANIFEST_FACTS_CACHE
    return FactsCache(path, read_cached=not (force_full or full_recompute_requested()))


def _walk(root: Path, marker_ts: float) -> dict[str, int]:
    """返回 mtime 晚于开工标记的文件 → 字节数（跳过 ``_backup``）。"""
    changed: dict[str, int] = {}
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [name for name in dirnames if name not in SKIP_DIRS]
        for name in filenames:
            if name.endswith(('.tmp', '.temp')):
                continue
            path = Path(dirpath) / name
            try:
                stat = path.stat()
            except OSError:
                continue
            if stat.st_mtime > marker_ts:
                changed[path.relative_to(root).as_posix()] = stat.st_size
    return changed


# ----------------------------------------------------------------------
# 计划：缺口定位 + 输入集合哈希（落 _staging/<批次>/）
# ----------------------------------------------------------------------
def _plan_gaps(coverage: dict[tuple[str, str], dict[str, Any]], latest: date,
               data_types: Sequence[str], codes: set[str] | None,
               max_gap_days: int | None = DEFAULT_MAX_GAP_DAYS
               ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """缺口清单：返回 (待补, 长期停更跳过)。闸门与 fill_daily_gap 的 max_gap_days 同口径。"""
    gaps: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for (kind, code), row in coverage.items():
        if kind not in data_types:
            continue
        if codes is not None and code not in codes:
            continue
        local_end = row.get('end')
        if local_end is not None and local_end >= latest:
            continue
        item = {
            'data_type': kind,
            'ts_code': code,
            'local_end': None if local_end is None else str(local_end),
            'target_date': str(latest),
            'gap_days': None if local_end is None else (latest - local_end).days,
        }
        if (max_gap_days is not None and item['gap_days'] is not None
                and item['gap_days'] > max_gap_days):
            skipped.append(item)
        else:
            gaps.append(item)
    covered = {key[1] for key in coverage}
    for code in sorted(codes or ()):
        if code in covered:
            continue
        gaps.append({
            'data_type': 'stock', 'ts_code': code, 'local_end': None,
            'target_date': str(latest), 'gap_days': None,
        })
    key = lambda item: (item['data_type'], item['ts_code'])  # noqa: E731
    return sorted(gaps, key=key), sorted(skipped, key=key)


def _input_set(coverage: dict[tuple[str, str], dict[str, Any]], *,
               codes: set[str] | None, data_types: Sequence[str],
               start: date | None, end: date) -> dict[str, Any]:
    coverage_digest = _sha256_text(
        f'{kind}|{code}|{row.get("rows")}|{row.get("end")}'
        for (kind, code), row in sorted(coverage.items()))
    payload = {
        'codes': None if codes is None else sorted(codes),
        'data_types': list(data_types),
        'since': None if start is None else start.isoformat(),
        'target_date': end.isoformat(),
        'coverage_digest': coverage_digest,
    }
    payload['input_hash'] = _sha256_text(
        [f'{key}={json.dumps(value, ensure_ascii=False)}'
         for key, value in sorted(payload.items())])
    return payload


def _write_staging(batch_dir: Path, gaps: list[dict[str, Any]],
                   inputs: dict[str, Any]) -> None:
    """把缺口清单与输入集合哈希落到 ``_staging/<批次>/``（一次 apply 的输入）。"""
    import polars as pl  # noqa: PLC0415

    from src.data.atomic_write import write_text_atomic  # noqa: PLC0415

    plan_path = batch_dir / 'input_set.json'
    if plan_path.is_file():
        existing = json.loads(plan_path.read_text(encoding='utf-8'))
        if existing.get('status') == 'applied':
            message = (
                f'批次 {batch_dir.name} 已于 {existing.get("applied_at")} 执行过一次 apply。'
                f'禁止在同一批次上「补一批 → 复跑 → 再补」：请换一个新批次名重跑。')
            _log_event({'event': 'batch_rejected', 'batch': batch_dir.name,
                        'reason': 'already_applied',
                        'applied_at': existing.get('applied_at'),
                        'detail': message,
                        'ts': datetime.now().isoformat(timespec='seconds')})
            raise SystemExit(message)
        if existing.get('input_hash') != inputs['input_hash']:
            message = (
                f'批次 {batch_dir.name} 已存在且输入集合不同（{existing.get("input_hash")} '
                f'!= {inputs["input_hash"]}）。按 DG-35 不得在同一批次上「补一批 → 复跑 → '
                f'再补」：请换一个新批次名重跑。')
            _log_event({'event': 'batch_rejected', 'batch': batch_dir.name,
                        'reason': 'input_hash_mismatch',
                        'existing_input_hash': existing.get('input_hash'),
                        'input_hash': inputs['input_hash'],
                        'detail': message,
                        'ts': datetime.now().isoformat(timespec='seconds')})
            raise SystemExit(message)
        logger.info('[daily] 批次 {} 输入集合一致，按原清单执行', batch_dir.name)
    batch_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(gaps, schema={
        'data_type': pl.String, 'ts_code': pl.String, 'local_end': pl.String,
        'target_date': pl.String, 'gap_days': pl.Int64,
    }).write_parquet(batch_dir / 'gaps.parquet')
    payload = {**inputs, 'status': 'planned', 'gaps': len(gaps),
               'generated_at': datetime.now().isoformat(timespec='seconds')}
    write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', plan_path)


def _mark_applied(batch_dir: Path, summary: dict[str, Any]) -> None:
    from src.data.atomic_write import write_text_atomic  # noqa: PLC0415

    plan_path = batch_dir / 'input_set.json'
    if not plan_path.is_file():
        return
    payload = json.loads(plan_path.read_text(encoding='utf-8'))
    payload.update({'status': 'applied',
                    'applied_at': datetime.now().isoformat(timespec='seconds'),
                    **summary})
    write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2) + '\n', plan_path)


# ----------------------------------------------------------------------
# 执行：采集 → 回填 → 本地构建
# ----------------------------------------------------------------------
def _latest_local(coverage: dict[tuple[str, str], dict[str, Any]]) -> date | None:
    ends = [row['end'] for row in coverage.values() if row.get('end') is not None]
    return max(ends) if ends else None


def _run_fill(config: Any, store: LocalQuoteStore, latest: date, *,
              codes: set[str] | None, data_types: Sequence[str],
              start: date | None, max_gap_days: int | None) -> dict[str, Any]:
    from src.data.tencent.client import TencentClient  # noqa: PLC0415
    from src.data.tencent.daily_fill import fill_daily_gap  # noqa: PLC0415

    # 单连接串行：workers=1；min_interval 取客户端默认，不在此改写。
    return fill_daily_gap(
        config, store, TencentClient(), codes=codes, data_types=data_types,
        since=start, latest_trade_date=latest, workers=1,
        max_gap_days=max_gap_days)


def _kill_process_tree(proc: subprocess.Popen[str]) -> None:
    """终止子进程及其全部后代（含孙进程）。

    只 kill 直接子进程不够：脚本内部常再起 ``probe_*`` 之类的取证进程，它们
    继承 stdout/stderr 管道，直接子进程死了管道仍不关闭，``communicate()`` 会
    永远阻塞——2026-11-24 的 dividend_monthly 就卡在这里。
    """
    try:
        import psutil  # noqa: PLC0415 - 仅在超时路径需要

        parent = psutil.Process(proc.pid)
        victims = parent.children(recursive=True) + [parent]
        for item in victims:
            try:
                item.kill()
            except psutil.Error:
                continue
        psutil.wait_procs(victims, timeout=10)
        return
    except Exception:  # noqa: BLE001 - 兜底走 taskkill
        pass
    if os.name == 'nt':
        subprocess.run(['taskkill', '/F', '/T', '/PID', str(proc.pid)],
                       capture_output=True, text=True)
    else:
        proc.kill()


def _console_log_path(day: date | None = None) -> Path:
    """当日控制台日志：父/子进程 stdout+stderr 追加落盘，硬杀后仍可回溯。"""
    return CONSOLE_LOG_DIR / f'daily_{(day or date.today()):%Y%m%d}_console.log'


def _append_console(label: str, stream: str, text: str) -> None:
    """把子进程输出追加到当日控制台日志（逐段 flush；写失败不影响主流程）。"""
    if not (text or '').strip():
        return
    try:
        path = _console_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('a', encoding='utf-8', errors='replace') as handle:
            handle.write(f'[{datetime.now():%Y-%m-%d %H:%M:%S}] [{label}] {stream}\n')
            handle.write(text if text.endswith('\n') else text + '\n')
    except OSError as exc:
        logger.warning('[daily] 控制台日志写入失败（不影响执行）：{}', exc)


class _ConsoleTee:
    """stdout/stderr 双写：原样转发终端，同时追加到控制台日志。"""

    def __init__(self, stream: Any, handle: Any) -> None:
        self._stream = stream
        self._handle = handle

    def write(self, text: str) -> int:
        try:
            self._handle.write(text)
            self._handle.flush()
        except (OSError, ValueError):
            pass
        return self._stream.write(text)

    def flush(self) -> None:
        try:
            self._handle.flush()
        except (OSError, ValueError):
            pass
        self._stream.flush()

    def isatty(self) -> bool:
        return False

    def fileno(self) -> int:
        return self._stream.fileno()


#: 持有控制台日志句柄，避免被 GC 提前关闭（进程存续期间常开）。
_CONSOLE_HANDLES: list[Any] = []


def _install_console_tee() -> None:
    """启动时把本进程 stdout/stderr 落盘到 logs/daily_YYYYMMDD_console.log。"""
    try:
        path = _console_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open('a', encoding='utf-8', errors='replace')
    except OSError as exc:
        logger.warning('[daily] 无法打开控制台日志（{}），仅保留终端输出', exc)
        return
    _CONSOLE_HANDLES.append(handle)
    sys.stdout = _ConsoleTee(sys.stdout, handle)  # type: ignore[assignment]
    sys.stderr = _ConsoleTee(sys.stderr, handle)  # type: ignore[assignment]


def _child_env() -> dict[str, str]:
    """子进程环境：注入仓库根、标记脱离模式、关掉 Python 输出缓冲。"""
    env = dict(os.environ)
    existing = env.get('PYTHONPATH')
    env['PYTHONPATH'] = str(ROOT) if not existing else str(ROOT) + os.pathsep + existing
    env[DETACH_ENV] = '1'
    env['PYTHONUNBUFFERED'] = '1'
    return env


def _popen_detached(cmd: list[str], *, stdout: Any,
                    env: dict[str, str]) -> subprocess.Popen[Any]:
    """起一个不共享父控制台的进程（Windows 用 DETACHED_PROCESS + 新进程组）。

    ``CREATE_BREAKAWAY_FROM_JOB`` 在禁止 breakaway 的作业对象里会被拒（Access
    denied），此时退回不带 breakaway 的拉起；这两条都不需要管理员权限。
    """
    kwargs: dict[str, Any] = dict(stdin=subprocess.DEVNULL, stdout=stdout,
                                  stderr=subprocess.STDOUT, cwd=str(ROOT),
                                  env=env, close_fds=True)
    if os.name == 'nt':
        flags = _DETACHED_PROCESS | _CREATE_NEW_PROCESS_GROUP
        try:
            return subprocess.Popen(cmd, creationflags=flags | _CREATE_BREAKAWAY_FROM_JOB,
                                    **kwargs)
        except OSError:
            return subprocess.Popen(cmd, creationflags=flags, **kwargs)
    return subprocess.Popen(cmd, start_new_session=True, **kwargs)


def _relay(argv: list[str]) -> int:
    """中继：拉起真正的日更进程后立刻退出，让目标进程成为孤儿。

    目标进程的父进程（本中继）消失后，父终端按进程树硬杀时枚举不到它；
    本中继自己只打印一行 ``pid=<n>`` 到拉起方的管道，不等待目标进程。
    """
    payload = json.loads(argv[argv.index(DETACH_RELAY_FLAG) + 1])
    log_path = Path(payload['log'])
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open('a', encoding='utf-8', errors='replace') as handle:
        proc = _popen_detached(list(payload['cmd']), stdout=handle, env=_child_env())
    print(f'pid={proc.pid}', flush=True)
    return 0


def _spawn_detached(cmd: list[str], log_path: Path) -> int:
    """把 ``cmd`` 以脱离当前终端的方式拉起，返回最终进程 pid（0=没取到）。

    Windows 走两跳：本进程 → 中继（拉起目标后立即退出）→ 目标。非 Windows 直接
    用 ``start_new_session``。``cmd`` 的 stdout/stderr 在 OS 层追加到 ``log_path``。
    2026-11-25 16:11 那轮日更就是被拉起它的终端连带硬杀（pid 31604 无 run_end）。
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    env = _child_env()
    if os.name != 'nt':
        with log_path.open('a', encoding='utf-8', errors='replace') as handle:
            return _popen_detached(cmd, stdout=handle, env=env).pid
    payload = json.dumps({'cmd': cmd, 'log': str(log_path)}, ensure_ascii=False)
    relay = [sys.executable, str(_SELF), DETACH_RELAY_FLAG, payload]
    proc = _popen_detached(relay, stdout=subprocess.PIPE, env=env)
    try:
        out, _ = proc.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        proc.kill()
        return 0
    if isinstance(out, bytes):
        out = out.decode('utf-8', 'replace')
    match = re.search(r'pid=(\d+)', out or '')
    return int(match.group(1)) if match else 0


def _detach_started(pid: int) -> bool:
    """统一日志里是否已有该 pid 的 run_start（子进程已进入主流程）。"""
    return any(item.get('event') == 'run_start' and item.get('pid') == pid
               for item in _read_log_tail())


def _verify_detach(pid: int, wait: float | None = None) -> str | None:
    """脱离启动自检：``wait`` 秒内须「子进程存活 + 日志已出现 run_start」。

    通过返回 None，否则返回失败原因。子进程若在这段窗口内正常跑完（``--only``
    单步等小任务），run_start 已经出现，按成功处理，不误判为启动失败。
    """
    limit = DETACH_START_CHECK_SECONDS if wait is None else wait
    deadline = time.monotonic() + limit
    while True:
        if _detach_started(pid):
            return None
        if not _pid_alive(pid):
            return '子进程已退出且日志无 run_start'
        if time.monotonic() >= deadline:
            return f'{limit:.0f}s 内日志未出现 run_start'
        time.sleep(0.5)


def _run_module(cmd: list[str], label: str, *, verbose: bool = False,
                timeout: float | None = None) -> int:
    """跑一个独立子进程，返回退出码。

    默认吞掉过程输出（单入口只留一行汇总），仅在该步失败时回显末尾几行；
    ``verbose`` 打开后原样透传到 stderr；``timeout`` 为硬超时（秒），超时即
    终止整棵进程树并返回 124，由调用方按失败记录，不拖住整轮。

    ``PYTHONPATH`` 注入仓库根：``tools/*.py`` 以脚本方式启动时 ``sys.path[0]``
    是 ``tools/``，不注入会让脚本里的 ``from src...`` 报 ModuleNotFoundError。
    """
    logger.info('[daily] {}：{}', label, ' '.join(cmd[1:]))
    env = dict(os.environ)
    existing = env.get('PYTHONPATH')
    env['PYTHONPATH'] = (str(ROOT) if not existing
                         else str(ROOT) + os.pathsep + existing)
    proc = subprocess.Popen(cmd, cwd=str(ROOT), stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True,
                            encoding='utf-8', errors='replace', env=env)
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        _kill_process_tree(proc)
        try:
            stdout, stderr = proc.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            stdout, stderr = '', ''
        _append_console(label, 'stdout', stdout)
        _append_console(label, 'stderr', stderr)
        logger.warning('[daily] {} 超时（>{:.0f}s），已终止整棵进程树并记失败',
                       label, timeout)
        sys.stderr.write(f'[daily] {label} 超时（>{timeout:.0f}s），已终止\n')
        return 124
    _append_console(label, 'stdout', stdout)
    _append_console(label, 'stderr', stderr)
    if verbose:
        for stream in (stdout, stderr):
            if stream:
                sys.stderr.write(stream if stream.endswith('\n') else stream + '\n')
    elif proc.returncode != 0:
        tail = '\n'.join((stderr or stdout or '').strip().splitlines()[-5:])
        if tail:
            sys.stderr.write(f'[daily] {label} 失败输出（末尾 5 行）：\n{tail}\n')
    return proc.returncode


def _age_days(path: Path) -> int | None:
    """文件距今天数（按本地日期）；文件不存在返回 None。"""
    if not path.is_file():
        return None
    return (date.today() - date.fromtimestamp(path.stat().st_mtime)).days


def _basic_checked_age(root: Path, rels: tuple[str, str]) -> int | None:
    """距上次「检查」的天数：优先 manifest 的 ``last_checked_at``。

    名录每日检查、有变更才落盘，无变更时 parquet 的 mtime 不动；若按 mtime 判
    到期，会把「今天已检查过」误当成「没检查过」而重复跑。
    """
    manifest = root / rels[1]
    if manifest.is_file():
        try:
            payload = json.loads(manifest.read_text(encoding='utf-8'))
            value = payload.get('last_checked_at')
            if isinstance(value, str):
                return (date.today() - date.fromisoformat(value[:10])).days
        except (OSError, ValueError):
            pass
    return _age_days(root / rels[0])


def _basic_check_record(root: Path, rels: tuple[str, str]) -> dict[str, Any]:
    """读名录 manifest 的最近一次检查记录（检查日期与是否有变更）。"""
    manifest = root / rels[1]
    if not manifest.is_file():
        return {}
    try:
        payload = json.loads(manifest.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}
    record = dict(payload.get('last_check') or {})
    record['last_checked_at'] = payload.get('last_checked_at')
    return record


def _basic_due(root: Path, refresh_days: int) -> dict[str, tuple[str, str]]:
    """距上次检查超过 ``refresh_days`` 天的名录（文件缺失也算到期）。"""
    due: dict[str, tuple[str, str]] = {}
    for key, rels in AUX_BASIC_OUTPUTS.items():
        age = _basic_checked_age(root, rels)
        if age is None or age >= refresh_days:
            due[key] = rels
    return due


def _aux_expected(root: Path, refresh_days: int,
                  pool_refresh_days: int = DEFAULT_POOL_REFRESH_DAYS) -> set[str]:
    """新增旁路会写的件，并入日更预期清单（只作白名单，不强制必须出现）。"""
    expected = set(AUX_ST_OUTPUTS)
    expected.update(AUX_BREADTH_OUTPUTS)
    expected.update(AUX_INDEX_VALUATION_OUTPUTS)
    for key, rels in _basic_due(root, refresh_days).items():
        expected.update(rels)
    if _pool_due(root, pool_refresh_days):
        expected.update(AUX_POOL_OUTPUTS)
    return expected


def _pool_due(root: Path, refresh_days: int) -> bool:
    """候选池距上次重建是否已到周更阈值（文件缺失也算到期）。"""
    age = _age_days(root / AUX_POOL_OUTPUTS[0])
    return age is None or age >= refresh_days


def _pool_codes(path: Path) -> set[str]:
    """候选池当前成分（用于重建前后对比进出）。"""
    if not path.is_file():
        return set()
    try:
        return set(pl.read_parquet(path, columns=['ts_code'])['ts_code'].to_list())
    except (OSError, pl.exceptions.PolarsError):
        return set()


def _excluded_codes(root: Path) -> tuple[int, int]:
    """排除清单：返回（排除只数，带原因与评估日期的条目数）。"""
    path = root / 'meta' / 'multi_asset' / 'etf_pool_excluded.json'
    if not path.is_file():
        return 0, 0
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return 0, 0
    return int(payload.get('count') or len(payload.get('codes') or [])), len(
        payload.get('items') or [])


def _run_pool_refresh(plan: _Plan, args: argparse.Namespace) -> str:
    """候选池按周重建：到期才跑，输出一行进出变动；失败只告警。"""
    root = plan.root
    if args.skip_pool_refresh:
        logger.info('[daily] 已跳过候选池重建（--skip-pool-refresh）')
        return ''
    if not _pool_due(root, args.pool_refresh_days):
        logger.info('[daily] 候选池未到期（阈值 {} 天），本轮不重建', args.pool_refresh_days)
        return ''
    pool_path = root / AUX_POOL_OUTPUTS[0]
    before = _pool_codes(pool_path)
    code = _run_module(
        [sys.executable, str(ROOT / 'tools' / 'build_multi_asset_etf_pool.py'), '--apply'],
        '候选池重建')
    after = _pool_codes(pool_path)
    if code != 0 or not after:
        logger.warning('[daily] 候选池重建未完成（返回 {}），只告警', code)
        return '[候选池] 重建未完成（只告警，不影响日更结果）'
    entered = len(after - before) if before else 0
    exited = len(before - after) if before else 0
    excluded, with_reason = _excluded_codes(root)
    as_of = '-'
    try:
        as_of = str(pl.read_parquet(pool_path, columns=['coverage_end'])['coverage_end']
                    .max())[:10]
    except (OSError, pl.exceptions.PolarsError):
        pass
    return (f'[候选池] as_of {as_of} | 池内 {len(after)} 只 | 新入池 {entered} / 出池 {exited} | '
            f'按规则排除 {excluded} 只（{with_reason} 条带原因与评估日期，见 '
            'etf_pool_excluded.json）')


def _valuation_last_date(root: Path) -> date | None:
    """读估值表末日（只读一列）；文件缺失或读取失败返回 None。"""
    path = root / 'meta' / 'financials' / 'valuation_daily.parquet'
    if not path.is_file():
        return None
    try:
        value = pl.read_parquet(path, columns=['trade_date'])['trade_date'].max()
    except (OSError, pl.exceptions.PolarsError):
        return None
    return value if isinstance(value, date) else None


def _run_valuation(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """全市场日频估值增量：起点由脚本从本地估值末日 + 1 自动推算，终点取 plan.latest。

    失败只记录、不影响后续步骤；源端当日未更新时本次落 0 行、末日不动，
    次日起点不变、终点前移，不会把未采到的日期永久标记完成。

    ``--until`` 透传 ``plan.latest``（即单入口 ``--end``）：次日补跑时名录只到昨日，
    若终点仍取「最近交易日」（= 今日）会触发名录新鲜度守卫误判；显式指定终点后
    守卫的 as_of 与名录末日一致，不再误拒。
    """
    if args.skip_valuation:
        return True, '已跳过（--skip-valuation）'
    if not args.allow_remote:
        return True, '未允许联网，跳过（只告警）'
    last = _valuation_last_date(plan.root)
    if last is None:
        return False, '估值表缺失，无法推算起点'
    code = _run_module(
        [sys.executable, str(ROOT / 'tools' / 'collect_financials.py'),
         '--only', 'valuation', '--since', 'auto',
         '--until', plan.latest.isoformat()], '全市场估值增量', timeout=1500)
    after = _valuation_last_date(plan.root)
    ok = code == 0 and after is not None and after >= plan.latest
    return ok, f'末日 {last}→{after or "-"} | rc={code}'


def _run_index_valuation(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """指数月末估值（非致命）：只在当月最后一个交易日执行一次。

    数据源 ``index_weight × daily_basic``，由脚本自行断点续跑；非月末直接跳过，
    不产生任何网络请求。失败只记录，不影响后续步骤与门禁。
    """
    if args.skip_index_valuation:
        return True, '已跳过（--skip-index-valuation）'
    if not args.allow_remote:
        return True, '未允许联网，跳过（只告警）'
    if not _is_month_end_signal(plan.root, plan.latest):
        return True, f'{plan.latest} 非月末，跳过'
    plan.expected.update(AUX_INDEX_VALUATION_OUTPUTS)
    code = _run_module(
        [sys.executable, str(ROOT / 'tools' / 'collect_index_valuation.py'),
         '--apply'], '指数月末估值', timeout=600)
    return code == 0, f'{plan.latest} 月末快照 | rc={code}'


def _step_factor_panel(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """因子面板增量（非致命）：只算目标交易日截面，按 signal_date 分区 upsert。

    面板以 ``valuation_daily`` 为驱动表，故排在本步之前的估值增量之后；该日
    无估值快照（非交易日/估值未更新）时脚本自己跳过并返回 0，不写空分区。
    """
    if args.skip_factor_panel:
        return True, '已跳过（--skip-factor-panel）'
    plan.expected.update(AUX_FACTOR_PANEL_OUTPUTS)
    code = _run_module(
        [sys.executable, str(ROOT / 'tools' / 'factor_panel.py'),
         '--apply', '--mode', 'incremental', '--date', plan.latest.isoformat(),
         '--store-root', str(plan.root)], '因子面板增量')
    return code == 0, f'{plan.latest} 当日截面 | rc={code}'


def _is_month_end_signal(root: Path, day: date) -> bool:
    """``day`` 是否为当月最后一个交易日（下一交易日落到下个月）。"""
    from src.data.trading_calendar import next_trading_day  # noqa: PLC0415

    nxt = next_trading_day(day, root)
    return nxt is None or (nxt.year, nxt.month) != (day.year, day.month)


def _step_b_topn_picks(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """B 股东盈余比候选名单（非致命）：只在信号日为月末调仓日时产出。

    名单 = 申万一级行业内 B 前 2 只 → 合并取前 50 只等权，只写 ``reports/``，
    不碰 ``data/``、不产生任何交易指令。非调仓日直接跳过，不落盘。
    """
    if args.skip_b_topn_picks:
        return True, '已跳过（--skip-b-topn-picks）'
    if not _is_month_end_signal(plan.root, plan.latest):
        return True, f'{plan.latest} 非月末调仓日，跳过'
    code = _run_module(
        [sys.executable, str(ROOT / 'tools' / 'b_topn_picks.py'),
         '--date', plan.latest.isoformat(),
         '--store-root', str(plan.root)], 'B 候选名单')
    return code == 0, f'{plan.latest} 月末调仓日 | rc={code}'


def _run_breadth(plan: _Plan) -> tuple[bool, str]:
    """市场宽度日更：增量补齐快照，行情末根未前进时不动盘；失败只告警。"""
    path = plan.root / AUX_BREADTH_OUTPUTS[0]
    try:
        before = path.stat().st_mtime_ns
    except OSError:
        before = 0
    try:
        from src.data.services.breadth_service import refresh  # noqa: PLC0415
        frame = refresh(plan.root)
    except Exception as exc:  # noqa: BLE001 - 派生表失败不能反噬日更
        return False, f'刷新异常：{type(exc).__name__}: {exc}'
    if frame.is_empty() or 'trade_date' not in frame.columns:
        return True, '无行情可算，未落盘'
    last = str(frame['trade_date'].max())[:10]
    try:
        after = path.stat().st_mtime_ns
    except OSError:
        after = 0
    state = '已补齐' if after != before else '无新增未落盘'
    return True, f'末日 {last} | {frame.height:,} 行 | {state}'


def _step_st(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """ST 快照 + supplement 重建 + 区间表日更；失败只记录，不改日更结果。

    顺序固定为「快照 → supplement → 规范表」：``st_history build`` 只负责把
    supplement 并入规范表，若先合并再重建 supplement，规范表的覆盖窗口仍停在
    上次构建日。这里用 ``build`` 而不是 ``apply``：``apply`` = build + 再次
    ``st_history build``，与紧随其后的规范表重建重复，还会多打一份快照。
    """
    if args.skip_st_refresh:
        return True, '已跳过（--skip-st-refresh）'
    store_args = ['--store-root', str(plan.root)]
    snapshot = _run_module(
        [sys.executable, '-m', 'src.data.risk_flags', 'refresh',
         '--as-of', plan.latest.isoformat(), *store_args], 'ST 快照刷新')
    if snapshot != 0:
        return False, f'快照 rc={snapshot} / supplement 与规范表未重建'
    supplement = _run_module(
        [sys.executable, '-m', 'src.data.st_name_history', *store_args, 'build'],
        'ST 补充覆盖重建')
    if supplement != 0:
        # supplement 写盘是「区间 → 覆盖 → csv → manifest」的事务，失败可能停在
        # 中间态；此时不重建规范表，避免把半套 supplement 并进正式表。次日重跑即可。
        logger.warning('[daily] ST 补充覆盖重建失败（rc={}），本轮跳过规范表重建；'
                       'supplement 可能停留在上一版或中间态，需重跑第 8 步', supplement)
        return False, f'快照 rc=0 / supplement rc={supplement} / 规范表未重建'
    build = _run_module(
        [sys.executable, '-m', 'src.data.st_history', *store_args, 'build'],
        'ST 区间表重建')
    return build == 0, f'快照 rc=0 / supplement rc=0 / 重建 rc={build}'


def _panel_end(root: Path, sub: str, prefix: str) -> date | None:
    """月分区族的最新 ``trade_date``（文件名排序取最后一月）。"""
    files = sorted((root / sub).glob(f'{prefix}[0-9][0-9][0-9][0-9][0-9][0-9].parquet'))
    if not files:
        return None
    return pl.read_parquet(files[-1], columns=['trade_date'])['trade_date'].max()


def _panel_sessions(root: Path, latest: date, count: int) -> list[date]:
    """取 ``latest`` 及之前的最近 ``count`` 个交易日（breadth 为准，缺失退日历）。"""
    breadth = root / 'meta' / 'breadth' / 'breadth_daily.parquet'
    if breadth.is_file():
        days = sorted({day for day in pl.read_parquet(
            breadth, columns=['trade_date'])['trade_date'].to_list()
            if day is not None and day <= latest})
        if days:
            return days[-count:]
    from src.data.trading_calendar import trading_dates  # noqa: PLC0415

    days = list(trading_dates(latest - timedelta(days=count * 3 + 15), latest, ROOT))
    return days[-count:]


def _hist_month_digests(root: Path) -> dict[str, str]:
    """逐月指纹（名称段/官方区间/可信覆盖）：只有窗口之前的月份变化才算历史变更。

    按月分桶而非整表一个 hash：``name_history`` 每天会追加新简称段，整表 hash
    必然天天变；按月比对后，新增当月行不影响历史月份，重写旧区间才会被发现。
    """
    parts: dict[str, list[str]] = {}

    def _feed(path: Path, columns: list[str], date_col: str) -> None:
        if not path.is_file():
            return
        frame = pl.read_parquet(path, columns=columns)
        frame = frame.filter(pl.col(date_col).is_not_null())
        frame = frame.with_columns(pl.col(date_col).dt.strftime('%Y-%m').alias('_m'))
        for (month,), part in frame.sort(['_m', 'ts_code']).group_by(
                '_m', maintain_order=True):
            parts.setdefault(month, []).append(str(part.drop('_m').hash_rows().sum()))

    # name_history 的 end_date 会在改名当天把旧简称段收口，属于「新信息」而不是
    # 历史改写；coverage_end 每天随行情推进。两者都不进指纹，否则每天都判历史变更。
    _feed(root / 'meta' / 'name_history.parquet',
          ['ts_code', 'name', 'start_date'], 'start_date')
    _feed(root / 'meta' / 'st' / 'st_intervals.parquet',
          ['ts_code', 'start_date', 'end_date', 'flag'], 'start_date')
    _feed(root / 'meta' / 'st' / 'st_coverage.parquet',
          ['ts_code', 'coverage_start'], 'coverage_start')
    return {month: hashlib.sha256('|'.join(values).encode('utf-8')).hexdigest()
            for month, values in parts.items()}


def _missing_panel_months(root: Path, sub: str, prefix: str,
                          latest: date) -> list[str]:
    """已有分区序列中间的月份空洞（当月不算，缺首个历史月才算历史变更）。"""
    names = sorted(path.name for path in (root / sub).glob(f'{prefix}[0-9]*.parquet'))
    if not names:
        return []
    first = names[0][len(prefix):-len('.parquet')]
    present = set(names)
    cursor = date(int(first[:4]), int(first[4:6]), 1)
    missing: list[str] = []
    while (cursor.year, cursor.month) < (latest.year, latest.month):
        tag = f'{cursor:%Y%m}'
        if f'{prefix}{tag}.parquet' not in present:
            missing.append(tag)
        cursor = date(cursor.year + (cursor.month == 12),
                      1 if cursor.month == 12 else cursor.month + 1, 1)
    return missing


def _derived_state_path(root: Path) -> Path:
    return root / 'meta' / 'st' / 'daily' / 'incremental_state.json'


def _derived_window(plan: _Plan, args: argparse.Namespace) -> dict[str, Any]:
    """本次增量重建起点：默认最近 N 个交易日所在月分区（整月重写）。

    触发全量的条件：``--full-derived``、已有月分区出现中间空洞（历史被删/丢）、
    窗口之前的 ST 输入指纹与上次不一致（名称段/区间/覆盖被修订）。
    月末当日所在月本身就是「整月重建」；窗口跨月时上月一并重写，
    覆盖上月最后一个交易日之后才到的补丁。
    """
    count = max(1, args.derived_lookback_days)
    days = _panel_sessions(plan.root, plan.latest, count)
    window_start = (days[0] if days else plan.latest).replace(day=1)
    window_end = days[-1] if days else plan.latest
    hist_months = _hist_month_digests(plan.root)
    state: dict[str, Any] = {}
    state_path = _derived_state_path(plan.root)
    if state_path.is_file():
        try:
            state = json.loads(state_path.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            state = {}
    base = {'hist_months': hist_months, 'window_start': window_start}
    if args.full_derived:
        return {**base, 'since': date(2005, 1, 1), 'mode': 'full',
                'note': '--full-derived 强制全量'}
    missing = _missing_panel_months(plan.root, 'meta/st/daily', 'st_daily_', window_end)
    if missing:
        return {**base, 'since': date(2005, 1, 1), 'mode': 'full',
                'note': f'月分区缺 {len(missing)} 个（{missing[0]}~{missing[-1]}）'}
    boundary = f'{window_start:%Y-%m}'
    stored = state.get('hist_months') or {}
    changed = [month for month, digest in stored.items()
               if month < boundary and hist_months.get(month) != digest]
    changed.extend(month for month in hist_months
                   if stored and month < boundary and month not in stored)
    if changed:
        changed.sort()
        return {**base, 'since': date(2005, 1, 1), 'mode': 'full',
                'note': f'历史 ST 输入已变更（{len(changed)} 个月，最早 {changed[0]}）'}
    note = f'最近 {count} 个交易日 → 整月 {window_start:%Y-%m}'
    if (window_start.year, window_start.month) != (window_end.year, window_end.month):
        note += f' + 跨月 {window_end:%Y-%m}'
    if _is_month_end_signal(plan.root, window_end):
        note += '（月末整月重建）'
    return {**base, 'since': window_start, 'mode': 'incremental', 'note': note}


def _save_derived_state(plan: _Plan, args: argparse.Namespace,
                        window: dict[str, Any]) -> None:
    from src.data.atomic_write import write_text_atomic  # noqa: PLC0415

    path = _derived_state_path(plan.root)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(json.dumps({
        'schema': 'st_daily_incremental/v2',
        'last_run': date.today().isoformat(),
        'lookback_sessions': args.derived_lookback_days,
        'mode': window['mode'],
        'since': window['since'].isoformat(),
        'window_start': window['window_start'].isoformat(),
        'hist_months': window['hist_months'],
    }, ensure_ascii=False, indent=2), path)


def _step_st_daily(plan: _Plan, args: argparse.Namespace,
                   state: dict[str, Any]) -> tuple[bool, str]:
    """ST 逐日面板增量重建（月分区整月重写）；未推进到行情末日即判失败。"""
    if args.skip_st_daily:
        state['st_daily'] = {'ok': True, 'skipped': True}
        return True, '已跳过（--skip-st-daily）'
    if args.codes:
        state['st_daily'] = {'ok': True, 'skipped': True}
        return True, '代码清单补数模式，跳过低频全市场派生表'
    window = _derived_window(plan, args)
    cmd = [sys.executable, str(ROOT / 'tools' / 'build_st_daily.py'),
           '--since', window['since'].isoformat()]
    if window['mode'] != 'full':
        cmd.append('--incremental')
    started = time.perf_counter()
    code = _run_module(cmd, 'ST 逐日面板重建')
    seconds = time.perf_counter() - started
    quote_end = _daily_index_end(plan.root, kinds=('stock',))
    panel_end = _panel_end(plan.root, 'meta/st/daily', 'st_daily_')
    ok = (code == 0 and quote_end is not None and panel_end is not None
          and panel_end >= quote_end)
    state['st_daily'] = {'ok': ok, 'since': window['since'].isoformat(),
                         'mode': window['mode'], 'seconds': seconds}
    detail = (f"{window['mode']} since {window['since']}（{window['note']}）| "
              f'面板末日 {panel_end} / 行情末日 {quote_end} | {seconds:.1f}s rc={code}')
    if not ok:
        logger.warning('[daily] ST 逐日面板未推进到行情末日（面板 {} / 行情 {}），'
                       '阻断涨跌停逐日，避免用旧 ST 面板生成', panel_end, quote_end)
        return False, detail
    if window['mode'] != 'full':
        _save_derived_state(plan, args, window)
    return True, detail


def _step_limit_daily(plan: _Plan, args: argparse.Namespace,
                      state: dict[str, Any]) -> tuple[bool, str]:
    """涨跌停逐日增量重建；上游 ST 未成功/未推进到行情末日时直接阻断。"""
    if args.skip_limit_daily:
        return True, '已跳过（--skip-limit-daily）'
    if args.skip_st_daily:
        return True, '已跳过（上游 ST 逐日已跳过）'
    if args.codes:
        return True, '代码清单补数模式，跳过低频全市场派生表'
    upstream = state.get('st_daily') or {}
    if not upstream.get('ok'):
        logger.warning('[daily] 涨跌停逐日被阻断：上游 ST 逐日失败/未执行')
        return False, '上游 ST 逐日失败/未执行，阻断生成（不使用旧 ST 面板）'
    quote_end = _daily_index_end(plan.root, kinds=('stock',))
    st_end = _panel_end(plan.root, 'meta/st/daily', 'st_daily_')
    if quote_end is None or st_end is None or st_end < quote_end:
        return False, f'ST 面板末日 {st_end} < 行情末日 {quote_end}，阻断生成'
    since = upstream.get('since') or _derived_window(plan, args)['since'].isoformat()
    mode = upstream.get('mode') or 'incremental'
    cmd = [sys.executable, str(ROOT / 'tools' / 'build_limit_board.py'),
           '--since', since]
    if mode != 'full':
        cmd.append('--incremental')
    started = time.perf_counter()
    code = _run_module(cmd, '涨跌停逐日重建')
    seconds = time.perf_counter() - started
    limit_end = _panel_end(plan.root, 'meta/limit/daily', 'limit_daily_')
    ok = code == 0 and limit_end is not None and limit_end >= quote_end
    if not ok:
        logger.warning('[daily] 涨跌停逐日未推进到行情末日（表 {} / 行情 {}）',
                       limit_end, quote_end)
    return ok, (f'{mode} since {since} | 表末日 {limit_end} / 行情末日 {quote_end}'
                f' | {seconds:.1f}s rc={code}')


def _step_basic(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """fund_basic / index_basic 名录每日检查（有变更才落盘）。"""
    if args.skip_basic_refresh:
        return True, '已跳过（--skip-basic-refresh）'
    due = _basic_due(plan.root, args.basic_refresh_days)
    if not due:
        return True, '未到检查周期'
    problems: list[str] = []
    done: list[str] = []
    for key, rels in due.items():
        flag = '--update-fund' if key == 'fund' else '--update-index'
        code = _run_module([sys.executable, '-m', 'src.main', flag],
                           f'{key}_basic 名录检查', timeout=900)
        record = _basic_check_record(plan.root, rels)
        checked_today = (str(record.get('last_checked_at') or '')[:10]
                         == date.today().isoformat())
        changed = bool(record.get('changed'))
        if code != 0 or not checked_today:
            problems.append(f'{key} rc={code}（检查日期未推进）')
        else:
            done.append(f'{key}={"有变更" if changed else "无变更"}')
    return not problems, ' | '.join(done + problems) or '无到期名录'


def _run_freshness(root: Path) -> tuple[bool, str]:
    """日更收尾跑一次保鲜体检，返回（是否无 error/warning，单行状态）。"""
    cmd = [sys.executable, '-m', 'src.data.quality.freshness', '--summary',
           '--store-root', str(root)]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True,
                          encoding='utf-8', errors='replace')
    if proc.returncode not in (0, 1):
        return False, f'体检异常（rc={proc.returncode}）：{(proc.stderr or "").strip()[:200]}'
    line = (proc.stdout or '').strip().splitlines()
    return proc.returncode == 0, (line[-1] if line else '无输出')


def _run_version(root: Path, changed) -> tuple[bool, str]:
    """记录数据版本，并给本次重建的派生表打「基于哪版数据」标记。

    只写 ``meta/data_version.json`` 与 ``meta/derived_versions.json``，不改数据、
    不触发任何重建或重跑；失败只告警，不影响日更结果。
    """
    try:
        from src.data.data_version import (
            audit_derived,
            format_audit_line,
            record_version,
            stamp_rebuilt,
        )
        record_version(root)
        stamped = stamp_rebuilt(root, changed)
        return True, f'{format_audit_line(audit_derived(root))} | 本次标记派生 {len(stamped)} 张'
    except Exception as exc:  # noqa: BLE001 - 标记失败不能反噬日更
        return False, f'{type(exc).__name__}: {exc}'


def _run_build(*, since: date | None, end: date | None,
               codes: set[str] | None, skip_factor_verify: bool = False) -> int:
    cmd = [sys.executable, '-m', 'src.data.local_store', 'build']
    if codes:
        cmd += ['--codes', ','.join(sorted(codes))]
    elif since is not None:
        cmd += ['--since', since.isoformat()]
    else:
        cmd += ['--incremental']
    if end is not None:
        cmd += ['--max-date', end.isoformat()]
    if skip_factor_verify:
        cmd.append('--skip-factor-verify')
    return _run_module(cmd, '本地构建')


def _build_targets(plan: _Plan) -> set[str]:
    """本轮 build 会重写的行情件（与 ``build._target_is_stale`` 同一判定）。

    预期集合必须与构建写盘口径一致：源 ``.day`` 被重新同步后，即使末根日期
    没前进，只要末行内容变了，构建也会重写该件；只按日期推导会把这些正常
    刷新误报成「预期外改动」。在构建前一刻取当前覆盖度，避免计划时点与构建
    时点之间的采集写入造成口径差。
    """
    tdx_root = getattr(getattr(getattr(plan, 'config', None), 'data', None),
                       'tdx_data_path', None)
    if not tdx_root or not Path(tdx_root).is_dir():
        return set()
    from src.data.local_store.build import stale_targets  # noqa: PLC0415

    rels: set[str] = set()
    for kind, ts_code, _path in stale_targets(
            Path(tdx_root), plan.store, data_type='all',
            codes=plan.codes, max_date=plan.latest):
        rel = plan.store.quote_path(kind, ts_code).relative_to(plan.root).as_posix()
        rels.add(rel)
        rels.add(Path(rel).with_suffix('.manifest.json').as_posix())
    return rels


# ----------------------------------------------------------------------
# 校验：逐件对 manifest + 汇总对账 + 预期差集
# ----------------------------------------------------------------------
def _declared_nodes(payload: Any, rel: str, root: Path,
                    base: Path | None = None) -> list[dict[str, Any]]:
    """递归找出所有声明了 ``rel`` 且带统计字段的节点。"""
    hits: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        node = payload
        local_base = base
        declared_dir = node.get('dir')
        if isinstance(declared_dir, str):
            local_base = root.joinpath(*[p for p in declared_dir.replace('\\', '/').split('/')
                                         if p and p not in ('data', 'local_store')])
        if any(key in node for key in STAT_KEYS):
            declared = node.get('path') or node.get('source_parquet')
            if isinstance(declared, str):
                text = declared.replace('\\', '/')
                if text.startswith(STORE_PREFIX + '/'):
                    text = text[len(STORE_PREFIX) + 1:]
                if not os.path.isabs(text):
                    for prefix in (root, local_base, root.parent):
                        if prefix is None:
                            continue
                        candidate = (prefix / text).resolve()
                        if candidate == (root / rel).resolve():
                            hits.append(node)
                            break
        for value in node.values():
            if isinstance(value, (dict, list)):
                hits.extend(_declared_nodes(value, rel, root, local_base))
    elif isinstance(payload, list):
        for item in payload:
            hits.extend(_declared_nodes(item, rel, root, base))
    return hits


def _find_entry(root: Path, rel: str) -> dict[str, Any] | None:
    """找声明该文件的登记节点：先同级 sidecar，再同目录 manifest.json。"""
    path = root / rel
    for manifest in (path.with_suffix('.manifest.json'), path.parent / 'manifest.json'):
        if not manifest.is_file():
            continue
        try:
            payload = json.loads(manifest.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            continue
        nodes = _declared_nodes(payload, rel, root)
        if not nodes and manifest.name == 'manifest.json':
            # 同目录以文件名为键的写法：{"files": {"a.parquet": {...}}}
            files = payload.get('files') if isinstance(payload, dict) else None
            if isinstance(files, dict):
                node = files.get(path.name)
                if isinstance(node, dict) and any(key in node for key in STAT_KEYS):
                    nodes = [node]
        if nodes:
            return nodes[0]
    return None


def _refresh(root: Path, changed: dict[str, int],
             cache: FactsCache | None = None) -> int:
    """把本次改动件按磁盘实测回写进各自的 manifest（sidecar 优先，其次目录级）。"""
    grouped: dict[str, list[str]] = {}
    for rel in sorted(changed):
        if not rel.startswith('quotes/') or not rel.endswith('.parquet'):
            continue
        side_rel = Path(rel).with_suffix('.manifest.json').as_posix()
        if (root / side_rel).is_file():
            grouped.setdefault(side_rel, []).append(rel)
    for rel in sorted(changed):
        # factors/ 与 meta/ 同为目录级 manifest：因子面板增量（factor_panel）写在 factors/，
        # 2026-11-20 起因未回写目录 manifest，回写校验报 rows/bytes/sha256 不符、门禁致命失败
        if not rel.startswith(('meta/', 'factors/')) or not rel.endswith('.parquet'):
            continue
        parent_rel = (Path(rel).parent / 'manifest.json').as_posix()
        if (root / parent_rel).is_file():
            grouped.setdefault(parent_rel, []).append(rel)
    anomalies = [rel for rel in changed if rel.startswith('meta/anomalies/')
                 and rel.endswith(('.csv', '.tsv'))]
    if anomalies and (root / 'meta' / 'anomalies' / 'manifest.json').is_file():
        grouped['meta/anomalies/manifest.json'] = anomalies
    if 'meta/daily_index.parquet' in changed:
        grouped['meta/daily_index.manifest.json'] = ['meta/daily_index.parquet']
    refreshed = 0
    for manifest_rel, files in grouped.items():
        manifest = root / manifest_rel
        if not manifest.is_file():
            continue
        refreshed += len(refresh_manifest_files(
            manifest, root, [root / item for item in files], facts_cache=cache))
    return refreshed


def _summary_problems(root: Path) -> list[str]:
    """同级 manifest 汇总字段对账（只核自洽：分块求和 == 合计）。"""
    problems: list[str] = []
    quotes = root / 'quotes' / 'manifest.json'
    if quotes.is_file():
        try:
            payload = json.loads(quotes.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            payload = {}
        for kind, block in (payload.get('universe') or {}).items():
            detail = block.get('markets_detail') or {}
            if not isinstance(detail, dict) or not detail:
                continue
            for field in ('files', 'rows', 'bytes'):
                if field not in block:
                    continue
                total = sum(int(item.get(field) or 0) for item in detail.values())
                if total != int(block.get(field) or 0):
                    problems.append(
                        f'quotes/manifest.json universe.{kind}.{field} 分块和 {total} '
                        f'!= 合计 {block.get(field)}')
    factors = root / 'factors' / 'manifest.json'
    if factors.is_file():
        try:
            payload = json.loads(factors.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            payload = {}
        totals = payload.get('totals') if isinstance(payload, dict) else None
        if isinstance(totals, dict):
            tables = [item for item in (payload.get('files') or {}).values()
                      if isinstance(item, dict) and item.get('kind') == 'parquet']
            for field in ('rows', 'bytes'):
                if field in totals and sum(int(item.get(field) or 0) for item in tables) != int(totals[field]):
                    problems.append(
                        f'factors/manifest.json totals.{field} 与 parquet 条目求和不符')
    return problems


def _verify(root: Path, changed: dict[str, int], expected: set[str],
            before: dict[tuple[str, str], dict[str, Any]],
            after: dict[tuple[str, str], dict[str, Any]],
            cache: FactsCache | None = None) -> dict[str, Any]:
    mismatches: list[str] = []
    unregistered: list[str] = []
    for rel in sorted(changed):
        if not rel.endswith(DATA_SUFFIXES) or rel.endswith('.manifest.json'):
            continue
        node = _find_entry(root, rel)
        if node is None:
            unregistered.append(rel)
            continue
        facts = file_facts(root / rel, cache)
        for key in STAT_KEYS:
            if key in node and node[key] != facts[key]:
                mismatches.append(f'{rel}:{key}(manifest={node[key]} disk={facts[key]})')
    unexpected = sorted(
        rel for rel in changed
        if rel not in expected and not rel.startswith(SIDE_PREFIXES))
    added = deleted = 0
    for key in set(before) | set(after):
        old = int((before.get(key) or {}).get('rows') or 0)
        new = int((after.get(key) or {}).get('rows') or 0)
        added += max(0, new - old)
        deleted += max(0, old - new)
    problems = mismatches + [f'预期外改动 {rel}' for rel in unexpected] + _summary_problems(root)
    return {
        'added': added, 'deleted': deleted, 'problems': problems,
        'unregistered': unregistered, 'unexpected': unexpected,
    }


# ----------------------------------------------------------------------
# 主流程
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class _StepOutcome:
    key: str
    name: str
    critical: bool
    ok: bool
    detail: str
    seconds: float


def _step_exit_code(detail: str) -> int | None:
    """从步骤明细里取子进程退出码（``rc=N``）；无子进程的判据失败返回 None。"""
    match = re.search(r'\brc=(-?\d+)\b', detail or '')
    return int(match.group(1)) if match else None


def _run_steps(steps: Sequence[tuple[str, str, bool, Callable[[], tuple[bool, str]]]],
               state: dict[str, Any] | None = None) -> list[_StepOutcome]:
    """按顺序执行步骤：单步失败或异常只记录，继续跑后续步骤。

    传入 ``state`` 时把非致命失败（步名 + 退出码）写进 ``state['failed_noncritical']``，
    供收尾体检走既有 warning 通道汇总；退出码口径不变。
    """
    outcomes: list[_StepOutcome] = []
    for key, name, critical, func in steps:
        started = time.perf_counter()
        try:
            ok, detail = func()
        except Exception as exc:  # noqa: BLE001 - 单步异常不得中断后续步骤
            ok, detail = False, f'{type(exc).__name__}: {exc}'
        outcomes.append(_StepOutcome(key, name, critical, ok, detail,
                                     time.perf_counter() - started))
        if state is not None and not ok and not critical:
            state.setdefault('failed_noncritical', []).append({
                'step': key, 'name': name, 'exit': _step_exit_code(detail),
                'detail': detail})
        logger.info('[daily] 步骤 {} {}：{}', key, 'ok' if ok else '失败', detail)
    return outcomes


@dataclass(frozen=True)
class _Plan:
    root: Path
    config: Any
    store: LocalQuoteStore
    factor_store: LocalFactorStore
    data_types: list[str]
    codes: set[str] | None
    latest: date
    coverage: dict[tuple[str, str], dict[str, Any]]
    gaps: list[dict[str, Any]]
    skipped: list[dict[str, Any]]
    inputs: dict[str, Any]
    expected: set[str]
    plan_quote: set[str]
    range_label: str
    marker_ts: float


def _build_plan(args: argparse.Namespace) -> _Plan:
    root = Path(args.store_root) if args.store_root else Path(default_store_root())
    if not root.is_dir():
        raise SystemExit(f'local_store 根不存在：{root}')
    data_types = [item.strip() for item in args.data_types.split(',') if item.strip()]
    codes = _load_codes(args)

    from src.utils.config import Config  # noqa: PLC0415

    config = Config()
    store = LocalQuoteStore(root)
    factor_store = LocalFactorStore(root, enabled=True)
    marker_ts = datetime.now().timestamp()

    coverage = _coverage(root)
    latest = args.end
    if latest is None:
        if args.allow_remote and args.apply:
            from src.data.tencent.client import TencentClient  # noqa: PLC0415
            from src.data.tencent.daily_fill import resolve_latest_trade_date  # noqa: PLC0415

            latest = resolve_latest_trade_date(TencentClient())
        else:
            latest = _latest_local(coverage)
    if latest is None:
        raise SystemExit('无法确定目标交易日：请显式给 --end，或加 --allow-remote 在 apply 时自动探测')

    gaps, skipped = _plan_gaps(coverage, latest, data_types, codes, args.max_gap_days)
    inputs = _input_set(coverage, codes=codes, data_types=data_types,
                        start=args.start, end=latest)
    expected = {store.quote_path(item['data_type'], item['ts_code']).relative_to(root).as_posix()
                for item in gaps}
    plan_quote = set(expected)
    for rel in list(expected):
        expected.add(Path(rel).with_suffix('.manifest.json').as_posix())
    expected.update(SIDE_OUTPUTS)
    expected.update(AUX_VALUATION_OUTPUTS)
    expected.update(AUX_DIVIDEND_OUTPUTS)
    expected.update(AUX_CODE_TABLE_OUTPUTS)
    expected |= _aux_expected(root, args.basic_refresh_days, args.pool_refresh_days)
    if codes:
        # --codes 模式下 build 会无条件重写这些标的，属显式声明的写目标。
        known: dict[str, set[str]] = {}
        for (kind, code) in coverage:
            known.setdefault(code, set()).add(kind)
        for code in sorted(codes):
            for kind in sorted(known.get(code) or {'stock'}):
                rel = store.quote_path(kind, code).relative_to(root).as_posix()
                expected.add(rel)
                expected.add(Path(rel).with_suffix('.manifest.json').as_posix())
                plan_quote.add(rel)

    return _Plan(root=root, config=config, store=store, factor_store=factor_store,
                 data_types=data_types, codes=codes, latest=latest,
                 coverage=coverage, gaps=gaps,
                 skipped=skipped, inputs=inputs, expected=expected,
                 plan_quote=plan_quote,
                 range_label=f'{(args.start or "全部")}~{latest}', marker_ts=marker_ts)


def _run_target_holdings(trade_day: date, capital: float) -> int:
    """日更收尾后顺带生成 v3 目标持仓清单（独立子进程，只读消费、不改日更结果）。"""
    script = ROOT / 'tools' / 'target_holdings.py'
    try:
        proc = subprocess.run(
            [sys.executable, str(script), '--date', trade_day.isoformat(),
             '--capital', str(capital)],
            cwd=str(ROOT), check=False)
    # 兜底理由：清单生成是可选旁路，任何异常都只告警，不影响日更原有退出码
    except Exception as exc:  # noqa: BLE001
        logger.warning('[daily] 目标持仓清单生成异常（日更结果不受影响）：{}', exc)
        return 1
    if proc.returncode != 0:
        logger.warning('[daily] 目标持仓清单生成返回非零 {}（日更结果不受影响）',
                       proc.returncode)
    return proc.returncode


def _step_code_table(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """证券码表：通达信当日码表 → PIT 名称历史 → 股票名录。"""
    root = plan.root
    notes: list[str] = []
    names = _run_module([sys.executable, str(ROOT / 'tools' / 'build_names_json.py'),
                         '--store-root', str(root)], '证券码表 names.json')
    notes.append(f'names.json rc={names}')
    history = 0
    if args.allow_remote:
        history = _run_module([sys.executable, str(ROOT / 'tools' / 'build_name_history.py'),
                               '--apply'], '名称历史 name_history', timeout=1200)
        notes.append(f'name_history rc={history}')
    else:
        notes.append('name_history 跳过（未允许联网）')
    basic = _run_module([sys.executable, str(ROOT / 'tools' / 'build_stock_basic.py'),
                         '--store-root', str(root), '--promote'], '股票名录 stock_basic')
    notes.append(f'stock_basic rc={basic}')
    return names == 0 and history == 0 and basic == 0, ' | '.join(notes)


def _step_industry(plan: _Plan) -> tuple[bool, str]:
    code = _run_module(
        [sys.executable, '-m', 'src.data.industry', 'refresh',
         '--as-of', plan.latest.isoformat(), '--store-root', str(plan.root)], '行业快照',
        timeout=600)
    return code == 0, f'as_of {plan.latest} | rc={code}'


def _step_fill(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    if not args.allow_remote:
        return True, '未允许联网，跳过采集'
    stats = _run_fill(plan.config, plan.store, plan.latest, codes=plan.codes,
                      data_types=plan.data_types, start=args.start,
                      max_gap_days=args.max_gap_days)
    if stats.get('rows_added'):
        plan.store.flush_meta()  # 采集写盘后立即回填 daily_index 与其 manifest
    failed = int(stats.get('failed_symbols') or 0)
    aborted = stats.get('aborted')
    detail = f"新增 {int(stats.get('rows_added') or 0):,} 行 | 失败 {failed} 个"
    failed_codes = stats.get('failed_codes') or []
    if failed_codes:
        detail += f"（{', '.join(str(code) for code in failed_codes[:5])}）"
    if aborted:
        return False, f'{detail} | 闸门拦下：{aborted}'
    return failed == 0, detail


def _step_build(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    # 前置守卫：通达信盘后日结未落地时 build 只会读到旧的 .day，先等它落地（可关）。
    # 超时即判本步失败，不改后面的 daily_index 末日断言、也不写任何数据。
    guard_ok, guard_note = _await_tdx_landing(plan, args, plan.latest)
    if not guard_ok:
        return False, guard_note
    # 只做构建 + 末日断言，不带 build 自带的因子副本一致性门禁：那条断言要求
    # 基金行情（第 5 步）与基金因子（第 6 步）都已是目标日，放在这里会先于
    # 修复步骤失败并形成死锁。一致性断言统一在第 6 步刷新完成后执行。
    # 预期写目标与构建同判定、取构建前一刻的覆盖度，源文件同日刷新才不会误报。
    rewritten = _build_targets(plan)
    plan.expected.update(rewritten)
    plan.plan_quote.update(rewritten)
    code = _run_build(since=args.start, end=plan.latest, codes=plan.codes,
                      skip_factor_verify=True)
    if code != 0:
        return False, f'rc={code}'
    expected = _recent_completed_trade_day(plan.root)
    if args.end is not None and args.end < expected:
        detail = (f'rc=0 | 回补模式（--end {args.end} 早于最近交易日 {expected}），'
                  '跳过末日断言')
        return True, f'{guard_note} | {detail}' if guard_note else detail
    ok, note = _check_daily_index_end(plan.root, expected)
    if ok:
        # 构建会把「通达信里仍有历史、但早已退市」的旧码也落成分片；停更状态表
        # 若不同步重建，保鲜体检会把它们全判成「未登记断裂」直接报 error
        #（2026-11-24 实测 81 只：000508.SZ 等末根停在 1997~2002 年）。
        from src.data.quality.stale_status import build_stale_status  # noqa: PLC0415

        build_stale_status(plan.root)
        plan.expected.update(('meta/universe/stale_status.parquet',
                              'meta/universe/manifest.json'))
    detail = f'rc=0 | {note}'
    return ok, f'{guard_note} | {detail}' if guard_note else detail


def _step_fund_quotes(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """基金行情刷新：通达信本地 lday 增量；末日已到目标则跳过不重算（致命步）。"""
    if args.skip_fund_refresh:
        return True, '已跳过（--skip-fund-refresh）'
    target = plan.latest
    before = _daily_index_end(plan.root, kinds=('fund',))
    if before is not None and before >= target:
        return True, f'末日 {before} 已到 {target}，跳过不重算'
    if before is None:
        return False, '基金行情在 meta/daily_index 中缺失（无 fund 覆盖）'
    behind = sorted(code for code, end in _quote_end_by_code(plan.root, 'fund').items()
                    if end < target)
    _register_quote_paths(plan, 'fund', behind)
    code = _run_module([
        sys.executable, '-m', 'src.data.local_store', 'build',
        '--data-type', 'fund', '--incremental', '--max-date', target.isoformat(),
        '--skip-derive-turnover', '--skip-anomaly-lists', '--skip-factor-verify',
    ], '基金行情刷新')
    after = _daily_index_end(plan.root, kinds=('fund',))
    ok = code == 0 and after is not None and after >= target
    return ok, f'末日 {before}→{after or "-"} | 落后 {len(behind)} 只 | rc={code}'


def _step_futures(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """期货日线按交易日增量追加 → 重建连续/换月/合约三表（非致命）。

    采集失败即不重建：避免在旧数据上刷出新的 ``built_at`` 掩盖停更。
    无新交易日时采集与构建均不落盘（见 ``fetch/build`` 的幂等分支）。
    """
    code = _run_module([sys.executable, str(ROOT / 'tools' / 'fetch_futures_daily.py')],
                       '期货日线采集', timeout=600)
    if code != 0:
        return False, f'采集返回 {code}，该日未记进度、未重建'
    build = _run_module([sys.executable, str(ROOT / 'tools' / 'build_futures_continuous.py'),
                         '--apply'], '期货连续合约重建')
    plan.expected.update(AUX_FUTURES_OUTPUTS)
    futures_dir = plan.root / 'quotes' / 'futures'
    if futures_dir.is_dir():
        for path in futures_dir.rglob('*.parquet'):
            rel = path.relative_to(plan.root).as_posix()
            plan.expected.add(rel)
            plan.expected.add(Path(rel).with_suffix('.manifest.json').as_posix())
    if build != 0:
        return False, f'重建返回 {build}'
    return True, '增量追加 + 三表重建完成'


def _read_optional_frame(path: Path) -> pl.DataFrame:
    if not path.is_file():
        return pl.DataFrame()
    try:
        return pl.read_parquet(path)
    except (OSError, pl.exceptions.PolarsError):
        return pl.DataFrame()


def _factor_copy_detail(plan: _Plan,
                        kinds: tuple[str, ...] | None = None) -> dict[str, Any]:
    """刷新后复验行情内嵌因子副本 vs 因子快照；返回各类型明细（含不一致代码）。"""
    quote_store = plan.store or LocalQuoteStore(plan.root, enabled=True)
    factor_store = plan.factor_store or LocalFactorStore(plan.root, enabled=True)
    result = verify_quote_factor_consistency(
        quote_store, factor_store, **({'kinds': kinds} if kinds else {}))
    if 'error' in result:
        raise RuntimeError(str(result['error']))
    return result


def _repair_quote_factor_copies(plan: _Plan, detail: dict[str, Any]) -> list[str]:
    """副本与快照不一致时，按快照定向重写这些标的的行情副本。

    快照是基准：不一致说明行情副本停在旧口径（典型场景是 dividend_monthly
    重建了因子快照、但被超时截断没来得及回写行情副本，而因子末日已对齐，
    下一步就直接跳过重算——2026-11-24 的 38 只分级基金即此）。
    """
    fixed: list[str] = []
    stock_codes = list(detail.get('stock', {}).get('mismatch_code_list') or [])
    fund_codes = list(detail.get('fund', {}).get('mismatch_code_list') or [])
    if stock_codes:
        from src.data.local_store.factor_rebuild import (  # noqa: PLC0415
            rebuild_factors_from_local,
        )

        rebuild_factors_from_local(plan.store, plan.factor_store,
                                   codes=stock_codes, include_existing=True)
        _register_quote_paths(plan, 'stock', stock_codes)
        fixed.append(f'个股 {len(stock_codes)} 只')
    if fund_codes:
        from src.data.local_store.fund_factors import (  # noqa: PLC0415
            rebuild_fund_factors,
        )

        dividends = _read_optional_frame(plan.root / 'factors' / 'fund_dividend.parquet')
        splits = _read_optional_frame(plan.root / 'factors' / 'fund_share_change.parquet')
        rebuild_fund_factors(plan.root, dividends, splits, codes=fund_codes)
        _register_quote_paths(plan, 'fund', fund_codes)
        fixed.append(f'基金 {len(fund_codes)} 只')
    if fixed:
        plan.expected.update(AUX_FACTOR_OUTPUTS)
    return fixed


def _step_factors(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """复权因子刷新 → 收尾复验行情内嵌副本（致命步）。

    第 4 步 build 已改带 ``--skip-factor-verify``：它自带的副本一致性断言依赖
    本步结果。断言在这里执行——个股/指数/基金刷新完之后再校验，覆盖本次运行
    刷新的范围；真正的滞后（重算没推进末日或副本仍不一致）照样判红。
    """
    notes: list[str] = []
    remaining_stock: list[str] = []
    remaining_fund: list[str] = []
    if args.skip_factor_refresh:
        notes.append('已跳过刷新（--skip-factor-refresh）')
    else:
        stock_codes = _stale_factor_codes(plan.root, 'stock')
        fund_codes = _stale_factor_codes(plan.root, 'fund')
        if not stock_codes and not fund_codes:
            notes.append(
                f'个股/基金因子末日均对齐行情（{_daily_index_end(plan.root)}），跳过不重算')
        else:
            _register_quote_paths(plan, 'stock', stock_codes)
            _register_quote_paths(plan, 'fund', fund_codes)
            plan.expected.update(AUX_FACTOR_OUTPUTS)
            if stock_codes:
                from src.data.local_store.factor_rebuild import (  # noqa: PLC0415
                    rebuild_factors_from_local,
                )

                stats = rebuild_factors_from_local(
                    plan.store, plan.factor_store, codes=stock_codes, include_existing=True)
                notes.append(f'个股 {len(stock_codes)} 只 → 新 {int(stats.get("codes_added") or 0)} 只'
                             f' / {int(stats.get("rows_added") or 0):,} 行')
            if fund_codes:
                from src.data.local_store.fund_factors import (  # noqa: PLC0415
                    rebuild_fund_factors,
                )

                dividends = _read_optional_frame(plan.root / 'factors' / 'fund_dividend.parquet')
                splits = _read_optional_frame(plan.root / 'factors' / 'fund_share_change.parquet')
                stats = rebuild_fund_factors(plan.root, dividends, splits, codes=fund_codes)
                notes.append(f'基金 {len(fund_codes)} 只 / {int(stats.get("factor_rows") or 0):,} 行')
            remaining_stock = _stale_factor_codes(plan.root, 'stock')
            remaining_fund = _stale_factor_codes(plan.root, 'fund')
            if remaining_stock or remaining_fund:
                notes.append(f'仍落后 个股 {len(remaining_stock)} 只 / 基金 {len(remaining_fund)} 只')
    detail = _factor_copy_detail(plan)
    mismatch = {kind: int(info.get('mismatch_codes') or 0)
                for kind, info in detail.items()}
    if any(mismatch.values()):
        fixed = _repair_quote_factor_copies(plan, detail)
        recheck = tuple(kind for kind, count in mismatch.items() if count)
        detail = _factor_copy_detail(plan, recheck)
        mismatch.update({kind: int(info.get('mismatch_codes') or 0)
                         for kind, info in detail.items()})
        notes.append(f'副本不一致已按快照定向重写：{" / ".join(fixed) or "无可修复类型"}')
    if any(mismatch.values()):
        notes.append('行情内嵌因子副本与快照不一致：个股 {} 只 / 指数 {} 只 / 基金 {} 只'.format(
            mismatch.get('stock', 0), mismatch.get('index', 0), mismatch.get('fund', 0)))
        return False, ' | '.join(notes)
    notes.append('副本一致（个股/指数/基金 0/0/0）')
    return not (remaining_stock or remaining_fund), ' | '.join(notes)


def _step_dividend_monthly(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """月度分红全量（非致命）：季度/半年度除权除息 + 月度分红全量 + 因子重建。

    调用 ``tools/refresh_dividend_monthly.py``（5 阶段串行 + 断点续跑），
    失败只告警、不阻断后续步骤。脚本内部自行重建受影响标的的复权因子。
    """
    if args.skip_dividend_monthly:
        return True, '已跳过（--skip-dividend-monthly）'
    if not args.allow_remote:
        return True, '未允许联网，跳过（只告警）'
    due, why = _dividend_monthly_due(args.dividend_monthly_days)
    if not due:
        _note_dividend_monthly_skip('not_due', why, args.dividend_monthly_days, plan.latest)
        return True, f'未到期，跳过（{why}）'
    plan.expected.update(AUX_DIVIDEND_OUTPUTS)
    plan.expected.update(AUX_FACTOR_OUTPUTS)
    cmd = [sys.executable, str(ROOT / 'tools' / 'refresh_dividend_monthly.py'), '--apply']
    resume = _dividend_monthly_resume_dir()
    # 有未完成断点时续跑原工作目录（目录名带日期，换目标日会从头再取证 1,696 只）
    cmd += ['--work-dir', str(resume)] if resume else ['--date', plan.latest.isoformat()]
    code = _run_module(cmd, '月度分红全量', timeout=DIVIDEND_MONTHLY_TIMEOUT_S)
    if code == 124:
        _note_dividend_monthly_skip(
            'timeout', f'超时 {DIVIDEND_MONTHLY_TIMEOUT_S // 3600}h 被终止',
            args.dividend_monthly_days, plan.latest)
    work = resume or (ROOT / 'work' / f'分红月度全量_{plan.latest:%Y-%m-%d}')
    fund_codes, stock_codes = _dividend_monthly_affected(work)
    # 2026-11-24：入库与个股因子重建会重写受影响标的的行情副本，与 dividend 步一样
    # 登记进 manifest 白名单，否则校验把它们判为「预期外改动」（div_a 批次 diffs=15）
    if fund_codes:
        _register_quote_paths(plan, 'fund', fund_codes)
    if stock_codes:
        _register_quote_paths(plan, 'stock', stock_codes)
    if fund_codes or stock_codes:
        plan.expected.update(('meta/daily_index.parquet', 'meta/daily_index.manifest.json'))
    return code == 0, (f'{why} | 登记行情 基金 {len(fund_codes)} / 个股 {len(stock_codes)} 只'
                       f' | rc={code}')


def _dividend_monthly_affected(work: Path) -> tuple[list[str], list[str]]:
    """从月度分红断点读取本轮受影响标的：基金取入库 rows csv 的代码，个股取因子重建清单。"""
    try:
        state = json.loads((work / 'state.json').read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return [], []
    phases = state.get('phases') or {}
    fund: set[str] = set()
    rows_csv = (phases.get('fund_ingest') or {}).get('rows_csv')
    if rows_csv and Path(rows_csv).is_file():
        import csv  # noqa: PLC0415
        with open(rows_csv, encoding='utf-8', newline='') as handle:
            fund = {row['ts_code'] for row in csv.DictReader(handle) if row.get('ts_code')}
    stock = [str(c) for c in ((phases.get('stock_factors') or {}).get('affected_codes') or [])]
    return sorted(fund), sorted(set(stock))


def _dividend_monthly_states() -> list[tuple[Path, dict[str, Any]]]:
    out: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted((ROOT / 'work').glob('分红月度全量_*/state.json')):
        try:
            out.append((path.parent, json.loads(path.read_text(encoding='utf-8'))))
        except (OSError, ValueError):
            continue
    return out


def _dividend_monthly_resume_dir() -> Path | None:
    """最近一个未完成（running/aborted/failed）的工作目录，用于断点续跑。"""
    for work, state in reversed(_dividend_monthly_states()):
        if state.get('status') == 'done':
            return None
        return work
    return None


def _dividend_monthly_lag() -> tuple[str | None, int | None]:
    """最近一次 status=done 的完成日与其距今天数；无记录返回 (None, None)。"""
    finished = [str(s.get('updated_at') or '')[:10] for _, s in _dividend_monthly_states()
                if s.get('status') == 'done']
    if not finished:
        return None, None
    try:
        last = date.fromisoformat(max(finished))
    except ValueError:
        return None, None
    return last.isoformat(), (date.today() - last).days


def _dividend_monthly_due(days: int) -> tuple[bool, str]:
    """距最近一次 status=done 的完成日 ≥ days，或存在未完成断点，即到期。"""
    if _dividend_monthly_resume_dir() is not None:
        return True, '存在未完成断点，续跑'
    last, age = _dividend_monthly_lag()
    if age is None:
        return True, '无完成记录'
    return age >= days, f'上次完成 {last}，距今 {age} 天 / 阈值 {days} 天'


def _note_dividend_monthly_skip(reason: str, why: str, days: int,
                                target: date | None) -> None:
    """月度分红跳过留痕：统一日志事件 + 告警行（不改变失败判据，只求可检索）。

    ``reason``：``not_due``（未到期）/ ``timeout``（超时被杀）。事件里带上次成功
    日期与距今天数，避免「分红几轮没更新而没人知道」。
    """
    last, age = _dividend_monthly_lag()
    _log_event({'event': 'dividend_monthly_skip', 'reason': reason, 'why': why,
                'last_success': last, 'days_since_success': age,
                'threshold_days': days,
                'target_date': target.isoformat() if target else None,
                'ts': datetime.now().isoformat(timespec='seconds')})
    logger.warning('[daily] 月度分红全量跳过（{}）：{}；上次成功 {}，距今 {} 天，阈值 {} 天',
                   reason, why, last or '无记录', '?' if age is None else age, days)


def _dividend_monthly_lag_warning(threshold: int) -> str | None:
    """月度分红滞后告警行（走已有 warning 通道，不计 error、不改退出码）。"""
    last, age = _dividend_monthly_lag()
    if age is None or threshold <= 0 or age <= threshold:
        return None
    return (f'[WARNING] dividend_monthly（月度分红全量）· stale_state：'
            f'上次成功 {last}，距今 {age} 天（阈值 {threshold} 天）')


def _step_dividend(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """分红日频增量（非致命）：基金年度批量 + 个股近两报告期，只追加/补缺。

    脚本内按源隔离失败：失败源不写盘、不删行，另一源照常；受影响标的的复权
    因子与行情内嵌副本在同一步重建。这里只把会写的件登记进 manifest 白名单，
    并从状态台账取受影响标的登记行情路径。
    """
    if args.skip_dividend_refresh:
        return True, '已跳过（--skip-dividend-refresh）'
    if not args.allow_remote:
        return True, '未允许联网，跳过（只告警）'
    plan.expected.update(AUX_DIVIDEND_OUTPUTS)
    code = _run_module(
        [sys.executable, str(ROOT / 'tools' / 'refresh_dividend_daily.py'),
         '--store-root', str(plan.root), '--date', plan.latest.isoformat(),
         '--bootstrap-days', str(args.dividend_bootstrap_days)], '分红日频增量',
        timeout=900)
    state_path = plan.root / 'meta' / 'dividend_status' / 'daily_refresh_state.json'
    try:
        state = json.loads(state_path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        state = {}
    last = state.get('last_result') or {}
    sources = (last.get('sources') if last.get('target') == plan.latest.isoformat() else None) or {
        kind: state.get(kind) or {} for kind in ('fund', 'stock')}
    notes: list[str] = []
    for kind in ('fund', 'stock'):
        item = sources.get(kind) or {}
        codes = [str(value) for value in (item.get('affected_codes') or [])]
        if codes:
            _register_quote_paths(plan, kind, codes)
            plan.expected.update(AUX_FACTOR_OUTPUTS)
        if item.get('ok') and not item.get('skipped'):
            rows = item.get('added_rows', item.get('appended_rows', 0))
            notes.append(f'{kind} 新增 {int(rows or 0)} 行 / 影响 {len(codes)} 只')
        elif not item.get('ok'):
            notes.append(f'{kind} 失败：{str(item.get("error") or "-")[:60]}')
    detail = ' | '.join(notes) or '无增量'
    return code == 0, f'{detail} | rc={code}'


def _step_events(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """公告事件库日更（非致命）：逐自然日抓取 → 稳定化收敛 → 追加入库。

    ``--allow-empty`` 是硬要求：单日窗口下稀疏关键词（如「重大合同」）常态返回
    结构正常的 ``totalRecordNum=0``，不加该开关会在第一个空关键词处判失败并停止，
    整轮补采无法推进（2026-11-24 实测）；结构异常（非 dict / 缺键 / 类型错）仍按
    失败处理。覆盖声明 ``coverage.end`` 不由本步推进（口径：需人工隔日推进）。
    """
    if not args.allow_remote:
        return True, '未允许联网，跳过（只告警）'
    plan.expected.update(AUX_EVENTS_OUTPUTS)
    code = _run_module(
        [sys.executable, str(ROOT / 'tools' / 'update_events.py'),
         '--store-root', str(plan.root), '--date', plan.latest.isoformat(),
         '--allow-empty'], '公告事件库日更', timeout=900)
    return code == 0, f'rc={code}'


def _step_margin(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """融资融券明细日更（非致命）。

    两融按 T+1 发布：工具内部只补 ``day < today`` 且未完成的交易日，源端当天
    尚无数据时以 ``[SKIP]`` 跳过（不记失败、不推进进度），下一轮自动重试；
    真正的行数异常/接口失败仍返回 1。--end 传目标交易日，--start 用工具默认
    （状态文件已完成的日期零请求）。
    """
    if not args.allow_remote:
        return True, '未允许联网，跳过（只告警）'
    code = _run_module(
        [sys.executable, str(ROOT / 'tools' / 'fetch_margin_detail.py'),
         '--end', plan.latest.isoformat()], '融资融券明细日更', timeout=900)
    return code == 0, f'rc={code}'


def _sw_index_paths(root: Path) -> set[str]:
    """801xxx 分片的正式件与侧车（白名单用；只登记已存在的文件）。"""
    out: set[str] = set()
    for path in (root / 'quotes' / 'index' / 'sh').glob('801*.parquet'):
        rel = path.relative_to(root).as_posix()
        out.add(rel)
        out.add(Path(rel).with_suffix('.manifest.json').as_posix())
    return out


def _step_sw_index(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """申万一级行业指数增量（非致命）：只追加源端已有的新日期。

    源端 trend 接口返回全历史，无区间参数；本步仍按「本地末根之后」筛选，
    重复执行不产生重复行。口径不追 T 日：源端 OHLC 落后时记侧车 ``tail_gap``，
    由保鲜体检按「已知源端滞后」放行，不判数据断裂。
    """
    if args.skip_sw_index:
        return True, '已跳过（--skip-sw-index）'
    if not args.allow_remote:
        return True, '未允许联网，跳过（只告警）'
    plan.expected.update(_sw_index_paths(plan.root))
    stage = ROOT / '_staging' / f'sw_index_{plan.latest:%Y%m%d}'
    summary_path = stage / 'summary.json'
    try:
        summary_path.unlink()
    except OSError:
        pass
    code = _run_module(
        [sys.executable, str(ROOT / 'tools' / 'build_sw_industry_index.py'),
         '--re-pull', '--store-root', str(plan.root), '--stage-dir', str(stage),
         '--summary-json', str(summary_path)], '申万行业指数增量', timeout=600)
    if code != 0:
        return False, f'rc={code}（非致命：只记录，不阻断后续步骤）'
    try:
        summary = json.loads(summary_path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        return False, f'rc=0 但汇总不可读：{type(exc).__name__}'
    gap = summary.get('tail_gap') or {}
    added = int(summary.get('added_rows') or 0)
    detail = (f'新增 {added:,} 行 / {int(summary.get("codes_added") or 0)} 只'
              f' | 本地末日 {summary.get("local_last")}'
              f' | tail_gap {gap.get("start") or "-"}~{gap.get("end") or "-"}'
              f'（{int(gap.get("rows") or 0)} 个交易日）')
    if added == 0:
        detail = f'无新增（源端停在 {summary.get("source_last")}）| ' + detail
    print(f'[日更] 申万行业指数增量 | {detail}')
    return True, detail


def _step_pool(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    line = _run_pool_refresh(plan, args)
    return '未完成' not in line, (line or '未到期/已跳过')


def _step_manifest(plan: _Plan, args: argparse.Namespace,
                   state: dict[str, Any]) -> tuple[bool, str]:
    """回写本次改动件的 manifest，并做逐件对账与预期差集检查（致命步）。

    哈希/行数走 ``work/cache/manifest_facts.json`` 增量复用：``(path, size, mtime)``
    全一致才复用，任一项变化即重算；``QUANT_MANIFEST_FULL_HASH=1`` 可强制全量重算。
    复用的只是「同一文件的事实怎么取」，变更集判定与逐件对账判据均不改变。
    """
    root = plan.root
    cache = _manifest_facts_cache(bool(getattr(args, 'manifest_full_hash', False)))
    changed = _walk(root, plan.marker_ts)
    refreshed = _refresh(root, changed, cache)
    report = _verify(root, changed, plan.expected, plan.coverage, _coverage(root), cache)
    digest = _sha256_text(f'{rel}:{file_facts(root / rel, cache)["sha256"]}'
                          for rel in sorted(changed)
                          if (root / rel).is_file())
    for rel in report['unregistered']:
        logger.warning('[daily] 未登记（manifest 无该件条目）：{}', rel)
    remaining, _ = _plan_gaps(_coverage(root), plan.latest, plan.data_types,
                              plan.codes, args.max_gap_days)
    if remaining:
        logger.warning('[daily] 仍有缺口 {} 个（采集侧无新增/未服务，按 DG-35 不再复跑，'
                       '需补请另开批次）', len(remaining))
    state.update(changed=changed, report=report, digest=digest)
    cache.save()
    detail = (f'变更 {len(changed)} 件 | manifest 回写 {refreshed} 条 | '
              f"+{report['added']:,}/-{report['deleted']:,} 行 | 差异 {len(report['problems'])} | "
              f'哈希 重算 {cache.recomputed}/复用 {cache.hits}')
    if report['problems']:
        detail += ' | ' + '；'.join(report['problems'][:3])
    return not report['problems'], detail


def _step_clean_set(plan: _Plan) -> tuple[bool, str]:
    code = _run_module(
        [sys.executable, '-m', 'src.data.quality.clean_set', 'build',
         '--as-of', plan.latest.isoformat(), '--root', str(plan.root)], '洁净集快照')
    return code == 0, f'as_of {plan.latest} | rc={code}'


def _step_backup() -> tuple[bool, str]:
    code = _run_module([sys.executable, str(ROOT / 'tools' / 'backup_critical_assets.py')],
                       '不可重建资产备份')
    return code == 0, f'rc={code}'


def _step_gate(plan: _Plan) -> tuple[bool, str]:
    code = _run_module([sys.executable, str(ROOT / 'tools' / 'daily_data_gate.py'),
                        '--store-root', str(plan.root)], '每日数据门禁')
    return code == 0, f'rc={code}'


def _noncritical_warning_lines(state: dict[str, Any]) -> list[str]:
    """把本轮非致命步骤失败渲染成体检 warning 行（只告警，不改退出码）。"""
    lines: list[str] = []
    for item in state.get('failed_noncritical') or []:
        code = item.get('exit')
        lines.append(f'[WARNING] {item.get("step")}（{item.get("name", "")}）'
                     f'· 非致命步骤失败：exit={code if code is not None else "?"}')
    return lines


def _step_dataset_health(plan: _Plan, args: argparse.Namespace,
                         state: dict[str, Any]) -> tuple[bool, str]:
    """日更最后一道闸门：按契约只读体检，异常只判失败、不回滚已入库步骤。

    分级沿用 ``tools/dataset_health_check.py``：契约违规 / schema 不符 / 空表 /
    关键列空值 / ``max_date`` 滞后超阈值算异常；ledger 未登记、目录级 manifest
    缺失或滞后算告警。滞后阈值取契约里的 ``max_lag_days``（与
    ``freshness`` 表体检 ``LAG_WARN_MAX`` 同为 5 自然日），不另造一套。
    """
    out_dir = ROOT / 'artifacts' / 'dataset_health'
    proc = subprocess.run(
        [sys.executable, str(ROOT / 'tools' / 'dataset_health_check.py'),
         '--root', str(plan.root), '--json'],
        cwd=str(ROOT), capture_output=True, text=True,
        encoding='utf-8', errors='replace')
    if proc.returncode not in (0, 1):
        tail = (proc.stderr or proc.stdout or '').strip().splitlines()
        return False, f'体检未跑通（rc={proc.returncode}）：{tail[-1][:200] if tail else "无输出"}'

    as_of = date.today().isoformat()
    report_path = out_dir / f'{as_of}.json'
    try:
        report = json.loads(report_path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        return False, f'体检报告读取失败（{report_path.name}）：{exc}'

    summary = report.get('summary') or {}
    errors = int(summary.get('error') or 0)
    warns = int(summary.get('warn') or 0)
    lines = [
        f'# 数据体检摘要 {report.get("as_of")}',
        f'- 覆盖数据集：{summary.get("datasets", 0)}；异常 {errors}；仅告警 {warns}；'
        f'健康 {summary.get("ok", 0)}',
        '- 异常：契约违规 / schema 不符 / 空表 / 关键列空值 / max_date 滞后超阈值',
        '- 告警：ledger 未登记 / 目录级 manifest 缺失或滞后',
        f'- 报告：{(out_dir / (as_of + ".md")).relative_to(ROOT).as_posix()}',
        '',
    ]
    bad: list[str] = []
    for item in report.get('results') or []:
        kinds = [f.get('kind') for f in (item.get('findings') or [])
                 if f.get('level') == 'error']
        if kinds:
            bad.append(f'{item.get("key")}({",".join(sorted({str(k) for k in kinds}))})')
        for finding in item.get('findings') or []:
            level = 'ERROR' if finding.get('level') == 'error' else 'WARNING'
            line = (f'[{level}] {item.get("key")}（{item.get("label", "")}）'
                    f'· {finding.get("kind")}：{finding.get("detail")}')
            lines.append(line)
            print(line)
            if level == 'ERROR':
                logger.error('[daily] {}', line)
            else:
                logger.warning('[daily] {}', line)

    batch_dir = state.get('batch_dir')
    lag_line = _dividend_monthly_lag_warning(
        args.dividend_monthly_days or DEFAULT_DIVIDEND_MONTHLY_DAYS)
    if lag_line:
        # 月度分红滞后走已有 warning 通道：只告警、不计 error、不改退出码。
        lines.append(lag_line)
        print(lag_line)
        logger.warning('[daily] {}', lag_line)
    for line in _noncritical_warning_lines(state):
        # 非致命步骤失败同样只走既有 warning 通道：不计 error、不改退出码。
        lines.append(line)
        print(line)
        logger.warning('[daily] {}', line)
    if batch_dir:
        batch_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / 'dataset_health_summary.md').write_text(
            '\n'.join(lines) + '\n', encoding='utf-8')
    state['health'] = {'error': errors, 'warn': warns}
    detail = f'异常 {errors} / 告警 {warns}'
    if lag_line:
        detail += ' | 月度分红滞后'
    if bad:
        detail += ' | ' + ', '.join(bad)
    return errors == 0, detail


def _step_derived_builder(plan: _Plan, dataset: str, script: str,
                          label: str) -> tuple[bool, str]:
    """本地派生表日更：跑 ``tools/<script> --apply``，末根未前进不算失败。

    幂等键：按交易日重写整表，断点分块续算；落盘后由构建脚本自行补登 ledger。
    """
    import polars as pl  # noqa: PLC0415
    from src.data.dataset_contracts import contract_for_key  # noqa: PLC0415

    contract = contract_for_key(dataset)
    target = plan.root / contract.pattern
    try:
        before = target.stat().st_mtime_ns
    except OSError:
        before = 0
    code = _run_module([sys.executable, str(ROOT / 'tools' / script),
                        '--store-root', str(plan.root), '--apply'], label)
    if code != 0:
        return False, f'{script} rc={code}'
    try:
        after = target.stat().st_mtime_ns
    except OSError:
        return False, '未落盘'
    if after == before:
        return True, '无新增未落盘'
    frame = pl.read_parquet(target, columns=[contract.date_column])
    last = str(frame[contract.date_column].max())[:10]
    return True, f'末日 {last} | {frame.height:,} 行 | 已补齐'


def _step_suspension(plan: _Plan) -> tuple[bool, str]:
    """停复牌日表（非致命）：``extended_builders suspension --commit`` 全量重算。

    口径「交易日历 − 实际有 bar 的交易日」，只读本地行情，不联网；末日跟随
    统一交易日历的权威末日。失败只告警，不影响后续步骤。
    """
    code = _run_module(
        [sys.executable, '-m', 'src.data.quality.extended_builders', 'suspension',
         '--root', str(plan.root), '--as-of', plan.latest.isoformat(), '--commit'],
        '停复牌日表', timeout=1800)
    if code != 0:
        return False, f'rc={code}'
    try:
        cov = json.loads((plan.root / 'meta' / 'suspension' / 'coverage.json')
                         .read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return True, 'rc=0（coverage 未读到）'
    return True, (f'末日 {cov.get("end")} | {int(cov.get("suspended_rows") or 0):,} 行 / '
                  f'{int(cov.get("suspended_codes") or 0):,} 只 | rc=0')


def _step_macro(plan: _Plan, args: argparse.Namespace) -> tuple[bool, str]:
    """宏观两融日更：源端补缺 → 合并校验 → 追加发布（需 --allow-remote）。

    幂等键：只补源端缺失日，按 (date, market) 追加；发布前断言既有行不变。
    """
    if not args.allow_remote:
        return True, '本轮跳过（未加 --allow-remote）'
    plan.expected.update(AUX_MACRO_OUTPUTS)
    tool = ROOT / 'tools' / 'margin_daily.py'
    for sub in ('collect', 'build'):
        code = _run_module([sys.executable, str(tool), sub], f'宏观两融 {sub}', timeout=600)
        if code != 0:
            return False, f'{sub} rc={code}'
    code = _run_module([sys.executable, str(tool), 'publish', '--yes'], '宏观两融发布',
                       timeout=600)
    if code != 0:
        return False, f'publish rc={code}'
    import polars as pl  # noqa: PLC0415
    from src.data.dataset_contracts import contract_for_key  # noqa: PLC0415

    contract = contract_for_key('macro_market_margin')
    frame = pl.read_parquet(plan.root / contract.pattern, columns=['date'])
    return True, f'末日 {str(frame["date"].max())[:10]} | {frame.height:,} 行 | 已发布'


def _step_list(plan: _Plan, args: argparse.Namespace,
               state: dict[str, Any]) -> list[tuple[str, str, bool,
                                                    Callable[[], tuple[bool, str]]]]:
    """按 STEP_PLAN 组装可执行步骤（顺序与 dry-run 展示一致）。"""
    runners: dict[str, Callable[[], tuple[bool, str]]] = {
        'code_table': lambda: _step_code_table(plan, args),
        'industry': lambda: _step_industry(plan),
        'fill': lambda: _step_fill(plan, args),
        'build': lambda: _step_build(plan, args),
        'fund_quote': lambda: _step_fund_quotes(plan, args),
        'futures': lambda: _step_futures(plan, args),
        'factor': lambda: _step_factors(plan, args),
        'dividend_monthly': lambda: _step_dividend_monthly(plan, args),
        'dividend': lambda: _step_dividend(plan, args),
        'events': lambda: _step_events(plan, args),
        'sw_index': lambda: _step_sw_index(plan, args),
        'st': lambda: _step_st(plan, args),
        'basic': lambda: _step_basic(plan, args),
        'valuation': lambda: _run_valuation(plan, args),
        'margin': lambda: _step_margin(plan, args),
        'index_valuation': lambda: _run_index_valuation(plan, args),
        'factor_panel': lambda: _step_factor_panel(plan, args),
        'b_topn_picks': lambda: _step_b_topn_picks(plan, args),
        'breadth': lambda: _run_breadth(plan),
        'breadth_ma': lambda: _step_derived_builder(
            plan, 'breadth_ma_daily', 'build_breadth_ma.py', '均线占比宽度'),
        'state_vector': lambda: _step_derived_builder(
            plan, 'state_vector_daily', 'build_state_vector.py', '市场状态向量'),
        'macro': lambda: _step_macro(plan, args),
        'st_daily': lambda: _step_st_daily(plan, args, state),
        'limit_daily': lambda: _step_limit_daily(plan, args, state),
        'pool': lambda: _step_pool(plan, args),
        'suspension': lambda: _step_suspension(plan),
        'manifest': lambda: _step_manifest(plan, args, state),
        'clean_set': lambda: _step_clean_set(plan),
        'freshness': lambda: _run_freshness(plan.root),
        'version': lambda: _run_version(plan.root, state.get('changed') or {}),
        'backup': _step_backup,
        'gate': lambda: _step_gate(plan),
        'dataset_health': lambda: _step_dataset_health(plan, args, state),
    }
    return [(key, name, critical, runners[key])
            for key, name, critical in STEP_PLAN]


def _summary_line(plan: _Plan, outcomes: Sequence[_StepOutcome],
                  state: dict[str, Any], seconds: float) -> str:
    failed = [item for item in outcomes if not item.ok]
    fatal = [item for item in failed if item.critical]
    report = state.get('report') or {}
    changed = len(state.get('changed') or ())
    return (f'[日更] {plan.range_label} | {len(outcomes)} 步 '
            f'ok {len(outcomes) - len(failed)} / 失败 {len(failed)}（致命 {len(fatal)}）'
            f' | 变更 {changed} 件 +{report.get("added", 0):,}/-{report.get("deleted", 0):,} 行'
            f' | 总耗时 {seconds:.1f}s')


def _print_run_summary(plan: _Plan, outcomes: Sequence[_StepOutcome],
                       state: dict[str, Any], seconds: float) -> int:
    print(_summary_line(plan, outcomes, state, seconds))
    for item in outcomes:
        if not item.ok:
            mark = '致命' if item.critical else '只记录'
            print(f'[失败] {item.name}（{mark}）| {item.detail}')
    return 1 if any(item.critical and not item.ok for item in outcomes) else 0


def _dividend_skip_note(args: argparse.Namespace) -> str:
    if args.skip_dividend_refresh:
        return '  ← 本轮跳过（--skip-dividend-refresh）'
    if not args.allow_remote:
        return '  ← 本轮跳过（未加 --allow-remote）'
    return f'  ← 基金年度批量 + 个股近两报告期，增量窗口 {args.dividend_bootstrap_days} 天兜底'


def _sw_index_skip_note(args: argparse.Namespace) -> str:
    if args.skip_sw_index:
        return '  ← 本轮跳过（--skip-sw-index）'
    if not args.allow_remote:
        return '  ← 本轮跳过（未加 --allow-remote）'
    return '  ← 801xxx 只追加本地末根之后的日期，不追 T 日'


def _dividend_monthly_skip_note(args: argparse.Namespace) -> str:
    if args.skip_dividend_monthly:
        return '  ← 本轮跳过（--skip-dividend-monthly）'
    if not args.allow_remote:
        return '  ← 本轮跳过（未加 --allow-remote）'
    due, why = _dividend_monthly_due(args.dividend_monthly_days)
    if not due:
        return f'  ← 本轮跳过（{why}）'
    return f'  ← 5 阶段串行（{why}；超时 {DIVIDEND_MONTHLY_TIMEOUT_S // 3600}h）'


def _factor_panel_skip_note(args: argparse.Namespace) -> str:
    if args.skip_factor_panel:
        return '  ← 本轮跳过（--skip-factor-panel）'
    return '  ← 只算目标日截面，按 signal_date 分区 upsert'


def _b_topn_skip_note(plan: _Plan, args: argparse.Namespace) -> str:
    if args.skip_b_topn_picks:
        return '  ← 本轮跳过（--skip-b-topn-picks）'
    if not _is_month_end_signal(plan.root, plan.latest):
        return f'  ← {plan.latest} 非月末调仓日，跳过'
    return '  ← 月末调仓日，产出行业内前 2 → 合并前 50 等权'


def _factor_skip_note(plan: _Plan, args: argparse.Namespace) -> str:
    if args.skip_factor_refresh:
        return '  ← 本轮跳过（--skip-factor-refresh）'
    stock_stale = len(_stale_factor_codes(plan.root, 'stock'))
    fund_stale = len(_stale_factor_codes(plan.root, 'fund'))
    if stock_stale + fund_stale == 0:
        return '  ← 因子末日均已对齐行情，跳过'
    return f'  ← 个股 {stock_stale} 只 / 基金 {fund_stale} 只待增量重算'


def _skip_note(key: str, plan: _Plan, args: argparse.Namespace) -> str:  # noqa: C901
    """dry-run 每步的跳过/口径提示（按步骤键逐条分支，拆函数反而更难对读）。"""
    if key == 'fill' and not args.allow_remote:
        return '  ← 本轮跳过（未加 --allow-remote）'
    if key == 'fund_quote':
        if args.skip_fund_refresh:
            return '  ← 本轮跳过（--skip-fund-refresh）'
        end = _daily_index_end(plan.root, kinds=('fund',))
        if end is None:
            return '  ← 无 fund 覆盖，执行时会判失败'
        if end >= plan.latest:
            return f'  ← 末日 {end} 已最新，跳过'
        behind = sum(1 for value in _quote_end_by_code(plan.root, 'fund').values()
                     if value < plan.latest)
        return f'  ← 末日 {end} → {plan.latest}，落后 {behind} 只'
    if key == 'factor':
        return _factor_skip_note(plan, args)
    if key in ('dividend_monthly', 'dividend'):
        if key == 'dividend_monthly':
            return _dividend_monthly_skip_note(args)
        return _dividend_skip_note(args)
    if key == 'sw_index':
        return _sw_index_skip_note(args)
    if key == 'valuation' and (args.skip_valuation or not args.allow_remote):
        return '  ← 本轮跳过'
    if key == 'index_valuation' and (args.skip_index_valuation or not args.allow_remote
                                     or not _is_month_end_signal(plan.root, plan.latest)):
        return '  ← 本轮跳过'
    if key == 'factor_panel':
        return _factor_panel_skip_note(args)
    if key == 'b_topn_picks':
        return _b_topn_skip_note(plan, args)
    if key == 'st' and args.skip_st_refresh:
        return '  ← 本轮跳过'
    if key in ('st_daily', 'limit_daily'):
        if args.codes:
            return '  ← 代码清单补数模式跳过'
        if key == 'st_daily' and args.skip_st_daily:
            return '  ← 本轮跳过（--skip-st-daily）'
        if key == 'limit_daily' and (args.skip_limit_daily or args.skip_st_daily):
            return '  ← 本轮跳过'
        window = _derived_window(plan, args)
        lead = '依赖 ST 逐日 | ' if key == 'limit_daily' else ''
        return f"  ← {lead}{window['mode']} since {window['since']}（{window['note']}）"
    if key == 'basic' and (args.skip_basic_refresh
                           or not _basic_due(plan.root, args.basic_refresh_days)):
        return '  ← 本轮跳过（未到期/已关闭）'
    if key == 'pool' and (args.skip_pool_refresh
                          or not _pool_due(plan.root, args.pool_refresh_days)):
        return '  ← 本轮跳过（未到期/已关闭）'
    return ''


def _print_dry_plan(plan: _Plan, args: argparse.Namespace) -> None:
    print(f'[日更 dry-run] {plan.range_label} | 覆盖 {len(plan.coverage)} 个标的 | '
          f'待补 {len(plan.gaps)} 个 | 计划写 {len(plan.plan_quote)} 件 | '
          f'输入集合哈希 {plan.inputs["input_hash"][:12]}')
    for index, (key, name, critical) in enumerate(STEP_PLAN, 1):
        mark = '（致命）' if critical else ''
        print(f'  {index:>2}. {name}{mark}{_skip_note(key, plan, args)}')
    index = {key: no for no, (key, _, _) in enumerate(STEP_PLAN, 1)}
    print(f'  顺序约束：备份（第 {index["backup"]} 步）在所有写 data/ 的步骤之后，'
          f'门禁（第 {index["gate"]} 步）在备份之后，数据集体检（第 '
          f'{index["dataset_health"]} 步）是只读收尾闸门：异常只改退出码、'
          '不回滚已完成的入库步骤；任一步失败只记录并继续，致命步失败返回 1。')


def _log_event(payload: dict[str, Any]) -> None:
    """统一日志：锁与 run 事件追加到同一份 JSONL（失败不影响执行）。"""
    try:
        DAILY_LOG.parent.mkdir(parents=True, exist_ok=True)
        with DAILY_LOG.open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + '\n')
    except OSError as exc:
        logger.warning('[daily] 日志写入失败（不影响执行）：{}', exc)


def _pid_alive(pid: int) -> bool:
    """进程存活判定（不发信号，避免误杀）。"""
    if pid <= 0:
        return False
    if os.name == 'nt':
        try:
            import ctypes  # noqa: PLC0415 - 仅 Windows

            handle = ctypes.windll.kernel32.OpenProcess(0x00100000, False, pid)
        except Exception:  # noqa: BLE001 - 判定失败按存活处理，宁可拒绝也不覆盖
            return True
        if not handle:
            return False
        ctypes.windll.kernel32.CloseHandle(handle)
        return True
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _read_log_tail(limit: int = 400) -> list[dict[str, Any]]:
    """读统一日志末尾若干条事件（解析失败的行跳过）。"""
    try:
        lines = DAILY_LOG.read_text(encoding='utf-8', errors='replace').splitlines()
    except OSError:
        return []
    events: list[dict[str, Any]] = []
    for line in lines[-limit:]:
        try:
            item = json.loads(line)
        except ValueError:
            continue
        if isinstance(item, dict):
            events.append(item)
    return events


def _unfinished_run(pid: int) -> tuple[bool, dict[str, Any] | None]:
    """该 pid 的 run_start 之后是否缺 run_end（硬杀/异常退出），并给出最后一条事件。"""
    events = _read_log_tail()
    if not events:
        return False, None
    last_end = max((i for i, item in enumerate(events)
                    if item.get('event') == 'run_end'), default=-1)
    for index in range(len(events) - 1, -1, -1):
        item = events[index]
        if item.get('event') != 'run_start':
            continue
        if int(item.get('pid') or 0) != pid:
            continue
        return index > last_end, events[-1]
    return False, events[-1]


def _lock_mtime() -> str:
    """锁文件 mtime（ISO 秒级）；取不到返回 '?'。"""
    try:
        return datetime.fromtimestamp(LOCK_PATH.stat().st_mtime).isoformat(timespec='seconds')
    except OSError:
        return '?'


def _acquire_lock(token: dict[str, Any], target: str | None = None) -> Path:
    """入口锁 + run 令牌：同一时间只允许一个写数据进程；陈旧锁归档告警后接管。

    ``target`` 是本次想跑的目标交易日（拿不到时给 'auto（未探测）'）。被活跃锁挡下
    时也要落一条 ``lock_blocked`` 事件——外界据此区分「没跑」与「被挡」。
    """
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    for _ in range(2):
        try:
            fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            info: dict[str, Any] = {}
            try:
                info = json.loads(LOCK_PATH.read_text(encoding='utf-8'))
            except (OSError, ValueError):
                info = {}
            pid = int(info.get('pid') or 0)
            alive = _pid_alive(pid)
            try:
                age = (datetime.now()
                       - datetime.fromisoformat(str(info.get('started_at')))).total_seconds() / 3600
            except (TypeError, ValueError):
                age = LOCK_MAX_AGE_HOURS + 1.0
            if alive and pid != os.getpid() and age <= LOCK_MAX_AGE_HOURS:
                lock_mtime = _lock_mtime()
                _log_event({'event': 'lock_blocked', 'pid': os.getpid(),
                            'holder_pid': pid, 'lock_mtime': lock_mtime,
                            'holder_started_at': info.get('started_at'),
                            'target_date': target or 'auto（未探测）',
                            'lock_path': str(LOCK_PATH),
                            'ts': datetime.now().isoformat(timespec='seconds')})
                message = (f'[daily] 已有数据更新进程在运行（pid {pid}，起始 '
                           f'{info.get("started_at", "?")}，锁 {LOCK_PATH}）；本次退出')
                logger.error('{}；见 lock_blocked', message)
                raise SystemExit(message)
            if not alive:
                # 「锁在、pid 已不存在、日志无对应 run_end」= 上一轮被硬杀且没收尾：
                # 不能静默接管，先落一条可检索的告警事件（含 pid、锁 mtime、最后事件）。
                unfinished, last_event = _unfinished_run(pid)
                if unfinished:
                    lock_mtime = _lock_mtime()
                    logger.error('[daily] 上一轮未收尾（pid {} 已不存在，锁 mtime {}，'
                                 '最后事件 {}），按陈旧锁归档后继续；见 stale_lock_unfinished_run',
                                 pid, lock_mtime, last_event)
                    _log_event({'event': 'stale_lock_unfinished_run', 'pid': pid,
                                'lock_mtime': lock_mtime, 'last_event': last_event,
                                'started_at': info.get('started_at'),
                                'ts': datetime.now().isoformat(timespec='seconds')})
            archived = LOCK_PATH.with_name(
                f'{LOCK_PATH.name}.stale.{datetime.now():%Y%m%d%H%M%S}')
            os.replace(LOCK_PATH, archived)
            logger.warning('[daily] 陈旧锁（pid {}，存活 {}，{:.1f}h）已归档到 {}，继续执行',
                           pid, alive, age, archived)
            _log_event({'event': 'stale_lock', 'pid': pid, 'alive': alive,
                        'hours': round(age, 2), 'archived': archived.name})
            continue
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(token, handle, ensure_ascii=False)
        _log_event({'event': 'lock_acquired', **token})
        return LOCK_PATH
    raise SystemExit(f'[daily] 两次尝试仍未取得入口锁：{LOCK_PATH}')


def _release_lock(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    except OSError as exc:
        logger.warning('[daily] 锁释放失败（需人工清理 {}）：{}', path, exc)
        return
    _log_event({'event': 'lock_released', 'pid': os.getpid()})


def _run_apply(plan: _Plan, args: argparse.Namespace,
               only: tuple[str, ...] = ()) -> int:
    batch = args.batch or f'daily_{plan.latest.strftime("%Y%m%d")}'
    batch_dir = ROOT / '_staging' / batch
    _write_staging(batch_dir, plan.gaps, plan.inputs)

    state: dict[str, Any] = {'batch_dir': batch_dir}
    steps = [item for item in _step_list(plan, args, state)
             if not only or item[0] in only]
    _log_event({'event': 'run_start', 'pid': os.getpid(), 'argv': sys.argv[1:],
                'range': plan.range_label, 'steps': [item[0] for item in steps],
                'ts': datetime.now().isoformat(timespec='seconds')})
    started = time.perf_counter()
    outcomes = _run_steps(steps, state)
    seconds = time.perf_counter() - started

    report = state.get('report') or {}
    _mark_applied(batch_dir, {
        'changed_files': len(state.get('changed') or ()),
        'rows_added': report.get('added', 0),
        'rows_deleted': report.get('deleted', 0),
        'diffs': len(report.get('problems') or ()),
        'agg_sha256': state.get('digest'),
        'health_errors': (state.get('health') or {}).get('error', 0),
        'health_warns': (state.get('health') or {}).get('warn', 0),
    })
    code = _print_run_summary(plan, outcomes, state, seconds)
    done = {item.key for item in outcomes}
    _log_event({'event': 'run_end', 'exit': code, 'seconds': round(seconds, 2),
                'failed': [item.key for item in outcomes if not item.ok],
                'failed_noncritical': [
                    {'step': item['step'], 'name': item['name'],
                     'exit': item['exit']}
                    for item in (state.get('failed_noncritical') or [])],
                'not_run': [item[0] for item in _step_list(plan, args, state)
                            if item[0] not in done],
                'ts': datetime.now().isoformat(timespec='seconds')})
    if args.target_holdings:
        _run_target_holdings(plan.latest, args.capital)
    return code


def _target_hint(args: argparse.Namespace) -> str:
    """被锁挡下时报告的「本次目标交易日」：显式 --end 优先，否则回退本地最新末根。"""
    if args.end:
        return args.end.isoformat()
    try:
        root = Path(args.store_root) if args.store_root else Path(default_store_root())
        latest = _latest_local(_coverage(root))
        return latest.isoformat() if latest else 'auto（未探测）'
    except Exception:  # noqa: BLE001 - 仅用于留痕，取不到不阻断
        return 'auto（未探测）'


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    if DETACH_RELAY_FLAG in raw:
        return _relay(raw)

    parser = argparse.ArgumentParser(description='日更/补数单入口（一条命令 + 一行状态）')
    parser.add_argument('--start', '--since', dest='start', type=_parse_date, default=None,
                        help='起始日期（YYYY-MM-DD；--since 同义，作为采集/构建下界）')
    parser.add_argument('--end', '--until', dest='end', type=_parse_date, default=None,
                        help='目标交易日（--until 同义；缺省：允许联网时自动探测，否则取本地最新末根）')
    parser.add_argument('--codes', default='', help='逗号分隔的 ts_code 清单（补数用）')
    parser.add_argument('--code-file', default='', help='代码清单文件（一行一个，补数用）')
    parser.add_argument('--batch', default='', help='补数批次名，缺口落到 _staging/<批次>/')
    parser.add_argument('--data-types', default='stock,index',
                        help='参与采集的资产类型（默认 stock,index）')
    parser.add_argument('--max-gap-days', type=int, default=DEFAULT_MAX_GAP_DAYS,
                        help='末根距目标交易日超过该天数视为长期停更，不进待补清单（默认 90）')
    parser.add_argument('--store-root', default=None, help='local_store 根（默认取项目配置）')
    parser.add_argument('--apply', action='store_true', help='真正写盘（缺省 dry-run）')
    parser.add_argument('--dry-run', action='store_true',
                        help='显式 dry-run（默认行为；与 --apply 同给时以 dry-run 为准，不取锁不写盘）')
    parser.add_argument('--only', default='',
                        help='只执行指定步骤（逗号分隔步骤键，键见 dry-run 清单）')
    parser.add_argument('--full', action='store_true',
                        help='全量模式（等同 --full-derived：ST/涨跌停全量重建）')
    parser.add_argument('--allow-remote', action='store_true',
                        help='显式允许联网采集（缺省只跑本地构建与校验）')
    parser.add_argument('--verbose', action='store_true',
                        help='打开 INFO/DEBUG 过程日志（缺省静默，只留一行状态与告警）')
    parser.add_argument('--target-holdings', action='store_true',
                        help='apply 成功后顺带生成 v3 每日目标持仓清单（只读消费，'
                             '失败不改日更原有退出码）')
    parser.add_argument('--capital', type=float, default=1e9,
                        help='目标持仓清单的资金规模（元，仅 --target-holdings 生效）')
    parser.add_argument('--skip-st-refresh', action='store_true',
                        help='跳过 ST 区间表日更（risk_flags refresh + st_history build）')
    parser.add_argument('--skip-st-daily', action='store_true',
                        help='跳过 ST 逐日面板增量（tools/build_st_daily.py，meta/st/daily）')
    parser.add_argument('--skip-limit-daily', action='store_true',
                        help='跳过涨跌停逐日增量（tools/build_limit_board.py，meta/limit/daily）')
    parser.add_argument('--derived-lookback-days', type=int,
                        default=DERIVED_LOOKBACK_SESSIONS,
                        help='ST/涨跌停增量窗口：最近 N 个交易日所在月分区（默认 5）')
    parser.add_argument('--full-derived', action='store_true',
                        help='强制 ST/涨跌停全量重建（历史源被修订后使用）')
    parser.add_argument('--skip-fund-refresh', action='store_true',
                        help='跳过基金行情刷新（通达信本地增量）')
    parser.add_argument('--skip-factor-refresh', action='store_true',
                        help='跳过复权因子刷新（个股 + 基金增量重算）')
    parser.add_argument('--no-tdx-landing-guard', dest='tdx_landing_guard',
                        action='store_false',
                        help='关闭 build 前的通达信日结落地守卫（默认开启：每 5 分钟重试）')
    parser.add_argument('--tdx-landing-max-wait', type=int,
                        default=TDX_LANDING_MAX_WAIT_SECONDS,
                        help='通达信落地守卫最长等待秒数（默认 2400=40 分钟，超时 build 判失败）')
    parser.add_argument('--skip-dividend-refresh', action='store_true',
                        help='跳过分红日频增量（基金年度批量 + 个股近两报告期）')
    parser.add_argument('--skip-dividend-monthly', action='store_true',
                        help='跳过月度分红全量（季度/半年度除权除息 + 月度分红 + 因子重建）')
    parser.add_argument('--dividend-monthly-days', type=int,
                        default=DEFAULT_DIVIDEND_MONTHLY_DAYS,
                        help='月度分红全量距上次完成超过该天数才跑（默认 28；0=强制跑）')
    parser.add_argument('--dividend-bootstrap-days', type=int, default=30,
                        help='分红增量首次运行或断档时的回溯天数（默认 30，仅作窗口兜底）')
    parser.add_argument('--skip-sw-index', action='store_true',
                        help='跳过申万行业指数增量（801xxx 段，只追加不追 T 日）')
    parser.add_argument('--skip-valuation', action='store_true',
                        help='跳过全市场日频估值增量（collect_financials.py --only valuation）')
    parser.add_argument('--skip-index-valuation', action='store_true',
                        help='跳过指数月末估值增量（tools/collect_index_valuation.py --apply）')
    parser.add_argument('--skip-factor-panel', action='store_true',
                        help='跳过因子面板增量（tools/factor_panel.py --mode incremental）')
    parser.add_argument('--skip-b-topn-picks', action='store_true',
                        help='跳过 B 股东盈余比候选名单（仅月末调仓日产出，只写 reports/）')
    parser.add_argument('--skip-basic-refresh', action='store_true',
                        help='跳过 fund_basic / index_basic 名录每日检查')
    parser.add_argument('--basic-refresh-days', type=int, default=DEFAULT_BASIC_REFRESH_DAYS,
                        help='名录距上次检查超过该自然日数才检查（默认 1，即每天检查）')
    parser.add_argument('--skip-pool-refresh', action='store_true',
                        help='跳过多资产 ETF 候选池周更')
    parser.add_argument('--pool-refresh-days', type=int, default=DEFAULT_POOL_REFRESH_DAYS,
                        help='候选池距上次重建超过该天数才重建（默认 7）')
    parser.add_argument('--manifest-full-hash', action='store_true',
                        help='manifest 步强制全量重算 sha256/行数，忽略 (path,size,mtime) 缓存')
    parser.add_argument('--detach', action='store_true',
                        help='脱离父终端拉起本轮日更后立即返回（Windows 经中继两跳，'
                             '父终端退出/被硬杀不影响子进程；输出仍追加到 logs/ 控制台日志）')
    args = parser.parse_args(raw)
    if args.full:
        args.full_derived = True

    if args.detach:
        child_args = [item for item in raw if item != '--detach']
        log_path = _console_log_path()
        pid = _spawn_detached([sys.executable, str(_SELF), *child_args], log_path)
        if not pid:
            _log_event({'event': 'detach_failed', 'pid': 0, 'reason': 'no_pid',
                        'log': str(log_path),
                        'ts': datetime.now().isoformat(timespec='seconds')})
            sys.stderr.write(f'[daily] 脱离启动失败：中继未回报 pid，详见 {log_path}\n')
            return 3
        reason = _verify_detach(pid)
        if reason:
            _log_event({'event': 'detach_failed', 'pid': pid, 'reason': reason,
                        'log': str(log_path),
                        'ts': datetime.now().isoformat(timespec='seconds')})
            sys.stderr.write(f'[daily] 脱离启动失败（pid {pid}）：{reason}；'
                             f'详见 {log_path}\n')
            return 3
        print(f'[daily] 已脱离终端拉起（pid {pid}），stdout/stderr 追加到 {log_path}')
        return 0

    if not os.environ.get(DETACH_ENV):
        _install_console_tee()
    logger.remove()
    logger.add(sys.stderr, level='DEBUG' if args.verbose else 'WARNING')

    only = tuple(item.strip() for item in args.only.split(',') if item.strip())
    valid = {spec[0] for spec in STEP_PLAN}
    unknown = [item for item in only if item not in valid]
    if unknown:
        print(f'[daily] 未知步骤键：{",".join(unknown)}；可用键：{",".join(sorted(valid))}')
        return 2

    args.apply = bool(args.apply and not args.dry_run)
    if not args.apply:
        # dry-run：只盘点，不联网、不落盘（含 _staging）、不取锁
        plan = _build_plan(args)
        logger.info('[daily] 覆盖 {} 个标的，目标交易日 {}，待补 {} 个（长期停更跳过 {} 个），'
                    '输入集合哈希 {}', len(plan.coverage), plan.latest, len(plan.gaps),
                    len(plan.skipped), plan.inputs['input_hash'])
        _print_dry_plan(plan, args)
        if only:
            print(f'  本次只执行：{",".join(only)}（其余步骤不纳入）')
        return 0

    from src.data.dataset_contracts import (  # noqa: PLC0415
        RUN_TOKEN_ENV, RUN_TOKEN_FILE_ENV, build_run_token)

    step_keys = [spec[0] for spec in STEP_PLAN if not only or spec[0] in only]
    token = build_run_token(steps=step_keys)
    lock = _acquire_lock(token, target=_target_hint(args))
    os.environ[RUN_TOKEN_ENV] = json.dumps(token, ensure_ascii=False)
    os.environ[RUN_TOKEN_FILE_ENV] = str(lock)
    try:
        plan = _build_plan(args)
        logger.info('[daily] 覆盖 {} 个标的，目标交易日 {}，待补 {} 个（长期停更跳过 {} 个），'
                    '输入集合哈希 {}，run_id {}', len(plan.coverage), plan.latest,
                    len(plan.gaps), len(plan.skipped), plan.inputs['input_hash'],
                    token['run_id'])
        return _run_apply(plan, args, only)
    finally:
        os.environ.pop(RUN_TOKEN_ENV, None)
        os.environ.pop(RUN_TOKEN_FILE_ENV, None)
        _release_lock(lock)


if __name__ == '__main__':
    raise SystemExit(main())
