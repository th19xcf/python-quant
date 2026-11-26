#!/usr/bin/env python3

r"""数据集契约注册表 + 受控写入入口（最小可用版）。

对应 D 系列口径不一致的前三类成因：

1. **写入入口分散**：统一走 :func:`write_dataset`，写前按契约校验 schema 与
   日期列，不匹配直接抛 :class:`ContractViolation`，不再静默写坏；
2. **manifest / freshness 靠人工**：写入成功后同一入口自动登记
   ``rows / sha256 / max_date / bytes`` 到 ``meta/registry/auto_register.json``，
   单文件表还会顺带补丁本目录 ``manifest.json`` 的对应条目；
3. **命名与语义脱节**：契约里显式绑定 ``key`` 与产物路径，命名规则见
   ``docs/数据命名规范_2026-11-22.md``。

设计约束（有意保持最小）：

- 不重算历史数据、不改研究口径，只做「写前闸门 + 写后登记」；
- 分片表（``quotes/**``，一标的一文件）不逐片登记进总账：5,800+ 条每次全量
  重写既慢又易冲突，分片真值统一由 ``meta/daily_index.parquet`` 承担，
  ledger 只记该数据集的最近一次写入；
- 契约是**唯一事实来源**，写入侧与体检脚本共用同一份定义（
  ``tools/dataset_health_check.py``）。

扩展位：新增数据集只需在 :data:`CONTRACTS` 追加一条 :class:`DatasetContract`，
无需改体检脚本。
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Sequence

import polars as pl
from loguru import logger

from src.data.atomic_write import write_parquet_atomic, write_text_atomic
from src.data.local_store.paths import default_store_root
from src.data.quote_schema import QUOTE_COLUMNS, QUOTE_DTYPES

#: 自动登记 ledger（相对列存根）。
LEDGER_REL_PATH = 'meta/registry/auto_register.json'

#: 调度令牌 / 独立写入（放行阀）约定。
RUN_TOKEN_ENV = 'QUANT_RUN_TOKEN'  # noqa: S105 - 环境变量名，不是凭据
RUN_TOKEN_FILE_ENV = 'QUANT_RUN_TOKEN_FILE'  # noqa: S105 - 环境变量名
STANDALONE_ENV = 'QUANT_ALLOW_STANDALONE'
STANDALONE_REASON_ENV = 'QUANT_STANDALONE_REASON'
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TOKEN_REL = '_locks/daily_update.lock'  # noqa: S105 - 锁文件相对路径
STANDALONE_LEDGER_REL = 'meta/registry/standalone_writes.jsonl'

#: 步骤 → 该步允许写入的数据集键（步骤键与 tools/daily_update.py 的 STEP_PLAN 一致）。
#: 未登记的步骤不限制数据集，但体检会点名（见 :func:`unregistered_writers`）。
STEP_DATASETS: dict[str, tuple[str, ...]] = {
    # 码表步：build_name_history 走 write_dataset('name_history')；stock_basic 当前
    # 仍是裸写，但一并声明，避免后续改受控写入时再次触发越界。
    'code_table': ('name_history', 'stock_basic'),
    # 名录步经 src.main 合并式导出 fund_basic / index_basic（后者尚无契约键）。
    'basic': ('fund_basic',),
    'futures': ('futures_continuous', 'futures_contracts', 'futures_roll_events'),
    # 逐日面板写 st_daily_YYYYMM 分片；证据表写 st_daily_intervals。
    'st_daily': ('st_daily', 'st_daily_intervals'),
    'limit_daily': ('limit_daily',),
    # 公告事件库：正式表 + 覆盖声明；分片写 research/events（不在列存根下，不走令牌）
    'events': ('events',),
    'st': ('st_intervals', 'st_coverage', 'st_supplement_intervals',
           'st_supplement_coverage'),
    'valuation': ('valuation_daily',),
    'dividend': ('stock_dividend', 'fund_dividend'),
    'dividend_monthly': ('stock_dividend', 'fund_dividend'),
    'margin': ('margin_detail',),
    'macro': ('macro_market_margin',),
    'breadth_ma': ('breadth_ma_daily',),
    'state_vector': ('state_vector_daily',),
    'pool': ('etf_pool',),
}

#: 步骤 → 该步实际调用的 tools 脚本（体检据此判断「写数据但未登记」）。
STEP_SCRIPTS: dict[str, tuple[str, ...]] = {
    'code_table': ('build_names_json.py', 'build_name_history.py', 'build_stock_basic.py'),
    'futures': ('fetch_futures_daily.py', 'build_futures_continuous.py'),
    'dividend_monthly': ('refresh_dividend_monthly.py',),
    'dividend': ('refresh_dividend_daily.py',),
    'events': ('update_events.py',),
    'sw_index': ('build_sw_industry_index.py',),
    'valuation': ('collect_financials.py',),
    'margin': ('fetch_margin_detail.py',),
    'index_valuation': ('collect_index_valuation.py',),
    'factor_panel': ('factor_panel.py',),
    'b_topn_picks': ('b_topn_picks.py',),
    'st_daily': ('build_st_daily.py',),
    'limit_daily': ('build_limit_board.py',),
    'pool': ('build_multi_asset_etf_pool.py',),
    'backup': ('backup_critical_assets.py',),
    'gate': ('daily_data_gate.py',),
    'breadth_ma': ('build_breadth_ma.py',),
    'state_vector': ('build_state_vector.py',),
    'macro': ('margin_daily.py',),
}

#: 判断 tools 脚本是否写数据的文本标记（只认 parquet 数据集落盘；报告/文本类不算）。
WRITE_MARKERS = ('write_dataset(', 'write_parquet_atomic(')

#: 已登记但不在 STEP_PLAN 日更链路上的写数据脚本。
#: step：归属步骤（无则 None）；depends：前置数据；idem：幂等键；trigger：触发方式。
WRITER_REGISTRY: dict[str, dict[str, Any]] = {
    'backfill_delisted_quotes.py': {'step': 'fill', 'depends': 'fill', 'idem': '市场末根之后按 (ts_code, date) 补齐退市尾部', 'trigger': '按需'},
    'backfill_delisted_shards.py': {'step': None, 'depends': 'build', 'idem': '按 (ts_code, date) 走 write_dataset 补退市尾部，已存在行跳过（逐分片重入）', 'trigger': '按需'},
    'backfill_events.py': {'step': None, 'depends': 'update_events', 'idem': '按 (ts_code, event_date) 追加入库', 'trigger': '按需'},
    'backfill_share_div.py': {'step': None, 'depends': 'collect_stock_dividend', 'idem': '按 (ts_code, 除权日) 回填补列', 'trigger': '按需'},
    'build_breadth_ma.py': {'step': 'breadth_ma', 'depends': 'build', 'idem': '按交易日重写 breadth_ma_daily，断点分块续算', 'trigger': '日更'},
    'build_etf_equity_pool.py': {'step': 'pool', 'depends': 'pool', 'idem': '整表覆盖 meta/pools/etf_equity_pool.csv', 'trigger': '按需'},
    'build_cn_bond_tables.py': {'step': None, 'depends': 'macro', 'idem': '整表覆盖 cn_bond_index / cn_treasury_yield_curve', 'trigger': '按需'},
    'build_credit_yield_curve.py': {'step': None, 'depends': 'macro', 'idem': '整表覆盖信用收益率曲线', 'trigger': '按需'},
    'build_fund_corporate_actions.py': {'step': None, 'depends': 'factor', 'idem': '整表重建基金公司行为表', 'trigger': '按需'},
    'build_implied_suspension.py': {'step': None, 'depends': 'build', 'idem': '整表重建停牌推断表', 'trigger': '按需'},
    'build_new_share.py': {'step': None, 'depends': 'basic', 'idem': '按 (ts_code, 发行日) upsert 新股发行价', 'trigger': '按需'},
    'build_no_dividend_ledger.py': {'step': None, 'depends': 'dividend', 'idem': '整表重建无分红判定清单', 'trigger': '按需'},
    'build_session_ledger.py': {'step': None, 'depends': 'build', 'idem': '整表重建交易日 session 台账', 'trigger': '按需'},
    'build_st_history_from_tushare.py': {'step': 'st', 'depends': 'st', 'idem': '按代码重建 ST 名称变更区间', 'trigger': '按需'},
    'build_state_vector.py': {'step': 'state_vector', 'depends': 'breadth_ma', 'idem': '按交易日重写 state_vector_daily，断点分块续算', 'trigger': '日更'},
    'build_trading_calendar.py': {'step': None, 'depends': 'build', 'idem': '整表重建官方未来交易日历', 'trigger': '按需'},
    'build_universe_layers.py': {'step': None, 'depends': 'build', 'idem': '按交易日重写 L0-L5 分层矩阵', 'trigger': '按需'},
    'collect_stock_dividend.py': {'step': 'dividend', 'depends': 'dividend', 'idem': '按 (ts_code, 除权日) upsert 分红事件', 'trigger': '按需'},
    'derive_free_float_fill.py': {'step': None, 'depends': 'basic', 'idem': '整列重写自由流通派生列', 'trigger': '按需'},
    'derive_total_dividend.py': {'step': None, 'depends': 'dividend', 'idem': '整列重写分红派现总额派生列', 'trigger': '按需'},
    'fetch_etf_fee.py': {'step': None, 'depends': 'pool', 'idem': '整表覆盖 ETF 费率分段', 'trigger': '按需'},
    'fetch_margin_detail.py': {'step': 'margin', 'depends': 'build', 'idem': '按 (ts_code, date) upsert margin_detail', 'trigger': '日更（--allow-remote）'},
    'fill_free_float_shares.py': {'step': None, 'depends': 'basic', 'idem': '按 ts_code 覆盖自由流通股本', 'trigger': '按需'},
    'fill_internal_gaps.py': {'step': 'fill', 'depends': 'fill', 'idem': '按本地为空的交易日补 TDX 历史缺口', 'trigger': '按需'},
    'fund_split_repair.py': {'step': None, 'depends': 'factor', 'idem': '按 (ts_code, 折算日) 重算基金因子', 'trigger': '按需'},
    'margin_daily.py': {'step': 'macro', 'depends': 'build', 'idem': '仅补源端缺失日并按 (date, market) 追加，既有行断言不变', 'trigger': '日更（--allow-remote）'},
    'mark_fund_tradability.py': {'step': None, 'depends': 'basic', 'idem': '整列重写基金可交易标记', 'trigger': '按需'},
    'repair_quote_anomalies.py': {'step': 'build', 'depends': 'build', 'idem': '按契约唯一正确值就地修复（默认 dry-run）', 'trigger': '按需'},
    'reconcile_p2_metadata.py': {'step': None, 'depends': None, 'idem': '按代码对齐 fund_meta 元数据（JSON 覆盖）', 'trigger': '按需'},
    'treasury_yield.py': {'step': None, 'depends': 'macro', 'idem': '按交易日重写 10Y 国债收益率', 'trigger': '按需'},
    'update_events.py': {'step': 'events', 'depends': None, 'idem': '按自然日抓取追加入库', 'trigger': '日更（--allow-remote）'},
}

#: 一次性迁移/历史回补脚本：已归档到 tools/_oneshot/，保留可执行，不再纳入日常运行。
ARCHIVED_WRITERS: dict[str, str] = {
    'backfill_financials_history_sina.py': '2010 年前财务报表历史一次性补全',
    'backfill_fund_dividend.py': 'DG-35 基金分红逐只取证一次改完',
    'backfill_reference_history.py': 'P1 参考数据历史补采（可续跑）',
    'backfill_stock_dividend.py': 'DG-35 个股分红逐只取证一次改完',
    'fix_name_history_end_dates.py': 'name_history 缺收尾段一次性修补',
    'infer_post_g_segments.py': 'G 股改残留名称段一次性推断',
    'migrate_snapshot_history.py': 'G-098 快照序列合并迁移',
    'st_gapfill.py': '2026-11-21 ST 区间表缺口一次性补齐',
}


def build_run_token(*, steps: Iterable[str], run_id: str | None = None,
                    pid: int | None = None) -> dict[str, Any]:
    """调度器启动时生成一次 run 令牌（写锁文件 + 注入子进程环境变量）。"""
    scope = sorted({key for step in steps for key in STEP_DATASETS.get(step, ())})
    return {
        'run_id': run_id or f'{datetime.now():%Y%m%d%H%M%S}-{os.getpid()}',
        'pid': int(pid or os.getpid()),
        'started_at': datetime.now().isoformat(timespec='seconds'),
        'steps': list(steps),
        'datasets': scope,
    }


def _pid_alive(pid: int) -> bool:
    """进程存活判定（不发信号，避免误杀）。"""
    if pid <= 0:
        return False
    if os.name == 'nt':
        try:
            import ctypes  # noqa: PLC0415 - 仅 Windows

            handle = ctypes.windll.kernel32.OpenProcess(0x00100000, False, pid)
        except Exception:  # noqa: BLE001 - 判定失败按存活处理，宁可拒绝也不放行
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


def _standalone_request(argv: Sequence[str] | None = None) -> tuple[bool, str]:
    """显式独立写入放行：``--allow-standalone`` 或环境变量，理由取同批参数。"""
    args = list(sys.argv[1:] if argv is None else argv)
    enabled = '--allow-standalone' in args or os.environ.get(STANDALONE_ENV) == '1'
    if not enabled:
        return False, ''
    reason = os.environ.get(STANDALONE_REASON_ENV, '')
    for index, item in enumerate(args):
        if item.startswith('--standalone-reason='):
            reason = item.split('=', 1)[1]
        elif item == '--standalone-reason' and index + 1 < len(args):
            reason = args[index + 1]
    return True, reason or '未填写理由'


def _append_standalone_ledger(base: Path, record: dict[str, Any]) -> None:
    path = base / STANDALONE_LEDGER_REL
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('a', encoding='utf-8') as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + '\n')
    except OSError as exc:
        logger.warning(f'[contracts] 独立写 ledger 记录失败：{exc}')


def check_write_context(dataset: str, base: Path | str | None = None) -> dict[str, Any]:
    """写入层闸门：只放行携带有效调度令牌的进程；独立写入须显式放行并留痕。"""
    raw = os.environ.get(RUN_TOKEN_ENV)
    if raw:
        try:
            token = json.loads(raw)
        except ValueError as exc:
            raise ContractViolation(f'写入被拒：调度令牌不可解析（{exc}）') from exc
        token_path = Path(os.environ.get(RUN_TOKEN_FILE_ENV)
                          or PROJECT_ROOT / DEFAULT_TOKEN_REL)
        disk = _read_json(token_path)
        if not disk or disk.get('run_id') != token.get('run_id'):
            raise ContractViolation(
                f'写入被拒：令牌 {token.get("run_id")} 与调度锁文件 {token_path} 不一致或已过期')
        run_pid = int(disk.get('pid') or 0)
        if run_pid != os.getpid() and not _pid_alive(run_pid):
            raise ContractViolation(f'写入被拒：调度进程 pid {run_pid} 已退出，令牌失效')
        scope = [str(item) for item in (token.get('datasets') or disk.get('datasets') or [])]
        if scope and not any(fnmatch.fnmatch(dataset, pattern) for pattern in scope):
            raise ContractViolation(
                f'越界写入：本次 run 声明数据集 {scope}，{dataset} 不在步骤范围内')
        return {'mode': 'scheduled', 'run_id': token.get('run_id'),
                'run_pid': run_pid, 'steps': list(token.get('steps') or [])}
    allowed, reason = _standalone_request()
    if allowed:
        target = Path(base) if base is not None else Path(default_store_root())
        record = {'mode': 'standalone', 'dataset': dataset, 'pid': os.getpid(),
                  'reason': reason, 'argv': list(sys.argv),
                  'ts': datetime.now().isoformat(timespec='seconds')}
        _append_standalone_ledger(target, record)
        return {'mode': 'standalone', 'run_id': None, 'standalone_reason': reason}
    raise ContractViolation(
        f'写入被拒：{dataset} 未携带调度令牌。请经 tools/daily_update.py 执行，'
        '或显式加 --allow-standalone --standalone-reason <理由> 放行（会记入 ledger）')


#: tools/ 下的只读工具/库：源码里出现写函数名但不落盘数据（AST 分析、报表渲染等）。
READ_ONLY_TOOLS: dict[str, str] = {
    'lineage_writers.py': 'AST 血缘分析库：写函数名只出现在常量与判定表，自身不落盘',
}


def unregistered_writers() -> list[str]:
    """tools/ 下写数据但未登记进步骤清单的脚本（体检异常项）。"""
    registered = {name for names in STEP_SCRIPTS.values() for name in names}
    registered |= set(WRITER_REGISTRY)
    registered |= set(READ_ONLY_TOOLS)
    registered |= {'daily_update.py', 'dataset_health_check.py'}
    found: list[str] = []
    for path in sorted((PROJECT_ROOT / 'tools').glob('*.py')):
        if path.name in registered or path.name.startswith('_'):
            continue
        try:
            text = path.read_text(encoding='utf-8')
        except OSError:
            continue
        writes = any(marker in text for marker in WRITE_MARKERS)
        # LocalQuoteStore 多数场景是只读消费，仅当同时出现写入方法才算写数据。
        if not writes and 'LocalQuoteStore(' in text:
            writes = any(call in text for call in
                         ('write_quotes(', '.write_shard(', 'store.flush(', 'write_many('))
        if writes:
            found.append(path.name)
    return found


#: 更新频率 → 允许的日历日滞后上限（``max_lag_days`` 分档取值）。
#: daily 沿用既有口径 5 天；weekly 10 天；monthly 35 天（覆盖月末发布 + 节假）；
#: quarterly 200 天（报告期数据，年报最晚次年 4 月底）。
LAG_TIERS: dict[str, int] = {
    'daily': 5,
    'weekly': 10,
    'monthly': 35,
    'quarterly': 200,
}

#: 有意不纳入契约/体检的目录（有 parquet 但非研究数据表）：路径 -> 理由
NON_CONTRACT_DIRS: dict[str, str] = {
    'meta/dividend_status': '无分红台账（tools/build_no_dividend_ledger.py 逐只取证产物，'
                            '供人工复核；无下游消费者、非研究数据表，不补契约与 manifest）',
}


class ContractViolation(ValueError):
    """写入或体检发现数据不符合契约。"""


@dataclass(frozen=True)
class DatasetContract:
    """一份数据集契约。"""

    key: str
    label: str
    #: 展示用分类（行情 / ST / 涨跌停 / 两融 / 估值 / 财务）
    category: str
    #: 相对列存根的 glob（分片表用通配，单文件表用精确路径）
    pattern: str
    #: 权威日期列名（体检与新鲜度都用它，不再在各处硬编码 date/trade_date）
    date_column: str
    #: 必需列；``strict=True`` 时即完整列序
    columns: tuple[str, ...]
    #: 列名 -> polars dtype 字符串（只校验声明了的列）
    dtypes: dict[str, str]
    #: ``shard``（一标的一文件）/ ``monthly``（按月分区）/ ``single``（单文件）
    partition: str
    #: 必须非空的列（空值率门禁，>0 即异常）
    non_nullable: tuple[str, ...]
    #: 允许的日历日滞后上限；按 :data:`LAG_TIERS` 分档取值
    #: （daily 5 / weekly 10 / monthly 35 / quarterly 200），None 表示不做新鲜度判定
    max_lag_days: int | None
    #: True 表示列集合/列序/dtype 全等（行情分片），False 表示只校验必需列
    strict: bool = False
    #: 目录级 manifest（相对根）；用于体检时对账「登记值 vs 磁盘实测」
    manifest: str | None = None
    #: manifest 内条目前缀（``files[].path`` 以它开头的条目计入本数据集）
    manifest_prefix: str | None = None
    #: 备注（写清口径，避免同名不同义）
    note: str = ''
    #: 允许的额外列（写入时显式放行）
    allow_extra_columns: bool = False
    #: 数据更新频率（daily / weekly / monthly / quarterly），决定 ``max_lag_days``
    #: 取哪一档：daily=5、weekly=10、monthly=35、quarterly=200；未知频率按 daily
    update_frequency: str = 'daily'


def _quote_contract(kind: str, label: str) -> DatasetContract:
    columns = QUOTE_COLUMNS[kind]
    date_column = 'trade_date' if kind == 'futures' else 'date'
    return DatasetContract(
        key=f'quotes_{kind}',
        label=label,
        category='行情',
        pattern=f'quotes/{kind}/*/*.parquet',
        date_column=date_column,
        columns=columns,
        dtypes=dict(QUOTE_DTYPES[kind]),
        partition='shard',
        non_nullable=(date_column, 'ts_code'),
        max_lag_days=5,
        strict=True,
        manifest='quotes/manifest.json',
        note='一标的一文件；列集合/列序/dtype 全等，分片真值以磁盘为准',
    )


_LIMIT_COLUMNS = (
    'trade_date', 'ts_code', 'board', 'st_status', 'listing_days', 'listing_phase',
    'prev_close', 'prev_close_source', 'limit_pct', 'limit_basis', 'limit_up_price',
    'limit_down_price', 'touch_up', 'touch_down', 'close_at_up', 'close_at_down',
    'one_word_up', 'one_word_down', 'suspended', 'can_buy', 'can_sell',
)
_LIMIT_DTYPES = {
    'trade_date': 'Date', 'ts_code': 'String', 'board': 'String',
    'st_status': 'String', 'listing_days': 'Int32', 'listing_phase': 'String',
    'prev_close': 'Float64', 'prev_close_source': 'String', 'limit_pct': 'Float64',
    'limit_basis': 'String', 'limit_up_price': 'Float64',
    'limit_down_price': 'Float64', 'touch_up': 'Boolean', 'touch_down': 'Boolean',
    'close_at_up': 'Boolean', 'close_at_down': 'Boolean', 'one_word_up': 'Boolean',
    'one_word_down': 'Boolean', 'suspended': 'Boolean', 'can_buy': 'Boolean',
    'can_sell': 'Boolean',
}

_MARGIN_COLUMNS = (
    'trade_date', 'ts_code', 'rzye', 'rzmre', 'rzche', 'rqye', 'rqmcl', 'rqchl',
    'rzrqye', 'rqyl',
)
_MARGIN_DTYPES = {c: ('Date' if c == 'trade_date' else 'String' if c == 'ts_code'
                      else 'Float64') for c in _MARGIN_COLUMNS}

CONTRACTS: tuple[DatasetContract, ...] = (
    # ---- 行情 ----
    _quote_contract('stock', 'A股日线行情'),
    _quote_contract('fund', '基金日线行情'),
    _quote_contract('index', '指数日线行情'),
    _quote_contract('futures', '期货日线行情'),
    # ---- ST ----
    DatasetContract(
        key='st_daily',
        label='ST 日状态',
        category='ST',
        pattern='meta/st/daily/st_daily_[0-9]*.parquet',
        date_column='trade_date',
        columns=('trade_date', 'ts_code', 'status', 'source'),
        dtypes={'trade_date': 'Date', 'ts_code': 'String',
                'status': 'String', 'source': 'String'},
        partition='monthly',
        non_nullable=('trade_date', 'ts_code', 'status'),
        max_lag_days=5,
        manifest='meta/st/daily/manifest.json',
        manifest_prefix='meta/st/daily/',
        note='按月分区；status 只表达当日是否 ST，不带区间语义',
    ),
    # ---- 涨跌停 ----
    DatasetContract(
        key='limit_daily',
        label='涨跌停日表',
        category='涨跌停',
        pattern='meta/limit/daily/limit_daily_[0-9]*.parquet',
        date_column='trade_date',
        columns=_LIMIT_COLUMNS,
        dtypes=_LIMIT_DTYPES,
        partition='monthly',
        non_nullable=('trade_date', 'ts_code'),
        max_lag_days=5,
        manifest='meta/limit/daily/manifest.json',
        manifest_prefix='meta/limit/daily/',
        note='含 touch/close/one_word 三类口径；limit_pct 是当日涨跌幅限制值',
    ),
    # ---- 两融 ----
    DatasetContract(
        key='margin_detail',
        label='两融明细',
        category='两融',
        pattern='meta/margin_detail/margin_detail_[0-9]*.parquet',
        date_column='trade_date',
        columns=_MARGIN_COLUMNS,
        dtypes=_MARGIN_DTYPES,
        partition='monthly',
        non_nullable=('trade_date', 'ts_code'),
        max_lag_days=5,
        manifest='meta/margin_detail/manifest.json',
        manifest_prefix='meta/margin_detail/',
        note='按月分区；空表月份表示源端无数据，不等价于全市场零余额',
    ),
    # ---- 估值 ----
    DatasetContract(
        key='valuation_daily',
        label='个股估值日表',
        category='估值',
        pattern='meta/financials/valuation_daily.parquet',
        date_column='trade_date',
        columns=('ts_code', 'trade_date', 'close', 'pe', 'pb', 'total_mv', 'circ_mv'),
        dtypes={'ts_code': 'String', 'trade_date': 'Date', 'close': 'Float64',
                'pe': 'Float64', 'pb': 'Float64', 'total_mv': 'Float64',
                'circ_mv': 'Float64'},
        partition='single',
        non_nullable=('ts_code', 'trade_date'),
        max_lag_days=5,
        manifest='meta/financials/manifest.json',
        manifest_prefix='meta/financials/valuation_daily.parquet',
        note='单文件全市场横截面；pe/pb 允许空值（亏损或净资产为负）',
    ),
    # ---- 财务 ----
    *(
        DatasetContract(
            key=f'financials_{table}',
            label=f'财务{table}',
            category='财务',
            pattern=f'meta/financials/{table}.parquet',
            date_column='end_date',
            columns=('ts_code', 'end_date', 'ann_date', 'source'),
            dtypes={'ts_code': 'String', 'end_date': 'Date',
                    'ann_date': 'Date', 'source': 'String'},
            partition='single',
            non_nullable=('ts_code', 'end_date'),
            # 报告期数据天然滞后：年报最晚次年 4 月底，给 200 天容差
            max_lag_days=LAG_TIERS['quarterly'],
            update_frequency='quarterly',
            manifest='meta/financials/manifest.json',
            manifest_prefix=f'meta/financials/{table}.parquet',
            note='按报告期（end_date）粒度，非交易日序列；ann_date 为公告日',
        )
        for table in ('income', 'balance', 'cashflow')
    ),
    # ---- 宏观 ----
    DatasetContract(
        key='macro_market_margin',
        label='市场宏观两融',
        category='宏观',
        pattern='meta/macro/cn_market_margin_daily.parquet',
        date_column='date',
        columns=('date', 'market', 'source', 'fin_buy', 'fin_balance',
                 'sec_sell_qty', 'sec_qty', 'sec_balance', 'total_balance'),
        dtypes={'date': 'Date', 'market': 'String', 'source': 'String',
                'fin_buy': 'Float64', 'fin_balance': 'Float64',
                'sec_sell_qty': 'Float64', 'sec_qty': 'Float64',
                'sec_balance': 'Float64', 'total_balance': 'Float64'},
        partition='single',
        non_nullable=('date', 'market'),
        max_lag_days=5,
        note='沪深两市宏观两融合计（date+market 唯一），非个股 margin_detail',
    ),
    # ---- ST 区间 / 覆盖 ----
    DatasetContract(
        key='st_intervals',
        label='ST 区间表',
        category='ST',
        pattern='meta/st/st_intervals.parquet',
        date_column='start_date',
        columns=('ts_code', 'start_date', 'end_date', 'flag', 'source',
                 'precision', 'fetched_at', 'evidence', 'raw_fetched_at'),
        dtypes={'ts_code': 'String', 'start_date': 'Date', 'end_date': 'Date',
                'flag': 'String', 'source': 'String', 'precision': 'String',
                'fetched_at': "Datetime(time_unit='us', time_zone=None)", 'evidence': 'String',
                'raw_fetched_at': "Datetime(time_unit='us', time_zone=None)"},
        partition='single',
        non_nullable=('ts_code', 'start_date'),
        max_lag_days=None,
        note='区间事件表；end_date 为空表示开区间',
    ),
    DatasetContract(
        key='st_coverage',
        label='ST 覆盖窗口',
        category='ST',
        pattern='meta/st/st_coverage.parquet',
        date_column='coverage_start',
        columns=('ts_code', 'coverage_start', 'coverage_end', 'source', 'fetched_at'),
        dtypes={'ts_code': 'String', 'coverage_start': 'Date',
                'coverage_end': 'Date', 'source': 'String', 'fetched_at': "Datetime(time_unit='us', time_zone=None)"},
        partition='single',
        non_nullable=('ts_code', 'coverage_start'),
        max_lag_days=None,
        note='逐标的状态可信窗口，不是日频面板',
    ),
    DatasetContract(
        key='st_supplement_intervals',
        label='ST 补充区间',
        category='ST',
        pattern='meta/st/supplement/supplement_intervals.parquet',
        date_column='start_date',
        columns=('ts_code', 'start_date', 'end_date', 'flag', 'source',
                 'precision', 'fetched_at'),
        dtypes={'ts_code': 'String', 'start_date': 'Date', 'end_date': 'Date',
                'flag': 'String', 'source': 'String', 'precision': 'String',
                'fetched_at': "Datetime(time_unit='us', time_zone=None)"},
        partition='single',
        non_nullable=('ts_code', 'start_date'),
        max_lag_days=None,
        note='补充覆盖层区间；由 st_name_history 重建',
    ),
    DatasetContract(
        key='st_supplement_coverage',
        label='ST 补充覆盖窗口',
        category='ST',
        pattern='meta/st/supplement/supplement_coverage.parquet',
        date_column='coverage_start',
        columns=('ts_code', 'coverage_start', 'coverage_end', 'source', 'fetched_at'),
        dtypes={'ts_code': 'String', 'coverage_start': 'Date',
                'coverage_end': 'Date', 'source': 'String', 'fetched_at': "Datetime(time_unit='us', time_zone=None)"},
        partition='single',
        non_nullable=('ts_code', 'coverage_start'),
        max_lag_days=None,
        note='补充覆盖层逐标的窗口',
    ),
    # ---- 分红 ----
    DatasetContract(
        key='fund_dividend',
        label='基金分红',
        category='分红',
        pattern='factors/fund_dividend.parquet',
        date_column='ex_date',
        columns=('ts_code', 'ex_date', 'record_date', 'cash_amount', 'source',
                 'fetched_at'),
        dtypes={'ts_code': 'String', 'ex_date': 'Date', 'record_date': 'Date',
                'cash_amount': 'Float64', 'source': 'String', 'fetched_at': "Datetime(time_unit='us', time_zone=None)"},
        partition='single',
        non_nullable=('ts_code', 'ex_date'),
        max_lag_days=None,
        note='基金除权除息；key=(ts_code, ex_date)',
    ),
    DatasetContract(
        key='stock_dividend',
        label='个股分红',
        category='分红',
        pattern='factors/stock_dividend.parquet',
        date_column='ex_date',
        columns=('ts_code', 'symbol', 'name', 'dividend_year', 'report_date',
                 'record_date', 'ex_date', 'pay_date', 'cash_div', 'share_div',
                 'total_div', 'rights_issue_price', 'rights_issue_ratio',
                 'source', 'report_period', 'plan_notice_date', 'notice_date',
                 'progress', 'total_shares', 'share_div_source',
                 'total_div_est_all', 'total_div_est_float', 'est_total_share',
                 'est_float_share', 'est_effective_date', 'total_div_est_rec_all',
                 'total_div_est_rec_float', 'est_rec_total_share',
                 'est_rec_float_share', 'est_rec_effective_date', 'no_ex_rights',
                 'share_div_fix', 'cash_div_fix', 'cash_div_diluted'),
        dtypes={'ts_code': 'String', 'symbol': 'String', 'name': 'String',
                'dividend_year': 'String', 'report_date': 'String',
                'record_date': 'Date', 'ex_date': 'Date', 'pay_date': 'String',
                'cash_div': 'Float64', 'share_div': 'Float64',
                'total_div': 'Float64', 'rights_issue_price': 'Float64',
                'rights_issue_ratio': 'Float64', 'source': 'String',
                'report_period': 'Date', 'plan_notice_date': 'Date',
                'notice_date': 'Date', 'progress': 'String',
                'total_shares': 'Float64', 'share_div_source': 'String',
                'total_div_est_all': 'Float64', 'total_div_est_float': 'Float64',
                'est_total_share': 'Int64', 'est_float_share': 'Int64',
                'est_effective_date': 'Date', 'total_div_est_rec_all': 'Float64',
                'total_div_est_rec_float': 'Float64',
                'est_rec_total_share': 'Int64', 'est_rec_float_share': 'Int64',
                'est_rec_effective_date': 'Date', 'no_ex_rights': 'Boolean',
                'share_div_fix': 'Float64', 'cash_div_fix': 'Float64',
                'cash_div_diluted': 'Float64'},
        partition='single',
        non_nullable=('ts_code',),
        max_lag_days=None,
        note='个股分红全表；ex_date 允许为空（预案未定除权日）',
    ),

    # ---- 第四批 ----
    DatasetContract(
        key='events',
        label='事件库',
        category='事件',
        pattern='meta/events/events.parquet',
        date_column='ann_date',
        columns=('event_id', 'ts_code', 'event_type', 'publish_time', 'ann_date', 'report_period', 'title', 'source'),
        dtypes={'event_id': 'String', 'ts_code': 'String', 'event_type': 'String', 'publish_time': "Datetime(time_unit='us', time_zone=None)", 'ann_date': 'Date', 'report_period': 'Date', 'title': 'String', 'source': 'String'},
        partition='single',
        non_nullable=('event_id', 'ts_code'),
        max_lag_days=LAG_TIERS['weekly'],
        update_frequency='weekly',
        note='公告事件库按周采集，ann_date 粒度、非每日必有新事件，故按 10 天档',
    ),
    DatasetContract(
        key='stock_shares',
        label='股本表',
        category='股本',
        pattern='meta/factors/stock_shares.parquet',
        date_column='effective_date',
        columns=('ts_code', 'effective_date', 'total_share', 'float_share', 'free_float_share', 'change_reason', 'source', 'fetched_at', 'free_float_share_filled', 'free_float_source'),
        dtypes={'ts_code': 'String', 'effective_date': 'Date', 'total_share': 'Int64', 'float_share': 'Int64', 'free_float_share': 'Int64', 'change_reason': 'String', 'source': 'String', 'fetched_at': "Datetime(time_unit='us', time_zone=None)", 'free_float_share_filled': 'Int64', 'free_float_source': 'String'},
        partition='single',
        non_nullable=('ts_code', 'effective_date'),
        max_lag_days=None,
    ),
    DatasetContract(
        key='implied_suspension',
        label='隐含停牌',
        category='停牌',
        pattern='meta/suspension/implied_suspension.parquet',
        date_column='trade_date',
        columns=('ts_code', 'trade_date', 'asset_class', 'reason', 'ohlc_flat', 'in_existing_suspension', 'conflicts_with_existing_suspension', 'source', 'detected_at'),
        dtypes={'ts_code': 'String', 'trade_date': 'Date', 'asset_class': 'String', 'reason': 'String', 'ohlc_flat': 'Boolean', 'in_existing_suspension': 'Boolean', 'conflicts_with_existing_suspension': 'Boolean', 'source': 'String', 'detected_at': "Datetime(time_unit='us', time_zone=None)"},
        partition='single',
        non_nullable=('ts_code', 'trade_date'),
        max_lag_days=None,
    ),
    DatasetContract(
        key='suspension_daily',
        label='停牌日表',
        category='停牌',
        pattern='meta/suspension/suspension_daily.parquet',
        date_column='trade_date',
        columns=('ts_code', 'trade_date', 'asset_class', 'market', 'is_suspended', 'run_id', 'run_len', 'run_pos', 'confidence', 'source', 'detected_at'),
        dtypes={'ts_code': 'String', 'trade_date': 'Date', 'asset_class': 'String', 'market': 'String', 'is_suspended': 'Boolean', 'run_id': 'String', 'run_len': 'Int32', 'run_pos': 'Int32', 'confidence': 'String', 'source': 'String', 'detected_at': 'Date'},
        partition='single',
        non_nullable=('ts_code', 'trade_date'),
        max_lag_days=None,
    ),
    DatasetContract(
        key='new_share',
        label='新股表',
        category='新股',
        pattern='meta/new_share.parquet',
        date_column='ipo_date',
        columns=('ts_code', 'ipo_date', 'issue_date', 'price', 'price_is_zero'),
        dtypes={'ts_code': 'String', 'ipo_date': 'Date', 'issue_date': 'Date', 'price': 'Float64', 'price_is_zero': 'Boolean'},
        partition='single',
        non_nullable=('ts_code',),
        max_lag_days=None,
    ),
    DatasetContract(
        key='name_history',
        label='名称历史',
        category='基础信息',
        pattern='meta/name_history.parquet',
        date_column='start_date',
        columns=('ts_code', 'name', 'start_date', 'end_date', 'ann_date', 'change_reason', 'source', 'fetched_at'),
        dtypes={'ts_code': 'String', 'name': 'String', 'start_date': 'Date', 'end_date': 'Date', 'ann_date': 'Date', 'change_reason': 'String', 'source': 'String', 'fetched_at': "Datetime(time_unit='us', time_zone=None)"},
        partition='single',
        non_nullable=('ts_code', 'start_date'),
        max_lag_days=None,
    ),
    DatasetContract(
        key='fund_corporate_actions',
        label='基金公司行为',
        category='分红',
        pattern='meta/fund_corporate_actions_2019plus.parquet',
        date_column='date',
        columns=('ts_code', 'date', 'prev_close', 'close', 'gap', 'adj_prev', 'adj_now', 'basis'),
        dtypes={'ts_code': 'String', 'date': 'Date', 'prev_close': 'Float64', 'close': 'Float64', 'gap': 'Float64', 'adj_prev': 'Float64', 'adj_now': 'Float64', 'basis': 'String'},
        partition='single',
        non_nullable=('ts_code', 'date'),
        max_lag_days=None,
    ),
    DatasetContract(
        key='universe_exclusions',
        label='样本剔除清单',
        category='运维',
        pattern='meta/universe/universe_exclusions.parquet',
        date_column='detected_at',
        columns=('ts_code', 'asset_class', 'reason_code', 'reason_text', 'last_trade_date', 'expected_rows', 'actual_rows', 'missing_rows', 'severity', 'detected_at'),
        dtypes={'ts_code': 'String', 'asset_class': 'String', 'reason_code': 'String', 'reason_text': 'String', 'last_trade_date': 'Date', 'expected_rows': 'Int64', 'actual_rows': 'Int64', 'missing_rows': 'Int64', 'severity': 'String', 'detected_at': 'Date'},
        partition='single',
        non_nullable=('ts_code',),
        max_lag_days=None,
    ),
    DatasetContract(
        key='etf_pool',
        label='ETF 候选池',
        category='多资产',
        pattern='meta/multi_asset/etf_pool.parquet',
        date_column='list_date',
        columns=('asset_class', 'sub_class', 'tier', 'in_scope', 'instrument_type', 'ts_code', 'name', 'fund_type', 'market', 'status', 'list_date', 'delist_date', 'first_date', 'last_date', 'rows', 'coverage_start', 'coverage_end', 'missing_days', 'missing_runs_ge3', 'max_gap_days', 'gap_segments', 'zero_volume_days', 'adv_20d', 'adv_60d', 'adv_120d', 'adv_all', 'amount_unit_ratio', 'factor_rows', 'factor_last_date', 'factor_missing_days', 'factor_missing_internal', 'factor_tail_lag_days', 'factor_change_count', 'ca_event_count', 'implied_event_count', 'event_not_in_ca', 'event_after_ca_end', 'event_covered_divsplit', 'event_unregistered_anywhere', 'div_event_count', 'split_event_count', 'event_in_divsplit_not_ca', 'unregistered_dates', 'gap_kind', 'fake_bar_hits', 'note'),
        dtypes={'asset_class': 'String', 'sub_class': 'String', 'tier': 'String', 'in_scope': 'Boolean', 'instrument_type': 'String', 'ts_code': 'String', 'name': 'String', 'fund_type': 'String', 'market': 'String', 'status': 'String', 'list_date': 'Date', 'delist_date': 'Date', 'first_date': 'Date', 'last_date': 'Date', 'rows': 'Int64', 'coverage_start': 'Date', 'coverage_end': 'Date', 'missing_days': 'Int64', 'missing_runs_ge3': 'Int64', 'max_gap_days': 'Int64', 'gap_segments': 'String', 'zero_volume_days': 'Int64', 'adv_20d': 'Float64', 'adv_60d': 'Float64', 'adv_120d': 'Float64', 'adv_all': 'Float64', 'amount_unit_ratio': 'Float64', 'factor_rows': 'Int64', 'factor_last_date': 'Date', 'factor_missing_days': 'Int64', 'factor_missing_internal': 'Int64', 'factor_tail_lag_days': 'Int64', 'factor_change_count': 'Int64', 'ca_event_count': 'Int64', 'implied_event_count': 'Int64', 'event_not_in_ca': 'Int64', 'event_after_ca_end': 'Int64', 'event_covered_divsplit': 'Int64', 'event_unregistered_anywhere': 'Int64', 'div_event_count': 'Int64', 'split_event_count': 'Int64', 'event_in_divsplit_not_ca': 'Int64', 'unregistered_dates': 'String', 'gap_kind': 'String', 'fake_bar_hits': 'Int64', 'note': 'String'},
        partition='single',
        non_nullable=('ts_code',),
        max_lag_days=LAG_TIERS['monthly'],
        update_frequency='monthly',
        note='ETF 池快照；date_column=list_date 为池内最新上市日而非更新日，按 35 天档判定',
    ),
    DatasetContract(
        key='stock_basic',
        label='证券基础信息',
        category='基础信息',
        pattern='meta/stock_basic.parquet',
        date_column='list_date',
        columns=('ts_code', 'name', 'name_basis', 'list_date', 'delist_date', 'status', 'exchange', 'market', 'board', 'delist_reason', 'list_date_basis', 'delist_date_basis', 'date_precision'),
        dtypes={'ts_code': 'String', 'name': 'String', 'name_basis': 'String', 'list_date': 'Date', 'delist_date': 'Date', 'status': 'String', 'exchange': 'String', 'market': 'String', 'board': 'String', 'delist_reason': 'String', 'list_date_basis': 'String', 'delist_date_basis': 'String', 'date_precision': 'String'},
        partition='single',
        non_nullable=('ts_code',),
        max_lag_days=None,
    ),

    # ---- 第五批 ----
    DatasetContract(
        key='cn_bond_index',
        label='中债债券总指数',
        category='宏观',
        pattern='meta/macro/cn_bond_index.parquet',
        date_column='date',
        columns=('date', 'index_name', 'value', 'source'),
        dtypes={'date': 'Date', 'index_name': 'String', 'value': 'Float64', 'source': 'String'},
        partition='single',
        non_nullable=('date',),
        max_lag_days=None,
    ),
    DatasetContract(
        key='cn_credit_yield_curve',
        label='中债 AAA 信用曲线',
        category='宏观',
        pattern='meta/macro/cn_credit_yield_curve.parquet',
        date_column='date',
        columns=('date', 'tenor_years', 'tenor', 'ytm_pct', 'curve', 'source'),
        dtypes={'date': 'Date', 'tenor_years': 'Float64', 'tenor': 'String', 'ytm_pct': 'Float64', 'curve': 'String', 'source': 'String'},
        partition='single',
        non_nullable=('date',),
        max_lag_days=None,
    ),
    DatasetContract(
        key='cn_treasury_yield_curve',
        label='国债收益率曲线',
        category='宏观',
        pattern='meta/macro/cn_treasury_yield_curve.parquet',
        date_column='date',
        columns=('date', 'tenor_years', 'tenor', 'ytm_pct', 'curve', 'source'),
        dtypes={'date': 'Date', 'tenor_years': 'Float64', 'tenor': 'String', 'ytm_pct': 'Float64', 'curve': 'String', 'source': 'String'},
        partition='single',
        non_nullable=('date',),
        max_lag_days=None,
    ),
    DatasetContract(
        key='cn_treasury_yield_10y',
        label='十年国债收益率',
        category='宏观',
        pattern='meta/macro/cn_treasury_yield_10y.parquet',
        date_column='date',
        columns=('date', 'y10', 'source'),
        dtypes={'date': 'Date', 'y10': 'Float64', 'source': 'String'},
        partition='single',
        non_nullable=('date',),
        max_lag_days=None,
    ),
    DatasetContract(
        key='futures_continuous',
        label='期货连续合约',
        category='多资产',
        pattern='meta/multi_asset/futures_continuous.parquet',
        date_column='trade_date',
        columns=('variety', 'asset_class', 'exchange', 'trade_date', 'contract', 'close_raw', 'settle_raw', 'close_adj', 'volume', 'oi', 'is_roll', 'ret', 'contract_delist_date'),
        dtypes={'variety': 'String', 'asset_class': 'String', 'exchange': 'String', 'trade_date': 'Date', 'contract': 'String', 'close_raw': 'Float64', 'settle_raw': 'Float64', 'close_adj': 'Float64', 'volume': 'Float64', 'oi': 'Float64', 'is_roll': 'Boolean', 'ret': 'Float64', 'contract_delist_date': 'Date'},
        partition='single',
        non_nullable=('variety', 'trade_date'),
        max_lag_days=None,
    ),
    DatasetContract(
        key='futures_contracts',
        label='期货合约表',
        category='多资产',
        pattern='meta/multi_asset/futures_contracts.parquet',
        date_column='list_date',
        columns=('ts_code', 'symbol', 'exchange', 'name', 'fut_code', 'multiplier', 'trade_unit', 'per_unit', 'quote_unit', 'quote_unit_desc', 'd_mode_desc', 'list_date', 'delist_date', 'd_month', 'last_ddate'),
        dtypes={'ts_code': 'String', 'symbol': 'String', 'exchange': 'String', 'name': 'String', 'fut_code': 'String', 'multiplier': 'Float64', 'trade_unit': 'String', 'per_unit': 'String', 'quote_unit': 'String', 'quote_unit_desc': 'String', 'd_mode_desc': 'String', 'list_date': 'Date', 'delist_date': 'Date', 'd_month': 'String', 'last_ddate': 'String'},
        partition='single',
        non_nullable=('ts_code',),
        max_lag_days=None,
    ),
    DatasetContract(
        key='futures_roll_events',
        label='期货换月事件',
        category='多资产',
        pattern='meta/multi_asset/futures_roll_events.parquet',
        date_column='trade_date',
        columns=('variety', 'trade_date', 'from_contract', 'to_contract', 'from_close_prev_day', 'to_close', 'from_oi', 'to_oi', 'return_pct', 'from_close_same_day', 'roll_gap_pct'),
        dtypes={'variety': 'String', 'trade_date': 'Date', 'from_contract': 'String', 'to_contract': 'String', 'from_close_prev_day': 'Float64', 'to_close': 'Float64', 'from_oi': 'Float64', 'to_oi': 'Float64', 'return_pct': 'Float64', 'from_close_same_day': 'Float64', 'roll_gap_pct': 'Float64'},
        partition='single',
        non_nullable=('variety', 'trade_date'),
        max_lag_days=None,
    ),
    DatasetContract(
        key='breadth_ma_daily',
        label='均线占比宽度日表',
        category='宽度',
        pattern='meta/breadth/breadth_ma_daily.parquet',
        date_column='date',
        columns=('date', 'sample_size', 'above_ma20', 'above_ma60', 'above_ma120', 'pct_above_ma20', 'pct_above_ma60', 'pct_above_ma120', 'new_high_250', 'new_low_250', 'sample_size_ma60', 'sample_size_ma120', 'sample_size_250',),
        dtypes={'date': 'Date', 'sample_size': 'Int64', 'above_ma20': 'Int64', 'above_ma60': 'Int64', 'above_ma120': 'Int64', 'pct_above_ma20': 'Float64', 'pct_above_ma60': 'Float64', 'pct_above_ma120': 'Float64', 'new_high_250': 'Int64', 'new_low_250': 'Int64', 'sample_size_ma60': 'Int64', 'sample_size_ma120': 'Int64', 'sample_size_250': 'Int64'},
        partition='single',
        non_nullable=('date',),
        max_lag_days=5,
        strict=True,
        note='与 breadth_daily 语义不同，互不覆盖',
    ),
    DatasetContract(
        key='state_vector_daily',
        label='市场状态向量日表',
        category='状态',
        pattern='meta/state/state_vector_daily.parquet',
        date_column='date',
        columns=('date', 'val_earn_yield', 'val_earn_yield_q', 'valuation_erp', 'valuation_erp_q', 'sent_ff_turnover', 'sent_ff_turnover_q', 'breadth_ma20', 'breadth_ma20_q', 'lev_margin_buy_ratio', 'lev_margin_buy_ratio_q', 'trend_er20', 'trend_er20_q', 'style_large_small_20d', 'style_large_small_20d_q', 'sent_ff_coverage', 'sent_ff_ffill_share', 'val_n_codes', 'val_pe_coverage', 'sent_n_codes', 'dims_available', 'dims_q_available', 'val_earn_yield_unwinsor', 'valuation_erp_unwinsor', 'quality_flags',),
        dtypes={'date': 'Date', 'val_earn_yield': 'Float64', 'val_earn_yield_q': 'Float64', 'valuation_erp': 'Float64', 'valuation_erp_q': 'Float64', 'sent_ff_turnover': 'Float64', 'sent_ff_turnover_q': 'Float64', 'breadth_ma20': 'Float64', 'breadth_ma20_q': 'Float64', 'lev_margin_buy_ratio': 'Float64', 'lev_margin_buy_ratio_q': 'Float64', 'trend_er20': 'Float64', 'trend_er20_q': 'Float64', 'style_large_small_20d': 'Float64', 'style_large_small_20d_q': 'Float64', 'sent_ff_coverage': 'Float64', 'sent_ff_ffill_share': 'Float64', 'val_pe_coverage': 'Float64', 'val_earn_yield_unwinsor': 'Float64', 'valuation_erp_unwinsor': 'Float64', 'val_n_codes': 'UInt32', 'sent_n_codes': 'UInt32', 'dims_available': 'Int32', 'dims_q_available': 'Int32', 'quality_flags': 'String'},
        partition='single',
        non_nullable=('date',),
        max_lag_days=5,
        strict=True,
        note='状态向量按日一行；quality_flags 说明缺失维度',
    ),
    DatasetContract(
        key='session_ledger',
        label='交易日台账',
        category='日历',
        pattern='meta/trading_calendar/session_ledger.parquet',
        date_column='date',
        columns=('date', 'status', 'kind', 'evidence', 'source', 'evidence_path', 'effective_from', 'effective_to', 'unresolved', 'decided_at', 'note',),
        dtypes={'date': 'Date', 'status': 'String', 'kind': 'String', 'evidence': 'String', 'source': 'String', 'evidence_path': 'String', 'effective_from': 'Date', 'effective_to': 'Date', 'unresolved': 'Boolean', 'decided_at': 'Date', 'note': 'String'},
        partition='single',
        non_nullable=('date', 'status',),
        max_lag_days=LAG_TIERS['monthly'],
        update_frequency='monthly',
        strict=True,
        note='特殊交易日裁定台账（74 行例外，非全量日历），仅有新例外时追加，故按 35 天档',
    ),
    DatasetContract(
        key='trading_calendar',
        label='官方未来交易日历',
        category='日历',
        pattern='meta/trading_calendar/official_future.parquet',
        date_column='trade_date',
        columns=('trade_date',),
        dtypes={'trade_date': 'Date'},
        partition='single',
        non_nullable=('trade_date',),
        max_lag_days=None,
        strict=True,
        note='官方接口覆盖段 + 工作日近似，唯一列 trade_date',
    ),
    DatasetContract(
        key='index_valuation',
        label='指数估值日表',
        category='估值',
        pattern='meta/index_valuation/index_valuation.parquet',
        date_column='trade_date',
        columns=('index_code', 'trade_date', 'pe', 'pe_ttm', 'pb', 'dividend_yield', 'pe_cov', 'n', 'source',),
        dtypes={'index_code': 'String', 'trade_date': 'Date', 'pe': 'Float64', 'pe_ttm': 'Float64', 'pb': 'Float64', 'dividend_yield': 'Float64', 'pe_cov': 'Float64', 'n': 'Int64', 'source': 'String'},
        partition='single',
        non_nullable=('index_code', 'trade_date',),
        max_lag_days=LAG_TIERS['monthly'],
        update_frequency='monthly',
        strict=True,
        note='逐指数逐日估值（源端按月更新，新鲜度按 35 天档）；pe_cov 为覆盖度',
    ),
    DatasetContract(
        key='etf_fee',
        label='ETF 费率表',
        category='多资产',
        pattern='meta/multi_asset/etf_fee.parquet',
        date_column='list_date',
        columns=('ts_code', 'name', 'asset_class', 'tier', 'status', 'list_date', 'delist_date', 'mgmt_fee_pct', 'custody_fee_pct', 'sales_service_fee_pct', 'mgmt_source', 'custody_source', 'sales_source', 'cross_check_status', 'mgmt_diff_pct', 'custody_diff_pct', 'has_fee_history', 'fee_history_segments', 'history_basis', 'history_precision', 'history_window_covered', 'missing_reason', 'burden_current_pct', 'burden_segmented_pct', 'burden_basis',),
        dtypes={'ts_code': 'String', 'name': 'String', 'asset_class': 'String', 'tier': 'String', 'status': 'String', 'list_date': 'String', 'delist_date': 'String', 'mgmt_fee_pct': 'Float64', 'custody_fee_pct': 'Float64', 'sales_service_fee_pct': 'Float64', 'mgmt_source': 'String', 'custody_source': 'String', 'sales_source': 'String', 'cross_check_status': 'String', 'mgmt_diff_pct': 'Float64', 'custody_diff_pct': 'Float64', 'has_fee_history': 'Boolean', 'fee_history_segments': 'String', 'history_basis': 'String', 'history_precision': 'String', 'history_window_covered': 'Boolean', 'missing_reason': 'String', 'burden_current_pct': 'Float64', 'burden_segmented_pct': 'Float64', 'burden_basis': 'String'},
        partition='single',
        non_nullable=('ts_code',),
        max_lag_days=None,
        strict=True,
        note='日期列为 String（源端原样），费率口径见 history_basis',
    ),
    DatasetContract(
        key='index_weight_monthly',
        label='指数权重月表',
        category='指数',
        pattern='meta/index_members/index_weight_monthly.parquet',
        date_column='trade_date',
        columns=('index_code', 'ts_code', 'trade_date', 'weight', 'source', 'fetched_at',),
        dtypes={'index_code': 'String', 'ts_code': 'String', 'trade_date': 'String', 'weight': 'Float64', 'source': 'String', 'fetched_at': 'String'},
        partition='single',
        non_nullable=('index_code', 'ts_code', 'trade_date',),
        max_lag_days=None,
        strict=True,
        note='按月快照；trade_date 为 String（源端月度标识）',
    ),
    DatasetContract(
        key='fund_share_change',
        label='基金份额变动',
        category='基金',
        pattern='factors/fund_share_change.parquet',
        date_column='ex_date',
        columns=('ts_code', 'ex_date', 'split_type', 'share_ratio', 'source', 'fetched_at',),
        dtypes={'ts_code': 'String', 'ex_date': 'Date', 'split_type': 'String', 'share_ratio': 'Float64', 'source': 'String', 'fetched_at': "Datetime(time_unit='us', time_zone=None)"},
        partition='single',
        non_nullable=('ts_code', 'ex_date',),
        max_lag_days=None,
        strict=True,
        note='份额拆分/折算事件表，非逐日序列',
    ),
    DatasetContract(
        key='fund_adj_factor',
        label='基金复权因子',
        category='基金',
        pattern='factors/fund_adj_factor.parquet',
        date_column='trade_date',
        columns=('ts_code', 'trade_date', 'qfq_factor', 'hfq_factor',),
        dtypes={'ts_code': 'String', 'trade_date': 'Date', 'qfq_factor': 'Float64', 'hfq_factor': 'Float64'},
        partition='single',
        non_nullable=('ts_code', 'trade_date',),
        max_lag_days=5,
        strict=True,
        note='逐基金逐日复权因子',
    ),
    DatasetContract(
        key='fund_basic',
        label='基金基础信息',
        category='基金',
        pattern='meta/fund_basic.parquet',
        date_column='list_date',
        columns=('id', 'ts_code', 'symbol', 'name', 'fund_type', 'market', 'list_date', 'delist_date', 'status', 'created_at', 'updated_at', 'is_tradable', 'tradable_reason', 'source',),
        dtypes={'id': 'Int64', 'ts_code': 'String', 'symbol': 'String', 'name': 'String', 'fund_type': 'String', 'market': 'String', 'list_date': 'Date', 'delist_date': 'Date', 'status': 'String', 'created_at': "Datetime(time_unit='us', time_zone=None)", 'updated_at': "Datetime(time_unit='us', time_zone=None)", 'is_tradable': 'Boolean', 'tradable_reason': 'String', 'source': 'String'},
        partition='single',
        non_nullable=('ts_code',),
        max_lag_days=None,
        strict=True,
        note='基金静态表；is_tradable 由 mark_fund_tradability 回写',
    ),
    DatasetContract(
        key='st_namechange_intervals_tushare',
        label='ST 名称变更区间（tushare 原始）',
        category='ST',
        pattern='meta/st/raw/tushare_namechange_intervals.parquet',
        date_column='start_date',
        columns=('ts_code', 'st_flag', 'start_date', 'end_date', 'st_type', 'source',),
        dtypes={'ts_code': 'String', 'st_flag': 'Boolean', 'start_date': 'Date', 'end_date': 'Date', 'st_type': 'String', 'source': 'String'},
        partition='single',
        non_nullable=('ts_code', 'start_date',),
        max_lag_days=None,
        strict=True,
        note='风险警示区间原始证据，供 st_history 派生',
    ),
    DatasetContract(
        key='st_daily_intervals',
        label='ST 日频区间证据',
        category='ST',
        pattern='meta/st/daily/st_daily_intervals.parquet',
        date_column='start_date',
        columns=('ts_code', 'name', 'start_date', 'end_date', 'ann_date', 'flag', 'source',),
        dtypes={'ts_code': 'String', 'name': 'String', 'start_date': 'Date', 'end_date': 'Date', 'ann_date': 'Date', 'flag': 'String', 'source': 'String'},
        partition='single',
        non_nullable=('ts_code', 'start_date',),
        max_lag_days=None,
        strict=True,
        note='由名称历史与公告还原的 ST 区间证据',
    ),
)

_BY_KEY: dict[str, DatasetContract] = {c.key: c for c in CONTRACTS}

#: 扩展位：后续要纳管的数据集（按同样字段补一条即可），当前不参与校验。
EXTENSION_SLOTS: tuple[str, ...] = (
    'industry_pit_sw（行业 PIT 区间表，已有 extended_contracts 覆盖）',
    'index_membership（指数成分进出区间）',
    'suspension（停复牌日表）',
    'factor_panel / stock_adj_factor（因子快照）',
)


def contract_for_key(key: str) -> DatasetContract:
    contract = _BY_KEY.get(key)
    if contract is None:
        raise KeyError(f'未注册的数据集契约: {key!r}；可用: {sorted(_BY_KEY)}')
    return contract


def contract_keys() -> list[str]:
    return list(_BY_KEY)


def contract_for_path(path: Path | str, root: Path | str | None = None) -> DatasetContract | None:
    """按产物路径反查契约（写入侧闸门用）；不在注册表内返回 None。"""
    base = Path(root) if root is not None else default_store_root()
    try:
        rel = Path(path).resolve().relative_to(base.resolve())
    except (ValueError, OSError):
        rel = Path(path)
    text = PurePosixPath(str(rel).replace('\\', '/')).as_posix()
    for contract in CONTRACTS:
        if fnmatch.fnmatch(text, contract.pattern):
            return contract
    return None


def write_dataset_at(
    frame: pl.DataFrame,
    path: Path | str,
    *,
    root: Path | str | None = None,
    writer: str | None = None,
    sort_by: str | None = None,
    allow_extra_columns: bool | None = None,
) -> dict[str, Any]:
    """按目标路径反查契约并受控写入；路径不在注册表内即拒绝（防新增裸写）。"""
    base = Path(root) if root is not None else default_store_root()
    target = Path(path)
    contract = contract_for_path(target, base)
    if contract is None:
        raise ContractViolation(f'{target} 未匹配任何数据集契约，拒绝写入')
    try:
        rel = target.resolve().relative_to(base.resolve()).as_posix()
    except (ValueError, OSError) as exc:
        raise ContractViolation(f'{target} 不在列存根 {base} 之下，拒绝写入') from exc
    return write_dataset(contract.key, frame, rel_path=rel, root=base,
                         writer=writer, sort_by=sort_by,
                         allow_extra_columns=allow_extra_columns)


def resolve_files(contract: DatasetContract, root: Path | str | None = None) -> list[Path]:
    base = Path(root) if root is not None else default_store_root()
    return sorted(base.glob(contract.pattern))


def _schema_map(frame_or_schema: Any) -> dict[str, str]:
    if isinstance(frame_or_schema, dict):
        return {str(k): str(v) for k, v in frame_or_schema.items()}
    if isinstance(frame_or_schema, (pl.DataFrame, pl.LazyFrame)):
        schema = (frame_or_schema.collect_schema()
                  if isinstance(frame_or_schema, pl.LazyFrame) else frame_or_schema.schema)
        return {str(k): str(v) for k, v in schema.items()}
    return {str(k): str(v) for k, v in frame_or_schema.items()}


def validate_schema(contract: DatasetContract, schema: Any, *,
                    allow_extra_columns: bool | None = None) -> None:
    """校验列集合 / 列序 / dtype；不读数据。"""
    actual = _schema_map(schema)
    allow_extra = (contract.allow_extra_columns if allow_extra_columns is None
                   else allow_extra_columns)
    expected = contract.columns
    missing = [c for c in expected if c not in actual]
    extra = [c for c in actual if c not in expected]
    problems: list[str] = []
    if missing:
        problems.append(f'缺列 {missing}')
    if extra and contract.strict and not allow_extra:
        problems.append(f'多列 {extra}（确需额外列的场合显式 allow_extra_columns=True）')
    if contract.strict:
        ordered = tuple(c for c in actual if c in expected)
        if ordered != expected:
            problems.append(f'列顺序不符：期望 {list(expected)}，实际 {list(ordered)}')
    for column, want in contract.dtypes.items():
        got = actual.get(column)
        if got is not None and got != want:
            problems.append(f'{column} dtype 期望 {want}，实际 {got}')
    if problems:
        raise ContractViolation(
            f'{contract.key}（{contract.label}）schema 不符合契约：' + '；'.join(problems))


def validate_frame(contract: DatasetContract, frame: pl.DataFrame, *,
                   allow_extra_columns: bool | None = None) -> None:
    """写入前完整校验：schema + 日期列类型 + 关键列空值。"""
    if frame.is_empty():
        raise ContractViolation(f'{contract.key} 拒绝写入空表（空表不产生登记项）')
    validate_schema(contract, frame, allow_extra_columns=allow_extra_columns)
    if contract.date_column not in frame.columns:
        raise ContractViolation(f'{contract.key} 缺日期列 {contract.date_column!r}')
    actual_dtype = str(frame.schema[contract.date_column])
    # 期货沿用交易所原生列名，日期列声明为 String（见 quote_schema），此处放行
    if actual_dtype != 'Date' and contract.dtypes.get(contract.date_column) != 'String':
        raise ContractViolation(
            f'{contract.key} 日期列 {contract.date_column!r} 必须为 Date，'
            f'实际 {actual_dtype}')
    nulls = frame.select(
        [pl.col(c).null_count().alias(c) for c in contract.non_nullable]).row(0, named=True)
    bad = {c: n for c, n in nulls.items() if n}
    if bad:
        raise ContractViolation(f'{contract.key} 关键列存在空值：{bad}')


def sha256_file(path: Path | str) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _update_ledger(root: Path, registration: dict[str, Any]) -> Path:
    """把登记项写入 ``meta/registry/auto_register.json``（原子替换）。"""
    path = root / LEDGER_REL_PATH
    payload = _read_json(path) or {'version': 1, 'datasets': {}}
    datasets = payload.setdefault('datasets', {})
    key = registration['dataset']
    entry = datasets.setdefault(key, {})
    if registration['partition'] == 'shard':
        # 分片逐文件明细交给 meta/daily_index.parquet，ledger 只留最近一次写入
        entry['last_write'] = registration
        entry['shards_touched'] = int(entry.get('shards_touched', 0)) + 1
    else:
        entry.setdefault('entries', {})[registration['path']] = registration
        entry['last_write'] = registration
    entry['updated_at'] = registration['registered_at']
    payload['updated_at'] = registration['registered_at']
    path.parent.mkdir(parents=True, exist_ok=True)
    write_text_atomic(json.dumps(payload, ensure_ascii=False, indent=2), path)
    return path


def _patch_dataset_manifest(root: Path, contract: DatasetContract,
                            rel_path: str, registration: dict[str, Any]) -> bool:
    """补丁本目录 ``manifest.json`` 中对应条目的 rows/bytes/sha256/date_range。"""
    if contract.partition == 'shard' or not contract.manifest:
        return False
    manifest_path = root / contract.manifest
    payload = _read_json(manifest_path)
    entries = payload.get('files')
    if not isinstance(entries, list):
        return False
    changed = False
    for item in entries:
        if not isinstance(item, dict):
            continue
        item_path = str(item.get('path') or '').replace('\\', '/')
        if item_path != rel_path and not item_path.endswith(rel_path):
            continue
        item['rows'] = registration['rows']
        item['bytes'] = registration['bytes']
        item['sha256'] = registration['sha256']
        if registration.get('max_date'):
            item['date_range'] = [registration.get('min_date'), registration['max_date']]
        changed = True
    if changed:
        payload['generated_at'] = registration['registered_at']
        write_text_atomic(
            json.dumps(payload, ensure_ascii=False, indent=2), manifest_path)
    return changed


def write_dataset(
    key: str,
    frame: pl.DataFrame,
    *,
    rel_path: str | None = None,
    root: Path | str | None = None,
    allow_extra_columns: bool | None = None,
    writer: str | None = None,
    sort_by: str | None = None,
) -> dict[str, Any]:
    """受控写入：契约校验 → 原子落盘 → 自动登记。

    ``rel_path`` 缺省时仅对 ``single`` 分区可用（直接取契约路径）；分片/月度
    分区必须显式给出相对列存根的目标路径，且必须匹配契约 glob。
    """
    contract = contract_for_key(key)
    base = Path(root) if root is not None else default_store_root()
    context = check_write_context(contract.key, base)
    if rel_path is None:
        if contract.partition != 'single':
            raise ContractViolation(
                f'{contract.key} 是 {contract.partition} 分区，必须显式给 rel_path')
        rel_path = contract.pattern
    rel_posix = PurePosixPath(str(rel_path).replace('\\', '/')).as_posix()
    if not fnmatch.fnmatch(rel_posix, contract.pattern):
        raise ContractViolation(
            f'{rel_path} 不属于 {contract.key} 的路径约定 {contract.pattern}')

    validate_frame(contract, frame, allow_extra_columns=allow_extra_columns)
    target = base / rel_posix
    if sort_by and sort_by in frame.columns:
        frame = frame.sort(sort_by)
    write_parquet_atomic(
        frame, target, compression='zstd', compression_level=3, statistics=True,
        allow_extra_columns=bool(
            contract.allow_extra_columns if allow_extra_columns is None
            else allow_extra_columns))

    registration = {
        'dataset': contract.key,
        'label': contract.label,
        'partition': contract.partition,
        'path': rel_posix,
        'rows': frame.height,
        'bytes': target.stat().st_size,
        'sha256': sha256_file(target),
        'min_date': _iso(frame[contract.date_column].min()),
        'max_date': _iso(frame[contract.date_column].max()),
        'columns': list(frame.columns),
        'writer': writer or 'unknown',
        'write_mode': context['mode'],
        'run_id': context.get('run_id'),
        'run_steps': context.get('steps'),
        'writer_pid': os.getpid(),
        'standalone_reason': context.get('standalone_reason'),
        'registered_at': datetime.now().isoformat(timespec='seconds'),
    }
    ledger = _update_ledger(base, registration)
    patched = _patch_dataset_manifest(base, contract, rel_posix, registration)
    registration['ledger'] = str(ledger.relative_to(base)).replace('\\', '/')
    registration['manifest_patched'] = patched
    logger.info(
        f'[contracts] {contract.key} 写入 {rel_posix}：{registration["rows"]:,} 行，'
        f'max_date={registration["max_date"]}，已登记 ledger'
        f'{" + manifest" if patched else ""}')
    return registration


def register_dataset(
    key: str,
    *,
    rel_path: str | None = None,
    root: Path | str | None = None,
    writer: str = 'unknown',
    write_mode: str = 'backfill',
    evidence: str = 'disk_scan',
) -> dict[str, Any]:
    """只登记不写数据：按磁盘实测补登 ledger（历史回填 / 自建原子写入后补登）。

    与 :func:`write_dataset` 的区别是不落盘 parquet；``rows / min_date / max_date``
    一律来自 parquet 实测，``sha256 / bytes`` 仅单文件表给出（分片真值由
    ``meta/daily_index.parquet`` 承担）。
    """
    contract = contract_for_key(key)
    base = Path(root) if root is not None else default_store_root()
    if rel_path is None:
        if contract.partition == 'single':
            rel_path = contract.pattern
        else:
            # 分片/月度表补登：取首个实存文件代表该数据集，聚合行数仍按全量实测
            existing = resolve_files(contract, base)
            if not existing:
                raise ContractViolation(f'{contract.key} 磁盘上无文件：{contract.pattern}')
            rel_path = existing[0].relative_to(base).as_posix()
    rel_posix = PurePosixPath(str(rel_path).replace('\\', '/')).as_posix()
    if not fnmatch.fnmatch(rel_posix, contract.pattern):
        raise ContractViolation(
            f'{rel_path} 不属于 {contract.key} 的路径约定 {contract.pattern}')
    files = resolve_files(contract, base)
    if not files:
        raise ContractViolation(f'{contract.key} 磁盘上无文件：{contract.pattern}')

    stats = pl.scan_parquet([str(p) for p in files]).select(
        pl.len().alias('rows'),
        pl.col(contract.date_column).min().alias('min_date'),
        pl.col(contract.date_column).max().alias('max_date'),
    ).collect().row(0, named=True)
    single = base / rel_posix
    single_cols = list(pl.read_parquet_schema(single).keys()) if single.is_file() else None
    registration = {
        'dataset': contract.key,
        'label': contract.label,
        'partition': contract.partition,
        'path': rel_posix,
        'rows': int(stats['rows']),
        'bytes': single.stat().st_size if single.is_file() else None,
        'sha256': sha256_file(single) if single.is_file() else None,
        'min_date': _iso(stats['min_date']),
        'max_date': _iso(stats['max_date']),
        'columns': single_cols,
        'writer': writer,
        'write_mode': write_mode,
        'evidence': evidence,
        'registered_at': datetime.now().isoformat(timespec='seconds'),
    }
    ledger = _update_ledger(base, registration)
    registration['ledger'] = str(ledger.relative_to(base)).replace('\\', '/')
    logger.info(
        f'[contracts] {contract.key} 补登 ledger：{registration["rows"]:,} 行，'
        f'max_date={registration["max_date"]}')
    return registration


def registration_summary(root: Path | str | None = None) -> dict[str, Any]:
    """ledger 现状（体检与排障用）。"""
    base = Path(root) if root is not None else default_store_root()
    payload = _read_json(base / LEDGER_REL_PATH)
    return payload or {'version': 1, 'datasets': {}}


def datasets_by_category() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for contract in CONTRACTS:
        out.setdefault(contract.category, []).append(contract.key)
    return out


def iter_contracts(keys: Iterable[str] | None = None) -> list[DatasetContract]:
    if keys is None:
        return list(CONTRACTS)
    return [contract_for_key(key) for key in keys]
