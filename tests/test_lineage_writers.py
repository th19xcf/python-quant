"""tools/lineage_writers.py 的判定口径回归用例（含变异验证）。

背景：2026-11-12 血缘审计抽样核对 5 处边，命中 5 假阳性 / 4 漏边 / 2 虚拟边。
旧口径（``build_lineage.writers_for`` + ``resolve_edges2`` 的表键）有三类根因：

1. **表名子串**：``'sh' in '<call:snapshot_path>'`` 把 snapshot / shares / shard 算成
   ``quotes/*/sh/{code}.parquet`` 的写者；
2. **就近提及**：表名出现在 docstring、``def load_xxx`` 或输入路径里即算写边；
3. **未解析目标照收**：``<call:refresh>`` 解析不出文件名仍产边。

本文件每条用例都断言「只认能解析出来的落盘目标」。变异验证放在文件末尾：把旧口径的
三个关键动作（无边界匹配 / 不回填包装函数实参 / 不展开字面量取值域）逐一注回去，
对应用例必须变红。
"""
from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tools import lineage_writers as lw  # noqa: E402

QUOTES_STOCK = 'data/local_store/quotes/stock/{sh,sz,bj}/<code>.parquet'
ST_STATUS = 'data/local_store/meta/st_status/st_status_history.parquet'
INDUSTRY_SNAP = 'data/local_store/meta/industry/industry_history.parquet'
ST_COVERAGE = 'data/local_store/meta/st/st_coverage.parquet'
ST_INTERVALS = 'data/local_store/meta/st/st_intervals.parquet'
ST_RAW = 'data/local_store/meta/st/raw/<name>.parquet'
FINANCIALS3 = 'data/local_store/meta/financials/{income,balance,cashflow}.parquet'
ETF_FEE = 'data/local_store/meta/multi_asset/etf_fee.parquet'
ETF_POOL = 'data/local_store/meta/multi_asset/etf_pool.parquet'
NAME_HISTORY = 'data/local_store/meta/name_history.parquet'
FUTURES_MULTI = ('data/local_store/meta/multi_asset/'
                 '{futures_contracts,futures_continuous,futures_roll_events}.parquet')


def _scan(tmp_path: Path, files: dict[str, str]) -> list[lw.WriteSite]:
    """把源码片段落到临时仓库，返回扫描出的写站点。"""
    for rel, src in files.items():
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(textwrap.dedent(src).lstrip('\n'), encoding='utf-8')
    return lw.scan(tmp_path)


def _edges(hits: list[dict]) -> set[tuple[str, int]]:
    return {(h['file'], h['line']) for h in hits}


def _files(hits: list[dict]) -> set[str]:
    return {h['file'] for h in hits}


# ---------------------------------------------------------------- 假阳性
def test_mention_only_is_not_a_writer(tmp_path: Path) -> None:
    """docstring / def 名 / 输入路径里出现表名，不构成写边（旧口径 5/5 假阳性的共性）。"""
    sites = _scan(tmp_path, {
        'tools/read_industry.py': '''
            from pathlib import Path

            def load_industry_snapshot(root):
                """读取 meta/industry/industry_YYYYMMDD.parquet，只读不写。"""
                return root / 'meta' / 'industry' / 'industry_history.parquet'
        ''',
        'tools/notes.py': '''
            """st_status / st_coverage 的口径说明，本文不落盘。"""
            DOC = 'st_status'
        ''',
    })
    assert lw.detect_writers(INDUSTRY_SNAP, sites) == []
    assert lw.detect_writers(ST_STATUS, sites) == []


def test_stock_shares_is_not_quotes_writer(tmp_path: Path) -> None:
    """``stock_shares`` 含子串 sh，旧口径会算成 quotes/stock/sh 的写者。"""
    sites = _scan(tmp_path, {'src/data/local_store/share_capital.py': '''
        from src.data.atomic_write import write_parquet_atomic

        def save_shares(root, frame):
            path = root / 'meta' / 'share_capital' / 'stock_shares.parquet'
            write_parquet_atomic(frame, path)
    '''})
    assert lw.detect_writers(QUOTES_STOCK, sites) == []


def test_unresolved_target_produces_no_edge(tmp_path: Path) -> None:
    """目标解析不出文件名（``<call:refresh>`` 一类）时不许产边。"""
    sites = _scan(tmp_path, {'src/data/breadth.py': '''
        from src.data.atomic_write import write_parquet_atomic

        def refresh(root, frame):
            path = build_path(root)          # 纯运行期取值，静态不可解析
            return write_parquet_atomic(frame, path)

        def build_path(root):
            return root / lookup_name()      # 未解析调用 → 无文件名
    '''})
    assert lw.detect_writers(QUOTES_STOCK, sites) == []
    assert lw.detect_writers(ST_COVERAGE, sites) == []


# ---------------------------------------------------------------- 漏边
def test_resolved_helper_target_goes_to_its_own_table(tmp_path: Path) -> None:
    """``snapshot_path()`` 的返回路径要落到 st_status，而不是被 quotes 子串吸走。"""
    sites = _scan(tmp_path, {'src/data/risk_flags.py': '''
        from src.data.atomic_write import write_parquet_atomic

        def snapshot_path(root):
            return root / 'meta' / 'st_status' / 'st_status_history.parquet'

        def save(store, frame):
            path = snapshot_path(store)
            write_parquet_atomic(frame, path)
    '''})
    assert _files(lw.detect_writers(ST_STATUS, sites)) == {'src/data/risk_flags.py'}
    assert lw.detect_writers(QUOTES_STOCK, sites) == []


def test_wrapper_helper_backfills_every_caller(tmp_path: Path) -> None:
    """``_write_parquet_atomic(frame, path)`` 的每个调用方目标都要算写者。"""
    sites = _scan(tmp_path, {'src/data/st_history.py': '''
        from src.data.atomic_write import write_parquet_atomic

        def _write_parquet_atomic(frame, path):
            write_parquet_atomic(frame, path)

        def _raw_dir(root):
            return root / 'meta' / 'st' / 'raw'

        def save_notices(root, frame):
            _write_parquet_atomic(frame, _raw_dir(root) / 'cninfo_notices.parquet')

        def save_intervals(root, frame):
            _write_parquet_atomic(frame, root / 'meta' / 'st' / 'st_intervals.parquet')

        def save_coverage(root, frame):
            _write_parquet_atomic(frame, root / 'meta' / 'st' / 'st_coverage.parquet')
    '''})
    assert _files(lw.detect_writers(ST_COVERAGE, sites)) == {'src/data/st_history.py'}
    assert _files(lw.detect_writers(ST_INTERVALS, sites)) == {'src/data/st_history.py'}
    assert _files(lw.detect_writers(ST_RAW, sites)) == {'src/data/st_history.py'}


def test_registry_driven_filename_attributes_by_directory(tmp_path: Path) -> None:
    """``table_path(table)`` 的目录在源码里写明、文件名是运行期变量时按目录归属。"""
    sites = _scan(tmp_path, {'src/data/financials/store.py': '''
        from src.data.atomic_write import write_parquet_atomic

        SCHEMAS = {'income': {}, 'balance': {}, 'cashflow': {}, 'valuation': {}}

        def table_path(table, root):
            name = 'valuation_daily' if table == 'valuation' else table
            return root / 'meta' / 'financials' / f'{name}.parquet'

        def _atomic_write(frame, path):
            write_parquet_atomic(frame, path)

        def write_table(frame, table, root):
            _atomic_write(frame, table_path(table, root))
    '''})
    assert _files(lw.detect_writers(FINANCIALS3, sites)) == {'src/data/financials/store.py'}


def test_argparse_default_target_is_used(tmp_path: Path) -> None:
    """``--target`` 的 default 就是代码声明的落盘路径，可解析。"""
    sites = _scan(tmp_path, {'tools/build_name_history.py': '''
        import argparse

        from pathlib import Path

        from src.data.atomic_write import write_parquet_atomic

        ROOT = Path(__file__).resolve().parents[1]
        TARGET = ROOT / 'data' / 'local_store' / 'meta' / 'name_history.parquet'

        def main(argv=None):
            parser = argparse.ArgumentParser()
            parser.add_argument('--target', default=str(TARGET))
            args = parser.parse_args(argv)
            target = Path(args.target)
            write_parquet_atomic(load_frame(), target)
    '''})
    assert _files(lw.detect_writers(NAME_HISTORY, sites)) == {'tools/build_name_history.py'}


def test_loop_over_filename_list_resolves_targets(tmp_path: Path) -> None:
    """``for name in ['etf_fee.parquet', ...]: write(TARGET / name)`` 要逐个归属。"""
    sites = _scan(tmp_path, {'tools/fetch_etf_fee.py': '''
        from pathlib import Path

        from src.data.atomic_write import write_parquet_atomic

        STAGING = Path('_staging/etf_fee')
        TARGET = Path('data/local_store/meta/multi_asset')

        def promote(frame):
            names = ['etf_fee.parquet', 'etf_fee.csv']
            for name in names:
                write_parquet_atomic(frame, TARGET / name)
    '''})
    assert _files(lw.detect_writers(ETF_FEE, sites)) == {'tools/fetch_etf_fee.py'}


# ---------------------------------------------------------------- 虚拟边
def test_staging_candidate_is_not_a_formal_writer(tmp_path: Path) -> None:
    """``_staging/<任务>/`` 下的候选文件不是正式表写者。"""
    sites = _scan(tmp_path, {'_staging/etf_fee_20261101/collect.py': '''
        from pathlib import Path

        from src.data.atomic_write import write_parquet_atomic

        OUT = Path(__file__).parent / 'etf_fee.parquet'

        def save(frame):
            write_parquet_atomic(frame, OUT)
    '''})
    assert lw.detect_writers(ETF_FEE, sites) == []


def test_file_anchor_keeps_staging_raw_out_of_st_raw(tmp_path: Path) -> None:
    """``Path(__file__).parent / 'raw'`` 是 staging 目录，不能吸收到 meta/st/raw。"""
    sites = _scan(tmp_path, {'_staging/index_fill_20261110/fetch.py': '''
        from pathlib import Path

        from src.data.atomic_write import write_parquet_atomic

        OUT = Path(__file__).parent / 'raw'

        def save(frame, name):
            write_parquet_atomic(frame, OUT / f'{name}.parquet')
    '''})
    assert lw.detect_writers(ST_RAW, sites) == []


def test_stale_record_line_is_dropped(tmp_path: Path) -> None:
    """上游快照记录的行号若在当前源码已不是写调用，整条记录作废（否则是虚边）。

    实测 ``edges_raw2.json`` 13 条只能靠记录产边的条目里有 7 条行号已漂到注释 /
    续行 / print 上；行号校验只对「本次已扫描的文件」生效。
    """
    table = 'data/local_store/meta/breadth/breadth_daily.parquet'
    sites = _scan(tmp_path, {'tools/build_breadth_ma.py': '''
        from src.data.atomic_write import write_parquet_atomic

        def save(root, frame):
            # 旧版这行曾是 write_parquet_atomic(frame, target)
            path = pick(root)
            write_parquet_atomic(frame, path)
    '''})
    expr = '<deep>/meta/breadth/breadth_daily.parquet'
    stale = [{'file': 'tools/build_breadth_ma.py', 'line': 4,
              'call': 'write_parquet_atomic', 'path_expr': expr}]
    assert lw.detect_writers(table, sites, records=stale) == []
    fresh = [dict(stale[0], line=6)]
    assert _edges(lw.detect_writers(table, sites, records=fresh)) == {
        ('tools/build_breadth_ma.py', 6)}


def test_pool_candidate_and_target_split_across_tables(tmp_path: Path) -> None:
    """同一个工具既写 staging 候选又写正式池：只有后者算 etf_pool 写者，且不算多资产期货。"""
    sites = _scan(tmp_path, {'tools/build_multi_asset_etf_pool.py': '''
        from pathlib import Path

        from src.data.atomic_write import write_parquet_atomic

        STAGING = Path('_staging/etf_pool')
        TARGET_DIR = Path('data/local_store/meta/multi_asset')

        def build(frame):
            pool_path = STAGING / 'etf_pool.parquet'
            write_parquet_atomic(frame, pool_path)
            write_parquet_atomic(frame, TARGET_DIR / 'etf_pool.parquet')
    '''})
    assert lw.detect_writers(FUTURES_MULTI, sites) == []
    assert _files(lw.detect_writers(ETF_POOL, sites)) == {'tools/build_multi_asset_etf_pool.py'}


# ---------------------------------------------------------------- 真实仓库抽样（11 处偏差）
_SAMPLE_FILES = ('src/data/st_history.py', 'src/data/risk_flags.py', 'src/data/industry.py')


@pytest.mark.skipif(not all((ROOT / f).is_file() for f in _SAMPLE_FILES),
                    reason='样本源码不在当前工作树')
def test_repo_sample_eleven_deviations() -> None:
    """抽样核对的 5 处边：5 假阳性 / 4 漏边 / 2 虚拟边，逐条钉住。"""
    sites = lw.scan(ROOT)
    inventory = [QUOTES_STOCK, ST_STATUS, INDUSTRY_SNAP, ST_COVERAGE, ST_INTERVALS, ST_RAW,
                 FINANCIALS3, ETF_FEE, ETF_POOL, NAME_HISTORY, FUTURES_MULTI]
    quotes = _edges(lw.detect_writers(QUOTES_STOCK, sites, inventory=inventory))
    assert ('src/data/local_store/store.py', 527) in quotes, '漏边：quote_path 经 write_daily 落盘'
    for false_positive in [('src/data/industry.py', 157),
                           ('src/data/local_store/share_capital.py', 261),
                           ('src/data/quality/extended_builders.py', 121),
                           ('src/data/risk_flags.py', 253),
                           ('src/data/services/breadth_service.py', 118)]:
        assert false_positive not in quotes, f'假阳性仍在：{false_positive}'

    assert ('src/data/risk_flags.py', 253) in _edges(
        lw.detect_writers(ST_STATUS, sites, inventory=inventory)), '漏边：st_status'
    assert ('src/data/industry.py', 157) in _edges(
        lw.detect_writers(INDUSTRY_SNAP, sites, inventory=inventory)), '漏边：industry_snap'
    assert ('src/data/st_history.py', 405) in _edges(
        lw.detect_writers(ST_COVERAGE, sites, inventory=inventory)), '漏边：st_coverage'

    futures = _edges(lw.detect_writers(FUTURES_MULTI, sites, inventory=inventory))
    assert ('tools/fetch_etf_fee.py', 593) not in futures, '虚边：docstring 里的目录说明'
    assert ('tools/build_multi_asset_etf_pool.py', 532) not in futures, '虚边：staging 候选路径'


# ---------------------------------------------------------------- 变异验证
def test_mutation_permissive_covers_kills_false_positive_cases(tmp_path: Path,
                                                              monkeypatch: pytest.MonkeyPatch) -> None:
    """把「无边界匹配」的旧口径放回去：假阳性用例必须变红。"""
    sites = _scan(tmp_path, {'src/data/local_store/share_capital.py': '''
        from src.data.atomic_write import write_parquet_atomic

        def save_shares(root, frame):
            path = root / 'meta' / 'share_capital' / 'stock_shares.parquet'
            write_parquet_atomic(frame, path)
    '''})
    assert lw.detect_writers(QUOTES_STOCK, sites) == []
    monkeypatch.setattr(lw, 'covers', lambda target, table_glob: True)
    assert lw.detect_writers(QUOTES_STOCK, sites) != [], '旧口径下本用例应当失败'


def test_mutation_dropping_helper_backfill_kills_missed_edge_cases(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """不回填包装函数实参（旧口径的漏边根因）：helper 用例必须变红。

    回填发生在 :func:`lw.scan` 里，所以变异必须在重新扫描之前注入——只改
    ``detect_writers`` 的输入是测不到的。
    """
    files = {'src/data/st_history.py': '''
        from src.data.atomic_write import write_parquet_atomic

        def _write_parquet_atomic(frame, path):
            write_parquet_atomic(frame, path)

        def save_coverage(root, frame):
            _write_parquet_atomic(frame, root / 'meta' / 'st' / 'st_coverage.parquet')
    '''}
    assert _files(lw.detect_writers(ST_COVERAGE, _scan(tmp_path, files))) == {
        'src/data/st_history.py'}
    monkeypatch.setattr(lw, '_fill_params', lambda patterns, *a, **k: patterns)
    assert lw.detect_writers(ST_COVERAGE, _scan(tmp_path, files)) == [], \
        '旧口径下本用例应当失败'


def test_mutation_dropping_literal_domains_kills_loop_case(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """不展开字面量取值域（旧口径解析不出 ``TARGET / name``）：循环用例必须变红。

    取值域在 :func:`lw.scan` 里展开，变异同样必须在重新扫描之前注入；断言钉的是
    「解析出的目标文件名」，取消取值域后文件名退化成未知段。
    """
    files = {'tools/fetch_etf_fee.py': '''
        from pathlib import Path

        from src.data.atomic_write import write_parquet_atomic

        TARGET = Path('data/local_store/meta/multi_asset')

        def promote(frame):
            for name in ['etf_fee.parquet']:
                write_parquet_atomic(frame, TARGET / name)
    '''}
    hits = lw.detect_writers(ETF_FEE, _scan(tmp_path, files))
    assert [h['target'] for h in hits] == ['data/local_store/meta/multi_asset/etf_fee.parquet']
    monkeypatch.setattr(lw, '_literal_values', lambda *a, **k: [])
    assert [h['target'] for h in lw.detect_writers(ETF_FEE, _scan(tmp_path, files))] \
        != ['data/local_store/meta/multi_asset/etf_fee.parquet'], \
        '旧口径下本用例应当失败'
