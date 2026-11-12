"""生成 lineage.json + 表清单 CSV + 基础层校准清单（全部来自已落盘证据）。

输入（均已生成）：
  work/inventory_full.json      行数/字节/manifest 状态
  work/edges_raw2.json          src/tools/_staging 的读写点扫描（仅作路径表达式来源）
  src/tools/_staging 源码       AST 落盘目标解析（tools/lineage_writers.py）
  work/gaps.tsv                 G-编号
  outputs/zero_consumer_evidence.json  四通道证据
  outputs/universe_consistency.json    口径实测
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(r"e:\code\python\quant")
WORK = ROOT / "2026-11-12" / "data-lineage" / "work"
#: 输出目录可用 ``LINEAGE_OUT`` 覆盖：验收重跑写到临时目录，避免覆盖既定产物
OUT = ROOT / "2026-11-12" / "data-lineage" / "outputs"
OUT_DIR = Path(os.environ.get("LINEAGE_OUT") or OUT)
OUT_DIR.mkdir(parents=True, exist_ok=True)

sys.path.insert(0, str(ROOT))

from tools import lineage_writers as lw  # noqa: E402  (写者判定：只认可解析的落盘目标)

inv = json.loads((WORK / "inventory_full.json").read_text(encoding="utf-8"))
edges = json.loads((WORK / "edges_raw2.json").read_text(encoding="utf-8"))
zero = json.loads((OUT / "zero_consumer_evidence.json").read_text(encoding="utf-8"))
cons = json.loads((OUT / "universe_consistency.json").read_text(encoding="utf-8"))

rows_by_rel = {t["rel"]: t for t in inv["tables"]}
fam = {f["data_type"]: f for f in inv["quote_families"]}

# ---------------------------------------------------------------- 表定义
# klass: base=基础层（外部源/原始行情直接落地，不经其他表推导）
#        derived=派生层（可由上游表算出，给出代码位置）
#        mixed=既落地又参与推导 / 含人工覆盖（不硬塞进任一边）
T = []


def add(**kw):
    T.append(kw)


# ===== L0 基础：行情族 =====
add(id="quotes_stock", cn="个股日线（含 qfq_factor 列）", layer="L0", klass="base",
    path="data/local_store/quotes/stock/{sh,sz,bj}/<code>.parquet",
    rows=fam["stock"]["rows"], files=fam["stock"]["codes"],
    writer_primary="src/data/local_store/store.py:510（LocalQuoteStore.save，原子写）",
    writer_note="常规入口 = 采集/同步服务经 store 落盘；修补脚本（如缺口回填）多为 _staging 一次性脚本",
    inputs=[], consumers_note="几乎全部下游派生表的唯一原始输入",
    registered=True, rebuildable="可重建但成本高（外部源逐票重抓）",
    second_copy="有：G:\\git\\python-quant 镜像",
    reconciled="与通达信 .day 抽样对拍（docs/通达信单股票校准指南.md）",
    defects="非交易日 bar 已另有任务在查（标『已在查』）；BJ 市场 357 只", in_progress=True,
    gaps=["G-101"])
add(id="quotes_index", cn="指数日线", layer="L0", klass="base",
    path="data/local_store/quotes/index/{sh,sz,bj}/<code>.parquet",
    rows=fam["index"]["rows"], files=fam["index"]["codes"],
    writer_primary="src/data/local_store/store.py:510",
    writer_note="同上；指数无复权因子列", inputs=[],
    registered=True, rebuildable="可重建（外部源）", second_copy="有：G: 镜像",
    reconciled="与指数官方月线未系统对拍（未见记录）", defects="未见", in_progress=False)
add(id="quotes_fund", cn="ETF/LOF 日线", layer="L0", klass="base",
    path="data/local_store/quotes/fund/{sh,sz}/<code>.parquet",
    rows=fam["fund"]["rows"], files=fam["fund"]["codes"],
    writer_primary="src/data/local_store/store.py:510",
    inputs=[], registered=True, rebuildable="可重建（外部源）", second_copy="有：G: 镜像",
    reconciled="未见系统对拍记录", defects="未见", in_progress=False)
add(id="quotes_futures", cn="期货合约日线", layer="L0", klass="base",
    path="data/local_store/quotes/futures/{cffex,shfe,ine}/<合约>.parquet",
    rows=fam["futures"]["rows"], files=fam["futures"]["codes"],
    writer_primary="src/data/local_store/store.py:510",
    inputs=[], registered=True, rebuildable="可重建（外部源）", second_copy="有：G: 镜像",
    reconciled="未见", defects="无任何 manifest（三张派生表同样）", in_progress=False)

# ===== L0 基础：外部采集 =====
add(id="st_raw", cn="ST 判定原始证据（9 件：公告/名称变更/机构库/复核覆盖）", layer="L0", klass="base",
    path="data/local_store/meta/st/raw/*.parquet",
    rows=None, files=9,
    writer_primary="未定位到常规写入口（按 docs 记录为人工/一次性采集）",
    inputs=[], registered=True, rebuildable="不可从本地重算（外部证据）",
    second_copy="有：G: 镜像", reconciled="逐条见 docs/证据链",
    defects="7 张无 manifest（G-095）", gaps=["G-095"], in_progress=False)
add(id="st_status", cn="ST 状态日快照（7 份带日期文件）", layer="L0", klass="base",
    path="data/local_store/meta/st_status/st_status_<YYYYMMDD>.parquet",
    rows=None, files=7, writer_primary="未定位到常规写入口",
    inputs=[], registered=True, rebuildable="需外部源重抓", second_copy="有：G: 镜像",
    reconciled="st_intervals 与其做过快照对照（layers manifest 内 st_snapshot_vs_interval.json）",
    defects="日期文件名多版本并存；clean_set 曾硬编码 20261103（G-098）", gaps=["G-098"], in_progress=False)
add(id="industry_snap", cn="行业归属日快照（7 份带日期文件）", layer="L0", klass="base",
    path="data/local_store/meta/industry/industry_<YYYYMMDD>.parquet",
    rows=None, files=7, writer_primary="未定位到常规写入口",
    inputs=[], registered=True, rebuildable="需外部源重抓", second_copy="有：G: 镜像",
    reconciled="industry_pit 构建时与其对拍（派生层侧）",
    defects="日期文件名多版本并存（同 G-098 一类）", gaps=["G-098"], in_progress=False)
add(id="index_weight", cn="指数成分权重月表（外部）", layer="L0", klass="base",
    path="data/local_store/meta/index_members/index_weight_monthly.parquet",
    rows=None, writer_primary="外部接口拉取脚本（tools/ 下），未逐行核实",
    inputs=[], registered=True, rebuildable="可重抓（外部源）", second_copy="有：G: 镜像",
    reconciled="membership_intervals 派生侧对拍", defects="未见", in_progress=False)
add(id="valuation", cn="估值日表（PE/PB/DV/总市值）", layer="L0", klass="base",
    path="data/local_store/meta/financials/valuation_daily.parquet",
    rows=None, writer_primary="src/data/financials/collector.py（常规采集入库）",
    inputs=[], registered=True, rebuildable="可重建（外部源 + 本地补丁）",
    second_copy="有：G: 镜像", reconciled="source 列字段级标注（DG-19 先例）",
    defects="2018 前需 valuation_pre2018_patch 补丁", in_progress=False)
add(id="financials3", cn="三大报表（income/balance/cashflow）", layer="L0", klass="base",
    path="data/local_store/meta/financials/{income,balance,cashflow}.parquet",
    rows=None, writer_primary="src/data/financials/collector.py",
    inputs=[], registered=True, rebuildable="可重建（外部源）", second_copy="有：G: 镜像",
    reconciled="与交易所口径存在差异记录", defects="ann_date 覆盖不全（G-045 相关）", in_progress=False)
add(id="ann_date_map", cn="公告日映射", layer="L1", klass="derived",
    path="data/local_store/meta/financials/ann_date_map.parquet",
    writer_primary="src/data/financials/collector.py（由报表+公告推导）",
    inputs=["financials3"], registered=True, rebuildable="可重建",
    second_copy="有：G: 镜像", reconciled="2026-11-12 做过合并修复（G-045）",
    defects="历史公告日覆盖不全", gaps=["G-045"], in_progress=False)
add(id="fund_basic", cn="基金名录", layer="L1", klass="base",
    path="data/local_store/meta/fund_basic.parquet",
    writer_primary="MySQL 合并导出（外部整理）", inputs=[], registered=True,
    rebuildable="需外部库", second_copy="有：G: 镜像", reconciled="未见",
    defects="未见", in_progress=False)
add(id="index_basic", cn="指数名录", layer="L1", klass="base",
    path="data/local_store/meta/index_basic.parquet",
    writer_primary="MySQL 合并导出（外部整理）", inputs=[], registered=True,
    rebuildable="需外部库", second_copy="有：G: 镜像", reconciled="未见",
    defects="未见", in_progress=False)
add(id="names_json", cn="通达信代码表（names.json）", layer="L0", klass="base",
    path="data/local_store/meta/names.json",
    writer_primary="通达信导出（外部）", inputs=[], registered=True,
    rebuildable="可重新导出", second_copy="有：G: 镜像", reconciled="name_history 派生侧对拍",
    defects="未见", in_progress=False)
add(id="events", cn="公告事件表", layer="L0", klass="base",
    path="data/local_store/meta/events/events.parquet",
    writer_primary="src/data/events.py + tools/backfill_events.py + tools/update_events.py（多写者）",
    inputs=[], registered=True, rebuildable="可重建（外部公告）", second_copy="有：G: 镜像",
    reconciled="见事件抽取校验脚本", defects="多写入口，口径需确认", in_progress=False)
add(id="stock_dividend", cn="个股分红送转（含交易所公告修正）", layer="L0", klass="base",
    path="data/local_store/factors/stock_dividend.parquet",
    writer_primary="src/data/local_store/build.py:979（常规）；tools/collect_stock_dividend.py（采集）",
    inputs=[], registered=True, rebuildable="可重建（外部源 + 公告修正）",
    second_copy="有：G: 镜像", reconciled="raw+fix 双层对拍（DG-30 先例）",
    defects="历史分红漂移已修（2026-11-05）", in_progress=False)
add(id="fund_dividend", cn="基金分红", layer="L0", klass="base",
    path="data/local_store/factors/fund_dividend.parquet",
    writer_primary="src/data/local_store/fund_factors.py", inputs=[], registered=True,
    rebuildable="可重建（外部源）", second_copy="有：G: 镜像", reconciled="回测对拍通过",
    defects="未见", in_progress=False)
add(id="fund_share_change", cn="基金份额变动", layer="L0", klass="base",
    path="data/local_store/factors/fund_share_change.parquet",
    writer_primary="src/data/local_store/fund_factors.py", inputs=[], registered=True,
    rebuildable="可重建（外部源）", second_copy="有：G: 镜像", reconciled="回测对拍通过",
    defects="未见", in_progress=False)
add(id="stock_shares", cn="股本与流通股本", layer="L0", klass="mixed",
    path="data/local_store/meta/factors/stock_shares.parquet",
    writer_primary="src/data/local_store/share_capital.py + tools/derive_free_float_fill.py:225（两个写入口）",
    inputs=[], registered=True, rebuildable="外部股本 + 本地推导补全（free_float）",
    second_copy="有：G: 镜像", reconciled="自由流通股本为派生补全",
    defects="两写入口口径需确认", in_progress=False)
add(id="stock_turnover", cn="个股换手率", layer="L0", klass="mixed",
    path="data/local_store/factors/stock_turnover.parquet",
    writer_primary="src/data/local_store/turnover.py（turn 来自 baostock，float_share 来自 stock_shares）",
    inputs=["stock_shares", "quotes_stock"], registered=True,
    rebuildable="可重建（外部 turn + 本地股本）", second_copy="有：G: 镜像",
    reconciled="有 check_turnover 类校验", defects="依赖 stock_shares 自由流通股本口径",
    in_progress=False)
add(id="macro", cn="宏观五表（国债10Y/两融/国债曲线/信用曲线/债券指数）", layer="L0", klass="base",
    path="data/local_store/meta/macro/{cn_treasury_yield_10y,cn_market_margin_daily,cn_treasury_yield_curve,cn_credit_yield_curve,cn_bond_index}.parquet",
    rows=None, writer_primary="tools/treasury_yield.py、tools/margin_daily.py、tools/build_cn_bond_tables.py",
    inputs=[], registered=True, rebuildable="可重建（外部源）", second_copy="有：G: 镜像",
    reconciled="state_vector 派生侧使用", defects="两融历史起点晚（2010）", in_progress=False)
add(id="etf_fee", cn="ETF 费率表", layer="L0", klass="base",
    path="data/local_store/meta/multi_asset/etf_fee.parquet",
    writer_primary="外部费率抓取脚本", inputs=[], registered=True,
    rebuildable="可重抓", second_copy="有：G: 镜像", reconciled="未见",
    defects="未见", in_progress=False)

# ===== L1 名录/日历 =====
add(id="stock_basic", cn="个股基础名录", layer="L1", klass="mixed",
    path="data/local_store/meta/stock_basic.parquet",
    writer_primary="tools/build_stock_basic.py（常规，可反复跑）",
    inputs=["quotes_stock", "names_json", "name_history", "breadth_daily"],
    registered=True, rebuildable="可重建（由行情首末行 + 名称快照推导）",
    second_copy="有：G: 镜像", reconciled="与行情首末行逐值对拍",
    defects="把 breadth_daily 当交易日历（跨层反向依赖，build_stock_basic.py:205-210）",
    in_progress=False)
add(id="name_history", cn="证券简称历史（PIT）", layer="L1", klass="derived",
    path="data/local_store/meta/name_history.parquet",
    writer_primary="tools/build_name_history.py（常规）",
    inputs=["names_json", "st_raw", "quotes_stock"], registered=True,
    rebuildable="可重建", second_copy="有：G: 镜像", reconciled="PIT 简称与公告对拍",
    defects="2023-2025 部分退市标的缺「退」段（universe_filter 文档已记）", in_progress=False)
add(id="listing_intervals", cn="上市区间（含退市）", layer="L1", klass="derived",
    path="data/local_store/meta/universe/listing_intervals.parquet",
    writer_primary="src/data/quality/listing_intervals.py:77（常规）",
    inputs=["quotes_stock"], registered=True, rebuildable="可重建",
    second_copy="有：G: 镜像", reconciled="与行情首末行对拍",
    defects="未见", in_progress=False)
add(id="daily_index", cn="全库行情索引（逐票首末日/行数/大小）", layer="L1", klass="derived",
    path="data/local_store/meta/daily_index.parquet",
    writer_primary="行情入库后刷新（store/sync 链路）",
    inputs=["quotes_stock", "quotes_index", "quotes_fund"], registered=True,
    rebuildable="可重建（扫盘）", second_copy="有：G: 镜像",
    reconciled="行数与磁盘对拍：11 行差异 2026-11-12 记录（G-045 相关）",
    defects="与磁盘行数存在 11 行差异", in_progress=False)
add(id="code_migration", cn="旧码迁移映射", layer="L1", klass="derived",
    path="data/local_store/meta/code_migration.parquet",
    writer_primary="src/data/quality/code_migration.py（常规）",
    inputs=["quotes_stock", "name_history"], registered=True, rebuildable="可重建",
    second_copy="有：G: 镜像", reconciled="未见", defects="无 manifest", in_progress=False)

# ===== L2 派生 =====
add(id="stock_adj_factor", cn="个股复权因子表", layer="L2", klass="derived",
    path="data/local_store/factors/stock_adj_factor.parquet",
    writer_primary="src/data/local_store/build.py:977 + src/data/local_store/factor_rebuild.py（常规）",
    inputs=["quotes_stock", "stock_dividend"], registered=True, rebuildable="可重建",
    second_copy="有：G: 镜像", reconciled="除权参考价残差 0.000000（历史验收）",
    defects="3 只标的登记与实物不符——已在查（他任务，600872/600305/600989）", in_progress=True)
add(id="fund_adj_factor", cn="基金复权因子", layer="L2", klass="derived",
    path="data/local_store/factors/fund_adj_factor.parquet",
    writer_primary="src/data/local_store/fund_factors.py:469 + tools/fund_split_repair.py:357（两个写入口）",
    inputs=["quotes_fund", "fund_dividend", "fund_share_change"], registered=True,
    rebuildable="可重建", second_copy="有：G: 镜像", reconciled="回测对拍通过",
    defects="多写入口（拆分修正脚本会改同一文件）", in_progress=False)
add(id="fund_corp_actions", cn="基金公司行为汇总（2019+）", layer="L2", klass="derived",
    path="data/local_store/meta/fund_corporate_actions_2019plus.parquet",
    writer_primary="未定位到常规入口（由基金分红/拆分汇总）",
    inputs=["fund_dividend", "fund_share_change", "fund_adj_factor"], registered=True,
    rebuildable="可重建", second_copy="有：G: 镜像", reconciled="未见", defects="写入口未定位",
    in_progress=False)
add(id="breadth_daily", cn="市场宽度日表", layer="L2", klass="derived",
    path="data/local_store/meta/breadth/breadth_daily.parquet",
    writer_primary="src/data/services/breadth_service.py:353（常规 refresh）",
    inputs=["quotes_stock"], registered=True, rebuildable="可重建（读行情 qfq_factor 列）",
    second_copy="有：G: 镜像", reconciled="与行情逐日对拍",
    defects="被 stock_basic 当交易日历使用（反向依赖上游）", in_progress=False)
add(id="breadth_ma_daily", cn="宽度均线/新高新低（250 日）", layer="L2", klass="derived",
    path="data/local_store/meta/breadth/breadth_ma_daily.parquet",
    writer_primary="tools/build_breadth_ma.py（常规，--apply）",
    inputs=["quotes_stock", "stock_basic", "listing_intervals", "code_migration"],
    registered=True, rebuildable="可重建", second_copy="有：G: 镜像",
    reconciled="与 breadth_daily 隔离验收", defects="未见", in_progress=False)
add(id="industry_pit", cn="行业归属 PIT（申万/通用/变更）", layer="L2", klass="derived",
    path="data/local_store/meta/industry_pit/{industry_pit,industry_pit_sw,industry_change_pit}.parquet",
    writer_primary="src/data/industry_pit.py（常规）",
    inputs=["industry_snap", "stock_basic", "name_history"], registered=True,
    rebuildable="可重建（依赖行业快照）", second_copy="有：G: 镜像",
    reconciled="与 industry 快照对拍", defects="依赖 7 份快照的日期覆盖", in_progress=False)
add(id="membership_intervals", cn="指数成分区间", layer="L2", klass="derived",
    path="data/local_store/meta/index_members/membership_intervals.parquet",
    writer_primary="src/data/quality/extended_builders.py:629（build_index_membership →"
                   " _write_frame(_path(store, 'index_membership'))）",
    writer_note="路径由注册表驱动：_path 取 _SPEC[key].path（extended_builders.py:106-107），"
                "_SPEC 由 extended_contracts.py:63 的 CONTRACTS 展开，该表 path 登记在"
                " extended_contracts.py:107；静态解析停在 _SPEC[key].path，故 writers_detected"
                " 为 0 而写者确实存在。tools/rebuild_index_constituents.py:15 明确声明不写权威"
                " 路径，不是该表写入口",
    inputs=["index_weight", "index_basic"], registered=True, rebuildable="可重建",
    second_copy="有：G: 镜像", reconciled="与月权重表对拍", defects="未见", in_progress=False)
add(id="etf_pool", cn="ETF 可选池", layer="L2", klass="derived",
    path="data/local_store/meta/multi_asset/etf_pool.parquet",
    writer_primary="tools/build_multi_asset_etf_pool.py（常规）",
    inputs=["fund_basic", "quotes_fund", "fund_adj_factor", "etf_fee"], registered=True,
    rebuildable="可重建", second_copy="有：G: 镜像", reconciled="回测对拍",
    defects="未见", in_progress=False)
add(id="etf_equity_pool", cn="ETF 权益池（CSV）", layer="L2", klass="derived",
    path="data/local_store/meta/pools/etf_equity_pool*.csv",
    writer_primary="tools/build_etf_equity_pool.py（一次性?）",
    inputs=["quotes_fund", "fund_share_change", "fund_dividend"], registered=True,
    rebuildable="可重建", second_copy="有：G: 镜像", reconciled="未见",
    defects="被 universe layers L5 读取（下游反向引用）", in_progress=False)
add(id="futures_multi_asset", cn="期货合约/连续/移仓事件", layer="L2", klass="derived",
    path="data/local_store/meta/multi_asset/{futures_contracts,futures_continuous,futures_roll_events}.parquet",
    writer_primary="tools/build_futures_continuous.py（常规）",
    inputs=["quotes_futures"], registered=True, rebuildable="可重建", second_copy="有：G: 镜像",
    reconciled="未见", defects="三张表无 manifest", in_progress=False)

# ===== L3 状态判定 =====
add(id="suspension_daily", cn="停牌日表", layer="L3", klass="derived",
    path="data/local_store/meta/suspension/suspension_daily.parquet",
    writer_primary="src/data/quality/extended_builders.py（build_suspension）",
    inputs=["quotes_stock"], registered=True, rebuildable="可重建",
    second_copy="有：G: 镜像", reconciled="与公告证据对拍",
    defects="未见", in_progress=False)
add(id="implied_suspension", cn="隐含停牌（无行情推断）", layer="L3", klass="derived",
    path="data/local_store/meta/suspension/implied_suspension.parquet",
    writer_primary="无常规定义入口：src/、tools/、tests/ 内无任何语句写该路径（写入方为仓库外"
                   "一次性脚本）",
    writer_note="证据：全仓引用该路径的 12 处均为读或文档——suspension_union.py:27 列为并表源、"
                ":130 读取校验，clean_set.py:974 _load_implied_suspension 读取，"
                "build_universe_layers.py:87 读取，check_entity_dedupe.py:77 读取，"
                "tests/test_suspension_union.py:25 只往临时目录写夹具；"
                "grep 'implied_suspension.parquet' 无 write_* / to_parquet 调用",
    inputs=["quotes_stock"], registered=True, rebuildable="逻辑可重建，脚本未入库",
    second_copy="有：G: 镜像", reconciled="未见",
    defects="只能靠一次性脚本更新——不可复现资产（DG 判定类缺陷）", in_progress=False)
add(id="st_price_inferred", cn="按价格推断的 ST 区间", layer="L3", klass="derived",
    path="data/local_store/meta/st/st_price_inferred.parquet",
    writer_primary="src/data/st_price_infer.py（常规）",
    inputs=["quotes_stock", "name_history"], registered=True, rebuildable="可重建",
    second_copy="有：G: 镜像", reconciled="与公告证据对拍", defects="未见", in_progress=False)
add(id="st_intervals", cn="ST 区间（最终版）", layer="L3", klass="mixed",
    path="data/local_store/meta/st/st_intervals.parquet",
    writer_primary="src/data/st_history.py（build_store，区间合并 + 人工复核覆盖）",
    inputs=["st_status", "st_raw", "st_price_inferred", "supplement_intervals"],
    registered=True, rebuildable="可重建（但含人工裁定覆盖层）", second_copy="有：G: 镜像",
    reconciled="与快照对照（st_snapshot_vs_interval.json）",
    defects="人工覆盖层不可由代码重建，改动需留证", gaps=["G-104"], in_progress=False)
add(id="st_coverage", cn="ST 证据覆盖率", layer="L3", klass="derived",
    path="data/local_store/meta/st/st_coverage.parquet",
    writer_primary="src/data/st_history.py:1133",
    inputs=["st_intervals", "st_raw"], registered=True, rebuildable="可重建",
    second_copy="有：G: 镜像", reconciled="—", defects="曾发生 null 写回事故（DG-31 反例）",
    in_progress=False)
add(id="supplement_intervals", cn="ST 人工补充区间", layer="L3", klass="base",
    path="data/local_store/meta/st/supplement/*.parquet",
    writer_primary="人工补充（一次性脚本）", inputs=[], registered=True,
    rebuildable="不可重建（人工证据）", second_copy="有：G: 镜像", reconciled="逐条留证",
    defects="人工资产，必须与 st_intervals 同批审计", in_progress=False)
add(id="universe_exclusions", cn="不可交易/异常剔除清单", layer="L3", klass="derived",
    path="data/local_store/meta/universe/universe_exclusions.parquet",
    writer_primary="src/data/quality/tradable_universe.py:978（常规）",
    inputs=["quotes_stock", "daily_index"], registered=True, rebuildable="可重建",
    second_copy="有：G: 镜像", reconciled="—", defects="未见", in_progress=False)
add(id="universe_price_jumps", cn="价格跳变异常清单", layer="L3", klass="derived",
    path="data/local_store/meta/universe/universe_price_jumps.parquet",
    writer_primary="质量检查脚本（常规）", inputs=["quotes_stock"], registered=True,
    rebuildable="可重建", second_copy="有：G: 镜像", reconciled="—", defects="未见",
    in_progress=False)
add(id="stale_status", cn="数据新鲜度状态", layer="L3", klass="derived",
    path="data/local_store/meta/universe/stale_status.parquet",
    writer_primary="src/data/quality/stale_status.py", inputs=["daily_index", "universe_exclusions"],
    registered=True, rebuildable="可重建", second_copy="有：G: 镜像", reconciled="—",
    defects="未见", in_progress=False)
add(id="state_vector_daily", cn="市场状态向量日表", layer="L3", klass="derived",
    path="data/local_store/meta/state/state_vector_daily.parquet",
    writer_primary="tools/build_state_vector.py（常规）+ 一次性补丁（_staging/state_margin_patch_20261112）",
    inputs=["valuation", "quotes_index", "stock_shares", "macro", "breadth_ma_daily", "quotes_stock"],
    registered=True, rebuildable="可重建", second_copy="有：G: 镜像",
    reconciled="tools/verify_state_vector.py 校验", defects="src/ 生产链路零消费者（见证据）",
    in_progress=False)
add(id="anomalies", cn="质量诊断三表（隔离行/占位码/覆盖缺口）", layer="L3", klass="derived",
    path="data/local_store/meta/anomalies/{quarantine_rows,placeholder_codes,coverage_gaps}.parquet",
    writer_primary="质量检查脚本", inputs=["quotes_stock"], registered=True,
    rebuildable="可重建", second_copy="有：G: 镜像", reconciled="—", defects="coverage_gaps 无 manifest",
    in_progress=False)

# ===== L4 universe 分层 =====
add(id="universe_layers", cn="股票池分层矩阵 L0–L5（六张布尔长表）", layer="L4", klass="derived",
    path="data/local_store/meta/universe/layers/l{0..5}_*.parquet",
    rows=18922964, files=6,
    writer_primary="tools/build_universe_layers.py（常规，--apply；另有大量一次性重建脚本）",
    inputs=["stock_basic", "listing_intervals", "suspension_daily", "implied_suspension",
            "st_intervals", "st_coverage", "name_history", "code_migration", "etf_equity_pool", "quotes_stock"],
    registered=True, rebuildable="可重建（全量重算约数十分钟）", second_copy="有：G: 镜像",
    reconciled="与 st 快照对照；与 universe_filter 实测如 outputs/universe_consistency.json",
    defects="零生产消费者；与 universe_filter 口径已分叉（流动性阈值 2e7 vs 1e7 等）",
    gaps=[], in_progress=False)

# ===== L5 研究/回测/建议 =====
add(id="analysis_monthly", cn="月度分析面板", layer="L5", klass="derived",
    path="data/local_store/meta/research_inputs/analysis_monthly.parquet",
    writer_primary="tools/research/build_analysis_monthly.py", inputs=["valuation", "financials3",
    "quotes_stock", "stock_adj_factor", "stock_turnover", "stock_shares"], registered=True,
    rebuildable="可重建", second_copy="有：G: 镜像", reconciled="研究侧对拍", defects="未见",
    in_progress=False)
add(id="new_factors", cn="扩展因子面板", layer="L5", klass="derived",
    path="data/local_store/meta/research_inputs/new_factors.parquet",
    writer_primary="tools/research/build_new_factors.py", inputs=["analysis_monthly", "financials3"],
    registered=True, rebuildable="可重建", second_copy="有：G: 镜像", reconciled="研究侧对拍",
    defects="下游 build_multifactor_signals 依赖", in_progress=False)

# ---------------------------------------------------------------- 边
# 写者判定收敛到 tools/lineage_writers.py：只有「落盘目标能解析成目录+文件名，且与表路径
# 逐段兼容」的调用才算写者。旧口径的表名子串匹配（quotes/*/sh → snapshot/shares/shard）、
# 就近提及（docstring、def 名）与未解析目标（<call:refresh>）三类假阳性一并移除。
_SITES = lw.scan(ROOT)
_RECORDS = [r for rows in edges["writers"].values() for r in rows]
_INVENTORY = [t["path"] for t in T]


def writers_for(tid: str, path: str) -> list[dict]:
    return [{"file": w["file"], "line": w["line"], "mode": w["mode"]}
            for w in lw.detect_writers(path, _SITES, _RECORDS, _INVENTORY)]


def rows_for(path: str) -> tuple[int | None, int | None]:
    if path.startswith("data/local_store/quotes/"):
        key = path.split("/")[3]
        f = fam.get(key)
        return (f["rows"], f["codes"]) if f else (None, None)
    rel = path.replace("data/local_store/", "")
    for r, v in rows_by_rel.items():
        if rel.startswith(r.split("{")[0].rstrip("/")) or r.startswith(rel.split("{")[0].rstrip("/")):
            return v["rows"], None
    return None, None


tables_out = []
for t in T:
    rows, files = rows_for(t["path"])
    tables_out.append({
        "id": t["id"], "name": t["cn"], "layer": t["layer"], "class": t["klass"],
        "path": t["path"], "rows": t.get("rows", rows), "files": t.get("files", files),
        "writer_primary": t.get("writer_primary"), "writer_note": t.get("writer_note"),
        "writers_detected": writers_for(t["id"], t["path"]),
        "derived_from": t.get("inputs", []),
        "registered": t.get("registered"),
        "rebuildable": t.get("rebuildable"), "second_copy": t.get("second_copy"),
        "reconciled": t.get("reconciled"), "known_defects": t.get("defects"),
        "gaps": t.get("gaps", []), "in_progress": t.get("in_progress", False),
    })

# 缺口登记号按 G-xxx 原文逐表落位（gaps.tsv 全文核对后确定）
GAPS = {
    "quotes_stock": ["G-061", "G-084", "G-087", "G-106", "G-104"],
    "quotes_index": ["G-001", "G-002", "G-003", "G-105", "G-106"],
    "quotes_fund": ["G-004", "G-013", "G-020", "G-021", "G-026", "G-062", "G-064", "G-106"],
    "quotes_futures": ["G-049", "G-050", "G-051", "G-058"],
    "st_raw": ["G-095"],
    "st_status": ["G-098"],
    "industry_snap": ["G-098"],
    "valuation": ["G-036", "G-037", "G-059", "G-108", "G-109", "G-110"],
    "financials3": ["G-060", "G-068", "G-089", "G-090"],
    "fund_basic": ["G-015", "G-016", "G-017", "G-025", "G-044"],
    "index_basic": ["G-046", "G-047", "G-048"],
    "events": ["G-069", "G-070", "G-071", "G-072", "G-073", "G-074", "G-075"],
    "stock_dividend": ["G-076"],
    "stock_shares": ["G-077"],
    "stock_turnover": ["G-077", "G-078", "G-079", "G-084"],
    "daily_index": ["G-045"],
    "membership_intervals": ["G-039"],
    "fund_corp_actions": ["G-018"],
    "fund_adj_factor": ["G-019"],
    "etf_fee": ["G-030", "G-031", "G-032", "G-033", "G-034"],
    "macro": ["G-038", "G-052", "G-053", "G-054", "G-055", "G-056", "G-057"],
    "st_intervals": ["G-095", "G-104"],
    "state_vector_daily": ["G-081", "G-082"],
    "anomalies": ["G-104"],
    "universe_layers": [],
    "analysis_monthly": ["G-096"],
    "new_factors": ["G-083", "G-085", "G-096"],
}
for t in tables_out:
    if t["id"] in GAPS:
        t["gaps"] = GAPS[t["id"]]

derivation = []
for t in tables_out:
    for up in t["derived_from"]:
        derivation.append({"upstream": up, "downstream": t["id"], "evidence": t["writer_primary"]})

reverse = [
    {"downstream": "stock_basic", "upstream": "breadth_daily",
     "evidence": "tools/build_stock_basic.py:205-210 以 breadth_daily 的日期作交易日历",
     "impact": "宽度表任一重算/延迟都会连带 stock_basic 与全部下游（listing_intervals 除外）"},
    {"downstream": "universe_layers", "upstream": "stock_basic",
     "evidence": "tools/build_universe_layers.py:82-95 UPSTREAM_FILES",
     "impact": "L4 全量重算依赖 L1 名录"},
]

zero_summary = {s["token"]: {"code_files": s["code_files"], "doc_files": s["doc_file_count"]}
                for s in zero["summary"]}

payload = {
    "generated_by": "2026-11-12/data-lineage/work/build_lineage.py",
    "store_root": "data/local_store",
    "layers": {
        "L0": "原始采集（外部源/原始行情直接落地）",
        "L1": "基础名录与日历",
        "L2": "价格派生与因子",
        "L3": "状态判定（ST/停牌/可交易/状态向量）",
        "L4": "universe 分层（L0–L5 布尔长表）",
        "L5": "研究面板/回测与建议输入",
    },
    "tables": tables_out,
    "derivation_edges": derivation,
    "reverse_dependencies": reverse,
    "zero_consumer_evidence": zero_summary,
    "universe_consistency": cons,
    "counts": {
        "tables": len(tables_out),
        "base": sum(1 for t in tables_out if t["class"] == "base"),
        "derived": sum(1 for t in tables_out if t["class"] == "derived"),
        "mixed": sum(1 for t in tables_out if t["class"] == "mixed"),
    },
}
(OUT_DIR / "lineage.json").write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")

# CSV：表清单
import csv

with (OUT_DIR / "table_inventory.csv").open("w", encoding="utf-8-sig", newline="") as f:
    w = csv.writer(f)
    w.writerow(["id", "语义", "层", "定性", "正式路径", "行数", "文件数", "写入口", "派生自",
                "已登记权威路径", "可低成本重建", "第二份同语义", "与权威源对拍", "已知缺陷",
                "关联G", "已在查"])
    for t in tables_out:
        w.writerow([t["id"], t["name"], t["layer"], t["class"], t["path"], t["rows"], t["files"],
                    t["writer_primary"], "、".join(t["derived_from"]) or "—",
                    "是" if t["registered"] else "否", t["rebuildable"], t["second_copy"],
                    t["reconciled"], t["known_defects"], "、".join(t["gaps"]) or "—",
                    "是" if t["in_progress"] else "否"])

# CSV：基础层校准清单
with (OUT_DIR / "base_layer_calibration.csv").open("w", encoding="utf-8-sig", newline="") as f:
    w = csv.writer(f)
    w.writerow(["语义", "正式路径", "写入口（常规/一次性）", "能否低成本重建", "有无第二份同语义",
                "已登记权威路径", "与权威源对拍", "当前已知缺陷", "关联G", "已在查"])
    for t in tables_out:
        if t["class"] not in ("base", "mixed"):
            continue
        w.writerow([t["name"], t["path"], t["writer_primary"], t["rebuildable"],
                    t["second_copy"], "是" if t["registered"] else "否", t["reconciled"],
                    t["known_defects"], "、".join(t["gaps"]) or "—",
                    "是" if t["in_progress"] else "否"])

print("tables", len(tables_out), payload["counts"])
print("derivation edges", len(derivation))
print("saved", OUT_DIR / "lineage.json", OUT_DIR / "table_inventory.csv", OUT_DIR / "base_layer_calibration.csv")
