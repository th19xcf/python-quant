"""manifest 语义键的单一文本源。

生产者脚本在写 manifest 前调用 :func:`apply_semantics`，把各键合并进即将落盘的 dict，
保证重跑重建 manifest 时语义不丢失。每个键的取值都是对 ``docs/数据权威路径清单.md``
登记行或代码写入点的引用；改动文本时须同步核对所引行号仍然成立。
"""

SEMANTICS: dict[str, dict[str, str]] = {
    "factors/manifest.json": {
        "authority": "各表在 docs/数据权威路径清单.md 各有唯一正式路径登记行：个股复权因子 L72、基金复权因子 L73、个股换手率 L74、基金分红事件 L75、基金份额折算与分拆 L76、个股分红 L37；本文件只是这些正式表的目录级记账，不替代其中任何一张。",
        "source": "由 src/data/factors_manifest.py 生成：MANIFEST_NAME 常量 L34、_SUB_MANIFESTS L54、写入点 L209；逐文件 rows / cols / codes / 日期区间与上游 quotes 及 meta/factors/stock_shares 的 mtime 全部读磁盘实测、不推算（模块 docstring L3-L9）。六张表的字段说明见同目录 meta.json 与 fund_meta.json。"
    },
    "meta/anomalies/manifest.json": {
        "authority": "清单 L68（隔离行清单是唯一运行期隔离清单，命中即从因子与回测样本剔除，禁止调用点临时写死名单）、L69（占位代码清单）；本目录 CSV 台账是这两份正式清单的明细，不自立权威。",
        "dataset": "本目录承载两类运行期异常台账：隔离行清单 quarantine_rows.parquet（一行 = 一个被隔离的 (标的, 交易日)，判据与来源逐行记录）与占位代码清单 placeholder_codes.parquet，以及 fund_fake_bar_bars.csv 等明细台账；逐文件 rows / sha256 见 files 段。",
        "source": "隔离行由 tools/repair_quote_anomalies.py 产出（写点 L266；判据 docstring L41-L50：源端 volume 与 amount 全为 null 的坏行移出正式行情）；占位代码清单的读侧常量在 src/data/quality/quarantine.py:29-30（QUARANTINE_FILE / PLACEHOLDER_FILE）；本 manifest 无代码写点，人工维护。",
        "semantics": "隔离行只从因子与回测样本中剔除，不修改行情原值（清单 L68 原文）；禁止在调用点临时写死代码名单。CSV 明细与 parquet 清单是同一判据的两种视图，明细不得反过来当数据源。"
    },
    "meta/breadth/manifest.json": {
        "authority": "清单 L28（市场宽度由本地 quotes/stock 逐日计算，MySQL market_breadth 只是旁路导出）、L29（breadth_ma_daily 与 breadth_daily 语义不同，不得互相替代）。",
        "dataset": "两张语义不同的日频表：breadth_daily（涨跌家数、涨停跌停家数、新高新低）与 breadth_ma_daily（站上 MA20 / MA60 / MA120 的标的占比）；逐文件 rows / cols / sha256 见 files。",
        "source": "breadth_daily 由 src/data/services/breadth_service.py 的 refresh 写出（写点 L355；口径 docstring L5-L12：成交额 > 0 且 pre_close 非空、涨跌停按 round_half_up(pre_close × (1 ± limit_pct), 2)、新股首日不计入）；breadth_ma_daily 由 tools/build_breadth_ma.py 产出（manifest_path L120-121，均线口径见 docstring L19 起）。"
    },
    "meta/financials/manifest.json": {
        "authority": "清单 L38（财报到 PIT 是唯一带 ann_date 可见性 API 的事实表，meta/financials_pit/ 是原始采集副本、无消费者）、L27（财报公告日映射单表是唯一公告日映射载体）、L26（valuation_daily 为日频估值唯一落盘表）。",
        "source": "三张事实表 income / balance / cashflow 由 src/data/financials/store.py 承担 PIT 可见性（latest_announced_as_of 与 latest_announced_panel L217-260，读路径 L117-125）；2010-03-31 起走东财批量接口、1990–2009 由新浪 CompanyFinanceService.getFinanceReport2022 补齐，source 列逐行标记（清单 L38）。公告日映射单表由 collector.py:281 fetch_ann_date_map 与 store.py:185-232 维护（清单 L27）；本 manifest 无代码写点，人工维护。"
    },
    "meta/fund_basic.manifest.json": {
        "authority": "清单 L22：唯一本地正式表；MySQL fund_basic 只作合并式补空来源，禁止整表覆盖。",
        "dataset": "一只基金 / ETF 一行：代码、名称、管理人、基金类型、状态等基础字段；manifest 记录行数、列数、SHA256 与合并式导出的补空与冲突统计，逐文件明细见 files。",
        "source": "由 src/data/services/local_first_basic.py 的 export_fund_basic 导出：FUND_MANIFEST 常量 L34、FUND_PATH / FUND_MANIFEST 绑定 L84、合并式写入与差异登记在导出段 L300-360；模块 docstring L3-L11 规定只补本地空值、冲突保留本地并登记差异到 _staging/local_first_<日期>/。",
        "semantics": "一行 = 一只基金 / ETF；本表是合并式导出的结果而不是库的快照，因此同一列可同时含库值与本地取证值——冲突时保留本地并留差异登记，禁止整表覆盖（清单 L22 原文）。"
    },
    "meta/fund_corporate_actions_2019plus.manifest.json": {
        "authority": "清单 L33：判据为本地复权因子实际调整日；旧 tushare pre_close 口径已废弃（2020-02-26 凭空多 43 条）；用途边界为事件查询与留证表，不是除权完备性清单。",
        "dataset": "场内基金除权事件表（164 只 / 299 行 / 2019-09-18~2025-03-19）：一行 = 一次复权因子实际调整事件，列出 ts_code、date、basis 与前后因子；逐文件 rows / sha256 见 files。",
        "source": "由 tools/build_fund_corporate_actions.py 产出：MANIFEST_NAME L83、manifest_path 与写入 L265-L302；判据为本地 fund_adj_factor.hfq_factor 的相对跳变 |rel| ≥ 1e-4（docstring L5-L12），不是 tushare pre_close；重跑幂等 299 行（L25 的 --no-write 复现口径）。"
    },
    "meta/index_basic.manifest.json": {
        "authority": "清单 L23：唯一本地正式表（1,335 行）；base_point 全空属已知事实，不得填 0 或推算；若库中将来出现该列值，导出报错要求显式决策列类型，不自动改写。",
        "dataset": "一只指数一行（1,335 行）：代码、名称、交易所、发布日、base_point 等；逐文件 rows / cols / sha256 见 files。",
        "source": "由 src/data/services/local_first_basic.py 的 export_index_basic 导出：INDEX_MANIFEST 常量 L36、INDEX_PATH / INDEX_MANIFEST 绑定 L89、合并式写入与差异登记同导出段；--update-index 落库后自动调用（清单 L23）。",
        "semantics": "一行 = 一只指数；合并式导出只补本地空值，base_point 全空是库侧同样为空的已知事实，不得填 0 或按其它指数推算（清单 L23 原文）；库中若出现该列值，导出会报错要求显式决策列类型。"
    },
    "meta/index_members/manifest.json": {
        "authority": "清单 L55（指数成分月度权重面板是成分进出差分的唯一上游来源，成分区间只能由该面板推出）、L24（成分区间表为正式表，out_date 有真值、带 in_date_known 左删失标记；旧 index_membership 表已废弃）。"
    },
    "meta/industry_pit/manifest.json": {
        "semantics": "本目录三张表口径不同、互不替代：industry_pit_sw.parquet 是申万 PIT 正式表（清单 L57，按 in_date / out_date 组织，申万与通达信行业不得混用）；industry_pit.parquet 是通达信近 6 日快照视图（清单 L58，非正式 PIT，禁止 as-of 早于 2026-11-03 回溯）；industry_change_pit.parquet 是变更事件与增量断点表（清单 L59，是事件流不是分类事实表）。三表均由 src/data/industry_pit.py 写出（manifest_path L88、produced_by L456）。"
    },
    "meta/macro/cn_bond_index.manifest.json": {
        "authority": "清单 L47：唯一债券原生指数表（3 指数 × 6,215 日）；一律取财富指数「总值」段，净价与全价不得替代。"
    },
    "meta/macro/cn_credit_yield_curve.manifest.json": {
        "authority": "清单 L49：唯一信用曲线长表（7 个期限）；AA 与 AA+ 未采，不得用其它期限或评级代填。"
    },
    "meta/macro/cn_treasury_yield_curve.manifest.json": {
        "authority": "清单 L48：唯一多期限曲线长表；与单期 10Y 表语义不同、互不替代（10Y 重叠 5,175 日最大差 0.000000pp）；2Y/4Y/15Y/20Y/50Y 不得插值。"
    },
    "meta/macro/manifest.json": {
        "authority": "本目录各表在清单分别登记：10 年期国债收益率 L31、沪深两融日频汇总 L32、债券指数 L47、国债曲线 L48、信用曲线 L49；本文件是目录级汇总说明，不替代其中任何一张。",
        "dataset": "宏观目录聚合，逐表语义在 files 段区分：10 年期国债到期收益率（清单 L31）、沪深两融日频汇总（L32）、债券指数（L47）、国债收益率曲线（L48）、信用债收益率曲线（L49）。",
        "source": "cn_treasury_yield_10y 走 tools/build_state_vector.py 的上游采集链，数据源分段为 chinabond:bond_china_yield 与 eastmoney:bond_zh_us_rate（清单 L31）；cn_market_margin_daily 由 stock_margin_sse / macro_china_market_margin_sz / stock_margin_szse 回填并以 source 列逐行标记（清单 L32）；债券指数与两条曲线由 tools/build_cn_bond_tables.py 产出（写点 L145-146、两份 manifest L182-185），采集入口 tools/fetch_cn_bond_data.py（INDEX_CATEGORIES L43、YIELD_YEARS L46、THROTTLE L48）。本文件无代码写点，人工维护。"
    },
    "meta/multi_asset/etf_fee.manifest.json": {
        "authority": "清单 L36：唯一逐只 ETF 费率表；core 153 只三方核对一致、502 只一致加 16 只单源；销售服务费仅 159003.SZ 有值，不填 0。",
        "dataset": "逐只 ETF 费率表（518 行 = etf_pool in_scope 全量）：管理费、托管费、销售服务费与历史分段；可读副本 etf_fee.csv、缺口清单 etf_fee_gaps.csv、统计 etf_fee_summary.json 同目录。",
        "semantics": "一行 = 一只 ETF 的当前费率加历史分段（segments）；销售服务费缺失记 null，不填 0（docstring L10）；费率按逐版产品资料概要解析，parse_tag 记解析口径版本。由 tools/fetch_etf_fee.py 产出（staging manifest 写点 L583-584）。"
    },
    "meta/multi_asset/futures_manifest.json": {
        "authority": "清单 L43：唯一分合约日线层（997 文件 / 153,176 行）；字段为交易所口径含 settle 与 oi，新浪主力连续不得替代。",
        "semantics": "本 manifest 描述单合约层 quotes/futures/{cffex,shfe,ine}/<ts_code>.parquet：一行 = 一个合约的一个交易日，字段为交易所口径（含 settle 与 oi）；与 futures_continuous.parquet（清单 L44 连续口径）语义不同——前者是原始事实、后者是按换月规则派生的连续序列，不得互换。由 tools/build_futures_continuous.py 写出（说明 L24、写点 L352）。"
    },
    "meta/multi_asset/manifest.json": {
        "authority": "清单 L34：多资产 ETF 候选池为唯一正式表（795 行 = 518 in_scope + 277 out_of_scope，不含外部补数）；同目录另有期货主力连续 L44、换月事件 L45、合约清单 L46、ETF 费率 L36、单合约日线 L43，各自独立登记。",
        "dataset": "多资产 ETF 候选池：etf_pool.parquet（795 行 = 518 in_scope + 277 out_of_scope）、可读副本 etf_gap_detail.csv 与 etf_event_gaps.csv、统计 summary.json；逐文件 rows / sha256 见 files。",
        "source": "由 tools/build_multi_asset_etf_pool.py 产出（产物清单 docstring L5-L11、manifest dict L570、写入 L603）；输入为 fund_basic、quotes/fund、fund_adj_factor 与交易日历，不含外部补数（清单 L34）。"
    },
    "meta/pools/manifest.json": {
        "authority": "清单 L35：唯一 ETF 分层清单（tier=full 1,338 只 / std3y 677 只 / pool 138 只，共 2,153 行）；2026-11-12 由 etf_equity_pool_20261104.csv 正名，as_of 2026-11-04、built_at 2026-11-12。",
        "source": "由 tools/build_etf_equity_pool.py 重建（docstring L3-L35；输出文件名固定不带版本号，符合 DG-4；--as-of、口径、行数与 SHA256 记入本 manifest，写点 L307-L323）；输入为 meta/names.json、quotes/fund、fund_share_change、fund_dividend 与 meta/anomalies/fund_fake_bar_bars.csv（L23、L30）。"
    },
    "meta/st/raw/reviewed_conflicts.manifest.json": {
        "authority": "清单 L66：唯一不可再生人工裁定输入，运行期直接读取并合并进 st_intervals 与 st_coverage；丢失即口径丢失，故不进 _staging/、不得重建。",
        "dataset": "三张人工裁定输入：reviewed_conflicts.parquet（冲突取舍）、reviewed_coverage.parquet（覆盖窗口逐只结论）、reviewed_pending.parquet（证据不足结论）；逐文件 rows / sha256 见 files。",
        "source": "人工录入，无采集脚本；运行期由 src/data/st_history.py 读取：reviewed_conflicts_path L274-276、reviewed_pending_path L279-281、reviewed_coverage_path L284-286，合并点为 apply_reviewed_conflicts L953（调用 L1269）与 apply_reviewed_coverage L1077（调用 L1279）。",
        "semantics": "一行 = 一次人工裁定；reviewed_coverage 优先级最高，套用在快照合并与延伸之上（st_history.py:102）。三张表是固定输入而不是抓取产物：重跑 ST 构建不会重新生成它们，因此不得删除、不得迁进 _staging/（清单 L66）。"
    },
    "meta/st/supplement/manifest.json": {
        "authority": "清单 L65：唯一正式区间之外的人工补充层，合并顺序为补充层覆盖正式层；替换时整目录按 DG-11 移入 _backup/。",
        "dataset": "补充区间表 supplement_intervals.parquet 与补充覆盖表 supplement_coverage.parquet（2 个区间 / 2,336 行覆盖）；逐文件 rows / sha256 见 files。",
        "source": "由 src/data/st_name_history.py 写出：manifest 路径常量 L27、supplement_intervals_path L113-114、写点 L610、_parquet_entry 记账 L628；读侧合并顺序在 src/data/st_history.py:912（读 supplement_intervals.parquet）。",
        "semantics": "一行 = 一个补充 ST 区间或一条补充覆盖结论；合并优先级为「补充层覆盖正式层」，用于承载正式层未覆盖但已取证的段（清单 L65）；替换时整目录按 DG-11 移入 _backup/。"
    },
    "meta/state/manifest.json": {
        "authority": "清单 L42：六维状态向量唯一日频特征表；估值位因无独立全 A PE 表故降级为 valuation_daily 反推；分位窗口 1,250 日满窗才给值。",
        "dataset": "state_vector_daily.parquet（8,825 行 / 25 列 / 1990-12-19~2026-11-09）：六维市场状态向量，一行 = 一个交易日。",
        "source": "由 tools/build_state_vector.py 产出（manifest_path L176-177；六维口径 docstring L7-L12；缩尾与降级口径写在 dimensions 段）；校验脚本 tools/verify_state_vector.py:41 读同一 manifest 做一致性核对。"
    },
    "meta/stock_basic.manifest.json": {
        "authority": "清单 L15：唯一交易所口径法定上市日；产出方 tools/build_stock_basic.py；重建与现有表合并式兜底，不整表覆盖。",
        "dataset": "stock_basic.parquet：一只证券一行，含 list_date / delist_date / name / exchange 等字段，保留退市股；逐文件 rows / cols / sha256 见 files。",
        "source": "由 tools/build_stock_basic.py 重建（docstring L3-L12 是逐列推导依据表；产出路径 L50；_manifest L735、_promote L787-802 写 promoted_at 与回退信息）；只用本地已验证资产推导（行情首末行、名称快照、公告与退市证据、通达信 .day），不以 MySQL 为准（DG-16 / DG-17）。",
        "semantics": "一行 = 一只证券的上市 / 退市事实；list_date 是交易所口径法定上市日，与 listing_intervals 的行情实测首行不等价（清单 L79）；重建与现有表合并式兜底：候选空值保留现有值、现有独有代码整行保留，不整表覆盖（清单 L15）。"
    },
    "meta/universe/layers/manifest.json": {
        "authority": "清单 L16-L21 逐层登记 L0~L5（L0 保留退市股、L1 上市满 120 交易日、L2 停牌取两表并集、L3 只认 st_intervals 区间版、L4 前 20 日成交额均值 ≥ 2,000 万、L5 取 universe_filter 默认准入）；六层逐级收窄，不得跨层替代。",
        "dataset": "L0~L5 六层布尔长表（l0_all_history ~ l5_pool）、counts_by_year.csv 与 st_snapshot_vs_interval.json；一行 = 一个 (date, ts_code) 在某层的成员布尔值（清单 L16-L21）。",
        "source": "由 tools/build_universe_layers.py 产出（层定义 docstring L7-L16、_manifest L381、写入 L601-603）；L5 准入规则取 src/data/universe_filter.py 的 UniverseFilterConfig 默认值（docstring L12、L16），阈值与窗口写进 parameters。",
        "semantics": "六层是同一域逐级收窄而不是并列口径：L1 剔除次新（120 交易日）、L2 要求当日有成交且不在停牌并集、L3 只认 st_intervals 区间版并剔退市整理期、L4 前 20 日成交额均值 ≥ 2,000 万、L5 套板块准入；跨层替代会改变样本域（清单 L16-L21）。"
    },
    "quotes/index/sh/000922.SH.manifest.json": {
        "authority": "清单 L41：唯一 000922.SH 日线；baostock 主源，新浪至 2019-01-30 重叠 2,603 日、腾讯 4,439 日收盘价完全相等 0.0000；2005-01-04~2008-05-23 仅新浪单源，按两源核对要求未纳入。",
        "dataset": "000922.SH 日线（4,485 行 / 2008-05-26~2026-11-09）：一行 = 一个交易日的 OHLC、成交量、成交额与复权因子。",
        "source": "清单 L41 原文：baostock 主源，新浪至 2019-01-30 重叠 2,603 日、腾讯 4,439 日收盘价完全相等（最大差 0.0000）；2005-01-04~2008-05-23 仅新浪单源，按两源核对要求未纳入。",
        "semantics": "一行 = 一个交易日；起点 2008-05-26 是「两源核对通过」的边界，不是数据缺失——更早区间新浪有数据但无第二源可证（清单 L41）。"
    },
    "quotes/index/sh/000985.SH.manifest.json": {
        "authority": "清单 L40：唯一 000985.SH 日线（宽基基准候选）；价格东财主源 + 新浪腾讯交叉、3 个争议日取腾讯；成交额 27 个高换手日东财被截断为 1e12，改用中证指数公司官方日频 ×1e8 补齐并逐日记入 amount_fix。",
        "dataset": "000985.SH 日线（3,707 行 / 2011-08-02~2026-11-09）：一行 = 一个交易日的 OHLC、成交量、成交额与复权因子。",
        "source": "清单 L40 原文：价格东财主源 + 新浪腾讯交叉核对、3 个争议日取腾讯；成交额在 27 个高换手日被东财截断为 1e12，改用中证指数公司官方日频 ×1e8 补齐并逐日记入 amount_fix；2011-08-02 前为发布前回溯段，四个接口均无数据。写入链为 src/main.py 的指数日线补库标的池（清单 L23）。",
        "semantics": "一行 = 一个交易日；amount_fix 逐日记成交额的替换来源，不代表当日另有一套价格；价格争议日已按两源一致换成腾讯，未在价格上留混源标记（清单 L40）。"
    },
    "quotes/manifest.json": {
        "authority": "清单 L52（股票日线唯一层）、L53（场内基金日线）、L54（指数日线 1,335 文件与 index_basic 1,335 行逐一对应）；本文件只描述覆盖，不是数据本身。",
        "dataset": "quotes 目录覆盖声明：股票 / 场内基金 / 指数三类日线的文件数、行数与日期区间；声明 10,784 文件 / 25,912,277 行（清单 L52），逐目录明细见 files。",
        "source": "股票日线由 src/data/local_store/build.py 构建（清单 L52 称其为股票日线唯一构建入口）；场内基金与指数分别见清单 L53 / L54。本文件无代码写点，人工维护；实测 25,912,281 行与声明差 4 行，属陈旧登记（清单 L52 原文）。"
    }
}


def apply_semantics(manifest: dict, rel_path: str) -> dict:
    """把 ``rel_path`` 对应的语义键合并进 ``manifest``：已有非空值不覆盖，返回同一 dict。"""
    for key, text in SEMANTICS.get(rel_path, {}).items():
        if not manifest.get(key):
            manifest[key] = text
    return manifest
