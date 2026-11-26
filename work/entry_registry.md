# tools/ 未登记写数据脚本处置表（37 个）

判定：A=登记进清单（按需写入）；B=只读/分析（判定误报）；C=一次性/历史回补（归档 `tools/_oneshot/`）。
登记清单落在 [dataset_contracts.py](../src/data/dataset_contracts.py) 的 `WRITER_REGISTRY`（step/depends/idem/trigger 四字段）与 `ARCHIVED_WRITERS`。

| # | 脚本 | 判定 | 依据 | 去向 |
|---|------|------|------|------|
| 1 | advice_stateful_backtest.py | B | 仅 `LocalQuoteStore(enabled=True)` 读行情做回测，无任何写调用 | 修正判定规则，脚本留在 tools/ |
| 2 | backfill_delisted_quotes.py | A | 退市/停更标的尾部行情补采，新退市持续出现 | 登记（step=fill，按需） |
| 3 | backfill_events.py | A | 公告事件增量补数，日更漏采时续跑 | 登记（depends=update_events，按需） |
| 4 | backfill_financials_history_sina.py | C | 只补 `end_date < 2010-01-01` 历史三大报表 | tools/_oneshot/ |
| 5 | backfill_fund_dividend.py | C | DG-35 基金分红逐只取证一次改完 | tools/_oneshot/ |
| 6 | backfill_reference_history.py | C | P1 参考数据历史补采（可续跑） | tools/_oneshot/ |
| 7 | backfill_share_div.py | A | 送转字段缺失扫描与回补，新事件仍会产生缺失 | 登记（depends=collect_stock_dividend，按需） |
| 8 | backfill_stock_dividend.py | C | DG-35 个股分红逐只取证一次改完 | tools/_oneshot/ |
| 9 | build_breadth_ma.py | A | 宽度 MA 派生表，可按交易日重建 | 登记（depends=build，按需） |
| 10 | build_cn_bond_tables.py | A | 中债指数/收益率曲线正式表整理入口 | 登记（depends=macro，按需） |
| 11 | build_credit_yield_curve.py | A | 信用收益率曲线正式表入口 | 登记（depends=macro，按需） |
| 12 | build_fund_corporate_actions.py | A | 由复权因子跳变重出基金公司行为表 | 登记（depends=factor，按需） |
| 13 | build_implied_suspension.py | A | 停牌推断表正式重建入口 | 登记（depends=build，按需） |
| 14 | build_new_share.py | A | 新股发行价随 IPO 持续新增 | 登记（depends=basic，按需） |
| 15 | build_no_dividend_ledger.py | A | 无分红判定清单，随分红表更新重出 | 登记（depends=dividend，按需） |
| 16 | build_session_ledger.py | A | 交易日 session 台账正式重建 | 登记（depends=build，按需） |
| 17 | build_st_history_from_tushare.py | A | ST 名称变更区间（非权威）按需重建 | 登记（step=st，按需） |
| 18 | build_state_vector.py | A | 市场状态向量日频表，可按交易日重写分区 | 登记（depends=build，按需） |
| 19 | build_trading_calendar.py | A | 官方未来交易日历年度/假日更新 | 登记（depends=build，按需） |
| 20 | build_universe_layers.py | A | L0–L5 分层矩阵按交易日重写 | 登记（depends=build，按需） |
| 21 | collect_stock_dividend.py | A | 分红送转事件采集，供分红步骤消费 | 登记（step=dividend，按需） |
| 22 | derive_free_float_fill.py | A | 自由流通派生列重算 | 登记（depends=basic，按需） |
| 23 | derive_total_dividend.py | A | 分红派现总额派生列重算 | 登记（depends=dividend，按需） |
| 24 | fetch_etf_fee.py | A | ETF 费率分段随产品更新 | 登记（depends=pool，按需） |
| 25 | fetch_margin_detail.py | A | 两融明细每日新增（走 write_dataset） | 登记（depends=build，按需） |
| 26 | fill_free_float_shares.py | A | 自由流通股本常规产出入口 | 登记（depends=basic，按需） |
| 27 | fill_internal_gaps.py | A | 本地行情内部缺口补采 | 登记（step=fill，按需） |
| 28 | fix_name_history_end_dates.py | C | name_history 缺收尾段一次性修补 | tools/_oneshot/ |
| 29 | fund_split_repair.py | A | 基金份额折算因子修复，可复用 | 登记（depends=factor，按需） |
| 30 | infer_post_g_segments.py | C | G 股改残留名称段一次性推断 | tools/_oneshot/ |
| 31 | margin_daily.py | A | 两融日频正式采集/构建/发布入口 | 登记（depends=build，按需） |
| 32 | mark_fund_tradability.py | A | 基金可交易性标记重打 | 登记（depends=basic，按需） |
| 33 | migrate_snapshot_history.py | C | G-098 快照序列合并迁移 | tools/_oneshot/ |
| 34 | repair_quote_anomalies.py | A | 按契约巡检就地修复，可复用（默认 dry-run） | 登记（step=build，按需） |
| 35 | st_gapfill.py | C | 2026-11-21 ST 区间表缺口一次性补齐 | tools/_oneshot/ |
| 36 | treasury_yield.py | A | 10Y 国债收益率正式入口 | 登记（depends=macro，按需） |
| 37 | update_events.py | A | 公告事件日更正式入口 | 登记（按需） |

统计：A=28，B=1，C=8。

补充：判定规则收窄为“只认 parquet 数据集落盘”后，新核对的 5 个文本写入脚本（bench_perf_baseline、daily_advice、patrol_local 等）均只写报告/巡检产物，不计入写数据；其中 build_etf_equity_pool、reconcile_p2_metadata 已补登记进 `WRITER_REGISTRY`。
