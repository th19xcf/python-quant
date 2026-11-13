# writers_detected 口径修复：边集合差（2026-11-13）

- 修复前：`work/lineage_before_fix.json`（旧口径 writers_for，194 条边；由修复前的 outputs/lineage.json 冻结）
- 修复后：`work/rerun_20261113c/lineage.json`（tools/lineage_writers.py，157 条边）
- 增加 **85** 条，减少 **122** 条，涉及 48 张表
- 本文件只是差异清单；`governance_order.md` 与 `base_layer_calibration.csv` 未改动，是否重出施工单由主控决定。

## analysis_monthly

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `tools/research/build_analysis_monthly.py:294` | `df.select(out_cols).write_parquet(out_path)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `_staging/delisted_label_fix_20261111/rebuild_candidate.py:243` | `write_parquet_atomic(cand, CANDIDATE)` |
| 减 | `tools/research/build_analysis_monthly.py:295` | `print(f'analysis_monthly: {df.height:,} 行 × {len(out_cols)} 列 '` |

## ann_date_map

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/financials/store.py:500` | `write_parquet_atomic(plan, dest, compression='zstd', statistics=True)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `_staging/ann_date_map_20261112/build_and_verify.py:107` | `write_parquet_atomic(merged, CAND, compression='zstd', statistics=True)` |
| 减 | `_staging/ann_date_map_20261112/finish_migration.py:48` | `write_parquet_atomic(cand, formal, compression='zstd', statistics=True)` |
| 减 | `_staging/ann_date_map_20261112/migrate.py:51` | `write_parquet_atomic(cand, FORMAL, compression='zstd', statistics=True)` |
| 减 | `src/data/financials/store.py:209` | `def upsert_ann_date_map(frame: pl.DataFrame,` |

## anomalies

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `tools/repair_quote_anomalies.py:252` | `write_parquet_atomic(frame, path, compression='zstd', statistics=True)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `tools/reconcile_p2_metadata.py:182` | `# anomalies_placeholder_codes_20261112/，DG-13），本脚本不再写盘；` |

## breadth_daily

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/services/breadth_service.py:118` | `write_parquet_atomic(frame, path)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `_staging/derived_1993b_20261113/layer3_prepare.py:64` | `frame.write_parquet(cand)` |

## breadth_ma_daily

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `tools/build_breadth_ma.py:584` | `write_parquet_atomic(daily, target)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `tools/build_breadth_ma.py:420` | `out = stage / 'breadth_ma_daily.parquet'` |

## code_migration

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/quality/code_migration.py:770` | `write_parquet_atomic(frame, path)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/quality/clean_set.py:74` | `from src.data.quality.code_migration import load_entity_index` |
| 减 | `src/data/quality/code_migration.py:49` | `MAPPING_REL = Path('meta') / 'code_migration.parquet'` |
| 减 | `src/data/services/breadth_service.py:39` | `from src.data.quality.code_migration import load_entity_index` |
| 减 | `tools/build_universe_layers.py:48` | `from src.data.atomic_write import write_bytes_atomic, write_parquet_atomic  # noqa: E402` |

## daily_index

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/local_store/store.py:551` | `write_parquet_atomic(frame, self.meta_path, compression='zstd',` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/local_store/store.py:556` | `"""为 ``meta/daily_index.parquet`` 落 manifest：行数 / 字节 / sha256 / 列。` |
| 减 | `src/data/quality/listing_intervals.py:42` | `"""由 daily_index + stale_status 生成在市区间表。"""` |

## etf_equity_pool

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `tools/build_etf_equity_pool.py:241` | `write_text_atomic(buffer.getvalue(), target)` |

## etf_fee

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `tools/fetch_etf_fee.py:614` | `write_parquet_atomic(pl.read_parquet(src), TARGET / name)` |

## etf_pool

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `tools/build_multi_asset_etf_pool.py:532` | `pool_path = STAGING / 'etf_pool.parquet'` |
| 减 | `tools/fetch_etf_fee.py:593` | `目标目录 `meta/multi_asset/` 里已有 etf_pool.* 等他人产物，本次只新增` |

## events

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/events_truncation_20261111/recent_days.py:149` | `write_parquet_atomic(after, FORMAL, compression='zstd')` |
| 增 | `src/data/events.py:687` | `write_parquet_atomic(merged, path_out)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `_staging/adj_factor_crosscheck_20261111/tdx_layer1.py:176` | `tdx_ev_all.write_parquet(OUT / 'tdx_events.parquet')` |
| 减 | `src/data/events.py:609` | `raise ValueError(f'事件文件没有可用记录: {file_path}')` |
| 减 | `tools/anomalies/build_no_ex_rights_events.py:211` | `return 1` |
| 减 | `tools/build_fund_corporate_actions.py:261` | `raise RuntimeError('备份副本行数与正式文件不一致，中止替换')` |
| 减 | `tools/build_futures_continuous.py:307` | `write_parquet_atomic(rolls, OUT_DIR / 'futures_roll_events.parquet')` |

## financials3

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/financials/store.py:500` | `write_parquet_atomic(plan, dest, compression='zstd', statistics=True)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `_staging/ann_date_map_20261112/build_and_verify.py:107` | `write_parquet_atomic(merged, CAND, compression='zstd', statistics=True)` |
| 减 | `_staging/ann_date_map_20261112/finish_migration.py:48` | `write_parquet_atomic(cand, formal, compression='zstd', statistics=True)` |
| 减 | `_staging/ann_date_map_20261112/migrate.py:51` | `write_parquet_atomic(cand, FORMAL, compression='zstd', statistics=True)` |
| 减 | `_staging/changelog_fix_20261112/perf.py:29` | `write_parquet_atomic(frame, DST)` |
| 减 | `_staging/derived_1993b_20261113/stepB_delete_and_verify.py:62` | `write_parquet_atomic(` |
| 减 | `_staging/derived_catchup_20261112/fix_valuation_append_only.py:75` | `write_parquet_atomic(final, CUR)` |
| 减 | `_staging/valuation_history_20261111/merge_build.py:118` | `out.write_parquet(CAND)` |

## fund_adj_factor

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/regression_evidence/old_fund_factors.py:469` | `write_parquet_atomic(` |
| 增 | `_staging/regression_evidence/repro_red_state.py:53` | `pl.DataFrame({` |
| 增 | `src/data/local_store/fund_factors.py:476` | `write_parquet_atomic(` |
| 增 | `tools/fund_split_repair.py:358` | `write_parquet_atomic(` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `_staging/quote_fix_20261111/build_candidate.py:181` | `final.write_parquet(STG / "fund_adj_factor_candidate.parquet",` |
| 减 | `src/data/local_store/fund_factors.py:469` | `f'已用 {time.perf_counter() - started:.1f}s')` |
| 减 | `tools/fund_split_repair.py:357` | `['ts_code', 'trade_date'])` |

## fund_basic

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `_staging/gap_fix2_20261110/build_reverse_check_csv.py:49` | `out.write_csv(dst)` |

## fund_corp_actions

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `tools/build_fund_corporate_actions.py:82` | `REL_PATH = Path('meta') / 'fund_corporate_actions_2019plus.parquet'` |

## fund_dividend

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/regression_evidence/old_fund_factors.py:586` | `write_parquet_atomic(` |
| 增 | `src/data/local_store/fund_factors.py:596` | `write_parquet_atomic(` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/local_store/fund_factors.py:69` | `` |

## fund_share_change

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/regression_evidence/old_fund_factors.py:586` | `write_parquet_atomic(` |
| 增 | `src/data/local_store/fund_factors.py:596` | `write_parquet_atomic(` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `_staging/quote_fix_20261111/build_candidate.py:184` | `cand_sc.write_parquet(STG / "fund_share_change_candidate.parquet", compression="zstd")` |
| 减 | `src/data/local_store/fund_factors.py:70` | `FUND_DIVIDEND_FILENAME = 'fund_dividend.parquet'` |

## futures_multi_asset

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `tools/build_futures_continuous.py:29` | `.venv\\Scripts\\python.exe tools/build_futures_continuous.py --apply    # 构建并落正式路径` |
| 减 | `tools/build_multi_asset_etf_pool.py:532` | `pool_path = STAGING / 'etf_pool.parquet'` |
| 减 | `tools/build_multi_asset_etf_pool.py:560` | `write_parquet_atomic(frame, TARGET_DIR / 'etf_pool.parquet')` |
| 减 | `tools/fetch_etf_fee.py:593` | `目标目录 `meta/multi_asset/` 里已有 etf_pool.* 等他人产物，本次只新增` |

## index_weight

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/quality/extended_builders.py:26` | `口径：对 ``meta/index_members/index_weight_monthly.parquet``（tushare 月末` |
| 减 | `tools/pull_index_weight.py:244` | `out_path = OUT_DIR / "index_weight_monthly.parquet"` |

## industry_pit

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/industry_pit.py:101` | `write_parquet_atomic(frame, path)` |
| 增 | `src/data/quality/extended_builders.py:121` | `write_parquet_atomic(frame, path)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/industry_pit.py:96` | `"""原始响应留档目录（采集中间态）：按 DG-32 落仓库根 ``_staging/industry_pit_20261112/raw``。"""` |
| 减 | `src/data/quality/extended_builders.py:356` | `# 契约已改挂申万区间表 `industry_pit_sw.parquet`，产物路径不能再从` |

## industry_snap

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/industry.py:146` | `write_parquet_atomic(frame, path)` |

## listing_intervals

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/quality/listing_intervals.py:38` | `OUTPUT_RELATIVE = Path('meta/universe/listing_intervals.parquet')` |

## macro

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/gap_close_20261111/build_credit_curve.py:95` | `write_parquet_atomic(credit, out)` |
| 增 | `tools/build_credit_yield_curve.py:270` | `write_parquet_atomic(credit, out)` |

## membership_intervals

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/quality/extended_builders.py:25` | `- **指数成分进出区间表** ``meta/index_members/membership_intervals.parquet``` |

## name_history

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `tools/build_name_history.py:214` | `write_parquet_atomic(frame, target)` |
| 增 | `tools/fix_name_history_end_dates.py:455` | `write_parquet_atomic(patched, target)` |
| 增 | `tools/infer_post_g_segments.py:251` | `write_parquet_atomic(patched, target)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/st_name_history.py:61` | `from datetime import date, datetime, timedelta` |
| 减 | `tools/build_name_history.py:13` | ```name_history`` 只回答「某只票在某个历史时点叫什么」，是历史简称的唯一权威；` |
| 减 | `tools/fix_name_history_end_dates.py:72` | `TARGET = ROOT / 'data' / 'local_store' / 'meta' / 'name_history.parquet'` |
| 减 | `tools/infer_post_g_segments.py:63` | `TARGET = ROOT / 'data' / 'local_store' / 'meta' / 'name_history.parquet'` |
| 减 | `tools/research/eval_factor_candidates.py:452` | `return pl.read_parquet(ROOT / 'data/local_store/meta/name_history.parquet') \` |

## names_json

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `tools/build_names_json.py:176` | `tmp.replace(path)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `tools/build_name_history.py:14` | `**当前简称仍以 ``meta/names.json`` 为准**（通达信当日快照，覆盖全域名），` |

## new_factors

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `tools/research/build_new_factors.py:408` | `out.write_parquet(out_path)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `_staging/new_factors_audit_20261112/diff_panel.py:72` | `joined.select(['ts_code', 'date', c, f'{c}_new']).filter(diff_mask) \` |
| 减 | `_staging/new_factors_audit_20261112/fin_attribution.py:23` | `fin_old.write_parquet(STAGING / 'fin_old_upstream.parquet')` |
| 减 | `_staging/new_factors_audit_20261112/rebuild_old_upstream.py:50` | `out.write_parquet(STAGING / 'new_factors_old_upstream.parquet')` |
| 减 | `_staging/new_factors_audit_20261112/scan_quote_defect.py:36` | `out.write_parquet(OUT / 'quote_volume_defect.parquet')` |
| 减 | `_staging/new_factors_rebuild_20261112/dump_residual.py:35` | `allch.write_parquet(S / 'residual_cells_554.parquet')` |
| 减 | `tools/research/build_new_factors.py:409` | `print(f'new_factors: {out.height:,} 行 × {out.width} 列 → {out_path}', flush=True)` |

## quotes_fund

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/fund_factor_repair_20261111/repair_fund_factors.py:126` | `write_parquet_atomic(` |
| 增 | `_staging/g029_160136_20261110/build_fill.py:252` | `write_parquet_atomic(new, TARGET)` |
| 增 | `src/data/local_store/store.py:510` | `write_parquet_atomic(` |
| 增 | `tools/repair_quote_anomalies.py:252` | `write_parquet_atomic(frame, path, compression='zstd', statistics=True)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/industry.py:146` | `write_parquet_atomic(frame, path)` |
| 减 | `src/data/local_store/share_capital.py:261` | `prepared = _normalize_frame(frame).sort(['ts_code', 'effective_date'])` |
| 减 | `src/data/quality/extended_builders.py:121` | `write_parquet_atomic(frame, path)` |
| 减 | `src/data/risk_flags.py:159` | `write_parquet_atomic(frame, path)` |
| 减 | `src/data/services/breadth_service.py:118` | `write_parquet_atomic(frame, path)` |
| 减 | `src/data/st_history.py:1778` | `return [day for day in values if day is not None]` |
| 减 | `tools/backfill_events.py:194` | `write_parquet_atomic(frame, target, compression='zstd')` |
| 减 | `tools/derive_free_float_fill.py:225` | `'rule': (f'仅空记录且上一非空记录距该记录 effective_date <= {WINDOW} 个交易日时填充；'` |

## quotes_futures

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/local_store/store.py:510` | `write_parquet_atomic(` |
| 增 | `tools/repair_quote_anomalies.py:252` | `write_parquet_atomic(frame, path, compression='zstd', statistics=True)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `_staging/multi_asset_tier2_20261110/fill_settle_g058.py:42` | `out.write_parquet(STAGING / 'cffex_official_g058.parquet')` |
| 减 | `tools/eval_regime_filter.py:462` | `pl.DataFrame(result['baseline']['windows']).write_csv(` |
| 减 | `tools/research/l1_baseline/old_vs_new_49.py:67` | `scan.write_csv(OUT / 'old_vs_new_49.csv')` |
| 减 | `tools/verify_futures_official.py:128` | `pl.concat(rows, how='vertical_relaxed').write_parquet(` |

## quotes_index

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/index_amount_fix_20261110/apply_amount_fix.py:132` | `write_parquet_atomic(fixed, TARGET)` |
| 增 | `_staging/index_fill_20261110/build.py:175` | `write_parquet_atomic(frame, target)` |
| 增 | `src/data/local_store/store.py:510` | `write_parquet_atomic(` |
| 增 | `tools/repair_quote_anomalies.py:252` | `write_parquet_atomic(frame, path, compression='zstd', statistics=True)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/industry.py:146` | `write_parquet_atomic(frame, path)` |
| 减 | `src/data/local_store/share_capital.py:261` | `prepared = _normalize_frame(frame).sort(['ts_code', 'effective_date'])` |
| 减 | `src/data/quality/extended_builders.py:121` | `write_parquet_atomic(frame, path)` |
| 减 | `src/data/risk_flags.py:159` | `write_parquet_atomic(frame, path)` |
| 减 | `src/data/services/breadth_service.py:118` | `write_parquet_atomic(frame, path)` |
| 减 | `src/data/st_history.py:1778` | `return [day for day in values if day is not None]` |
| 减 | `tools/backfill_events.py:194` | `write_parquet_atomic(frame, target, compression='zstd')` |
| 减 | `tools/derive_free_float_fill.py:225` | `'rule': (f'仅空记录且上一非空记录距该记录 effective_date <= {WINDOW} 个交易日时填充；'` |

## quotes_stock

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/data_fix_20261112/fix_defect1.py:125` | `write_parquet_atomic(merged, LOCAL)` |
| 增 | `_staging/delisted_coverage_audit_20261111/fix_five_factor_rows.py:171` | `write_parquet_atomic(fixed, path)` |
| 增 | `_staging/delisted_coverage_audit_20261111/merge_unmerged_delisted.py:189` | `write_parquet_atomic(frame, quote_path(code))` |
| 增 | `src/data/local_store/store.py:510` | `write_parquet_atomic(` |
| 增 | `tools/backfill_delisted_quotes.py:234` | `write_parquet_atomic(` |
| 增 | `tools/backfill_delisted_quotes.py:269` | `write_parquet_atomic(frame, path, compression='zstd', statistics=True)` |
| 增 | `tools/backfill_delisted_quotes.py:313` | `write_parquet_atomic(frame, path, compression='zstd', statistics=True)` |
| 增 | `tools/repair_quote_anomalies.py:252` | `write_parquet_atomic(frame, path, compression='zstd', statistics=True)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/industry.py:146` | `write_parquet_atomic(frame, path)` |
| 减 | `src/data/local_store/share_capital.py:261` | `prepared = _normalize_frame(frame).sort(['ts_code', 'effective_date'])` |
| 减 | `src/data/quality/extended_builders.py:121` | `write_parquet_atomic(frame, path)` |
| 减 | `src/data/risk_flags.py:159` | `write_parquet_atomic(frame, path)` |
| 减 | `src/data/services/breadth_service.py:118` | `write_parquet_atomic(frame, path)` |
| 减 | `src/data/st_history.py:1778` | `return [day for day in values if day is not None]` |
| 减 | `tools/backfill_events.py:194` | `write_parquet_atomic(frame, target, compression='zstd')` |
| 减 | `tools/derive_free_float_fill.py:225` | `'rule': (f'仅空记录且上一非空记录距该记录 effective_date <= {WINDOW} 个交易日时填充；'` |

## st_coverage

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/derived_catchup_20261112/st_rebuild.py:112` | `write_parquet_atomic(frame, ST_DIR / name)` |
| 增 | `src/data/st_history.py:405` | `write_parquet_atomic(frame, path)` |

## st_intervals

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/derived_catchup_20261112/st_rebuild.py:112` | `write_parquet_atomic(frame, ST_DIR / name)` |
| 增 | `src/data/quality/clean_set.py:1772` | `write_text_atomic(` |
| 增 | `src/data/st_history.py:405` | `write_parquet_atomic(frame, path)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `tools/research/eval_factor_candidates.py:451` | `def load_st_intervals() -> pl.DataFrame:` |

## st_price_inferred

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/st_price_infer.py:518` | `write_parquet_atomic(frame, path)` |

## st_raw

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/derived_catchup_20261112/st_append_closure.py:45` | `write_parquet_atomic(updated, SRC)` |
| 增 | `_staging/derived_catchup_20261112/st_fix_closure_date.py:45` | `write_parquet_atomic(updated, SRC)` |
| 增 | `src/data/st_history.py:405` | `write_parquet_atomic(frame, path)` |
| 增 | `src/data/st_name_history.py:166` | `write_parquet_atomic(frame, path)` |
| 增 | `tools/build_st_history_from_tushare.py:339` | `write_parquet_atomic(frame, target)` |

## st_status

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/risk_flags.py:159` | `write_parquet_atomic(frame, path)` |

## stale_status

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/quality/stale_status.py:125` | `write_parquet_atomic(frame, target)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/quality/listing_intervals.py:42` | `"""由 daily_index + stale_status 生成在市区间表。"""` |
| 减 | `src/data/quality/stale_status.py:135` | `)` |

## state_vector_daily

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `tools/build_state_vector.py:1108` | `write_parquet_atomic(panel, target)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `_staging/derived_1993b_20261113/layer4_build.py:184` | `write_parquet_atomic(panel, cand_path)` |
| 减 | `tools/build_state_vector.py:704` | `out = stage / 'state_vector_daily.parquet'` |

## stock_adj_factor

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/regression_evidence/old_build.py:977` | `write_parquet_atomic(factors, factor_path, compression='zstd',` |
| 增 | `src/data/local_store/build.py:978` | `write_parquet_atomic(factors, factor_path, compression='zstd',` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/factors_manifest.py:39` | `'stock_adj_factor.parquet': (` |
| 减 | `src/data/local_store/build.py:977` | `# 的跨文件不一致，需要重新执行一次 refresh-factors 收敛，不能当作已一致。` |
| 减 | `src/data/local_store/factor_rebuild.py:10` | ```factors/stock_adj_factor.parquet``（原子替换），避免千万行级内存峰值；` |

## stock_basic

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `tools/build_stock_basic.py:897` | `write_parquet_atomic(candidate, staging / 'stock_basic.parquet')` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/risk_flags.py:44` | `SOURCE_STOCK_BASIC = 'stock_basic'` |
| 减 | `tools/build_name_history.py:15` | `上市日仍以 ``stock_basic.list_date`` 为准。三者只做对账，不互相覆盖——` |
| 减 | `tools/build_stock_basic.py:883` | `print(f'\nT 码判断 {len(tcode_decisions)} 条:')` |
| 减 | `tools/build_stock_basic.py:885` | `print(f"  {item['ts_code']} → 继任 {item['successor']}，"` |

## stock_dividend

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/regression_evidence/old_build.py:979` | `write_parquet_atomic(dividends, dividend_path, compression='zstd',` |
| 增 | `src/data/local_store/build.py:980` | `write_parquet_atomic(dividends, dividend_path, compression='zstd',` |
| 增 | `tools/backfill_share_div.py:424` | `write_parquet_atomic(filled, args.div)` |
| 增 | `tools/collect_stock_dividend.py:749` | `write_parquet_atomic(final, args.out, compression='zstd', statistics=True)` |
| 增 | `tools/derive_total_dividend.py:491` | `write_parquet_atomic(out, args.div, compression='zstd', statistics=True)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/factors_manifest.py:42` | `'stock_dividend.parquet': (` |
| 减 | `src/data/financials/collector.py:652` | `path = Path(self.store_root) / 'factors' / 'stock_dividend.parquet'` |
| 减 | `src/data/local_store/build.py:979` | `statistics=True)` |
| 减 | `tools/backfill_share_div.py:62` | `DEFAULT_DIV = PROJECT_ROOT / 'data/local_store/factors/stock_dividend.parquet'` |
| 减 | `tools/collect_stock_dividend.py:46` | `DEFAULT_OUT = PROJECT_ROOT / 'data/local_store/factors/stock_dividend.parquet'` |
| 减 | `tools/derive_total_dividend.py:67` | `DEFAULT_DIV = PROJECT_ROOT / 'data/local_store/factors/stock_dividend.parquet'` |

## stock_shares

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/local_store/share_capital.py:262` | `write_parquet_atomic(` |
| 增 | `tools/derive_free_float_fill.py:290` | `write_parquet_atomic(out, src)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/local_store/share_capital.py:106` | `` |
| 减 | `tools/backfill_share_div.py:63` | `DEFAULT_SHARES = PROJECT_ROOT / 'data/local_store/meta/factors/stock_shares.parquet'` |
| 减 | `tools/derive_free_float_fill.py:18` | ```free_float_share_filled = min(继承值, 当期 float_share)``。不加这一步时` |
| 减 | `tools/derive_free_float_fill.py:225` | `'rule': (f'仅空记录且上一非空记录距该记录 effective_date <= {WINDOW} 个交易日时填充；'` |
| 减 | `tools/derive_total_dividend.py:68` | `DEFAULT_SHARES = PROJECT_ROOT / 'data/local_store/meta/factors/stock_shares.parquet'` |

## stock_turnover

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/regression_evidence/old_turnover.py:537` | `write_parquet_atomic(` |
| 增 | `_staging/regression_evidence/old_turnover.py:967` | `write_parquet_atomic(` |
| 增 | `_staging/regression_evidence/repro_red_state.py:105` | `history.write_parquet(target)` |
| 增 | `_staging/regression_evidence/repro_red_state.py:119` | `history.write_parquet(target)` |
| 增 | `_staging/turnover_badfix_20261113/fix_bad_rows.py:154` | `write_parquet_atomic(` |
| 增 | `src/data/local_store/turnover.py:547` | `write_parquet_atomic(` |
| 增 | `src/data/local_store/turnover.py:980` | `write_parquet_atomic(` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/factors_manifest.py:45` | `'stock_turnover.parquet': (` |
| 减 | `src/data/local_store/turnover.py:465` | `# 回补编排` |

## supplement_intervals

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/st_name_history.py:166` | `write_parquet_atomic(frame, path)` |

## universe_exclusions

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/quality/tradable_universe.py:986` | `write_parquet_atomic(frame, out_dir / EXCLUSIONS_FILE)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/quality/tradable_universe.py:59` | `EXCLUSIONS_FILE = 'universe_exclusions.parquet'` |
| 减 | `src/data/quality/tradable_universe.py:978` | `out_dir.mkdir(parents=True, exist_ok=True)` |
| 减 | `tools/fill_internal_gaps.py:45` | `EXCLUSIONS = UNIVERSE / 'universe_exclusions.parquet'` |

## universe_layers

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `_staging/derived_1993b_20261113/layer5_apply.py:194` | `write_parquet_atomic(pl.read_parquet(src), dst)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `tools/build_universe_layers.py:59` | `LAYER_FILES: dict[str, str] = {` |

## universe_price_jumps

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/quality/tradable_universe.py:987` | `write_parquet_atomic(jump_detail, out_dir / PRICE_JUMPS_FILE)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `src/data/quality/tradable_universe.py:61` | `PRICE_JUMPS_FILE = 'universe_price_jumps.parquet'` |
| 减 | `src/data/quality/tradable_universe.py:979` | `sources = {` |
| 减 | `tools/fill_internal_gaps.py:46` | `JUMPS = UNIVERSE / 'universe_price_jumps.parquet'` |

## valuation

| 类型 | 边 | 该行源码 |
|---|---|---|
| 增 | `src/data/financials/store.py:269` | `write_parquet_atomic(` |
| 增 | `src/data/financials/store.py:500` | `write_parquet_atomic(plan, dest, compression='zstd', statistics=True)` |

| 类型 | 边 | 该行源码 |
|---|---|---|
| 减 | `_staging/changelog_fix_20261112/perf.py:29` | `write_parquet_atomic(frame, DST)` |
| 减 | `_staging/valuation_history_20261111/merge_build.py:118` | `out.write_parquet(CAND)` |
| 减 | `tools/research/eval_factor_candidates.py:427` | `ROOT / 'data/local_store/meta/financials/valuation_daily.parquet',` |

## 修复后仍为 0 条写者的 2 张表

### membership_intervals（写者存在，路径由注册表驱动，静态解析到此为止）

- 真实写调用：`src/data/quality/extended_builders.py:629` `_write_frame(_path(store, 'index_membership'), frame)`（函数 `build_index_membership`，:519）
- 机制：`_path` 返回 `root / _SPEC[key].path`（extended_builders.py:106-107），`_SPEC = {spec.key: spec for spec in CONTRACTS}`（:63），CONTRACTS 来自 `extended_contracts.py`，该表 path 登记在 extended_contracts.py:107。表名由注册表键解析，静态 AST 停在 `_SPEC[key].path`，故 `writers_detected` 为 0 而非无写者。
- 旧口径那条边是 `extended_builders.py:25` 的模块 docstring 提及，不是写调用。
- 「写入口」列已改写为上面这条真实写调用（tools/lineage/build_lineage.py 表定义），不再指向 `tools/rebuild_index_constituents.py`——该脚本 :15 明确声明不写权威路径。

### implied_suspension（仓库内确无常规定义入口）

- 全仓引用 `implied_suspension.parquet` 的 12 处全是读或文档：`src/data/quality/suspension_union.py:27`（并表源）、`:130`（读取校验）、`src/data/quality/clean_set.py:974`（`_load_implied_suspension`）、`tools/build_universe_layers.py:87`、`tools/check_entity_dedupe.py:77`、`tests/test_suspension_union.py:25`（只往临时目录写夹具）。
- 没有任何 `write_parquet*` / `to_parquet` 指向该路径，写入方为仓库外一次性脚本。
- 「写入口」列已写明「无常规定义入口 + 上述证据」，不留空字段；该表仍按不可复现资产登记缺陷。
