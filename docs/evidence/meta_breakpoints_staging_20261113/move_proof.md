# 8 个放错层断点迁移留证（2026-11-13）

迁移方式：读原字节 → 写 `_backup/` 快照 → 写 `_staging/<任务名>/` → 三方 SHA256 相等 → `logs/data_changelog.csv` 记 delete → 删原件。

| 原路径（`data/local_store/`） | 新路径 | `_backup/` 快照 | 字节 | SHA256 |
|---|---|---|---|---|
| `data/local_store/meta/anomalies/rights_verify_progress.json` | `_staging/rights_verify/rights_verify_progress.json` | `_backup/meta/anomalies_breakpoints_20261113/rights_verify_progress.json` | 92160 | `0ae7178072a9beb67370af41cbafa7b7546e3459a02c7a7ff8ca7464c364239e` |
| `data/local_store/meta/universe/internal_gap_fix_progress.json` | `_staging/internal_gap_fix/internal_gap_fix_progress.json` | `_backup/meta/universe_breakpoints_20261113/internal_gap_fix_progress.json` | 75406 | `a48cb4154c8a7e5bdc1ea0ba5ca3e14c9143d6e1c5d2003466e53e1745926f4e` |
| `data/local_store/meta/universe/internal_gap_fix_failed.csv` | `_staging/internal_gap_fix/internal_gap_fix_failed.csv` | `_backup/meta/universe_breakpoints_20261113/internal_gap_fix_failed.csv` | 38 | `0d4b6c1426db91d870f5e90f96af6ba2218868959a78bf0bb19f2fc3ff657d04` |
| `data/local_store/meta/st/fetch_state.json` | `_staging/st_history_fetch/fetch_state.json` | `_backup/meta/st_breakpoints_20261113/fetch_state.json` | 7491 | `c11bcd49bcb3c05db750e32658e6011a77be8f617b5f40e8f0d2e85cbdba13cc` |
| `data/local_store/meta/st/em_state.json` | `_staging/st_em_names/em_state.json` | `_backup/meta/st_breakpoints_20261113/em_state.json` | 607 | `b7b702ec230b2879bad643918077f06a50833597e8c468fc0401dfa368148b0e` |
| `data/local_store/meta/st/em_failures.csv` | `_staging/st_em_names/em_failures.csv` | `_backup/meta/st_breakpoints_20261113/em_failures.csv` | 11 | `468583525ffb97b907d42681fe2350bd6af87db1906960cfd38742275ff49f21` |
| `data/local_store/meta/st/candidate_state.json` | `_staging/st_candidate_state/candidate_state.json` | `_backup/meta/st_breakpoints_20261113/candidate_state.json` | 926 | `c829843fb62219ec63c1997524608f1d8b1b6176e72aeccfaf030ab3477c1cfe` |
| `data/local_store/meta/st/candidate_failures.csv` | `_staging/st_candidate_state/candidate_failures.csv` | `_backup/meta/st_breakpoints_20261113/candidate_failures.csv` | 52 | `68e1519e4b5811085d9e999eeab5e817c633035aadb186bf8b9af7988516a9e6` |

合计 8 件，迁移前后 SHA256 全部相等（三方比对）。
