#!/usr/bin/env python3

"""东财 F10 配股明细第三源的解析与比对逻辑。"""
import pandas as pd

import tools.verify_rights_issue as V


def test_parse_em_rows_converts_per10_ratio():
    rows = [
        {'EX_DIVIDEND_DATEE': '1998-02-09 00:00:00', 'ISSUE_PRICE': 6.0,
         'EVENT_EXPLAIN': '每10股配2.5股'},
        {'EX_DIVIDEND_DATEE': '1998-02-09', 'ISSUE_PRICE': 6.0,
         'EVENT_EXPLAIN': '每10股派1元（不配股）'},
        {'EX_DIVIDEND_DATEE': None, 'ISSUE_PRICE': 1.0, 'EVENT_EXPLAIN': '每10股配3股'},
    ]
    frame = V._parse_em_rows(rows)
    assert frame.shape[0] == 1
    assert frame.iloc[0]['em_ratio'] == 0.25
    assert frame.iloc[0]['em_price'] == 6.0


def test_em_rows_merges_same_day_and_rejects_price_conflict(monkeypatch):
    ev = pd.DataFrame([
        {'ts_code': '000417.SZ', 'ex_date': pd.Timestamp('1998-03-18'),
         'rights_issue_ratio': 0.1, 'rights_issue_price': 5.0},
        {'ts_code': '000417.SZ', 'ex_date': pd.Timestamp('1998-03-18'),
         'rights_issue_ratio': 0.1, 'rights_issue_price': 5.0},
        {'ts_code': '000528.SZ', 'ex_date': pd.Timestamp('1994-10-19'),
         'rights_issue_ratio': 0.85, 'rights_issue_price': 2.8},
    ])
    fake = {
        '000417.SZ': [{'EX_DIVIDEND_DATEE': '1998-03-18', 'ISSUE_PRICE': 5.0,
                       'EVENT_EXPLAIN': '每10股配2股'}],
        '000528.SZ': [{'EX_DIVIDEND_DATEE': '1994-10-19', 'ISSUE_PRICE': 3.0,
                       'EVENT_EXPLAIN': '每10股配8.5股'}],
    }
    monkeypatch.setattr(V, '_fetch_em_pgmx', lambda code: fake[code])
    monkeypatch.setattr(V.time, 'sleep', lambda _sec: None)

    rows = V._em_rows(ev, 0.01)

    assert [row['ts_code'] for row in rows] == ['000417.SZ']
    assert rows[0]['ex_date'] == '1998-03-18'
