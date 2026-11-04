"""固定基准脚本的纯函数单测（不触发任何矩阵读取）。"""

from __future__ import annotations

from datetime import date

import pytest

from tools.eval_fixed_benchmark import (
    Frozen,
    _num,
    all_positive_weights,
    as_windows,
    replace_weights,
)


def _window(selected: tuple[str, ...] = ('b', 'c')) -> Frozen:
    return Frozen(date(2024, 1, 1), date(2024, 6, 1), {'a': 0.3}, selected)


def test_all_positive_weights_splits_new_share() -> None:
    weights = all_positive_weights(_window(), {'a': 1.0})
    assert pytest.approx(weights['a']) == 0.3
    assert pytest.approx(weights['b']) == 0.35
    assert pytest.approx(weights['c']) == 0.35
    assert all(value > 0 for value in weights.values())


def test_all_positive_weights_handles_empty_selection() -> None:
    weights = all_positive_weights(_window(selected=()), {'a': 1.0})
    assert list(weights) == ['a']
    assert pytest.approx(weights['a']) == 0.3


def test_all_positive_weights_accumulates_baseline_overlap() -> None:
    weights = all_positive_weights(_window(selected=('a',)), {'a': 1.0})
    assert pytest.approx(weights['a']) == 0.3 + 0.7


def test_replace_weights_keeps_window_dates() -> None:
    original = [_window(selected=('b',))]
    replaced = replace_weights(original, lambda _: {'b': 1.0})
    assert replaced[0].test_start == original[0].test_start
    assert replaced[0].test_end == original[0].test_end
    assert replaced[0].weights == {'b': 1.0}
    assert replaced[0].selected == ('b',)


def test_as_windows_drops_selection() -> None:
    windows = as_windows([_window()])
    assert windows[0].test_start == date(2024, 1, 1)
    assert windows[0].weights == {'a': 0.3}
    assert not hasattr(windows[0], 'selected')


def test_num_handles_none_and_bool() -> None:
    assert _num(None, 1.5) == 1.5
    assert _num(True, 2.0) == 2.0
    assert _num(3, 0.0) == 3.0
    assert _num(2.5) == 2.5
    assert _num('x', 4.0) == 4.0
