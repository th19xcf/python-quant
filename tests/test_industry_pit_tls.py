"""申万下载 TLS 校验策略：默认校验、可补 CA、显式开关才降级。"""
from __future__ import annotations

import pytest

from src.data import industry_pit
from src.data.industry_pit import _sw_tls_verify


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv('SW_CA_BUNDLE', raising=False)
    monkeypatch.delenv('SW_ALLOW_INSECURE', raising=False)


def test_verify_enabled_by_default():
    assert _sw_tls_verify() is True


def test_ca_bundle_used_when_present(monkeypatch, tmp_path):
    pem = tmp_path / 'sw_chain.pem'
    pem.write_text('dummy', encoding='utf-8')
    monkeypatch.setenv('SW_CA_BUNDLE', str(pem))
    monkeypatch.setenv('SW_ALLOW_INSECURE', '1')  # CA 优先于降级开关
    assert _sw_tls_verify() == str(pem)


def test_missing_ca_bundle_fails_loudly(monkeypatch, tmp_path):
    monkeypatch.setenv('SW_CA_BUNDLE', str(tmp_path / 'missing.pem'))
    with pytest.raises(FileNotFoundError):
        _sw_tls_verify()


@pytest.mark.parametrize('flag', ['1', 'true', 'YES'])
def test_insecure_only_with_explicit_flag(monkeypatch, flag):
    monkeypatch.setenv('SW_ALLOW_INSECURE', flag)
    assert _sw_tls_verify() is False


def test_download_passes_verify_to_requests(monkeypatch, tmp_path):
    seen: dict = {}

    class _Resp:
        status_code = 200
        content = b'x' * 200_000
        headers = {'Last-Modified': 'Mon, 23 Nov 2026 00:00:00 GMT'}

    def fake_get(url, **kwargs):
        seen.update(kwargs)
        return _Resp()

    monkeypatch.setattr(industry_pit.requests, 'get', fake_get)
    path, modified = industry_pit.download_sw_history(tmp_path, refresh=True)
    assert seen['verify'] is True
    assert path.stat().st_size == 200_000
    assert modified is not None
