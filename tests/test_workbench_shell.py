"""回测工作台 Shell 冒烟：两通道共用的服务能起、四视图在、队列接口可用。

不启动任何图形界面，只验 `src/ui/workbench/server.py` 这一份实现。
"""
from __future__ import annotations

import json
import urllib.request

import pytest


@pytest.fixture(scope='module')
def workbench_url():
    from src.ui.workbench.server import ensure_server, shutdown

    url = ensure_server()
    yield url
    shutdown()


def test_shell_serves_four_views(workbench_url):
    with urllib.request.urlopen(workbench_url + '?theme=dark', timeout=60) as resp:
        html = resp.read().decode('utf-8')
    for view in ('view-new', 'view-queue', 'view-dash', 'view-list'):
        assert f'id="{view}"' in html, f'Shell 缺少视图 {view}'
    assert 'data-theme="dark"' in html, 'Shell 未按 ?theme=dark 切换深色令牌'


def test_queue_api_returns_job_list(workbench_url):
    with urllib.request.urlopen(workbench_url + 'api/queue', timeout=60) as resp:
        payload = json.loads(resp.read().decode('utf-8'))
    assert isinstance(payload.get('jobs'), list)


def test_ensure_server_is_singleton(workbench_url):
    from src.ui.workbench.server import ensure_server

    assert ensure_server() == workbench_url, 'ensure_server 重复调用应复用同一端口'
