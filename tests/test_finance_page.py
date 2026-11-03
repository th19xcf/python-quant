"""财务分析页 UI 测试：切标的触发、task_id 过滤、异常/竞态不崩溃。"""

from __future__ import annotations

from PySide6.QtCore import QObject, Signal

from src.ui.finance import FinancePage


class FakeTaskManager(QObject):
    """带真实 Qt 信号的假任务管理器，便于同步触发回调。"""

    task_completed = Signal(str, object)
    task_error = Signal(str, str)
    task_finished = Signal(str)

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[dict] = []
        self._next = 0

    def create_task(self, name, func, args=(), kwargs=None,
                    timeout_seconds=None, **_ignored):
        self._next += 1
        task_id = f'task-{self._next}'
        self.calls.append({
            'task_id': task_id,
            'name': name,
            'func': func,
            'args': args,
            'kwargs': kwargs or {},
            'timeout_seconds': timeout_seconds,
        })
        return task_id


def _snapshot(code: str = '600519.SH', name: str = '贵州茅台') -> dict:
    return {
        'ok': True,
        'code': code,
        'name': name,
        'as_of': '2026-11-03',
        'not_applicable': False,
        'report': {
            'period': '2026-09-30',
            'notice_date': '2026-10-30',
            'report_name': '2026三季报',
            'source': '东方财富·主要指标',
        },
        'metrics': {
            'roe': {'value': 18.5, 'display': '18.50%'},
            'net_margin': {'value': 49.2, 'display': '49.20%'},
        },
        'valuation': {
            'as_of': '2026-11-03',
            'source': '东方财富·估值分析',
            'metrics': {'pe_ttm': {'value': 22.4, 'display': '22.40'}},
        },
        'history': {
            'periods': ['2026-06-30', '2026-09-30'],
            'rows': [{'key': 'roe', 'label': 'ROE(加权)',
                      'values': ['12.10%', '18.50%']}],
        },
        'news': [{
            'time': '2026-10-30 18:36',
            'date': '2026-10-30',
            'type': '新闻',
            'title': '贵州茅台三季报净利润同比增长',
            'source': '每日经济新闻',
            'url': 'http://example.com/news/1',
            'summary': '公司披露三季报。',
        }],
        'industry': '白酒Ⅱ',
        'errors': [],
        'warnings': [],
    }


def _payload(page: FinancePage, snapshot=None) -> dict:
    return {
        'token': page._token,
        'code': page._target_code,
        'snapshot': _snapshot() if snapshot is None else snapshot,
        'error': '',
    }


def test_switch_stock_triggers_one_background_task(qapp):
    manager = FakeTaskManager()
    page = FinancePage(task_manager=manager)
    page.show()

    page.set_stock('600519.SH', '贵州茅台')
    assert len(manager.calls) == 1
    assert manager.calls[0]['args'][0] == '600519.SH'
    # 取数函数必须交给后台任务池，而不是在 UI 线程直接执行
    assert manager.calls[0]['func'].__name__ == '_fetch_finance_task'
    assert manager.calls[0]['timeout_seconds'] == 90

    # 同一标的重复打开：不重复请求
    page.set_stock('600519.SH', '贵州茅台')
    assert len(manager.calls) == 1
    page.close()


def test_switch_to_other_stock_invalidates_old_task(qapp):
    manager = FakeTaskManager()
    page = FinancePage(task_manager=manager)
    page.show()

    page.set_stock('600519.SH', '贵州茅台')
    old_task = manager.calls[0]['task_id']
    page.set_stock('000001.SZ', '平安银行')

    # 旧请求被作废，新标的会重新提交任务
    assert len(manager.calls) == 2
    assert manager.calls[1]['args'][0] == '000001.SZ'
    assert page._task_id == manager.calls[1]['task_id']

    # 旧 task_id 的迟到结果不得渲染
    page._on_task_completed(old_task, _payload(page, _snapshot('600519.SH', '贵州茅台')))
    assert page.title_label.text().endswith('000001.SZ')
    assert page.current_stack_is_content() is False
    page.close()


def test_completion_filters_by_task_id_then_renders(qapp):
    manager = FakeTaskManager()
    page = FinancePage(task_manager=manager)
    page.show()
    page.set_stock('600519.SH', '贵州茅台')
    task_id = page._task_id

    # 其他任务完成：必须忽略
    page._on_task_completed('other-task', _payload(page))
    assert page.current_stack_is_content() is False
    assert '正在获取' in page.snapshot_state_text()

    # 本任务完成：渲染内容
    page._on_task_completed(task_id, _payload(page))
    assert page.current_stack_is_content() is True
    assert '贵州茅台' in page.title_label.text()
    assert '2026-09-30' in page.meta_label.text()
    assert page.news_table.rowCount() == 1
    assert page.history_table.rowCount() == 1
    assert page._metric_values['roe'].text() == '18.50%'
    assert page._metric_values['pe_ttm'].text() == '22.40'
    page.close()


def test_error_and_malformed_result_show_state_without_crash(qapp):
    manager = FakeTaskManager()
    page = FinancePage(task_manager=manager)
    page.show()
    page.set_stock('600519.SH', '贵州茅台')
    task_id = page._task_id

    page._on_task_completed('other-task', {'unexpected': True})
    assert page.current_stack_is_content() is False

    page._on_task_completed(task_id, {'token': page._token})
    assert page.current_stack_is_content() is False
    assert '加载失败' in page.snapshot_state_text()

    page.set_stock('600519.SH', '贵州茅台', force=True)
    task_id = page._task_id
    page._on_task_error(task_id, 'ConnectionError: network down')
    assert 'network down' in page.snapshot_state_text()
    assert page._loading is False
    page.close()


def test_result_missing_token_is_rejected(qapp):
    manager = FakeTaskManager()
    page = FinancePage(task_manager=manager)
    page.show()
    page.set_stock('600519.SH', '贵州茅台')
    task_id = page._task_id

    # 缺 token 的结果来源不可信，按类型校验拒绝渲染
    page._on_task_completed(task_id, {
        'code': page._target_code,
        'snapshot': _snapshot(),
        'error': '',
    })
    assert page.current_stack_is_content() is False
    assert '加载失败' in page.snapshot_state_text()
    page.close()


def test_malformed_snapshot_is_rejected(qapp):
    manager = FakeTaskManager()
    page = FinancePage(task_manager=manager)
    page.show()
    page.set_stock('600519.SH', '贵州茅台')
    task_id = page._task_id

    page._on_task_completed(task_id, {
        'token': page._token,
        'code': page._target_code,
        'snapshot': ['not-a-dict'],
        'error': '',
    })
    assert page.current_stack_is_content() is False
    assert '加载失败' in page.snapshot_state_text()
    page.close()


def test_snapshot_with_missing_keys_renders_empty_state(qapp):
    manager = FakeTaskManager()
    page = FinancePage(task_manager=manager)
    page.show()
    page.set_stock('600519.SH', '贵州茅台')
    task_id = page._task_id

    page._on_task_completed(task_id, _payload(page, {'code': '600519.SH'}))
    assert page.current_stack_is_content() is False
    assert '暂无数据' in page.snapshot_state_text()
    page.close()


def test_after_close_callbacks_are_ignored(qapp):
    manager = FakeTaskManager()
    page = FinancePage(task_manager=manager)
    page.show()
    page.set_stock('600519.SH', '贵州茅台')
    task_id = page._task_id
    page.close()

    # 页面已销毁/关闭，迟到回调不得访问控件
    page._on_task_completed(task_id, _payload(page))
    page._on_task_error(task_id, 'late error')
    assert page._alive is False
