# ruff: noqa: T201, E741, B905
#!/usr/bin/env python3
"""修复 d4 文档里被反斜杠转义破坏的若干行。"""
from pathlib import Path

P = Path(r'e:\code\python\quant\2026-11-12\data-lineage\outputs\d4_graded_fund_evidence.md')
FIX = {
    402: '§0 结论 4 与 §9 动作 1 曾把 50 个 `verdict = akshare无对应事件` 里落在带内的 42 个判为「本地事件表缺登记、因子步长对」。复核发现两处口径错误：',
    416: '| 候选（`akshare无对应事件`） | 50 |',
    425: '- 本轮**未向 `fund_share_change.parquet` 写入任何行**：写前断言即命中重号并中止，落盘脚本 `2026-11-12/data-lineage/work/b2s2_apply_share_change.py` 停在 `assert`，文件保持 2,738 行、`source` 中 `price_implied` 计数 0。',
    455: '**公告取证状态**：18/18 未取得。`api.fund.eastmoney.com/f10/JJGG`（东财基金公告索引）在连续请求后对全部 11 只分级码返回 `TotalCount=0`，含 6 秒退避重试 3 次仍为空；按 **DG-25** 判定为限速/封禁信号，已停止加压，本轮不再重试。脚本与日志：`2026-11-12/data-lineage/work/b2s2_ann18.{py,log}`、结果 `b2s2_ann18.csv`。',
}


def main() -> None:
    lines = P.read_text(encoding='utf-8').splitlines()
    for n, text in FIX.items():
        lines[n - 1] = text
    P.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    bad = [(i + 1, repr(ln)) for i, ln in enumerate(lines)
           if any(ord(c) < 9 or 11 <= ord(c) <= 12 or 14 <= ord(c) <= 31 for c in ln)]
    print('control_char_lines', bad)
    for n in FIX:
        print(n, lines[n - 1][:120])


if __name__ == '__main__':
    main()
