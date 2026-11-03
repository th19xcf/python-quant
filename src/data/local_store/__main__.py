#!/usr/bin/env python3

"""``python -m src.data.local_store`` 的 CLI 入口。"""

import sys

from src.data.local_store.build import main

if __name__ == '__main__':
    sys.exit(main())
