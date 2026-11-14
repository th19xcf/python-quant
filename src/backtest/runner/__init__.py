"""回测执行层：中文配方 / spec.json → 既有引擎 → 运行记录。"""

from .errors import SpecError
from .registry import CONDITIONS, FACTORS
from .spec import SPEC_VERSION, dump_spec, load_spec, summary, validate_spec

__all__ = [
    'SPEC_VERSION', 'SpecError', 'FACTORS', 'CONDITIONS',
    'dump_spec', 'load_spec', 'summary', 'validate_spec',
]
