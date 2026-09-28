"""Analytics bootstrap: self-starting, crash-proof, run-once backfills.
With ``ENABLE_ANALYTICS``/``ANALYTICS_AUTO_BACKFILL`` on, the daily,
native and ERC-20 backfills run one at a time via a beat task until
``bookkeeping.bootstrap_complete()`` says done. Sibling of
``analytics/catchup/``: never import ``tasks.py`` here at module level.
"""

from .bookkeeping import bootstrap_complete
from .registry import build_default_stages
from .report import build_bootstrap_report
from .stage import Stage
from .tick import run_tick

__all__ = [
    "Stage",
    "bootstrap_complete",
    "build_bootstrap_report",
    "build_default_stages",
    "run_tick",
]
