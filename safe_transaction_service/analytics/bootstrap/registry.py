"""The ordered stage registry: daily -> native -> ERC-20, strictly one
at a time. ``build_default_stages()`` builds fresh every tick so a
test's patched stages never leak between tests."""

from .daily import DailyStage
from .erc20 import Erc20Stage
from .native import NativeStage
from .stage import Stage


def build_default_stages() -> list[Stage]:
    return [DailyStage(), NativeStage(), Erc20Stage()]
