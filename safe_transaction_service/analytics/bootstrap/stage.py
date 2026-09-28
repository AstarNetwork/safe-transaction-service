"""The stage interface every bootstrap stage implements.
Three stages exist, in this fixed order: daily, native, ERC-20 (see
``registry.py``). ``"gave_up"`` in ``STAGE_STATES`` is reserved for
typing -- no ``status()`` ever returns it; that lives in
``AnalyticsBootstrapStage.gave_up_at`` instead.
"""

import abc

STAGE_STATES = ("pending", "running", "stalled", "failed", "gave_up", "done")


def _tasks():
    """Lazy import shared by ``erc20.py``/``native.py`` to avoid a cycle
    with ``tasks.py``, which imports this package eagerly."""
    from safe_transaction_service.analytics import tasks as tasks_module

    return tasks_module


class Stage(abc.ABC):
    """One step of the bootstrap, ordered by ``registry.py``."""

    name: str

    #: Whether a ``"stalled"`` report from this stage is resumed by
    #: something OTHER than the tick. ``Erc20Stage`` sets this ``True``:
    #: its watchdog task is the one resumer of a stalled run, since two
    #: resumers on the same ~5-minute cadence could each dispatch the
    #: same run's chain, and the shared lock only serialises the chains,
    #: not their existence. Every other stage leaves this ``False`` and
    #: resumes a stalled run itself, from its own ``start_or_resume()``.
    resumed_externally: bool = False

    @abc.abstractmethod
    def is_done(self) -> bool:
        """Whether this stage's durable marker says it's finished --
        re-derived from Postgres, never a "did we run this" flag."""
        raise NotImplementedError

    @abc.abstractmethod
    def status(self) -> str:
        """One of ``STAGE_STATES``, telling the tick whether the stage is
        genuinely active right now (``"running"``) or safe to act on."""
        raise NotImplementedError

    @abc.abstractmethod
    def start_or_resume(self) -> None:
        """Dispatch, adopt, or resume -- reusing the stage's own backfill
        dispatch code, never a copy. Must not raise."""
        raise NotImplementedError

    def progress(self) -> dict | None:
        """Cheap progress detail for `/summary/`; `None` by default."""
        return None
