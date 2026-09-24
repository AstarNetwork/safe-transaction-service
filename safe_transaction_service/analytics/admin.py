from django.contrib import admin
from django.http import HttpRequest

from .models import AnalyticsCatchupState


@admin.register(AnalyticsCatchupState)
class AnalyticsCatchupStateAdmin(admin.ModelAdmin):
    """Read-only view of the catch-up sweeper's bookkeeping.

    For diagnosis only — see "Analytics catch-up" in
    `analytics/implementation-notes.md`: the sweeper is the only writer of
    this table, and letting an operator edit a row by hand from the admin
    could desynchronise `core_ok` / `failed_steps` from what `compute_day`
    actually observed (see the docstring on the model). Nothing else in
    `analytics/` is registered here on purpose — this is the one model an
    operator needs to see without a shell.
    """

    list_display = (
        "kind",
        "key",
        "attempts",
        "core_ok",
        "failed_steps",
        "last_state",
        "last_code",
        "next_attempt_at",
        "updated_at",
    )
    list_filter = ("kind", "last_state")
    search_fields = ("key",)
    ordering = ["-updated_at"]

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        return False
