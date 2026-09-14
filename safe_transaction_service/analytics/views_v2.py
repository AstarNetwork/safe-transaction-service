import re
from datetime import date

from django.utils.dateparse import parse_datetime

from drf_spectacular.utils import extend_schema
from rest_framework.authentication import TokenAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response
from rest_framework.views import APIView

from safe_transaction_service.analytics.services.analytics_service import (
    get_analytics_service,
)

# Strict ISO calendar-date shape for the optional `from`/`to` range on the two
# active-* endpoints (phase-B T10). `date.fromisoformat` alone is too permissive
# for the contract §4.5 states — it accepts `20260105` and `2026-W01-1` — and
# `django.utils.dateparse.parse_date` additionally accepts `2026-1-5`, so the
# shape is pinned here and `fromisoformat` is left to reject impossible dates
# such as `2026-02-30`.
_ISO_DATE_RE = re.compile(r"\A\d{4}-\d{2}-\d{2}\Z")


def _parse_iso_date(value: str) -> date | None:
    """Parse a strict ISO ``YYYY-MM-DD`` calendar date, or ``None`` if the
    value is not one. Deliberately unrelated to ``/safe-creations/``'s lenient
    ``parse_datetime`` bounds, which are existing contract and stay lenient
    (spec §6)."""
    if not _ISO_DATE_RE.match(value):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _parse_iso_date_range(
    query_params,
) -> tuple[date | None, date | None, str | None]:
    """Read the optional ``from`` / ``to`` ISO date range shared by
    ``/active-safes/`` and ``/active-owners/`` (phase-B T10).

    Returns ``(date_from, date_to, error)``. Both bounds are independently
    optional; either being present makes the read ranged and the ``window``
    stops applying (§4.5, "the range wins"). ``error`` is a message **naming
    the offending parameter** — the task's acceptance clause — and is not
    ``None`` only when the caller must be told 400:

    - a bound that is not a strict ISO ``YYYY-MM-DD`` date, empty value
      included (``?from=`` is a supplied-and-unparseable bound, exactly as
      ``?breakdown=`` is a supplied-and-invalid breakdown);
    - ``from`` later than ``to``. Equal bounds are a valid single-day range.

    Range length is not capped, consistent with ``window`` not being capped
    under ``breakdown=day`` (spec Q21).
    """
    bounds: dict[str, date | None] = {}
    for name in ("from", "to"):
        raw = query_params.get(name)
        if raw is None:
            bounds[name] = None
            continue
        parsed = _parse_iso_date(raw)
        if parsed is None:
            return None, None, f"{name} must be an ISO date (YYYY-MM-DD)"
        bounds[name] = parsed
    date_from, date_to = bounds["from"], bounds["to"]
    if date_from is not None and date_to is not None and date_from > date_to:
        return None, None, "from must not be after to"
    return date_from, date_to, None


class AnalyticsMultisigTxsByOriginListView(APIView):
    swagger_schema = None
    renderer_classes = (JSONRenderer,)
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(exclude=True)
    def get(self, request, format=None):
        analytics_service = get_analytics_service()
        return Response(analytics_service.get_safe_transactions_per_safe_app())


class AnalyticsSummaryView(APIView):
    """A.1 — Fleet-level summary metrics (direct query)."""

    swagger_schema = None
    renderer_classes = (JSONRenderer,)
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(exclude=True)
    def get(self, request, format=None):
        analytics_service = get_analytics_service()
        return Response(analytics_service.get_summary())


class AnalyticsActiveSafesView(APIView):
    """A.2 — Active Safes count by window (Redis-cached).

    ``breakdown`` is optional and the only accepted value is ``day``
    (phase-B T9). Omitting it returns the scalar payload byte-identically;
    ``breakdown=day`` appends the per-day series. The ``7d|30d|90d``
    ``window`` validation below is unchanged and applies either way —
    ``window`` is simply not capped *further* under ``breakdown=day``
    (spec Q21).

    ``from`` / ``to`` are optional strict ISO ``YYYY-MM-DD`` bounds
    (phase-B T10). Either one present makes the read ranged and the
    ``window`` stops applying — "the range wins" (§4.5) — which the payload
    reports as ``window: null``. Absent, the response is byte-identical to
    the pre-T10 one. An unparseable bound, or ``from`` after ``to``, is a
    400 naming the parameter.

    The three guards run in order window → breakdown → range, cheapest and
    most basic first, so a request wrong in two ways reports ``window``. A
    range does **not** waive the ``window`` guard: precedence decides which
    days are counted, not which parameters are validated.
    """

    swagger_schema = None
    renderer_classes = (JSONRenderer,)
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(exclude=True)
    def get(self, request, format=None):
        window = request.query_params.get("window", "30d")
        if window not in ("7d", "30d", "90d"):
            return Response(
                {"error": "window must be one of: 7d, 30d, 90d"}, status=400
            )
        breakdown = request.query_params.get("breakdown")
        if breakdown is not None and breakdown != "day":
            return Response({"error": "breakdown must be: day"}, status=400)
        date_from, date_to, range_error = _parse_iso_date_range(request.query_params)
        if range_error is not None:
            return Response({"error": range_error}, status=400)
        analytics_service = get_analytics_service()
        return Response(
            analytics_service.get_active_safes(
                window, breakdown=breakdown, date_from=date_from, date_to=date_to
            )
        )


class AnalyticsSafeCreationsView(APIView):
    """A.3 — Safe creations time series (direct query)."""

    swagger_schema = None
    renderer_classes = (JSONRenderer,)
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(exclude=True)
    def get(self, request, format=None):
        interval = request.query_params.get("interval", "day")
        if interval not in ("day", "week", "month"):
            return Response(
                {"error": "interval must be one of: day, week, month"}, status=400
            )
        date_from = request.query_params.get("from")
        date_to = request.query_params.get("to")
        parsed_from = parse_datetime(date_from) if date_from else None
        parsed_to = parse_datetime(date_to) if date_to else None
        analytics_service = get_analytics_service()
        return Response(
            analytics_service.get_safe_creations(parsed_from, parsed_to, interval)
        )


class AnalyticsActiveOwnersView(APIView):
    """A.4 — Active owners by window (Redis-cached).

    ``breakdown`` behaves exactly as on ``/active-safes/`` (phase-B T9):
    optional, ``day`` the only accepted value, absent means a
    byte-identical scalar payload. The existing ``7d|30d|90d`` ``window``
    validation is unchanged.

    ``from`` / ``to`` behave exactly as on ``/active-safes/`` too (phase-B
    T10): optional strict ISO ``YYYY-MM-DD`` bounds, either one of them
    overriding ``window`` (reported as ``window: null``), an unparseable
    bound or ``from`` after ``to`` a 400 naming the parameter, and no cap on
    range length.
    """

    swagger_schema = None
    renderer_classes = (JSONRenderer,)
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(exclude=True)
    def get(self, request, format=None):
        window = request.query_params.get("window", "30d")
        if window not in ("7d", "30d", "90d"):
            return Response(
                {"error": "window must be one of: 7d, 30d, 90d"}, status=400
            )
        breakdown = request.query_params.get("breakdown")
        if breakdown is not None and breakdown != "day":
            return Response({"error": "breakdown must be: day"}, status=400)
        date_from, date_to, range_error = _parse_iso_date_range(request.query_params)
        if range_error is not None:
            return Response({"error": range_error}, status=400)
        analytics_service = get_analytics_service()
        return Response(
            analytics_service.get_active_owners(
                window, breakdown=breakdown, date_from=date_from, date_to=date_to
            )
        )


class AnalyticsTxVolumeView(APIView):
    """A.5 — TX volume metrics by window (direct query).

    ``breakdown`` is optional and the only accepted value is ``day``
    (phase-B T8). Omitting it returns the scalar payload byte-identically;
    ``breakdown=day`` appends the per-day series. ``window`` stays
    unvalidated here — ``_parse_window`` falls back to 30 on anything it
    cannot parse — and is not capped under ``breakdown=day`` (spec Q21).
    """

    swagger_schema = None
    renderer_classes = (JSONRenderer,)
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(exclude=True)
    def get(self, request, format=None):
        window = request.query_params.get("window", "30d")
        breakdown = request.query_params.get("breakdown")
        if breakdown is not None and breakdown != "day":
            return Response({"error": "breakdown must be: day"}, status=400)
        analytics_service = get_analytics_service()
        return Response(analytics_service.get_tx_volume(window, breakdown=breakdown))


class AnalyticsSafeSegmentsView(APIView):
    """A.6 — Safe segments by owner count (Redis-cached)."""

    swagger_schema = None
    renderer_classes = (JSONRenderer,)
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(exclude=True)
    def get(self, request, format=None):
        analytics_service = get_analytics_service()
        return Response(analytics_service.get_safe_segments())


class AnalyticsTvlView(APIView):
    """A.7 — TVL (approximate via net-flow, Redis-cached)."""

    swagger_schema = None
    renderer_classes = (JSONRenderer,)
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(exclude=True)
    def get(self, request, format=None):
        analytics_service = get_analytics_service()
        return Response(analytics_service.get_tvl())


class AnalyticsTokenVolumeView(APIView):
    """A.8 — Token volume metrics by window (direct query).

    ``breakdown`` is optional and the only accepted value is ``day``,
    exactly as on ``/tx-volume/`` (phase-B T8) down to the error wording.
    Omitting it returns the scalar payload byte-identically;
    ``breakdown=day`` appends the per-day top-N series and the
    ``days_token_cap`` that describes its depth.

    ``window`` stays unvalidated here — ``_parse_window`` falls back to 30
    on anything it cannot parse — and is not capped under
    ``breakdown=day`` (spec Q21). What bounds the response instead is the
    per-day cap, which is why that cap is a payload key rather than an
    implementation detail.
    """

    swagger_schema = None
    renderer_classes = (JSONRenderer,)
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(exclude=True)
    def get(self, request, format=None):
        window = request.query_params.get("window", "30d")
        breakdown = request.query_params.get("breakdown")
        if breakdown is not None and breakdown != "day":
            return Response({"error": "breakdown must be: day"}, status=400)
        analytics_service = get_analytics_service()
        return Response(analytics_service.get_token_volume(window, breakdown=breakdown))
