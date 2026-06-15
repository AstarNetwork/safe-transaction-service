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
    """A.2 — Active Safes count by window (Redis-cached)."""

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
        analytics_service = get_analytics_service()
        return Response(analytics_service.get_active_safes(window))


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
    """A.4 — Active owners by window (Redis-cached)."""

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
        analytics_service = get_analytics_service()
        return Response(analytics_service.get_active_owners(window))


class AnalyticsTxVolumeView(APIView):
    """A.5 — TX volume metrics by window (direct query)."""

    swagger_schema = None
    renderer_classes = (JSONRenderer,)
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(exclude=True)
    def get(self, request, format=None):
        window = request.query_params.get("window", "30d")
        analytics_service = get_analytics_service()
        return Response(analytics_service.get_tx_volume(window))


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
    """A.8 — Token volume metrics by window (direct query)."""

    swagger_schema = None
    renderer_classes = (JSONRenderer,)
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(exclude=True)
    def get(self, request, format=None):
        window = request.query_params.get("window", "30d")
        analytics_service = get_analytics_service()
        return Response(analytics_service.get_token_volume(window))
