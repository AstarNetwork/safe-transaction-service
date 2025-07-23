from rest_framework.authentication import TokenAuthentication
from rest_framework.generics import ListAPIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response
from drf_spectacular.utils import extend_schema

from safe_transaction_service.analytics.services.analytics_service import (
    get_analytics_service,
)


class AnalyticsMultisigTxsByOriginListView(ListAPIView):
    pagination_class = None
    swagger_schema = None
    renderer_classes = (JSONRenderer,)
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(exclude=True)
    def get(self, request, format=None):
        analytics_service = get_analytics_service()
        return Response(analytics_service.get_safe_transactions_per_safe_app())


class AnalyticsSafeStatisticsView(ListAPIView):
    """
    Returns Safe statistics including:
    - total_safes: Total number of created Safes (all proxy factories included)
    - total_owners: Total number of owners across all Safes
    - unique_owners: Number of unique owner addresses
    - balance_wei: Total native token balance across all Safes (in Wei)
    - safes_with_balance: Number of Safes with non-zero native token balance
    - timestamp: ISO timestamp of when the statistics were last calculated
    
    This endpoint:
    - Requires token authentication
    - Does not appear in Swagger documentation
    - Returns cached data updated by periodic tasks
    """
    pagination_class = None
    swagger_schema = None
    renderer_classes = (JSONRenderer,)
    authentication_classes = [TokenAuthentication]
    permission_classes = [IsAuthenticated]

    @extend_schema(exclude=True)
    def get(self, request, format=None):
        analytics_service = get_analytics_service()
        return Response(analytics_service.get_safe_statistics())
