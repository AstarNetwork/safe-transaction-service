from django.urls import path

from . import views_v2

app_name = "analytics"

urlpatterns = [
    path(
        "multisig-transactions/by-origin/",
        views_v2.AnalyticsMultisigTxsByOriginListView.as_view(),
        name="analytics-multisig-txs-by-origin",
    ),
    path(
        "safe-statistics/",
        views_v2.AnalyticsSafeStatisticsView.as_view(),
        name="analytics-safe-statistics",
    ),
]
