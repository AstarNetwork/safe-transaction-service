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
        "summary/",
        views_v2.AnalyticsSummaryView.as_view(),
        name="analytics-summary",
    ),
    path(
        "active-safes/",
        views_v2.AnalyticsActiveSafesView.as_view(),
        name="analytics-active-safes",
    ),
    path(
        "safe-creations/",
        views_v2.AnalyticsSafeCreationsView.as_view(),
        name="analytics-safe-creations",
    ),
    path(
        "active-owners/",
        views_v2.AnalyticsActiveOwnersView.as_view(),
        name="analytics-active-owners",
    ),
    path(
        "tx-volume/",
        views_v2.AnalyticsTxVolumeView.as_view(),
        name="analytics-tx-volume",
    ),
    path(
        "safe-segments/",
        views_v2.AnalyticsSafeSegmentsView.as_view(),
        name="analytics-safe-segments",
    ),
    path(
        "tvl/",
        views_v2.AnalyticsTvlView.as_view(),
        name="analytics-tvl",
    ),
    path(
        "token-volume/",
        views_v2.AnalyticsTokenVolumeView.as_view(),
        name="analytics-token-volume",
    ),
]
