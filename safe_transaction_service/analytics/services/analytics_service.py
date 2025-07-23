import json
from functools import cache

from safe_transaction_service.utils.redis import get_redis


@cache
def get_analytics_service() -> "AnalyticsService":
    return AnalyticsService()


class AnalyticsService:
    REDIS_TRANSACTIONS_PER_SAFE_APP = "analytics_transactions_per_safe_app"
    REDIS_SAFE_STATISTICS = "analytics_safe_statistics"

    def get_safe_transactions_per_safe_app(self) -> list[dict]:
        redis = get_redis()
        analytic_result = redis.get(self.REDIS_TRANSACTIONS_PER_SAFE_APP)
        if analytic_result:
            return json.loads(analytic_result)
        else:
            return []

    def get_safe_statistics(self) -> dict:
        redis = get_redis()
        analytic_result = redis.get(self.REDIS_SAFE_STATISTICS)
        if analytic_result:
            return json.loads(analytic_result)
        else:
            return {
                "total_safes": 0,
                "total_owners": 0,
                "unique_owners": 0,
                "balance_wei": 0,
                "safes_with_balance": 0,
                "timestamp": None
            }
