from django.test import RequestFactory, TestCase, override_settings
from django.views.debug import get_exception_reporter_filter

from ...loggers.exception_filters import CustomExceptionReporterFilter


class TestCustomExceptionReporterFilter(TestCase):
    def test_configured_as_default_filter(self):
        self.assertIsInstance(
            get_exception_reporter_filter(None), CustomExceptionReporterFilter
        )

    def test_django_default_keys_are_kept(self):
        exception_filter = CustomExceptionReporterFilter()
        for key in ("SECRET_KEY", "AWS_SECRET_ACCESS_KEY", "ENS_SUBGRAPH_API_KEY"):
            with self.subTest(key=key):
                self.assertEqual(
                    exception_filter.cleanse_setting(key, "sensitive-value"),
                    exception_filter.cleansed_substitute,
                )

    def test_extra_keys_are_cleansed(self):
        exception_filter = CustomExceptionReporterFilter()
        for key in ("CELERY_BROKER_URL", "EVENTS_QUEUE_URL", "REDIS_URL"):
            with self.subTest(key=key):
                self.assertEqual(
                    exception_filter.cleanse_setting(key, "sensitive-value"),
                    exception_filter.cleansed_substitute,
                )

    def test_non_sensitive_keys_are_not_cleansed(self):
        exception_filter = CustomExceptionReporterFilter()
        for key in (
            "ETH_L2_NETWORK",
            "REDIS_TIMEOUT_SECONDS",
            "STATIC_URL",
            "TIME_ZONE",
        ):
            with self.subTest(key=key):
                self.assertEqual(
                    exception_filter.cleanse_setting(key, "public-value"),
                    "public-value",
                )

    def test_nested_settings_are_cleansed(self):
        exception_filter = CustomExceptionReporterFilter()
        cleansed = exception_filter.cleanse_setting(
            "DATABASES",
            {"default": {"NAME": "postgres", "PASSWORD": "sensitive-value"}},
        )
        self.assertEqual(cleansed["default"]["NAME"], "postgres")
        self.assertEqual(
            cleansed["default"]["PASSWORD"], exception_filter.cleansed_substitute
        )

    @override_settings(DEBUG=False)
    def test_request_meta_is_cleansed(self):
        request = RequestFactory().get(
            "/", headers={"authorization": "Bearer sensitive-value"}
        )
        request.META["REDIS_URL"] = "redis://user:sensitive-value@redis:6379/0"
        exception_filter = CustomExceptionReporterFilter()
        safe_meta = exception_filter.get_safe_request_meta(request)
        self.assertEqual(
            safe_meta["HTTP_AUTHORIZATION"], exception_filter.cleansed_substitute
        )
        self.assertEqual(safe_meta["REDIS_URL"], exception_filter.cleansed_substitute)
