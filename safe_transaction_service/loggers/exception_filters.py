import re

from django.views.debug import SafeExceptionReporterFilter

# Setting/`request.META` name fragments considered sensitive on top of the ones
# Django already ships with (`API|AUTH|TOKEN|KEY|SECRET|PASS|SIGNATURE|HTTP_COOKIE`).
# They all hold connection URIs with credentials embedded on them. Matching is
# case insensitive and done as a substring search on the key name.
EXTRA_HIDDEN_SETTINGS = (
    "BROKER_URL",  # CELERY_BROKER_URL
    "QUEUE_URL",  # EVENTS_QUEUE_URL
    "REDIS_URL",  # REDIS_URL
)


class CustomExceptionReporterFilter(SafeExceptionReporterFilter):
    """
    `SafeExceptionReporterFilter` with an extended list of sensitive keys.

    It only affects Django error reports (technical 500 page, `mail_admins`
    reports and `get_safe_settings`), it does **not** redact regular
    application logs.

    https://docs.djangoproject.com/en/dev/howto/error-reporting/#filtering-error-reports
    """

    cleansed_substitute = "*****"
    # Built on top of Django's own pattern so upstream additions are kept
    hidden_settings = re.compile(
        SafeExceptionReporterFilter.hidden_settings.pattern
        + "|"
        + "|".join(EXTRA_HIDDEN_SETTINGS),
        flags=re.I,
    )
