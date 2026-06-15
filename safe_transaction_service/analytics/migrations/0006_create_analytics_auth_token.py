"""Create the analytics API auth token from the ``ANALYTICS_AUTH_TOKEN`` env var.

The ``/v2/analytics/`` endpoints require DRF ``TokenAuthentication``; until now
the token had to be created by hand (admin or ``drf_create_token``). This
migration lets deployments provision it declaratively: if ``ANALYTICS_AUTH_TOKEN``
is set when ``migrate`` runs, a dedicated ``analytics`` service user is created
(no password, token-only) and the token is stored with that exact key. If the
env var is unset the migration is a no-op, so test runs and instances that
manage tokens manually are unaffected.

Note: migrations run once per database. Rotating the token later means deleting
the row (admin or shell) and re-creating it — changing the env var alone won't
re-trigger this.
"""

import os

from django.contrib.auth.hashers import make_password
from django.db import migrations

ENV_VAR = "ANALYTICS_AUTH_TOKEN"
SERVICE_USERNAME = "analytics"


def create_analytics_token(apps, schema_editor):
    key = os.environ.get(ENV_VAR)
    if not key:
        return
    User = apps.get_model("auth", "User")
    Token = apps.get_model("authtoken", "Token")
    user, _ = User.objects.get_or_create(
        username=SERVICE_USERNAME,
        defaults={"is_active": True, "password": make_password(None)},
    )
    # Token.user is a OneToOneField: get_or_create keeps an existing token
    # (manually provisioned or from a previous run) instead of failing.
    Token.objects.get_or_create(user=user, defaults={"key": key})


class Migration(migrations.Migration):
    dependencies = [
        ("analytics", "0005_drop_safe_statistics_snapshot"),
        ("auth", "0001_initial"),
        ("authtoken", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(create_analytics_token, migrations.RunPython.noop),
    ]
