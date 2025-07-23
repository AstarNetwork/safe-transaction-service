from django.core.management.base import BaseCommand

from safe_transaction_service.analytics.tasks import get_safe_statistics_task


class Command(BaseCommand):
    help = "Calculate and cache Safe statistics (total safes, owners, unique owners)"

    def handle(self, *args, **options):
        self.stdout.write("Calculating Safe statistics...")
        
        result = get_safe_statistics_task()
        
        if result:
            self.stdout.write(
                self.style.SUCCESS("Safe statistics calculated and cached successfully")
            )
        else:
            self.stdout.write(
                self.style.ERROR("Failed to calculate Safe statistics")
            )
