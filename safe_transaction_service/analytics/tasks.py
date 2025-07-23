import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from django.conf import settings
from django.db.models import Count, F, Q
from django.utils import timezone

from celery import app
from dateutil.relativedelta import relativedelta

from safe_transaction_service.analytics.services.analytics_service import (
    AnalyticsService,
)
from safe_transaction_service.history.models import MultisigTransaction, SafeContract, SafeLastStatus
from safe_transaction_service.history.services.balance_service import BalanceService, BalanceServiceProvider
from safe_transaction_service.utils.celery import task_timeout
from safe_transaction_service.utils.redis import get_redis
from safe_transaction_service.utils.tasks import LOCK_TIMEOUT
from safe_transaction_service.utils.utils import chunks

logger = logging.getLogger(__name__)


def _get_native_balance_batch(safe_addresses: list[str], balance_service: BalanceService) -> tuple[int, int]:
    """
    Get native token balances for a batch of Safe addresses.
    
    :param safe_addresses: List of Safe addresses to get balances for
    :param balance_service: BalanceService instance
    :return: Tuple of (total_balance_wei, safes_with_balance_count)
    """
    batch_total_balance = 0
    batch_safes_with_balance = 0
    
    for address in safe_addresses:
        try:
            # Use balance_service.get_balances to get native token balance
            balances, _ = balance_service.get_balances(address)
            
            # Find the native token balance (token_address is None for native token)
            native_balance = 0
            for balance in balances:
                if balance.token_address is None:  # Native token (ETH)
                    native_balance = balance.balance
                    break
            
            batch_total_balance += native_balance
            if native_balance > 0:
                batch_safes_with_balance += 1
        except Exception as e:
            logger.warning(f"Failed to get balance for Safe {address}: {e}")
            continue
    
    return batch_total_balance, batch_safes_with_balance


def _calculate_native_balances_batched() -> tuple[int, int]:
    """
    Calculate native token balances for all Safes using database batching and concurrent processing.
    This approach is memory-efficient for processing millions of Safe addresses.
    
    :return: Tuple of (total_balance_wei, total_safes_with_balance)
    """
    db_batch_size = 1000
    balance_batch_size = 50
    max_workers = 5
    
    balance_service = BalanceServiceProvider()
    total_balance_wei = 0
    total_safes_with_balance = 0
    processed_count = 0
    
    # Get total count for progress tracking
    total_safes = SafeContract.objects.count()
    logger.info(f"Starting batched balance calculation for {total_safes} Safes")
    
    # Process Safe addresses in database batches to avoid loading all into memory
    queryset = SafeContract.objects.values_list('address', flat=True).order_by('address')
    
    offset = 0
    while True:
        # Fetch a batch of addresses from the database
        safe_addresses_batch = list(queryset[offset:offset + db_batch_size])
        
        if not safe_addresses_batch:
            break  # No more addresses to process
        
        logger.info(f"Processing database batch {offset // db_batch_size + 1}: "
                   f"{len(safe_addresses_batch)} addresses (offset: {offset})")
        
        # Process this database batch using concurrent balance checking
        try:
            # Split the database batch into smaller chunks for concurrent processing
            address_chunks = list(chunks(safe_addresses_batch, balance_batch_size))
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all balance checking jobs for this database batch
                future_to_chunk = {
                    executor.submit(_get_native_balance_batch, chunk, balance_service): chunk
                    for chunk in address_chunks
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_chunk):
                    chunk = future_to_chunk[future]
                    try:
                        batch_balance, batch_safes_with_balance = future.result()
                        total_balance_wei += batch_balance
                        total_safes_with_balance += batch_safes_with_balance
                        processed_count += len(chunk)
                    except Exception as e:
                        logger.error(f"Balance checking failed for chunk: {e}")
                        processed_count += len(chunk)  # Still count as processed
                        continue
            
            logger.info(f"Completed database batch. Total processed: {processed_count}/{total_safes} Safes. "
                       f"Running total balance: {total_balance_wei} wei")
        
        except Exception as e:
            logger.error(f"Database batch processing failed at offset {offset}: {e}")
            # Continue with next batch rather than failing completely
        
        offset += db_batch_size
    
    logger.info(f"Batched balance calculation completed. Final results: "
               f"Total balance: {total_balance_wei} wei, "
               f"Safes with balance: {total_safes_with_balance}/{processed_count}")
    
    return total_balance_wei, total_safes_with_balance


@app.shared_task()
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def get_transactions_per_safe_app_task():
    today = timezone.now()
    last_week = today - relativedelta(days=7)
    last_month = today - relativedelta(months=1)
    last_year = today - relativedelta(years=1)

    queryset = (
        MultisigTransaction.objects.filter(origin__name__isnull=False)
        .values(name=F("origin__name"), url=F("origin__url"))
        .annotate(
            total_tx=Count("origin__name"),
            tx_last_week=Count("origin__name", filter=Q(created__gt=last_week)),
            tx_last_month=Count("origin__name", filter=Q(created__gt=last_month)),
            tx_last_year=Count("origin__name", filter=Q(created__gt=last_year)),
        )
        .order_by("-total_tx")
    )

    if queryset:
        redis_key = AnalyticsService.REDIS_TRANSACTIONS_PER_SAFE_APP
        redis = get_redis()
        redis.set(redis_key, json.dumps(list(queryset)))
        return True
    return False


@app.shared_task()
@task_timeout(timeout_seconds=LOCK_TIMEOUT)
def get_safe_statistics_task():
    """
    Calculate Safe statistics including:
    - Total number of created Safes (all proxy factories included)
    - Total number of owners
    - Number of unique owners
    - Total native token balance across all Safes
    - Number of Safes with non-zero balance
    """
    try:
        # Total number of created Safes (all proxy factories included)
        total_safes = SafeContract.objects.count()
        
        # Get all owners from SafeLastStatus to get current state
        # This gives us the most up-to-date owner information for each Safe
        owners_data = SafeLastStatus.objects.exclude(
            owners__isnull=True
        ).exclude(
            owners=[]
        ).values_list('owners', flat=True)
        
        # Count total owners and unique owners
        all_owners = []
        for owner_list in owners_data:
            if owner_list:  # Ensure the list is not empty
                all_owners.extend(owner_list)
        
        total_owners = len(all_owners)
        unique_owners = len(set(all_owners))
        
        # Calculate native token balances for all Safes
        logger.info(f"Starting balance calculation for {total_safes} Safes")
        
        total_balance_wei = 0
        total_safes_with_balance = 0
        
        if total_safes > 0:
            try:
                total_balance_wei, total_safes_with_balance = _calculate_native_balances_batched()
                logger.info(
                    f"Balance calculation completed. Total balance: {total_balance_wei} wei, "
                    f"Safes with balance: {total_safes_with_balance}/{total_safes}"
                )
            except Exception as e:
                logger.error(f"Failed to calculate balances: {e}")
                # Continue without balance data rather than failing the entire task
        
        statistics = {
            "total_safes": total_safes,
            "total_owners": total_owners,
            "unique_owners": unique_owners,
            "balance_wei": total_balance_wei,
            "safes_with_balance": total_safes_with_balance,
            "timestamp": timezone.now().isoformat(),
        }
        
        redis_key = AnalyticsService.REDIS_SAFE_STATISTICS
        redis = get_redis()
        redis.set(redis_key, json.dumps(statistics))
        return True
    except Exception as e:
        logger.error(f"Safe statistics task failed: {e}")
        # In case of any error, return False but don't raise
        # This prevents the task from failing completely
        return False
