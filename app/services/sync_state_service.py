from datetime import UTC, datetime, timedelta
from typing import Optional

from app.repository.last_sync_repository import LastSyncRepository


GR_INITIAL_BACKFILL_START = datetime(2024, 1, 1, tzinfo=UTC)
SYNC_NAME = "gift_reggie_wishlist_sync"

class SyncStateService:
    def __init__(self, last_sync_repository: LastSyncRepository) -> None:
        self.last_sync_repository = last_sync_repository

    def get_api_updated_after(self) -> datetime:
        last_sync_row = self.last_sync_repository.get_last_sync(SYNC_NAME)

        last_successful_run_time: Optional[datetime] = None
        if last_sync_row is not None:
            last_successful_run_time = last_sync_row.last_run_time

        print(f"Last successful run time from last_sync table: {last_successful_run_time}")

        if last_successful_run_time is None:
            last_successful_run_time = GR_INITIAL_BACKFILL_START

        print(f"Effective watermark used for this run: {last_successful_run_time}")

        query_parameter_updated_after = last_successful_run_time - timedelta(minutes=2)

        print(
            f"API parameter updated_after (after applying 2-minute overlap): "
            f"{query_parameter_updated_after}"
        )

        return query_parameter_updated_after