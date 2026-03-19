from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID


@dataclass(frozen=True)
class RawWishlistRow:
    """
    Exact row shape for raw_gr_wishlists.
    One row per wishlist.
    """
    wishlist_id: int
    email: Optional[str]
    first_name: Optional[str]
    last_name: Optional[str]
    source_created_at: datetime
    source_updated_at: Optional[datetime]
    last_seen_at: datetime
    synced_at: datetime
    last_run_id: UUID


@dataclass(frozen=True)
class RawWishlistOrgHandleRow:
    """
    Exact row shape for raw_gr_wishlist_org_handles.
    One row per (wishlist_id, org_handle).
    """
    wishlist_id: int
    org_handle: str
    source_updated_at: Optional[datetime]
    last_seen_at: datetime
    removed_at: Optional[datetime]
    synced_at: datetime
    last_run_id: UUID

@dataclass(frozen=True)
class LastSyncRow:
    sync_name: str
    last_run_id: UUID
    last_run_time: datetime