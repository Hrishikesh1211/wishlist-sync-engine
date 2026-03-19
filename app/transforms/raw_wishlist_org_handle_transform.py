from datetime import datetime
from typing import List
from uuid import UUID

from app.schemas.db_schema import RawWishlistOrgHandleRow
from app.schemas.gift_reggie_schema import WishlistDTO


def build_raw_wishlist_org_handle_rows_for_single_wishlist(
    wishlist: WishlistDTO,
    run_id: UUID,
    synced_at: datetime,
) -> List[RawWishlistOrgHandleRow]:
    rows: List[RawWishlistOrgHandleRow] = []

    unique_org_handles = {
        product.handle
        for product in wishlist.products
        if product.handle
    }

    for org_handle in unique_org_handles:
        row = RawWishlistOrgHandleRow(
            wishlist_id=wishlist.id,
            org_handle=org_handle,
            source_updated_at=wishlist.updated,
            last_seen_at=synced_at,
            removed_at=None,
            synced_at=synced_at,
            last_run_id=run_id,
        )
        rows.append(row)

    return rows


def build_raw_wishlist_org_handle_rows_data(
    wishlists: List[WishlistDTO],
    run_id: UUID,
    synced_at: datetime,
) -> List[RawWishlistOrgHandleRow]:
    """
    Transform a batch of validated WishlistDTO objects into
    raw_gr_wishlist_org_handles rows.
    """
    rows: List[RawWishlistOrgHandleRow] = []

    for wishlist in wishlists:
        wishlist_rows = build_raw_wishlist_org_handle_rows_for_single_wishlist(
            wishlist=wishlist,
            run_id=run_id,
            synced_at=synced_at,
        )
        rows.extend(wishlist_rows)

    return rows