from datetime import datetime, UTC
from typing import List
from uuid import UUID

from app.schemas.db_schema import RawWishlistRow
from app.schemas.gift_reggie_schema import WishlistDTO



def utc_now() -> datetime:
    return datetime.now(UTC)


def build_raw_wishlist_single_row(
    wishlist: WishlistDTO,
    run_id: UUID,
    synced_at: datetime,
) -> RawWishlistRow:
    """
    Transform one validated WishlistDTO into one raw_gr_wishlists row.
    """

    owner = wishlist.owner

    return RawWishlistRow(
        wishlist_id=wishlist.id,
        email=owner.email if owner else None,
        first_name=owner.first_name if owner else None,
        last_name=owner.last_name if owner else None,
        source_created_at=wishlist.created,
        source_updated_at=wishlist.updated,
        last_seen_at=synced_at,
        synced_at=synced_at,
        last_run_id=run_id,
    )


def build_raw_wishlist_rows_data(
    wishlists: List[WishlistDTO],
    run_id: UUID,
    synced_at: datetime,
) -> List[RawWishlistRow]:
    """
    Transform a batch of validated WishlistDTO objects into raw_gr_wishlists rows.
    """

    rows: List[RawWishlistRow] = []

    for wishlist in wishlists:
        row = build_raw_wishlist_single_row(
            wishlist=wishlist,
            run_id=run_id,
            synced_at=synced_at,
        )
        rows.append(row)

    return rows