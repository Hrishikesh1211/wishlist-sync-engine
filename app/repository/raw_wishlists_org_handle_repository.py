from dataclasses import asdict
from typing import List
from uuid import UUID

from app.schemas.db_schema import RawWishlistOrgHandleRow


class RawWishlistOrgHandlesRepository:
    """
    Repository for writing rows into raw_gr_wishlist_org_handles.
    """

    def __init__(self, connection) -> None:
        self.connection = connection


    # insert a new (wishlist_id, org_handle) if it does not exist
    # update existing (wishlist_id, org_handle) if it still exists
    # clear removed_at back to NULL if a previously removed row reappears
    def upsert_rows(self, rows: List[RawWishlistOrgHandleRow]) -> None:
        """
        Batch upsert rows into raw_gr_wishlist_org_handles.
        """
        if not rows:
            return

        sql = """
        INSERT INTO raw_gr_wishlist_org_handles (
            wishlist_id,
            org_handle,
            source_updated_at,
            last_seen_at,
            removed_at,
            synced_at,
            last_run_id
        )
        VALUES (
            %(wishlist_id)s,
            %(org_handle)s,
            %(source_updated_at)s,
            %(last_seen_at)s,
            %(removed_at)s,
            %(synced_at)s,
            %(last_run_id)s
        )
        ON CONFLICT (wishlist_id, org_handle)
        DO UPDATE SET
            source_updated_at = EXCLUDED.source_updated_at,
            last_seen_at = EXCLUDED.last_seen_at,
            removed_at = EXCLUDED.removed_at,
            synced_at = EXCLUDED.synced_at,
            last_run_id = EXCLUDED.last_run_id
        """

        payload = [asdict(row) for row in rows]

        with self.connection.cursor() as cursor:
            cursor.executemany(sql, payload)


    def mark_missing_org_handles_removed(
        self,
        *,
        wishlist_id: int,
        active_org_handles: List[str],
        removed_at,
        synced_at,
        run_id: UUID,
    ) -> None:
        """
        Mark org handles as removed for a wishlist if they are no longer present
        in the latest API response for that wishlist.
        """
        if active_org_handles:
            sql = """
            UPDATE raw_gr_wishlist_org_handles
            SET
                removed_at = %(removed_at)s,
                synced_at = %(synced_at)s,
                last_run_id = %(last_run_id)s
            WHERE wishlist_id = %(wishlist_id)s
              AND org_handle <> ALL(%(active_org_handles)s)
              AND removed_at IS NULL
            """
            params = {
                "wishlist_id": wishlist_id,
                "active_org_handles": active_org_handles,
                "removed_at": removed_at,
                "synced_at": synced_at,
                "last_run_id": run_id,
            }
        else:
            sql = """
            UPDATE raw_gr_wishlist_org_handles
            SET
                removed_at = %(removed_at)s,
                synced_at = %(synced_at)s,
                last_run_id = %(last_run_id)s
            WHERE wishlist_id = %(wishlist_id)s
              AND removed_at IS NULL
            """
            params = {
                "wishlist_id": wishlist_id,
                "removed_at": removed_at,
                "synced_at": synced_at,
                "last_run_id": run_id,
            }

        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)