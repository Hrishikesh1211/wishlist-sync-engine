from dataclasses import asdict
from typing import List

from app.schemas.db_schema import RawWishlistRow


class RawWishlistsRepository:
    """
    Repository for writing rows into raw_gr_wishlists.
    """

    def __init__(self, connection) -> None:
        self.connection = connection

    def upsert_rows(self, rows: List[RawWishlistRow]) -> None:
        """
        Batch upsert rows into raw_gr_wishlists.
        """
        if not rows:
            return

        sql = """
        INSERT INTO raw_gr_wishlists (
            wishlist_id,
            email,
            first_name,
            last_name,
            source_created_at,
            source_updated_at,
            last_seen_at,
            synced_at,
            last_run_id
        )
        VALUES (
            %(wishlist_id)s,
            %(email)s,
            %(first_name)s,
            %(last_name)s,
            %(source_created_at)s,
            %(source_updated_at)s,
            %(last_seen_at)s,
            %(synced_at)s,
            %(last_run_id)s
        )
        ON CONFLICT (wishlist_id)
        DO UPDATE SET
            email = EXCLUDED.email,
            source_updated_at = EXCLUDED.source_updated_at,
            last_seen_at = EXCLUDED.last_seen_at,
            synced_at = EXCLUDED.synced_at,
            last_run_id = EXCLUDED.last_run_id
        """
# For conflict - Remove all the columns from above that we 
# donot want to update if the row already exists.
        payload = [asdict(row) for row in rows]

        with self.connection.cursor() as cursor:
            cursor.executemany(sql, payload)