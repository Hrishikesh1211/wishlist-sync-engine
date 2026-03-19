from typing import Optional

from app.schemas.db_schema import LastSyncRow


class LastSyncRepository:
    def __init__(self, connection) -> None:
        self.connection = connection

    def get_last_sync(self, sync_name: str) -> Optional[LastSyncRow]:
        sql = """
        SELECT sync_name, last_run_id, last_run_time
        FROM last_sync
        WHERE sync_name = %s
        """

        with self.connection.cursor() as cursor:
            cursor.execute(sql, (sync_name,))
            row = cursor.fetchone()

        if not row:
            return None

        return LastSyncRow(
            sync_name=row[0],
            last_run_id=row[1],
            last_run_time=row[2],
        )

    def upsert_last_sync(self, row: LastSyncRow) -> None:
        sql = """
        INSERT INTO last_sync (
            sync_name,
            last_run_id,
            last_run_time
        )
        VALUES (
            %(sync_name)s,
            %(last_run_id)s,
            %(last_run_time)s
        )
        ON CONFLICT (sync_name)
        DO UPDATE SET
            last_run_id = EXCLUDED.last_run_id,
            last_run_time = EXCLUDED.last_run_time
        """

        with self.connection.cursor() as cursor:
            cursor.execute(sql, {
                "sync_name": row.sync_name,
                "last_run_id": row.last_run_id,
                "last_run_time": row.last_run_time,
            })