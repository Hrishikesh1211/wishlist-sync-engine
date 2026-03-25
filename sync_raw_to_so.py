import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT"),
    sslmode="require"   # IMPORTANT for Neon
)

cur = conn.cursor()

print("Starting sync process...")

cur.execute("""
SELECT wishlist_id, first_name, last_name
FROM raw_gr_wishlists;
""")
rows = cur.fetchall()

print(f"Found {len(rows)} wishlists")

for wishlist_id, first_name, last_name in rows:
    cur.execute("""
    INSERT INTO so_students (so_student_id, first_name, last_name)
    VALUES (%s, %s, %s)
    ON CONFLICT (so_student_id) DO UPDATE
    SET first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        updated_at = now();
    """, (wishlist_id, first_name, last_name))

    cur.execute("""
    SELECT org_handle
    FROM raw_gr_wishlist_org_handles
    WHERE wishlist_id = %s
      AND removed_at IS NULL;
    """, (wishlist_id,))
    orgs = cur.fetchall()

    cur.execute("""
    DELETE FROM so_favorite_edges
    WHERE so_student_id = %s;
    """, (wishlist_id,))

    for (org_handle,) in orgs:
        cur.execute("""
        INSERT INTO so_favorite_edges (so_student_id, org_handle)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING;
        """, (wishlist_id, org_handle))

conn.commit()
cur.close()
conn.close()

print("Sync completed successfully!")