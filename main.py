import json
from app.config.gift_reggie_config import load_gift_reggie_config
from app.clients.gift_reggie_http_client import WishlistApiClient
from app.services.validator import WishlistValidator

from dataclasses import asdict
from datetime import datetime, timedelta, UTC

from app.config.db_connection_config import load_db_config

from app.clients.db_client import PostgresClient

from app.transforms.raw_wishlist_transform import build_raw_wishlist_rows_data, utc_now
from app.repository.raw_wishlists_repository import RawWishlistsRepository
from app.repository.raw_wishlists_org_handle_repository import RawWishlistOrgHandlesRepository
from app.transforms.raw_wishlist_org_handle_transform import build_raw_wishlist_org_handle_rows_data


def main() -> None:

    # -----------------------------
    # Load configs
    # -----------------------------
    gift_reggie_config = load_gift_reggie_config()
    db_config = load_db_config()

    # -----------------------------
    # Initialize clients
    # -----------------------------

    gift_reggie_client = WishlistApiClient(gift_reggie_config)
    db_client = PostgresClient(db_config)
    db_connection = db_client.create_connection()

    raw_wishlists_repository = RawWishlistsRepository(db_connection)
    raw_wishlist_org_handles_repository = RawWishlistOrgHandlesRepository(db_connection)


    # -----------------------------
    # Fetch API data from Gift Reggie
    # -----------------------------

    query_parameter_updated_after = datetime.now(UTC) - timedelta(hours=15)
    
    incoming_raw_items = gift_reggie_client.get_all_wishlists(
        rows=gift_reggie_config.default_rows,
        updated=query_parameter_updated_after,
    )
    
    #Testing Purpose
    print(f"\nupdated_after = {query_parameter_updated_after.isoformat()}")
    print(f"raw API items fetched = {len(incoming_raw_items)}\n")

    print("----- RAW API RESPONSE SAMPLE -----")
    print(json.dumps(incoming_raw_items[:2], indent=2, ensure_ascii=False, default=str))

    # -----------------------------
    # Validate Incoming Data
    # -----------------------------
    gift_reggie_data_validator = WishlistValidator()
    GR_validation_result = gift_reggie_data_validator.validate_data(incoming_raw_items)

    if not GR_validation_result.valid:
        print("No valid wishlists returned from API")
        return

    # -----------------------------
    # For Testing Purpose
    # -----------------------------

    print(f"Fetched: {len(incoming_raw_items)}")
    print(f"Valid:   {len(GR_validation_result.valid)}")
    print(f"Invalid: {len(GR_validation_result.invalid)}")

    print(json.dumps([w.model_dump() for w in GR_validation_result.valid], indent=2, ensure_ascii=False, default=str))

    #Show first error if any (good for debugging)
    if GR_validation_result.invalid:
        first = GR_validation_result.invalid[0]
        print("\nFirst validation failure:")
        print(f"  wishlist_id: {first.wishlist_id}")
        print(f"  error: {first.error}")

    print("\nValidation Completed\n")

    # -----------------------------
    # Transform into DB rows
    # -----------------------------
    

    run_id = 1 #
    current_time = utc_now()

    raw_wishlist_table_rows = build_raw_wishlist_rows_data(
        wishlists=GR_validation_result.valid,
        run_id=run_id,
        synced_at=current_time,
    )

    raw_wishlist_org_handle_rows = build_raw_wishlist_org_handle_rows_data(
        wishlists=GR_validation_result.valid,
        run_id=run_id,
        synced_at=current_time,
    )

    #Testing Purpose
    print(f"first-table rows formed = {len(raw_wishlist_table_rows)}")
    print(f"second-table rows formed = {len(raw_wishlist_org_handle_rows)}\n")

    print("----- FIRST TABLE ROW SAMPLE -----")
    print(json.dumps([asdict(row) for row in raw_wishlist_table_rows[:5]], indent=2, default=str))

    print("\n----- SECOND TABLE ROW SAMPLE -----")
    print(json.dumps([asdict(row) for row in raw_wishlist_org_handle_rows[:10]], indent=2, default=str))

    # -----------------------------
    # Adding and Updating DB Rows
    # -----------------------------


    try:
        raw_wishlists_repository.upsert_rows(raw_wishlist_table_rows)
        raw_wishlist_org_handles_repository.upsert_rows(raw_wishlist_org_handle_rows)

        for wishlist in GR_validation_result.valid:
            active_org_handles = list(
                {
                    product.handle
                    for product in wishlist.products
                    if product.handle
                }
            )

            raw_wishlist_org_handles_repository.mark_missing_org_handles_removed(
                wishlist_id=wishlist.id,
                active_org_handles=active_org_handles,
                removed_at=current_time,
                synced_at=current_time,
                run_id=run_id,
            )

        db_connection.commit()

    except Exception:
        db_connection.rollback()
        raise
    finally:
        db_client.connection.close()


if __name__ == "__main__":
    main()