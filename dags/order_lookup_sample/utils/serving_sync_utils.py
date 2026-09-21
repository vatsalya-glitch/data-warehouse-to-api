"""Serving database sync utilities for order_lookup_sample pipeline.

This module contains stub interfaces for export, import, and lifecycle tasks.
Replace all TODO sections with real storage and database operations.
"""

from typing import List


def list_export_shards(storage_bucket: str, export_folder: str) -> List[str]:
    """
    Return the object-storage URIs produced by the export step.

    This is called before parallel import to discover what shards were created.

    Args:
        storage_bucket: Bucket name (e.g., "ecommerce-order-lookup-sample-exports")
        export_folder: Folder within bucket (e.g., "ecommerce_orders/exports")

    Returns:
        List of URIs (e.g., ["gs://bucket/folder/shard_001.parquet", ...])
    """
    # TODO: list objects in object storage
    # Example for GCS:
    #   from google.cloud import storage
    #   client = storage.Client()
    #   blobs = client.list_blobs(storage_bucket, prefix=export_folder)
    #   return [f"gs://{storage_bucket}/{blob.name}" for blob in blobs]
    return []


def import_single_shard(
    serving_db_conn,
    shard_uri: str,
    staging_table: str
) -> None:
    """
    Bulk-load one shard into the serving DB's staging table.

    Called in parallel for each shard (up to import_max_concurrency).

    Args:
        serving_db_conn: Connection to serving database
        shard_uri: URI of the shard file to import (e.g., "gs://bucket/shard.parquet")
        staging_table: Name of staging table to import into
    """
    # TODO: bulk-load one shard
    # Example for Postgres + Parquet:
    #   # Download shard from storage
    #   # Parse Parquet file
    #   # COPY INTO staging_table VALUES (...)
    pass


def validate_staging_indexes(
    serving_db_conn,
    staging_table: str,
    expected_indexes: List[dict]
) -> None:
    """
    Confirm every configured index actually exists on staging before swap.

    This is a safety check: if an index failed to build, we know before
    trying to swap.

    Args:
        serving_db_conn: Connection to serving database
        staging_table: Name of staging table
        expected_indexes: List of index definitions from config
                         Each: {"name": "idx_name", "columns": [...], "unique": true/false}

    Raises:
        IndexValidationError: if any expected index is missing
    """
    # TODO: query information_schema
    # Example for Postgres:
    #   SELECT indexname FROM pg_indexes
    #   WHERE tablename = staging_table
    # Check that every expected_index["name"] exists in this result
    pass


def atomic_swap(
    serving_db_conn,
    live_table: str,
    staging_table: str
) -> None:
    """
    Atomic table swap (rename staging to live) inside one transaction.

    This is the key trick of the whole pattern: it's a catalog-only operation
    (zero downtime), so the live table is NEVER empty or half-updated from
    the application's perspective.

    SQL executed:
      BEGIN;
        ALTER TABLE live_table RENAME TO live_table_old;
        ALTER TABLE staging_table RENAME TO live_table;
      COMMIT;

    Args:
        serving_db_conn: Connection to serving database
        live_table: Name of the live table
        staging_table: Name of the staging table
    """
    # TODO: execute in one transaction
    # BEGIN;
    # ALTER TABLE {live_table} RENAME TO {live_table}_old;
    # ALTER TABLE {staging_table} RENAME TO {live_table};
    # COMMIT;
    pass


def cleanup_export_shards(
    storage_client,
    storage_bucket: str,
    export_folder: str
) -> None:
    """
    Delete this run's export files from object storage.

    Called after successful import to free up storage.

    Args:
        storage_client: Connection to object storage
        storage_bucket: Bucket name
        export_folder: Folder to clean up
    """
    # TODO: delete all objects in folder
    # Example for GCS:
    #   blobs = storage_client.list_blobs(storage_bucket, prefix=export_folder)
    #   for blob in blobs:
    #       blob.delete()
    pass


def cleanup_old_tables(
    serving_db_conn,
    live_table: str,
    old_table_suffix: str = "_old",
    old_old_table_suffix: str = "_old_old"
) -> None:
    """
    Drop the oldest backup table (keep previous cycle only).

    Called at the END of a successful run to clean up the table from
    TWO cycles ago. This ensures we always have one prior cycle available
    for manual rollback.

    Args:
        serving_db_conn: Connection to serving database
        live_table: Name of the live table
        old_table_suffix: Suffix for one-cycle-old table (default: "_old")
        old_old_table_suffix: Suffix for two-cycle-old table (default: "_old_old")
    """
    # TODO: drop old backup table
    # DROP TABLE {live_table}{old_old_table_suffix} IF EXISTS
    pass
