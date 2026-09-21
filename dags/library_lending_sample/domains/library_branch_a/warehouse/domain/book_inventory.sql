-- Phase 2: Build domain table — Book Inventory
-- Enrichment domain: LEFT JOINed onto loans (driving domain)

TRUNCATE TABLE staging.sample_book_inventory;

INSERT INTO staging.sample_book_inventory
SELECT
    book_id,
    book_title,
    author,
    isbn,
    publication_year,
    CURRENT_TIMESTAMP() as _loaded_at
FROM raw.library_inventory
WHERE book_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY book_id ORDER BY updated_at DESC) = 1;

-- TODO: replace with real book source and columns
