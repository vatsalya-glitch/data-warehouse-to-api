-- Phase 2: Build domain table — Patron Profile
-- Enrichment domain: LEFT JOINed onto loans (driving domain)

TRUNCATE TABLE staging.sample_patron_profile;

INSERT INTO staging.sample_patron_profile
SELECT
    patron_id,
    patron_name,
    patron_email,
    membership_tier,
    signup_date,
    CURRENT_TIMESTAMP() as _loaded_at
FROM raw.library_patrons
WHERE patron_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY patron_id ORDER BY updated_at DESC) = 1;

-- TODO: replace with real patron source, columns, and dedup tie-breaker
