-- Phase 2: Final lookup assembly
-- Join driving domain (loans) with enrichment domains (patrons, books)
-- Uses LEFT JOINs to prevent silent row loss

SELECT
    l.loan_id,
    l.patron_id,
    l.book_id,
    l.loan_date,
    l.due_date,
    l.return_date,
    p.patron_name,
    p.patron_email,
    p.membership_tier,
    p.signup_date,
    b.book_title,
    b.author,
    b.isbn,
    CURRENT_TIMESTAMP() as _loaded_at
FROM staging.sample_360_loan_records l
LEFT JOIN staging.sample_360_patron_profile p
    ON l.patron_id = p.patron_id
LEFT JOIN staging.sample_360_book_inventory b
    ON l.book_id = b.book_id
WHERE l.loan_id IS NOT NULL;

-- TODO: replace with real join logic
-- Key pattern: LEFT JOIN from driving domain (loans) onto enrichments
-- This prevents silent row loss if a patron/book is missing from enrichment tables
