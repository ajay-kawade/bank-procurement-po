-- scd2_insert_vendor_versions.sql
-- SCD2 pass 2 of 2: insert a new active row for
--   (a) vendors whose attributes changed (just expired in pass 1), and
--   (b) brand-new vendor_ids with no existing row.

INSERT INTO `{{ var.value.project_id }}.{{ var.value.curated_dataset }}.dim_vendor`
(
    vendor_sk, vendor_id, vendor_name, address, payment_terms,
    compliance_rating, preferred_status,
    effective_start_date, effective_end_date, is_current, attribute_hash
)
SELECT
    GENERATE_UUID()    AS vendor_sk,
    s.vendor_id,
    s.vendor_name,
    s.address,
    s.payment_terms,
    s.compliance_rating,
    s.preferred_status,
    CURRENT_DATE()     AS effective_start_date,
    NULL               AS effective_end_date,
    TRUE               AS is_current,
    TO_HEX(MD5(CONCAT(
        COALESCE(s.address,           ''),
        COALESCE(s.payment_terms,     ''),
        COALESCE(s.compliance_rating, ''),
        COALESCE(s.preferred_status,  '')
    )))                AS attribute_hash
FROM `your-project.staging.stg_vendor` s
LEFT JOIN `your-project.curated.dim_vendor` d
    ON s.vendor_id = d.vendor_id AND d.is_current = TRUE
WHERE d.vendor_id IS NULL;  -- true for: brand-new vendors, and vendors
                             -- just expired in pass 1 needing a replacement row
