-- scd2_expire_vendors.sql
-- SCD2 pass 1 of 2: expire any active dim_vendor row whose tracked
-- attributes no longer match the latest staging snapshot.
--
-- Why COALESCE on every field in the hash:
-- CONCAT returns NULL if ANY argument is NULL (standard SQL behavior).
-- Without COALESCE, a vendor with a NULL payment_terms would always
-- produce a NULL hash, NULL != NULL is never TRUE in SQL, so change
-- detection silently stops firing for that vendor forever.
-- COALESCE(..., '') replaces NULL with an empty string so the hash is
-- always a real value, never NULL.

UPDATE `your-project.curated.dim_vendor` AS target
SET
    is_current = FALSE,
    effective_end_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
WHERE target.is_current = TRUE
  AND target.vendor_id IN (
    SELECT s.vendor_id
    FROM `your-project.staging.stg_vendor` s
    JOIN `your-project.curated.dim_vendor` d
        ON s.vendor_id = d.vendor_id AND d.is_current = TRUE
    WHERE TO_HEX(MD5(CONCAT(
            COALESCE(s.address,           ''),
            COALESCE(s.payment_terms,     ''),
            COALESCE(s.compliance_rating, ''),
            COALESCE(s.preferred_status,  '')
          )))
          != d.attribute_hash
  );
