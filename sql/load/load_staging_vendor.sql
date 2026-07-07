-- load_staging_vendor.sql
-- Raw (GCS, via external table) -> Staging.
-- Dedup keeps the latest record per vendor_id in case the source sends
-- the same vendor more than once in a single extract.

INSERT INTO `your-project.staging.stg_vendor`
SELECT
    vendor_id,
    vendor_name,
    address,
    payment_terms,
    compliance_rating,
    preferred_status,
    CURRENT_TIMESTAMP() AS load_ts,
    dt
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY vendor_id ORDER BY ingestion_ts DESC) AS rn
    FROM `your-project.raw_external.vendor_master_external`
    WHERE dt = '{{ ds }}'
)
WHERE rn = 1;
