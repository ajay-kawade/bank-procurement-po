-- load_staging_po_line_items.sql
-- Raw (GCS, via external table) -> Staging.

INSERT INTO `your-project.staging.stg_po_line_items`
SELECT
    po_line_id,
    po_number,
    vendor_id,
    product_id,
    quantity_ordered,
    unit_price,
    quantity_ordered * unit_price AS line_amount,
    po_status,
    order_ts,
    CURRENT_TIMESTAMP() AS load_ts,
    dt
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY po_line_id ORDER BY ingestion_ts DESC) AS rn
    FROM `your-project.raw_external.po_line_items_external`
    WHERE dt = '{{ ds }}'
)
WHERE rn = 1;
