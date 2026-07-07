-- load_fact_po_line_items.sql
-- Staging -> Curated. Joins to the dim_vendor row that was effective on
-- the order date, not today's current row -- this is the entire point of
-- using SCD2 + surrogate keys instead of joining on vendor_id directly.

INSERT INTO `{{ var.value.project_id }}.{{ var.value.curated_dataset }}.fact_po_line_items`
SELECT
    s.po_line_id,
    s.po_number,
    v.vendor_sk,
    s.product_id,
    s.quantity_ordered,
    s.unit_price,
    s.line_amount,
    s.po_status,
    s.order_ts,
    s.load_ts
FROM `your-project.staging.stg_po_line_items` s
JOIN `your-project.curated.dim_vendor` v
    ON s.vendor_id = v.vendor_id
   AND DATE(s.order_ts) >= v.effective_start_date
   AND (DATE(s.order_ts) <= v.effective_end_date OR v.effective_end_date IS NULL)
WHERE s.dt = '{{ ds }}';
