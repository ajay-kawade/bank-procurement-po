-- dq_check_dim_vendor.sql
-- Two targeted checks on dim_vendor after every SCD2 run.
-- Both catch silent correctness failures that produce no runtime error
-- but cause every downstream join to return wrong results.
--
-- How this works in the DAG:
-- BigQuery scripting raises an error if either check finds failing rows,
-- which causes the Airflow task to fail and stops the fact load running
-- on top of broken dimension data.

DECLARE duplicate_current_rows INT64;
DECLARE null_hash_rows INT64;
DECLARE missing_vendor_rows INT64;

-- Check 1: exactly one is_current = TRUE row per vendor_id.
-- More than one means two concurrent MERGE/UPDATE runs raced and both
-- inserted an active row for the same vendor. The fact load join then
-- returns duplicate fact rows with no error, just silently wrong counts.
SET duplicate_current_rows = (
    SELECT COUNT(*)
    FROM (
        SELECT vendor_id, COUNT(*) AS cnt
        FROM `{{ var.value.project_id }}.{{ var.value.curated_dataset }}.dim_vendor`
        WHERE is_current = TRUE
        GROUP BY vendor_id
        HAVING cnt > 1
    )
);

-- Check 2: no active row should have a NULL attribute_hash.
-- A NULL hash means the COALESCE fix is missing (or was removed), and
-- change detection has silently stopped working for those vendors.
SET null_hash_rows = (
    SELECT COUNT(*)
    FROM `{{ var.value.project_id }}.{{ var.value.curated_dataset }}.dim_vendor`
    WHERE is_current = TRUE
      AND attribute_hash IS NULL
);

-- Check 3: every vendor in today's staging has a current row in dim_vendor.
-- If the insert pass silently failed for some vendors, the fact load JOIN
-- will drop those PO lines entirely with no error -- they just disappear.
SET missing_vendor_rows = (
    SELECT COUNT(*)
    FROM `{{ var.value.project_id }}.{{ var.value.staging_dataset }}.stg_vendor` s
    LEFT JOIN `{{ var.value.project_id }}.{{ var.value.curated_dataset }}.dim_vendor` d
        ON s.vendor_id = d.vendor_id AND d.is_current = TRUE
    WHERE d.vendor_id IS NULL
);

-- Raise an error if any check finds failing rows.
-- The error message tells you exactly which check failed and how many
-- rows are affected, so you don't have to dig through logs.
IF duplicate_current_rows > 0 THEN
    RAISE USING MESSAGE = CONCAT(
        'DQ FAIL — dim_vendor has ', CAST(duplicate_current_rows AS STRING),
        ' vendor_id(s) with more than one is_current = TRUE row. ',
        'Likely cause: concurrent SCD2 runs. ',
        'Fix: deduplicate dim_vendor and check max_active_runs on the DAG.'
    );
END IF;

IF null_hash_rows > 0 THEN
    RAISE USING MESSAGE = CONCAT(
        'DQ FAIL — dim_vendor has ', CAST(null_hash_rows AS STRING),
        ' active rows with NULL attribute_hash. ',
        'Likely cause: COALESCE missing in SCD2 SQL. ',
        'Fix: check scd2_expire_vendors.sql and scd2_insert_vendor_versions.sql.'
    );
END IF;

IF missing_vendor_rows > 0 THEN
    RAISE USING MESSAGE = CONCAT(
        'DQ FAIL — ', CAST(missing_vendor_rows AS STRING),
        ' vendor(s) in staging have no current row in dim_vendor. ',
        'Likely cause: SCD2 insert pass failed silently for some vendors. ',
        'Fact load aborted -- PO lines for these vendors would be silently dropped.'
    );
END IF;

-- All checks passed
SELECT 'dim_vendor DQ passed.' AS dq_status;
