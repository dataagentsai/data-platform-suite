-- fact_otif build (D18, ADR-003): one row per open order line, on time (by the date first promised) and in full.
-- Pure: reads only the input views the harness hands over (erp__sales_order_line, erp__delivery: every silver row,
-- active corrections already laid over), no writes. The harness adds dealer_sk and material_sk as of order_ts, the
-- window flags and the run id, then replaces the changed promised-date windows.
-- :business_tz is the platform's business time zone (a delivery counts on its local date).
WITH first_promise AS (  -- the date first promised: the line's first version in the source order (ADR-016)
    SELECT order_id, line_no, promised_date
    FROM (
        SELECT order_id, line_no, promised_date,
               ROW_NUMBER() OVER (PARTITION BY order_id, line_no ORDER BY _seq_from, _valid_from) AS version_no
        FROM erp__sales_order_line
    ) AS versions
    WHERE version_no = 1
),

open_lines AS (  -- current, not deleted, still open: cancelled and rejected lines are left out (contract)
    SELECT order_id, line_no, dealer_code, material_code, ordered_qty, order_ts
    FROM erp__sales_order_line
    WHERE _is_current AND NOT _is_deleted AND line_status = 'open'
),

delivered AS (
    SELECT order_id, line_no,
           SUM(delivered_qty) AS delivered_qty,
           MAX(delivered_ts) AS delivered_ts
    FROM erp__delivery
    WHERE _is_current AND NOT _is_deleted
    GROUP BY order_id, line_no
),

judged AS (
    SELECT o.order_id, o.line_no, o.dealer_code, o.material_code, o.ordered_qty, o.order_ts,
           f.promised_date,
           COALESCE(d.delivered_qty, 0) AS delivered_qty,
           COALESCE(TO_DATE(FROM_UTC_TIMESTAMP(d.delivered_ts, :business_tz)) <= f.promised_date, FALSE) AS on_time,
           COALESCE(d.delivered_qty, 0) >= o.ordered_qty AS in_full
    FROM open_lines AS o
    INNER JOIN first_promise AS f ON o.order_id = f.order_id AND o.line_no = f.line_no
    LEFT JOIN delivered AS d ON o.order_id = d.order_id AND o.line_no = d.line_no
)

SELECT order_id, line_no, dealer_code, material_code, promised_date, ordered_qty, delivered_qty,
       on_time, in_full,
       CASE WHEN on_time AND in_full THEN 1 ELSE 0 END AS on_time_in_full,
       order_ts
FROM judged
