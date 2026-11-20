-- fact_visit_compliance build (D18, ADR-003): one row per scheduled or completed visit: was it done within its allowed
-- window around the planned date, with every form recorded and every expected kit dispensed. Pure: reads only the input
-- views the harness hands over (edc__visit, supply__dispensing: every silver row, active planned-date corrections
-- already laid over), no writes. The harness adds subject_sk, site_sk and kit_sk as of scheduled_ts, the window flags
-- and the run id, then replaces the changed planned-date windows. Cancelled visits are left out (contract). The
-- treatment group is never read.
WITH visits AS (
    SELECT visit_id, subject_id, site_id, visit_code, planned_date, window_days, scheduled_ts, visit_status,
           actual_date, forms_expected, forms_recorded, kits_expected
    FROM edc__visit
    WHERE _is_current AND NOT _is_deleted AND visit_status IN ('scheduled', 'completed')
),

kits AS (
    SELECT visit_id, MIN(kit_id) AS kit_id, SUM(kits_dispensed) AS kits_dispensed
    FROM supply__dispensing
    WHERE _is_current AND NOT _is_deleted
    GROUP BY visit_id
),

judged AS (
    SELECT v.visit_id, v.subject_id, v.site_id, k.kit_id, v.visit_code, v.planned_date, v.scheduled_ts,
           v.forms_expected,
           CASE WHEN v.visit_status = 'completed' THEN v.forms_recorded ELSE 0 END AS forms_recorded,
           v.kits_expected,
           COALESCE(k.kits_dispensed, 0) AS kits_dispensed,
           COALESCE(v.visit_status = 'completed'
                    AND ABS(DATEDIFF(v.actual_date, v.planned_date)) <= v.window_days, FALSE) AS in_window,
           COALESCE(v.visit_status = 'completed' AND v.forms_recorded >= v.forms_expected, FALSE) AS all_recorded
    FROM visits AS v
    LEFT JOIN kits AS k ON v.visit_id = k.visit_id
)

SELECT visit_id, subject_id, site_id, kit_id, visit_code, planned_date, scheduled_ts,
       forms_expected, forms_recorded, kits_expected, kits_dispensed, in_window, all_recorded,
       CASE WHEN in_window AND all_recorded AND kits_dispensed >= kits_expected THEN 1 ELSE 0 END AS compliant
FROM judged
