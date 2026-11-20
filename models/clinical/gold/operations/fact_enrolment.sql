-- fact_enrolment build (D18, ADR-003): subjects screened and randomised per site, cohort and month, from the sites'
-- enrolment logs. Pure: reads only site__site_enrolment_log (every silver row; a re-sent or corrected log has replaced
-- its site's month there), no writes. The harness adds site_sk as of enrolment_month, the window flags and the run id,
-- then replaces the changed months. Counts only: no treatment group is read.
SELECT site_id, cohort, enrolment_month, subjects_screened, subjects_randomised
FROM site__site_enrolment_log
WHERE _is_current AND NOT _is_deleted
