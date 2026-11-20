-- fact_temperature_excursion build (D18, ADR-003): readings, readings outside 2 to 8 degrees and the highest reading per
-- shipment and day. Pure: reads only coldchain_stream__temperature_reading (every silver row; silver has dropped the
-- replayed readings), no writes. The day is the reading's local date in the business time zone (:business_tz).
SELECT shipment_id,
       CAST(FROM_UTC_TIMESTAMP(event_ts, :business_tz) AS DATE) AS reading_date,
       COUNT(1) AS readings,
       SUM(CASE WHEN temperature_c < 2.0 OR temperature_c > 8.0 THEN 1 ELSE 0 END) AS excursion_readings,
       MAX(temperature_c) AS max_temperature_c
FROM coldchain_stream__temperature_reading
WHERE _is_current AND NOT _is_deleted
GROUP BY 1, 2
