-- fact_sales build (D18, ADR-003): sell-in (open order lines) and sell-out (dealers' files) per dealer, material and
-- month. Pure: reads only erp__sales_order_line and partner__dealer_sellout (every silver row), no writes. Sell-out is
-- the current file per dealer and month (a re-sent file replaces its month in silver). The harness adds dealer_sk and
-- material_sk as of sale_month, the window flags and the run id, then replaces the changed months.
-- :business_tz is the platform's business time zone (an order counts in its local month).
WITH sell_in AS (
    SELECT dealer_code, material_code,
           CAST(DATE_TRUNC('MONTH', FROM_UTC_TIMESTAMP(order_ts, :business_tz)) AS DATE) AS sale_month,
           SUM(ordered_qty) AS units_sold_in,
           SUM(net_value) AS net_value
    FROM erp__sales_order_line
    WHERE _is_current AND NOT _is_deleted AND line_status = 'open'
    GROUP BY 1, 2, 3
),

sell_out AS (
    SELECT dealer_code, material_code,
           CAST(DATE_TRUNC('MONTH', sale_month) AS DATE) AS sale_month,
           SUM(units_sold) AS units_sold_out
    FROM partner__dealer_sellout
    WHERE _is_current AND NOT _is_deleted
    GROUP BY 1, 2, 3
)

SELECT COALESCE(i.dealer_code, o.dealer_code) AS dealer_code,
       COALESCE(i.material_code, o.material_code) AS material_code,
       COALESCE(i.sale_month, o.sale_month) AS sale_month,
       COALESCE(i.units_sold_in, 0) AS units_sold_in,
       COALESCE(o.units_sold_out, 0) AS units_sold_out,
       CAST(ROUND(COALESCE(i.net_value, 0), 2) AS DOUBLE) AS net_value
FROM sell_in AS i
FULL OUTER JOIN sell_out AS o
    ON i.dealer_code = o.dealer_code AND i.material_code = o.material_code AND i.sale_month = o.sale_month
