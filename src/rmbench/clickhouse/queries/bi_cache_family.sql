-- name: ch01_bi_sale_cache
SELECT
    fact_daily_od_bucket.service_departure_date AS departure_date,
    formatDateTime(subtractHours(fact_daily_od_bucket.event_datetime, 11), '%Y-%m-%d') AS sale_date,
    fact_daily_od_bucket.route_name AS route_name,
    fact_daily_od_bucket.service_status AS status,
    fact_daily_od_bucket.transporter_type AS transporter_type,
    CASE
        WHEN NOT match(fact_daily_od_bucket.service_number, '\\d') THEN false
        WHEN length(replaceRegexpAll(fact_daily_od_bucket.service_number, '\\D', '')) >= 1 THEN
            mod(CAST(right(replaceRegexpAll(fact_daily_od_bucket.service_number, '\\D', ''), 1) AS Int32), 2) != 0
        ELSE mod(CAST(substring(replaceRegexpAll(fact_daily_od_bucket.service_number, '\\D', ''), 1, 1) AS Int32), 2) != 0
    END AS is_odd,
    sum(fact_daily_od_bucket.cumulative_sum_net_bookings) AS bookings,
    sum(fact_daily_od_bucket.cumulative_sum_net_revenue_vat_inc) AS revenue_vat_inc,
    sum(fact_daily_od_bucket.cumulative_sum_net_revenue_vat_exc) AS revenue_vat_exc
FROM fact_daily_od_bucket
WHERE fact_daily_od_bucket.event_datetime >= toDateTime('__SALE_START__ 11:00:00', 'UTC')
  AND fact_daily_od_bucket.event_datetime <= toDateTime('__SALE_END__ 11:00:00', 'UTC')
GROUP BY
    departure_date,
    sale_date,
    route_name,
    status,
    transporter_type,
    is_odd
ORDER BY
    departure_date,
    sale_date,
    route_name,
    status,
    transporter_type,
    is_odd
;

-- name: ch02_bi_lid_cache
SELECT
    sub.capture_date,
    sub.departure_date,
    sub.route_name,
    sub.status,
    sub.transporter_type,
    sub.is_odd,
    sum(sub.availability) AS availability
FROM (
    SELECT
        formatDateTime(fact_daily_leg_physical_inventory.event_datetime, '%Y-%m-%d') AS capture_date,
        fact_daily_leg_physical_inventory.service_departure_date AS departure_date,
        fact_daily_leg_physical_inventory.route_name AS route_name,
        fact_daily_leg_physical_inventory.service_status AS status,
        fact_daily_leg_physical_inventory.transporter_type AS transporter_type,
        CASE
            WHEN NOT match(fact_daily_leg_physical_inventory.service_number, '\\d') THEN false
            WHEN length(replaceRegexpAll(fact_daily_leg_physical_inventory.service_number, '\\D', '')) >= 1 THEN
                mod(CAST(right(replaceRegexpAll(fact_daily_leg_physical_inventory.service_number, '\\D', ''), 1) AS Int32), 2) != 0
            ELSE mod(CAST(substring(replaceRegexpAll(fact_daily_leg_physical_inventory.service_number, '\\D', ''), 1, 1) AS Int32), 2) != 0
        END AS is_odd,
        fact_daily_leg_physical_inventory.service_number AS service_number,
        fact_daily_leg_physical_inventory.physical_inventory_name AS physical_inventory_name,
        min(fact_daily_leg_physical_inventory.physical_availability_end_day) AS availability
    FROM fact_daily_leg_physical_inventory
    WHERE fact_daily_leg_physical_inventory.event_datetime >= toDateTime('__SALE_START__ 11:00:00', 'UTC')
      AND fact_daily_leg_physical_inventory.event_datetime <= toDateTime('__SALE_END__ 11:00:00', 'UTC')
    GROUP BY
        capture_date,
        departure_date,
        route_name,
        status,
        transporter_type,
        is_odd,
        service_number,
        physical_inventory_name
) AS sub
GROUP BY
    sub.capture_date,
    sub.departure_date,
    sub.route_name,
    sub.status,
    sub.transporter_type,
    sub.is_odd
;

-- name: ch03_bi_departure_date_capture
WITH toDate('__DEPARTURE_START__') AS benchmark_today
SELECT
    sub.departure_date,
    sub.route_name,
    sub.status,
    sub.transporter_type,
    sub.is_odd,
    sum(sub.bookings) AS bookings,
    sum(sub.revenue_vat_inc) AS revenue_vat_inc,
    sum(sub.revenue_vat_exc) AS revenue_vat_exc,
    sum(sub.bookings) FILTER (WHERE sub.departure_date < benchmark_today) AS departed_bookings,
    sum(sub.revenue_vat_inc) FILTER (WHERE sub.departure_date < benchmark_today) AS departed_revenue_vat_inc,
    sum(sub.revenue_vat_exc) FILTER (WHERE sub.departure_date < benchmark_today) AS departed_revenue_vat_exc,
    sum(sub.lid) AS lid,
    sum(sub.max_leg_bookings) AS max_leg_bookings,
    sum(sub.forecasted_bookings) FILTER (WHERE sub.departure_date >= benchmark_today) AS forecasted_bookings,
    sum(sub.forecasted_revenue_vat_exc) FILTER (WHERE sub.departure_date >= benchmark_today) AS forecasted_revenue_vat_exc,
    sum(sub.forecasted_revenue_vat_inc) FILTER (WHERE sub.departure_date >= benchmark_today) AS forecasted_revenue_vat_inc,
    sum(sub.not_forecasted_bookings) FILTER (WHERE sub.departure_date >= benchmark_today) AS not_forecasted_bookings,
    sum(sub.not_forecasted_revenue_vat_exc) FILTER (WHERE sub.departure_date >= benchmark_today) AS not_forecasted_revenue_vat_exc,
    sum(sub.not_forecasted_revenue_vat_inc) FILTER (WHERE sub.departure_date >= benchmark_today) AS not_forecasted_revenue_vat_inc,
    sum(CASE WHEN sub.forecasted_bookings > 0 THEN 1 ELSE 0 END) FILTER (WHERE sub.departure_date >= benchmark_today) AS forecasted_service_count,
    count() FILTER (WHERE sub.departure_date >= benchmark_today) AS future_service_count
FROM (
    SELECT
        fact_daily_od_bucket.service_departure_date AS departure_date,
        fact_daily_od_bucket.route_name AS route_name,
        fact_daily_od_bucket.service_status AS status,
        fact_daily_od_bucket.transporter_type AS transporter_type,
        CASE
            WHEN NOT match(fact_daily_od_bucket.service_number, '\\d') THEN false
            WHEN length(replaceRegexpAll(fact_daily_od_bucket.service_number, '\\D', '')) >= 1 THEN
                mod(CAST(right(replaceRegexpAll(fact_daily_od_bucket.service_number, '\\D', ''), 1) AS Int32), 2) != 0
            ELSE mod(CAST(substring(replaceRegexpAll(fact_daily_od_bucket.service_number, '\\D', ''), 1, 1) AS Int32), 2) != 0
        END AS is_odd,
        fact_daily_od_bucket.service_number AS service_number,
        sum(fact_daily_od_bucket.cumulative_sum_net_bookings) AS bookings,
        sum(fact_daily_od_bucket.cumulative_sum_net_revenue_vat_inc) AS revenue_vat_inc,
        sum(fact_daily_od_bucket.cumulative_sum_net_revenue_vat_exc) AS revenue_vat_exc,
        argMin(fact_daily_od_bucket.service_lid, fact_daily_od_bucket.event_datetime) AS lid,
        argMin(fact_daily_od_bucket.service_max_leg_cumulative_sum_net_bookings, fact_daily_od_bucket.event_datetime) AS max_leg_bookings,
        CAST(sum(coalesce(fact_daily_od_bucket.od_cabin_forecasted_traffic, 0)) FILTER (WHERE fact_daily_od_bucket.od_cabin_pointer = 1) AS Int32) AS forecasted_bookings,
        sum(fact_daily_od_bucket.od_cabin_forecasted_revenue_vat_exc) FILTER (WHERE fact_daily_od_bucket.od_cabin_pointer = 1) AS forecasted_revenue_vat_exc,
        sum(fact_daily_od_bucket.od_cabin_forecasted_revenue_vat_inc) FILTER (WHERE fact_daily_od_bucket.od_cabin_pointer = 1) AS forecasted_revenue_vat_inc,
        sum(fact_daily_od_bucket.cumulative_sum_net_revenue_vat_exc) FILTER (WHERE fact_daily_od_bucket.od_cabin_forecasted_traffic IS NULL) AS not_forecasted_revenue_vat_exc,
        sum(fact_daily_od_bucket.cumulative_sum_net_revenue_vat_inc) FILTER (WHERE fact_daily_od_bucket.od_cabin_forecasted_traffic IS NULL) AS not_forecasted_revenue_vat_inc,
        sum(fact_daily_od_bucket.cumulative_sum_net_bookings) FILTER (WHERE fact_daily_od_bucket.od_cabin_forecasted_traffic IS NULL) AS not_forecasted_bookings
    FROM fact_daily_od_bucket
    WHERE fact_daily_od_bucket.service_departure_date >= toDate('__DEPARTURE_START__')
      AND fact_daily_od_bucket.service_departure_date < toDate('__DEPARTURE_END__')
      AND fact_daily_od_bucket.event_datetime >= toDateTime('__SALE_START__ 11:00:00', 'UTC')
      AND fact_daily_od_bucket.event_datetime <= toDateTime('__SALE_END__ 11:00:00', 'UTC')
    GROUP BY
        departure_date,
        route_name,
        status,
        transporter_type,
        is_odd,
        service_number
) AS sub
GROUP BY
    sub.departure_date,
    sub.route_name,
    sub.status,
    sub.transporter_type,
    sub.is_odd
;
