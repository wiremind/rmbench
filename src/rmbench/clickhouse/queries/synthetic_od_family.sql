-- name: q01_window_row_count
SELECT count()
FROM fact_daily_od_bucket
WHERE event_datetime >= toDateTime('__SALE_START__ 00:00:00', 'UTC')
  AND event_datetime < toDateTime('__SALE_END__ 00:00:00', 'UTC')
  AND service_departure_date >= toDate('__DEPARTURE_START__')
  AND service_departure_date < toDate('__DEPARTURE_END__')
;

-- name: q02_distinct_services
SELECT countDistinct(service_number)
FROM fact_daily_od_bucket
WHERE event_datetime >= toDateTime('__SALE_START__ 00:00:00', 'UTC')
  AND event_datetime < toDateTime('__SALE_END__ 00:00:00', 'UTC')
  AND service_departure_date >= toDate('__DEPARTURE_START__')
  AND service_departure_date < toDate('__DEPARTURE_END__')
;

-- name: q03_top_routes_by_bookings
SELECT
    route_name,
    sum(cumulative_sum_net_bookings) AS bookings,
    sum(cumulative_sum_net_revenue_vat_inc) AS revenue_vat_inc
FROM fact_daily_od_bucket
WHERE event_datetime >= toDateTime('__SALE_START__ 00:00:00', 'UTC')
  AND event_datetime < toDateTime('__SALE_END__ 00:00:00', 'UTC')
  AND service_departure_date >= toDate('__DEPARTURE_START__')
  AND service_departure_date < toDate('__DEPARTURE_END__')
  AND od_cabin_pointer = 1
GROUP BY route_name
ORDER BY bookings DESC
LIMIT 20
;

-- name: q04_top_markets_by_revenue
SELECT
    market_name,
    sum(cumulative_sum_net_revenue_vat_inc) AS revenue_vat_inc,
    sum(cumulative_sum_net_bookings) AS bookings
FROM fact_daily_od_bucket
WHERE event_datetime >= toDateTime('__SALE_START__ 00:00:00', 'UTC')
  AND event_datetime < toDateTime('__SALE_END__ 00:00:00', 'UTC')
  AND service_departure_date >= toDate('__DEPARTURE_START__')
  AND service_departure_date < toDate('__DEPARTURE_END__')
  AND od_cabin_pointer = 1
GROUP BY market_name
ORDER BY revenue_vat_inc DESC
LIMIT 20
;

-- name: q05_day_x_curve
SELECT
    day_x,
    sum(sum_net_bookings) AS daily_bookings,
    sum(sum_net_revenue_vat_inc) AS daily_revenue_vat_inc,
    sum(availability_end_day) AS net_availability
FROM fact_daily_od_bucket
WHERE event_datetime >= toDateTime('__SALE_START__ 00:00:00', 'UTC')
  AND event_datetime < toDateTime('__SALE_END__ 00:00:00', 'UTC')
  AND service_departure_date >= toDate('__DEPARTURE_START__')
  AND service_departure_date < toDate('__DEPARTURE_END__')
  AND od_cabin_pointer = 1
GROUP BY day_x
ORDER BY day_x
;

-- name: q06_pricing_change_families
SELECT
    family_name,
    bucket_name,
    count() AS rows_in_change,
    sum(sum_net_bookings) AS daily_bookings,
    sum(cumulative_sum_net_revenue_vat_inc) AS revenue_vat_inc
FROM fact_daily_od_bucket
WHERE event_datetime >= toDateTime('__SALE_START__ 00:00:00', 'UTC')
  AND event_datetime < toDateTime('__SALE_END__ 00:00:00', 'UTC')
  AND service_departure_date >= toDate('__DEPARTURE_START__')
  AND service_departure_date < toDate('__DEPARTURE_END__')
  AND (has_pricing_changed_day_x = 1 OR has_pricing_changed_bucket = 1)
GROUP BY family_name, bucket_name
ORDER BY rows_in_change DESC
LIMIT 50
;

-- name: q07_last_snapshot_market_cabin
SELECT
    market_name,
    cabin_name,
    sum(cumulative_sum_net_bookings) AS bookings,
    sum(cumul_availability_end_day) AS net_availability,
    sum(cumulative_sum_net_revenue_vat_inc) AS revenue_vat_inc
FROM fact_daily_od_bucket
WHERE event_datetime >= toDateTime('__SALE_START__ 00:00:00', 'UTC')
  AND event_datetime < toDateTime('__SALE_END__ 00:00:00', 'UTC')
  AND service_departure_date >= toDate('__DEPARTURE_START__')
  AND service_departure_date < toDate('__DEPARTURE_END__')
  AND od_cabin_pointer = 1
GROUP BY market_name, cabin_name
ORDER BY bookings DESC
LIMIT 50
;

-- name: q08_forecast_observed_gap_by_route
SELECT
    route_name,
    sum(ifNull(od_cabin_forecasted_traffic, 0)) AS forecasted_bookings,
    sum(ifNull(od_cabin_last_observed, 0)) AS observed_bookings,
    sum(ifNull(od_cabin_optimized_traffic, 0)) AS optimized_bookings,
    abs(forecasted_bookings - observed_bookings) AS forecast_gap
FROM fact_daily_od_bucket
WHERE event_datetime >= toDateTime('__SALE_START__ 00:00:00', 'UTC')
  AND event_datetime < toDateTime('__SALE_END__ 00:00:00', 'UTC')
  AND service_departure_date >= toDate('__DEPARTURE_START__')
  AND service_departure_date < toDate('__DEPARTURE_END__')
  AND od_cabin_pointer = 1
GROUP BY route_name
ORDER BY forecast_gap DESC
LIMIT 20
;
