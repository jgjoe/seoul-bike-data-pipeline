CREATE TABLE mart_station_day AS
WITH rental_agg AS (
    SELECT
        CAST(rent_at AS DATE) AS service_date,
        rent_station_no AS station_no,
        COUNT(*) AS rental_count,
        COUNT(*) FILTER (WHERE is_completed_trip) AS completed_trip_count,
        COUNT(DISTINCT bike_id) AS unique_bike_count,
        AVG(usage_min) AS avg_usage_min,
        MEDIAN(usage_min) AS median_usage_min,
        COUNT(*) FILTER (WHERE distance_metric_eligible) AS valid_distance_trip_count,
        COUNT(*) FILTER (WHERE NOT is_completed_trip) AS missing_return_count,
        COUNT(*) FILTER (WHERE station_history_match) AS station_history_match_count
    FROM rental_fact_input
    GROUP BY 1, 2
),
return_agg AS (
    SELECT
        CAST(return_at AS DATE) AS service_date,
        return_station_no AS station_no,
        COUNT(*) AS return_count
    FROM return_event_input
    GROUP BY 1, 2
),
station_keys AS (
    SELECT service_date, station_no FROM rental_agg
    UNION
    SELECT service_date, station_no FROM return_agg
)
SELECT
    keys.service_date,
    keys.station_no,
    COALESCE(rental.rental_count, 0)::BIGINT AS rental_count,
    COALESCE(returns.return_count, 0)::BIGINT AS return_count,
    COALESCE(rental.completed_trip_count, 0)::BIGINT AS completed_trip_count,
    COALESCE(rental.unique_bike_count, 0)::BIGINT AS unique_bike_count,
    rental.avg_usage_min::DOUBLE AS avg_usage_min,
    rental.median_usage_min::DOUBLE AS median_usage_min,
    COALESCE(rental.valid_distance_trip_count, 0)::BIGINT AS valid_distance_trip_count,
    COALESCE(rental.missing_return_count, 0)::BIGINT AS missing_return_count,
    COALESCE(rental.station_history_match_count, 0)::BIGINT
        AS station_history_match_count
FROM station_keys AS keys
LEFT JOIN rental_agg AS rental USING (service_date, station_no)
LEFT JOIN return_agg AS returns USING (service_date, station_no)
ORDER BY keys.service_date, keys.station_no;
