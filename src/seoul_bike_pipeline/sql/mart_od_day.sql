CREATE TABLE mart_od_day AS
SELECT
    CAST(rent_at AS DATE) AS service_date,
    rent_station_no AS origin_station_no,
    return_station_no AS destination_station_no,
    COUNT(*)::BIGINT AS trip_count,
    COUNT(DISTINCT bike_id)::BIGINT AS unique_bike_count,
    AVG(usage_min)::DOUBLE AS avg_usage_min,
    MEDIAN(usage_min)::DOUBLE AS median_usage_min,
    COUNT(*) FILTER (WHERE distance_metric_eligible)::BIGINT
        AS valid_distance_trip_count,
    AVG(distance_m) FILTER (WHERE distance_metric_eligible)::DOUBLE
        AS valid_distance_avg,
    MEDIAN(distance_m) FILTER (WHERE distance_metric_eligible)::DOUBLE
        AS valid_distance_median
FROM od_fact_input
WHERE is_completed_trip
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
