CREATE TABLE dim_station_history AS
WITH ordered AS (
    SELECT
        *,
        lag(snapshot_seq) OVER station_order AS previous_snapshot_seq,
        lag(station_name) OVER station_order AS previous_station_name,
        lag(district) OVER station_order AS previous_district,
        lag(address) OVER station_order AS previous_address,
        lag(latitude) OVER station_order AS previous_latitude,
        lag(longitude) OVER station_order AS previous_longitude,
        lag(lcd_capacity) OVER station_order AS previous_lcd_capacity,
        lag(qr_capacity) OVER station_order AS previous_qr_capacity,
        lag(operation_mode) OVER station_order AS previous_operation_mode
    FROM station_observation_input
    WINDOW station_order AS (
        PARTITION BY station_no
        ORDER BY snapshot_seq
    )
),
marked AS (
    SELECT
        *,
        CASE
            WHEN previous_snapshot_seq IS NULL THEN 1
            WHEN snapshot_seq <> previous_snapshot_seq + 1 THEN 1
            WHEN station_name IS DISTINCT FROM previous_station_name THEN 1
            WHEN district IS DISTINCT FROM previous_district THEN 1
            WHEN address IS DISTINCT FROM previous_address THEN 1
            WHEN latitude IS DISTINCT FROM previous_latitude THEN 1
            WHEN longitude IS DISTINCT FROM previous_longitude THEN 1
            WHEN lcd_capacity IS DISTINCT FROM previous_lcd_capacity THEN 1
            WHEN qr_capacity IS DISTINCT FROM previous_qr_capacity THEN 1
            WHEN operation_mode IS DISTINCT FROM previous_operation_mode THEN 1
            ELSE 0
        END AS starts_new_interval
    FROM ordered
),
grouped AS (
    SELECT
        *,
        sum(starts_new_interval) OVER (
            PARTITION BY station_no
            ORDER BY snapshot_seq
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS interval_group
    FROM marked
),
collapsed AS (
    SELECT
        station_no,
        min(snapshot_seq) AS first_snapshot_seq,
        max(snapshot_seq) AS last_snapshot_seq,
        min(applicable_from) AS applicable_from,
        first(station_name ORDER BY snapshot_seq) AS station_name,
        first(district ORDER BY snapshot_seq) AS district,
        first(address ORDER BY snapshot_seq) AS address,
        first(latitude ORDER BY snapshot_seq) AS latitude,
        first(longitude ORDER BY snapshot_seq) AS longitude,
        first(lcd_capacity ORDER BY snapshot_seq) AS lcd_capacity,
        first(qr_capacity ORDER BY snapshot_seq) AS qr_capacity,
        first(operation_mode ORDER BY snapshot_seq) AS operation_mode,
        list(observation_period ORDER BY snapshot_seq) AS observation_periods,
        list(station_version_id ORDER BY snapshot_seq) AS station_version_ids,
        list(source_version_id ORDER BY snapshot_seq) AS source_version_ids,
        count(*)::INTEGER AS observation_count
    FROM grouped
    GROUP BY station_no, interval_group
)
SELECT
    c.station_no,
    c.applicable_from,
    next_snapshot.applicable_from AS applicable_until,
    c.station_name,
    c.district,
    c.address,
    c.latitude,
    c.longitude,
    c.lcd_capacity,
    c.qr_capacity,
    c.operation_mode,
    c.observation_periods,
    c.station_version_ids,
    c.source_version_ids,
    c.observation_count
FROM collapsed AS c
LEFT JOIN station_snapshot_catalog AS next_snapshot
    ON next_snapshot.snapshot_seq = c.last_snapshot_seq + 1;
