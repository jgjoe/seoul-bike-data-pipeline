CREATE TABLE dim_date AS
SELECT
    CAST(calendar_date AS DATE) AS date,
    CAST(EXTRACT(YEAR FROM calendar_date) AS INTEGER) AS year,
    CAST(EXTRACT(MONTH FROM calendar_date) AS INTEGER) AS month,
    CAST(EXTRACT(DAY FROM calendar_date) AS INTEGER) AS day,
    CAST(strftime(calendar_date, '%u') AS INTEGER) AS day_of_week,
    CAST(strftime(calendar_date, '%u') IN ('6', '7') AS BOOLEAN) AS weekend_flag
FROM generate_series(
    DATE '2020-01-01',
    DATE '2026-06-30',
    INTERVAL 1 DAY
) AS series(calendar_date)
ORDER BY calendar_date;
