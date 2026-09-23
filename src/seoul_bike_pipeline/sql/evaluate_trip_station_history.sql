CREATE TABLE trip_station_enrichment AS
SELECT
    clean.*,
    history.applicable_from AS matched_station_applicable_from,
    history.applicable_until AS matched_station_applicable_until
FROM clean_input AS clean
LEFT JOIN station_history.dim_station_history AS history
    ON history.station_no = clean.rent_station_no
   AND CAST(clean.rent_at AS DATE) >= history.applicable_from
   AND (
        history.applicable_until IS NULL
        OR CAST(clean.rent_at AS DATE) < history.applicable_until
   )
WHERE clean.trusted_fact_eligible
ORDER BY clean.source_row_id;
