"""Seoul bike rental data pipeline.

Implemented through immutable Clean/station snapshot publish, dim_station_history,
trip station-history enrichment, versioned fact_trip_trusted publication, and
deterministic dim_date plus versioned mart_station_day and mart_od_day
and mart_source_month_quality warehouse publication, plus an
orchestration-independent month/range backfill workflow and Airflow control
plane.
"""
