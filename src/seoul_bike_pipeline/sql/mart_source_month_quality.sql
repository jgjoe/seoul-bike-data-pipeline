CREATE TABLE mart_source_month_quality AS
WITH clean_stats AS (
    SELECT
        COUNT(*)::BIGINT AS clean_row_count,
        COUNT(*) FILTER (WHERE trusted_fact_eligible)::BIGINT AS trusted_clean_count,
        COUNT(*) FILTER (WHERE row_quarantined)::BIGINT AS quarantine_count,
        COUNT(DISTINCT logical_trip_key)
            FILTER (WHERE logical_trip_conflict)::BIGINT AS logical_conflict_group_count,
        COUNT(DISTINCT logical_trip_key)
            FILTER (WHERE exact_duplicate_group)::BIGINT AS exact_duplicate_group_count,
        COUNT(*) FILTER (WHERE NOT is_completed_trip)::BIGINT AS incomplete_trip_count
    FROM quality_clean_input
),
issue_stats AS (
    SELECT
        COUNT(DISTINCT source_row_id)
            FILTER (WHERE rule_id = 'distance_m_zero')::BIGINT AS distance_zero_count,
        COUNT(DISTINCT source_row_id)
            FILTER (WHERE rule_id = 'distance_m_extreme_positive')::BIGINT AS extreme_distance_count,
        COUNT(DISTINCT source_row_id)
            FILTER (
                WHERE rule_id IN ('birth_year_invalid', 'birth_year_impossible')
            )::BIGINT AS invalid_birth_year_count,
        COUNT(DISTINCT source_row_id)
            FILTER (WHERE rule_id = 'sex_missing')::BIGINT AS sex_missing_count
    FROM quality_issue_input
),
fact_stats AS (
    SELECT
        COUNT(*)::BIGINT AS fact_row_count,
        COUNT(*) FILTER (WHERE NOT station_history_match)::BIGINT AS station_unmatched_count
    FROM quality_fact_input
)
SELECT
    context.source_month,
    context.source_version_id,
    context.source_row_count,
    clean.clean_row_count,
    fact.fact_row_count AS trusted_fact_count,
    clean.quarantine_count,
    clean.logical_conflict_group_count,
    clean.exact_duplicate_group_count,
    clean.incomplete_trip_count,
    fact.station_unmatched_count,
    (issues.distance_zero_count::DOUBLE / clean.clean_row_count::DOUBLE)
        AS distance_zero_rate,
    issues.extreme_distance_count,
    (issues.invalid_birth_year_count::DOUBLE / clean.clean_row_count::DOUBLE)
        AS invalid_birth_year_rate,
    (issues.sex_missing_count::DOUBLE / clean.clean_row_count::DOUBLE)
        AS sex_missing_rate,
    context.schema_version,
    context.artifact_hash,
    context.pipeline_version,
    context.clean_version_id,
    context.fact_version_id
FROM quality_context AS context
CROSS JOIN clean_stats AS clean
CROSS JOIN issue_stats AS issues
CROSS JOIN fact_stats AS fact;
