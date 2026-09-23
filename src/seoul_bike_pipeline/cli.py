"""``python -m seoul_bike_pipeline`` command line interface.

Source commands call the same business functions used by tests; no pipeline
logic lives in the CLI layer (PRD §13, §24).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .backfill import RUN_SUCCEEDED, run_backfill_range
from .canonical_dq import canonicalize_csv_source_unit
from .clean_publish import publish_clean_month
from .csv_source import read_csv_source_unit
from .date_dimension import publish_date_dimension
from .errors import SourceRegistrationError
from .fact_publish import publish_current_fact_trip_month
from .group_dq import classify_registered_source_month
from .od_day_mart import (
    plan_od_day_fact_revision_impact,
    publish_current_od_day_mart,
)
from .register import register_source
from .source_month_quality_mart import (
    plan_source_month_quality_revision_impact,
    publish_current_source_month_quality_mart,
)
from .source_projection import project_csv_source_unit
from .station_history import publish_station_history
from .station_day_mart import (
    plan_station_day_fact_revision_impact,
    publish_current_station_day_mart,
)
from .station_snapshot import publish_station_snapshot
from .trip_station_enrichment import evaluate_current_trip_station_enrichment
from .zip_members import inventory_registered_zip
from .zip_months import plan_zip_months


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="seoul-bike-pipeline", description="Seoul bike rental data pipeline"
    )
    commands = parser.add_subparsers(dest="command", required=True)

    source = commands.add_parser("source", help="source artifact operations")
    source_commands = source.add_subparsers(dest="source_command", required=True)

    pipeline = commands.add_parser(
        "pipeline", help="orchestration-independent pipeline workflows"
    )
    pipeline_commands = pipeline.add_subparsers(
        dest="pipeline_command", required=True
    )
    backfill_range = pipeline_commands.add_parser(
        "backfill-range",
        help="plan and execute an idempotent rental month/range backfill",
    )
    for option, help_text in (
        ("--manifest", "path of the source manifest JSON file"),
        ("--raw-root", "root for immutable Raw artifacts"),
        ("--station-root", "root for immutable station snapshot versions"),
        ("--clean-root", "root for immutable Clean month versions"),
        ("--warehouse-root", "root for warehouse dimensions/facts/marts"),
    ):
        backfill_range.add_argument(
            option, type=Path, required=True, help=help_text
        )
    backfill_range.add_argument(
        "--start-month", required=True, help="inclusive rental month YYYY-MM"
    )
    backfill_range.add_argument(
        "--end-month", required=True, help="inclusive rental month YYYY-MM"
    )
    backfill_range.add_argument(
        "--dataset-id", default="rental_history", help="rental source dataset id"
    )
    backfill_range.add_argument(
        "--transformation-version",
        required=True,
        help="explicit business transformation version shared by the run",
    )
    backfill_range.add_argument(
        "--configuration-json",
        default="{}",
        help="JSON object containing deterministic business configuration",
    )

    register = source_commands.add_parser(
        "register", help="register a source artifact as an immutable source version"
    )
    register.add_argument("artifact", type=Path, help="local path of the downloaded source artifact")
    register.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    register.add_argument(
        "--raw-root", type=Path, required=True, help="local root for immutable Raw artifacts"
    )
    register.add_argument("--dataset-id", required=True, help="source dataset id, e.g. rental_history")
    register.add_argument(
        "--logical-period", required=True, help="year or month the artifact covers, e.g. 2020 or 2020-01"
    )
    register.add_argument("--published-filename", required=True, help="official filename of the artifact")
    register.add_argument("--source-url", required=True, help="official locator of the artifact")
    register.add_argument("--license", required=True, help="source license / 이용허락")
    register.add_argument(
        "--source-modified-at",
        default=None,
        help="official source modified timestamp (ISO-8601; read as UTC when no offset is given)",
    )

    inventory_zip = source_commands.add_parser(
        "inventory-zip",
        help="inventory members of one registered immutable Raw ZIP source version",
    )
    inventory_zip.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    inventory_zip.add_argument(
        "--raw-root", type=Path, required=True, help="local root for immutable Raw artifacts"
    )
    inventory_zip.add_argument(
        "--source-version-id", required=True, help="registered parent source_version_id"
    )

    plan_zip = source_commands.add_parser(
        "plan-zip-months",
        help="map ZIP members to logical months and compare an explicit parent revision",
    )
    plan_zip.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    plan_zip.add_argument(
        "--raw-root", type=Path, required=True, help="local root for immutable Raw artifacts"
    )
    plan_zip.add_argument(
        "--current-source-version-id", required=True, help="current yearly ZIP source_version_id"
    )
    plan_zip.add_argument(
        "--previous-source-version-id",
        default=None,
        help="previous yearly ZIP source_version_id for revision comparison",
    )

    inspect_csv = source_commands.add_parser(
        "inspect-csv",
        help="stream and structurally validate one standalone CSV or ZIP CSV member",
    )
    inspect_csv.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    inspect_csv.add_argument(
        "--raw-root", type=Path, required=True, help="local root for immutable Raw artifacts"
    )
    inspect_csv.add_argument(
        "--source-version-id", required=True, help="registered parent source_version_id"
    )
    inspect_csv.add_argument(
        "--member-name",
        default=None,
        help="exact ZIP member name; omit for a standalone monthly CSV artifact",
    )

    project_csv = source_commands.add_parser(
        "project-csv",
        help="type source fields and validate rental month without Clean persistence",
    )
    project_csv.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    project_csv.add_argument(
        "--raw-root", type=Path, required=True, help="local root for immutable Raw artifacts"
    )
    project_csv.add_argument(
        "--source-version-id", required=True, help="registered parent source_version_id"
    )
    project_csv.add_argument(
        "--member-name",
        default=None,
        help="exact ZIP member name; omit for a standalone monthly CSV artifact",
    )

    canonicalize_csv = source_commands.add_parser(
        "canonicalize-csv",
        help="canonicalize one source unit and classify row-level DQ without Clean persistence",
    )
    canonicalize_csv.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    canonicalize_csv.add_argument(
        "--raw-root", type=Path, required=True, help="local root for immutable Raw artifacts"
    )
    canonicalize_csv.add_argument(
        "--source-version-id", required=True, help="registered parent source_version_id"
    )
    canonicalize_csv.add_argument(
        "--member-name",
        default=None,
        help="exact ZIP member name; omit for a standalone monthly CSV artifact",
    )

    classify_month = source_commands.add_parser(
        "classify-month",
        help="classify month-level logical-trip groups and finalize trusted eligibility",
    )
    classify_month.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    classify_month.add_argument(
        "--raw-root", type=Path, required=True, help="local root for immutable Raw artifacts"
    )
    classify_month.add_argument(
        "--source-version-id", required=True, help="current registered source_version_id"
    )
    classify_month.add_argument(
        "--source-month", required=True, help="rental processing month in YYYY-MM form"
    )

    publish_clean = source_commands.add_parser(
        "publish-clean-month",
        help="build, validate, and atomically publish one immutable Clean Parquet month",
    )
    publish_clean.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    publish_clean.add_argument(
        "--raw-root", type=Path, required=True, help="local root for immutable Raw artifacts"
    )
    publish_clean.add_argument(
        "--clean-root", type=Path, required=True, help="root for immutable Clean versions/current pointers"
    )
    publish_clean.add_argument(
        "--source-version-id", required=True, help="registered source_version_id to build"
    )
    publish_clean.add_argument(
        "--source-month", required=True, help="rental processing month in YYYY-MM form"
    )
    publish_clean.add_argument(
        "--transformation-version",
        required=True,
        help="explicit transformation code version, e.g. a Git commit id",
    )
    publish_clean.add_argument(
        "--configuration-json",
        default="{}",
        help="JSON object containing deterministic business configuration",
    )
    publish_station = source_commands.add_parser(
        "publish-station-snapshot",
        help="ingest, consolidate, validate, and immutably publish one station snapshot",
    )
    publish_station.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    publish_station.add_argument(
        "--raw-root", type=Path, required=True, help="local root for immutable Raw artifacts"
    )
    publish_station.add_argument(
        "--station-root", type=Path, required=True, help="root for station snapshot versions/current pointers"
    )
    publish_station.add_argument(
        "--source-version-id", required=True, help="registered station snapshot source_version_id"
    )
    publish_station.add_argument(
        "--transformation-version", required=True, help="explicit transformation code version"
    )
    publish_station.add_argument(
        "--configuration-json",
        default="{}",
        help="JSON object containing deterministic business configuration",
    )

    warehouse = commands.add_parser(
        "warehouse", help="trusted warehouse transformation operations"
    )
    warehouse_commands = warehouse.add_subparsers(
        dest="warehouse_command", required=True
    )
    publish_history = warehouse_commands.add_parser(
        "publish-station-history",
        help="build and atomically publish dim_station_history from current station snapshots",
    )
    publish_history.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    publish_history.add_argument(
        "--raw-root", type=Path, required=True, help="local root for immutable Raw artifacts"
    )
    publish_history.add_argument(
        "--station-root",
        type=Path,
        required=True,
        help="root for immutable station snapshot versions/current pointers",
    )
    publish_history.add_argument(
        "--warehouse-root",
        type=Path,
        required=True,
        help="root for versioned persistent DuckDB warehouse publication",
    )
    publish_history.add_argument(
        "--transformation-version",
        required=True,
        help="explicit transformation code version",
    )
    publish_history.add_argument(
        "--configuration-json",
        default="{}",
        help="JSON object containing deterministic business configuration",
    )
    evaluate_station_enrichment = warehouse_commands.add_parser(
        "evaluate-station-enrichment",
        help="evaluate current trusted Clean rows against current dim_station_history",
    )
    evaluate_station_enrichment.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    evaluate_station_enrichment.add_argument(
        "--raw-root", type=Path, required=True, help="local root for immutable Raw artifacts"
    )
    evaluate_station_enrichment.add_argument(
        "--station-root", type=Path, required=True, help="root for station snapshot versions/current pointers"
    )
    evaluate_station_enrichment.add_argument(
        "--clean-root", type=Path, required=True, help="root for immutable Clean month versions/current pointers"
    )
    evaluate_station_enrichment.add_argument(
        "--warehouse-root", type=Path, required=True, help="root containing current dim_station_history warehouse version"
    )
    evaluate_station_enrichment.add_argument(
        "--source-month", required=True, help="rental source month in YYYY-MM form"
    )
    publish_fact = warehouse_commands.add_parser(
        "publish-fact-trip-month",
        help="build and atomically publish one current trusted fact month",
    )
    publish_fact.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    publish_fact.add_argument(
        "--raw-root", type=Path, required=True, help="local root for immutable Raw artifacts"
    )
    publish_fact.add_argument(
        "--station-root",
        type=Path,
        required=True,
        help="root for immutable station snapshot versions/current pointers",
    )
    publish_fact.add_argument(
        "--clean-root",
        type=Path,
        required=True,
        help="root for immutable Clean month versions/current pointers",
    )
    publish_fact.add_argument(
        "--warehouse-root",
        type=Path,
        required=True,
        help="root for station history and trusted fact warehouse versions",
    )
    publish_fact.add_argument(
        "--source-month", required=True, help="rental source month in YYYY-MM form"
    )
    publish_fact.add_argument(
        "--transformation-version",
        required=True,
        help="explicit trusted fact transformation code version",
    )
    publish_fact.add_argument(
        "--configuration-json",
        default="{}",
        help="JSON object containing deterministic business configuration",
    )
    publish_date = warehouse_commands.add_parser(
        "publish-date-dimension",
        help="build and atomically publish the deterministic production dim_date",
    )
    publish_date.add_argument(
        "--warehouse-root",
        type=Path,
        required=True,
        help="root for versioned persistent DuckDB warehouse publication",
    )
    publish_date.add_argument(
        "--transformation-version",
        required=True,
        help="explicit date-dimension transformation code version",
    )
    publish_date.add_argument(
        "--configuration-json",
        default="{}",
        help="JSON object containing deterministic business configuration",
    )
    publish_station_day = warehouse_commands.add_parser(
        "publish-station-day-mart",
        help="build and atomically publish one service-month mart_station_day",
    )
    publish_station_day.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    publish_station_day.add_argument(
        "--raw-root", type=Path, required=True, help="root for immutable Raw artifacts"
    )
    publish_station_day.add_argument(
        "--station-root",
        type=Path,
        required=True,
        help="root for immutable station snapshot versions/current pointers",
    )
    publish_station_day.add_argument(
        "--clean-root",
        type=Path,
        required=True,
        help="root for immutable Clean month versions/current pointers",
    )
    publish_station_day.add_argument(
        "--warehouse-root",
        type=Path,
        required=True,
        help="root for fact/date inputs and station-day mart publication",
    )
    publish_station_day.add_argument(
        "--service-month", required=True, help="station-day service month in YYYY-MM form"
    )
    publish_station_day.add_argument(
        "--transformation-version",
        required=True,
        help="explicit station-day mart transformation code version",
    )
    publish_station_day.add_argument(
        "--configuration-json",
        default="{}",
        help="JSON object containing deterministic business configuration",
    )
    plan_station_day_revision = warehouse_commands.add_parser(
        "plan-station-day-revision",
        help="plan station-day service months affected by one trusted-fact revision",
    )
    for option, help_text in (
        ("--manifest", "path of the source manifest JSON file"),
        ("--raw-root", "root for immutable Raw artifacts"),
        ("--station-root", "root for immutable station snapshot versions"),
        ("--clean-root", "root for immutable Clean month versions"),
        ("--warehouse-root", "root containing immutable warehouse versions"),
    ):
        plan_station_day_revision.add_argument(
            option, type=Path, required=True, help=help_text
        )
    plan_station_day_revision.add_argument(
        "--source-month",
        required=True,
        help="revised trusted-fact rental month in YYYY-MM form",
    )
    plan_station_day_revision.add_argument(
        "--old-fact-version-id",
        required=True,
        help="historical fv1_ version before revision",
    )
    plan_station_day_revision.add_argument(
        "--new-fact-version-id",
        required=True,
        help="historical fv1_ version after revision",
    )
    publish_od_day = warehouse_commands.add_parser(
        "publish-od-day-mart",
        help="build and atomically publish one service-month mart_od_day",
    )
    for option, help_text in (
        ("--manifest", "path of the source manifest JSON file"),
        ("--raw-root", "root for immutable Raw artifacts"),
        ("--station-root", "root for immutable station snapshot versions/current pointers"),
        ("--clean-root", "root for immutable Clean month versions/current pointers"),
        ("--warehouse-root", "root for fact/date inputs and OD-day mart publication"),
    ):
        publish_od_day.add_argument(option, type=Path, required=True, help=help_text)
    publish_od_day.add_argument(
        "--service-month", required=True, help="OD-day service month in YYYY-MM form"
    )
    publish_od_day.add_argument(
        "--transformation-version",
        required=True,
        help="explicit OD-day mart transformation code version",
    )
    publish_od_day.add_argument(
        "--configuration-json",
        default="{}",
        help="JSON object containing deterministic business configuration",
    )
    plan_od_day_revision = warehouse_commands.add_parser(
        "plan-od-day-revision",
        help="plan OD-day service months affected by one trusted-fact revision",
    )
    for option, help_text in (
        ("--manifest", "path of the source manifest JSON file"),
        ("--raw-root", "root for immutable Raw artifacts"),
        ("--station-root", "root for immutable station snapshot versions"),
        ("--clean-root", "root for immutable Clean month versions"),
        ("--warehouse-root", "root containing immutable warehouse versions"),
    ):
        plan_od_day_revision.add_argument(
            option, type=Path, required=True, help=help_text
        )
    plan_od_day_revision.add_argument(
        "--source-month",
        required=True,
        help="revised trusted-fact rental month in YYYY-MM form",
    )
    plan_od_day_revision.add_argument(
        "--old-fact-version-id",
        required=True,
        help="historical fv1_ version before revision",
    )
    plan_od_day_revision.add_argument(
        "--new-fact-version-id",
        required=True,
        help="historical fv1_ version after revision",
    )
    publish_source_quality = warehouse_commands.add_parser(
        "publish-source-month-quality",
        help="build and atomically publish one source-version quality mart row",
    )
    for option, help_text in (
        ("--manifest", "path of the source manifest JSON file"),
        ("--raw-root", "root for immutable Raw artifacts"),
        ("--station-root", "root for immutable station snapshot versions/current pointers"),
        ("--clean-root", "root for immutable Clean month versions/current pointers"),
        ("--warehouse-root", "root for trusted fact inputs and quality mart publication"),
    ):
        publish_source_quality.add_argument(
            option, type=Path, required=True, help=help_text
        )
    publish_source_quality.add_argument(
        "--source-month",
        required=True,
        help="rental source month in YYYY-MM form",
    )
    publish_source_quality.add_argument(
        "--transformation-version",
        required=True,
        help="explicit source-month quality mart transformation code version",
    )
    publish_source_quality.add_argument(
        "--configuration-json",
        default="{}",
        help="JSON object containing deterministic business configuration",
    )
    plan_source_quality_revision = warehouse_commands.add_parser(
        "plan-source-month-quality-revision",
        help="plan quality mart source months affected by one source revision",
    )
    plan_source_quality_revision.add_argument(
        "--manifest", type=Path, required=True, help="path of the source manifest JSON file"
    )
    plan_source_quality_revision.add_argument(
        "--raw-root", type=Path, required=True, help="root for immutable Raw artifacts"
    )
    plan_source_quality_revision.add_argument(
        "--source-month",
        required=True,
        help="affected rental source month in YYYY-MM form",
    )
    plan_source_quality_revision.add_argument(
        "--old-source-version-id",
        required=True,
        help="historical sv1_ source version before revision",
    )
    plan_source_quality_revision.add_argument(
        "--new-source-version-id",
        required=True,
        help="historical sv1_ source version after revision",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "pipeline":
            if args.pipeline_command == "backfill-range":
                try:
                    configuration = json.loads(args.configuration_json)
                except json.JSONDecodeError as exc:
                    raise SourceRegistrationError(
                        f"invalid --configuration-json: {exc}"
                    ) from exc
                if not isinstance(configuration, dict):
                    raise SourceRegistrationError(
                        "--configuration-json must decode to a JSON object"
                    )
                _, result = run_backfill_range(
                    manifest=args.manifest,
                    raw_root=args.raw_root,
                    station_root=args.station_root,
                    clean_root=args.clean_root,
                    warehouse_root=args.warehouse_root,
                    start_month=args.start_month,
                    end_month=args.end_month,
                    transformation_version=args.transformation_version,
                    configuration=configuration,
                    dataset_id=args.dataset_id,
                )
                print(
                    f"BACKFILL_RUN status={result.status} "
                    f"range={result.start_month}..{result.end_month} "
                    f"date_version_id={result.date_version_id} "
                    f"failed={','.join(result.failed_source_months) or '-'}"
                )
                for month in result.months:
                    print(
                        f"BACKFILL_MONTH source_month={month.source_month} "
                        f"source_change={month.source_change} status={month.status} "
                        f"clean={month.clean_status or '-'} "
                        f"fact={month.fact_status or '-'} "
                        f"quality={month.quality_status or '-'} "
                        f"od_day={month.od_day_status or '-'} "
                        f"station_day={','.join(item.service_month + ':' + item.status for item in month.station_day) or '-'}"
                    )
                    for failure in month.failures:
                        print(
                            f"BACKFILL_FAILURE source_month={month.source_month} "
                            f"component={failure.component} "
                            f"target_month={failure.target_month} "
                            f"error_type={failure.error_type} "
                            f"message={failure.message}"
                        )
                return 0 if result.status == RUN_SUCCEEDED else 1
        if args.command == "warehouse":
            if args.warehouse_command == "plan-source-month-quality-revision":
                impact = plan_source_month_quality_revision_impact(
                    manifest=args.manifest,
                    raw_root=args.raw_root,
                    source_month=args.source_month,
                    old_source_version_id=args.old_source_version_id,
                    new_source_version_id=args.new_source_version_id,
                )
                print(
                    f"SOURCE_MONTH_QUALITY_REVISION_IMPACT source_month={impact.source_month} "
                    f"old_source_version_id={impact.old_source_version_id} "
                    f"new_source_version_id={impact.new_source_version_id} "
                    f"affected={','.join(impact.affected_source_months)}"
                )
                return 0
            if args.warehouse_command == "publish-source-month-quality":
                try:
                    configuration = json.loads(args.configuration_json)
                except json.JSONDecodeError as exc:
                    raise SourceRegistrationError(
                        f"invalid --configuration-json: {exc}"
                    ) from exc
                if not isinstance(configuration, dict):
                    raise SourceRegistrationError(
                        "--configuration-json must decode to a JSON object"
                    )
                result = publish_current_source_month_quality_mart(
                    manifest=args.manifest,
                    raw_root=args.raw_root,
                    station_root=args.station_root,
                    clean_root=args.clean_root,
                    warehouse_root=args.warehouse_root,
                    source_month=args.source_month,
                    transformation_version=args.transformation_version,
                    configuration=configuration,
                )
                print(
                    f"SOURCE_MONTH_QUALITY_PUBLISH_OK {result.mart_version_id} "
                    f"status={result.status} "
                    f"source_month={result.source_month} "
                    f"source_version_id={result.source_version_id} "
                    f"source_rows={result.source_row_count} "
                    f"clean_rows={result.clean_row_count} "
                    f"trusted={result.trusted_fact_count} "
                    f"quarantine={result.quarantine_count}"
                )
                return 0
            if args.warehouse_command == "plan-od-day-revision":
                impact = plan_od_day_fact_revision_impact(
                    manifest=args.manifest,
                    raw_root=args.raw_root,
                    station_root=args.station_root,
                    clean_root=args.clean_root,
                    warehouse_root=args.warehouse_root,
                    source_month=args.source_month,
                    old_fact_version_id=args.old_fact_version_id,
                    new_fact_version_id=args.new_fact_version_id,
                )
                print(
                    f"OD_DAY_REVISION_IMPACT source_month={impact.source_month} "
                    f"old_fact_version_id={impact.old_fact_version_id} "
                    f"new_fact_version_id={impact.new_fact_version_id} "
                    f"affected={','.join(impact.affected_service_months)}"
                )
                return 0
            if args.warehouse_command == "publish-od-day-mart":
                try:
                    configuration = json.loads(args.configuration_json)
                except json.JSONDecodeError as exc:
                    raise SourceRegistrationError(
                        f"invalid --configuration-json: {exc}"
                    ) from exc
                if not isinstance(configuration, dict):
                    raise SourceRegistrationError(
                        "--configuration-json must decode to a JSON object"
                    )
                result = publish_current_od_day_mart(
                    manifest=args.manifest,
                    raw_root=args.raw_root,
                    station_root=args.station_root,
                    clean_root=args.clean_root,
                    warehouse_root=args.warehouse_root,
                    service_month=args.service_month,
                    transformation_version=args.transformation_version,
                    configuration=configuration,
                )
                print(
                    f"OD_DAY_MART_PUBLISH_OK {result.mart_version_id} "
                    f"status={result.status} "
                    f"service_month={result.service_month} "
                    f"mart_rows={result.mart_row_count} "
                    f"trips={result.trip_count_total} "
                    f"valid_distance_trips={result.valid_distance_trip_count_total}"
                )
                return 0
            if args.warehouse_command == "plan-station-day-revision":
                impact = plan_station_day_fact_revision_impact(
                    manifest=args.manifest,
                    raw_root=args.raw_root,
                    station_root=args.station_root,
                    clean_root=args.clean_root,
                    warehouse_root=args.warehouse_root,
                    source_month=args.source_month,
                    old_fact_version_id=args.old_fact_version_id,
                    new_fact_version_id=args.new_fact_version_id,
                )
                print(
                    f"STATION_DAY_REVISION_IMPACT source_month={impact.source_month} "
                    f"old_fact_version_id={impact.old_fact_version_id} "
                    f"new_fact_version_id={impact.new_fact_version_id} "
                    f"affected={','.join(impact.affected_service_months)}"
                )
                return 0
            if args.warehouse_command == "publish-station-day-mart":
                try:
                    configuration = json.loads(args.configuration_json)
                except json.JSONDecodeError as exc:
                    raise SourceRegistrationError(
                        f"invalid --configuration-json: {exc}"
                    ) from exc
                if not isinstance(configuration, dict):
                    raise SourceRegistrationError(
                        "--configuration-json must decode to a JSON object"
                    )
                result = publish_current_station_day_mart(
                    manifest=args.manifest,
                    raw_root=args.raw_root,
                    station_root=args.station_root,
                    clean_root=args.clean_root,
                    warehouse_root=args.warehouse_root,
                    service_month=args.service_month,
                    transformation_version=args.transformation_version,
                    configuration=configuration,
                )
                print(
                    f"STATION_DAY_MART_PUBLISH_OK {result.mart_version_id} "
                    f"status={result.status} "
                    f"service_month={result.service_month} "
                    f"mart_rows={result.mart_row_count} "
                    f"rentals={result.rental_count_total} "
                    f"returns={result.return_count_total} "
                    f"completed={result.completed_trip_count_total} "
                    f"missing_return={result.missing_return_count_total}"
                )
                return 0
            if args.warehouse_command == "publish-date-dimension":
                try:
                    configuration = json.loads(args.configuration_json)
                except json.JSONDecodeError as exc:
                    raise SourceRegistrationError(
                        f"invalid --configuration-json: {exc}"
                    ) from exc
                if not isinstance(configuration, dict):
                    raise SourceRegistrationError(
                        "--configuration-json must decode to a JSON object"
                    )
                result = publish_date_dimension(
                    warehouse_root=args.warehouse_root,
                    transformation_version=args.transformation_version,
                    configuration=configuration,
                )
                print(
                    f"DATE_DIMENSION_PUBLISH_OK {result.date_version_id} "
                    f"status={result.status} "
                    f"coverage_start={result.coverage_start.isoformat()} "
                    f"coverage_end={result.coverage_end.isoformat()} "
                    f"date_rows={result.date_row_count}"
                )
                return 0
            if args.warehouse_command == "publish-fact-trip-month":
                try:
                    configuration = json.loads(args.configuration_json)
                except json.JSONDecodeError as exc:
                    raise SourceRegistrationError(
                        f"invalid --configuration-json: {exc}"
                    ) from exc
                if not isinstance(configuration, dict):
                    raise SourceRegistrationError(
                        "--configuration-json must decode to a JSON object"
                    )
                result = publish_current_fact_trip_month(
                    manifest=args.manifest,
                    raw_root=args.raw_root,
                    station_root=args.station_root,
                    clean_root=args.clean_root,
                    warehouse_root=args.warehouse_root,
                    source_month=args.source_month,
                    transformation_version=args.transformation_version,
                    configuration=configuration,
                )
                print(
                    f"FACT_TRIP_PUBLISH_OK {result.fact_version_id} "
                    f"status={result.status} "
                    f"source_month={result.source_month} "
                    f"clean_version_id={result.clean_version_id} "
                    f"history_version_id={result.history_version_id} "
                    f"fact_rows={result.fact_row_count} "
                    f"matched={result.station_history_match_count} "
                    f"unmatched={result.station_history_unmatched_count} "
                    f"warning_rows={result.warning_row_count}"
                )
                return 0
            if args.warehouse_command == "evaluate-station-enrichment":
                result = evaluate_current_trip_station_enrichment(
                    manifest=args.manifest,
                    raw_root=args.raw_root,
                    station_root=args.station_root,
                    clean_root=args.clean_root,
                    warehouse_root=args.warehouse_root,
                    source_month=args.source_month,
                )
                print(
                    f"STATION_ENRICHMENT_OK source_month={result.source_month} "
                    f"clean_version_id={result.clean_version_id} "
                    f"history_version_id={result.history_version_id} "
                    f"trusted_candidates={result.trusted_candidate_count} "
                    f"matched={result.station_history_match_count} "
                    f"unmatched={result.station_history_unmatched_count} "
                    f"warnings_added={result.station_history_warning_added_count}"
                )
                return 0
            if args.warehouse_command == "publish-station-history":
                try:
                    configuration = json.loads(args.configuration_json)
                except json.JSONDecodeError as exc:
                    raise SourceRegistrationError(
                        f"invalid --configuration-json: {exc}"
                    ) from exc
                if not isinstance(configuration, dict):
                    raise SourceRegistrationError(
                        "--configuration-json must decode to a JSON object"
                    )
                result = publish_station_history(
                    manifest=args.manifest,
                    raw_root=args.raw_root,
                    station_root=args.station_root,
                    warehouse_root=args.warehouse_root,
                    transformation_version=args.transformation_version,
                    configuration=configuration,
                )
                print(
                    f"STATION_HISTORY_PUBLISH_OK {result.history_version_id} "
                    f"status={result.status} "
                    f"snapshots={result.snapshot_count} "
                    f"input_observations={result.input_observation_count} "
                    f"history_rows={result.history_row_count} "
                    f"stations={result.distinct_station_count} "
                    f"open_intervals={result.open_interval_count}"
                )
                return 0
        if args.source_command == "register":
            result = register_source(
                artifact=args.artifact,
                manifest=args.manifest,
                raw_root=args.raw_root,
                dataset_id=args.dataset_id,
                logical_period=args.logical_period,
                published_filename=args.published_filename,
                source_url=args.source_url,
                license=args.license,
                source_modified_at=args.source_modified_at,
            )
            print(f"{result.status} {result.record.source_version_id}")
            return 0
        if args.source_command == "inventory-zip":
            result = inventory_registered_zip(
                manifest=args.manifest,
                raw_root=args.raw_root,
                source_version_id=args.source_version_id,
            )
            print(
                f"{result.status} {result.inventory.parent_source_version_id} "
                f"members={len(result.inventory.members)}"
            )
            return 0
        if args.source_command == "inspect-csv":
            summary = read_csv_source_unit(
                manifest=args.manifest,
                raw_root=args.raw_root,
                source_version_id=args.source_version_id,
                member_name=args.member_name,
            )
            print(
                f"CSV_OK {summary.source_version_id} "
                f"schema={summary.schema_version} encoding={summary.encoding} "
                f"rows={summary.row_count} member={ascii(summary.member_name)}"
            )
            return 0
        if args.source_command == "project-csv":
            summary = project_csv_source_unit(
                manifest=args.manifest,
                raw_root=args.raw_root,
                source_version_id=args.source_version_id,
                member_name=args.member_name,
            )
            print(
                f"PROJECT_OK {summary.source_version_id} "
                f"schema={summary.schema_version} rows={summary.row_count} "
                f"conversion_issues={summary.conversion_issue_count} "
                f"month_match={summary.month_match_count} "
                f"month_mismatch={summary.month_mismatch_count} "
                f"month_unavailable={summary.month_unavailable_count} "
                f"source_months={','.join(summary.expected_source_months)} "
                f"member={ascii(summary.member_name)}"
            )
            return 0
        if args.source_command == "canonicalize-csv":
            summary = canonicalize_csv_source_unit(
                manifest=args.manifest,
                raw_root=args.raw_root,
                source_version_id=args.source_version_id,
                member_name=args.member_name,
            )
            print(
                f"CANONICAL_OK {summary.source_version_id} "
                f"schema={summary.schema_version} rows={summary.row_count} "
                f"quarantined={summary.quarantined_row_count} "
                f"warning_rows={summary.warning_row_count} "
                f"warnings={summary.warning_issue_count} "
                f"completed={summary.completed_trip_count} "
                f"distance_eligible={summary.distance_metric_eligible_count} "
                f"source_months={','.join(summary.expected_source_months)} "
                f"member={ascii(summary.member_name)}"
            )
            return 0
        if args.source_command == "classify-month":
            summary = classify_registered_source_month(
                manifest=args.manifest,
                raw_root=args.raw_root,
                source_version_id=args.source_version_id,
                source_month=args.source_month,
            )
            print(
                f"GROUP_DQ_OK {summary.source_version_id} "
                f"source_month={summary.source_month} "
                f"clean={summary.clean_row_count} "
                f"trusted={summary.trusted_fact_eligible_count} "
                f"quarantined={summary.quarantined_row_count} "
                f"conflict_groups={summary.logical_conflict_group_count} "
                f"exact_duplicate_groups={summary.exact_duplicate_group_count} "
                f"conflict_rows={summary.logical_conflict_row_count} "
                f"exact_duplicate_rows={summary.exact_duplicate_row_count}"
            )
            return 0
        if args.source_command == "publish-clean-month":
            try:
                configuration = json.loads(args.configuration_json)
            except json.JSONDecodeError as exc:
                raise SourceRegistrationError(f"invalid --configuration-json: {exc}") from exc
            if not isinstance(configuration, dict):
                raise SourceRegistrationError("--configuration-json must decode to a JSON object")
            result = publish_clean_month(
                manifest=args.manifest,
                raw_root=args.raw_root,
                clean_root=args.clean_root,
                source_version_id=args.source_version_id,
                source_month=args.source_month,
                transformation_version=args.transformation_version,
                configuration=configuration,
            )
            print(
                f"CLEAN_PUBLISH_OK {result.clean_version_id} "
                f"status={result.status} "
                f"source_month={result.source_month} "
                f"source_version_id={result.source_version_id} "
                f"clean={result.clean_row_count} "
                f"trusted={result.trusted_fact_eligible_count} "
                f"quarantined={result.quarantined_row_count} "
                f"quarantine_issues={result.quarantine_issue_count}"
            )
            return 0
        if args.source_command == "publish-station-snapshot":
            try:
                configuration = json.loads(args.configuration_json)
            except json.JSONDecodeError as exc:
                raise SourceRegistrationError(f"invalid --configuration-json: {exc}") from exc
            if not isinstance(configuration, dict):
                raise SourceRegistrationError("--configuration-json must decode to a JSON object")
            result = publish_station_snapshot(
                manifest=args.manifest,
                raw_root=args.raw_root,
                station_root=args.station_root,
                source_version_id=args.source_version_id,
                transformation_version=args.transformation_version,
                configuration=configuration,
            )
            print(
                f"STATION_PUBLISH_OK {result.station_version_id} "
                f"status={result.status} "
                f"observation_period={result.observation_period} "
                f"source_version_id={result.source_version_id} "
                f"source_rows={result.source_row_count} "
                f"observations={result.observation_count} "
                f"quarantined_source_rows={result.quarantined_source_row_count} "
                f"quarantine_issues={result.quarantine_issue_count}"
            )
            return 0
        plan = plan_zip_months(
            manifest=args.manifest,
            raw_root=args.raw_root,
            current_source_version_id=args.current_source_version_id,
            previous_source_version_id=args.previous_source_version_id,
        )
    except (SourceRegistrationError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    for impact in plan.impacts:
        print(
            f"{impact.logical_month} {impact.status} "
            f"current_members={len(impact.current_members)} "
            f"previous_members={len(impact.previous_members)}"
        )
    return 0
