"""Airflow timetable primitives for the bounded production schedule.

The production data horizon is a scheduling concern, not a task-eligibility
constraint. A DAG-level ``end_date`` is therefore unsuitable: Airflow 3.3.x
propagates it to tasks, which prevents later manual historical runs from
creating task instances. This timetable keeps monthly data-interval semantics
while stopping automated interval creation after the configured source month.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pendulum
from airflow.timetables.interval import CronDataIntervalTimetable

if TYPE_CHECKING:
    from pendulum import DateTime

    from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction


class BoundedCronDataIntervalTimetable(CronDataIntervalTimetable):
    """Cron data intervals with an explicit last automated interval start."""

    def __init__(
        self,
        cron: str,
        *,
        timezone,
        last_interval_start: DateTime,
    ) -> None:
        super().__init__(cron, timezone=timezone)
        self._last_interval_start = last_interval_start

    @property
    def last_interval_start(self) -> DateTime:
        return self._last_interval_start

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        info = super().next_dagrun_info(
            last_automated_data_interval=last_automated_data_interval,
            restriction=restriction,
        )
        if info is None or info.data_interval.start > self._last_interval_start:
            return None
        return info

    @classmethod
    def deserialize(cls, data: dict[str, Any]):
        base = CronDataIntervalTimetable.deserialize(
            {
                "expression": data["expression"],
                "timezone": data["timezone"],
            }
        )
        return cls(
            data["expression"],
            timezone=base._timezone,
            last_interval_start=pendulum.parse(data["last_interval_start"]),
        )

    def serialize(self) -> dict[str, Any]:
        data = super().serialize()
        data["last_interval_start"] = self._last_interval_start.isoformat()
        return data
