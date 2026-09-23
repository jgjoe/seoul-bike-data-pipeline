"""Register the bounded Seoul Bike timetable with Airflow serialization."""

from airflow.plugins_manager import AirflowPlugin

from seoul_bike_pipeline.airflow_timetable import BoundedCronDataIntervalTimetable


class SeoulBikeTimetablePlugin(AirflowPlugin):
    name = "seoul_bike_timetable_plugin"
    timetables = [BoundedCronDataIntervalTimetable]
