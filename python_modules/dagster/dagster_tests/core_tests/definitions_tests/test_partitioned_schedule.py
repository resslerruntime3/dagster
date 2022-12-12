from datetime import datetime
from typing import cast

import dagster._seven.compat.pendulum as pendulum
from dagster import build_schedule_context, graph, op, repository
from dagster._core.definitions.partitioned_schedule import build_schedule_from_partitioned_job
from dagster._core.definitions.time_window_partitions import (
    TimeWindow,
    daily_partitioned_config,
    hourly_partitioned_config,
    monthly_partitioned_config,
    weekly_partitioned_config,
)

DATE_FORMAT = "%Y-%m-%d"


def time_window(start: str, end: str) -> TimeWindow:
    return TimeWindow(cast(datetime, pendulum.parse(start)), cast(datetime, pendulum.parse(end)))


def schedule_for_partitioned_config(
    partitioned_config, minute_of_hour=None, hour_of_day=None, day_of_week=None, day_of_month=None
):
    @op
    def my_op():
        pass

    @graph
    def my_graph():
        my_op()

    return build_schedule_from_partitioned_job(
        my_graph.to_job(config=partitioned_config),
        minute_of_hour=minute_of_hour,
        hour_of_day=hour_of_day,
        day_of_week=day_of_week,
        day_of_month=day_of_month,
        tags={"test_tag_key": "test_tag_value"},
    )


def test_daily_schedule():
    @daily_partitioned_config(start_date="2021-05-05")
    def my_partitioned_config(start, end):
        return {"start": str(start), "end": str(end)}

    keys = my_partitioned_config.get_partition_keys()

    assert keys[0] == "2021-05-05"
    assert keys[1] == "2021-05-06"

    partitions = my_partitioned_config.partitions_def.get_partitions()
    assert partitions[0].value == time_window("2021-05-05", "2021-05-06")

    assert my_partitioned_config.get_run_config_for_partition_key(keys[0]) == {
        "start": "2021-05-05T00:00:00+00:00",
        "end": "2021-05-06T00:00:00+00:00",
    }

    my_schedule = schedule_for_partitioned_config(
        my_partitioned_config, hour_of_day=9, minute_of_hour=30
    )
    assert my_schedule.cron_schedule == "30 9 * * *"

    run_request = my_schedule.evaluate_tick(
        build_schedule_context(
            scheduled_execution_time=datetime.strptime("2021-05-08", DATE_FORMAT)
        )
    ).run_requests[0]
    assert run_request.run_config == {
        "start": "2021-05-07T00:00:00+00:00",
        "end": "2021-05-08T00:00:00+00:00",
    }
    assert run_request.tags["test_tag_key"] == "test_tag_value"

    @repository
    def _repo():
        return [my_schedule]


def test_daily_schedule_with_offsets():
    @daily_partitioned_config(start_date="2021-05-05", minute_offset=15, hour_offset=2)
    def my_partitioned_config(start, end):
        return {"start": str(start), "end": str(end)}

    keys = my_partitioned_config.get_partition_keys()
    assert keys[0] == "2021-05-05"
    assert keys[1] == "2021-05-06"

    partitions = my_partitioned_config.partitions_def.get_partitions()
    assert partitions[0].value == time_window("2021-05-05T02:15:00", "2021-05-06T02:15:00")

    assert my_partitioned_config.get_run_config_for_partition_key(keys[0]) == {
        "start": "2021-05-05T02:15:00+00:00",
        "end": "2021-05-06T02:15:00+00:00",
    }

    my_schedule_default = schedule_for_partitioned_config(my_partitioned_config)
    assert my_schedule_default.cron_schedule == "15 2 * * *"

    my_schedule = schedule_for_partitioned_config(
        my_partitioned_config, hour_of_day=9, minute_of_hour=30
    )
    assert my_schedule.cron_schedule == "30 9 * * *"

    assert my_schedule.evaluate_tick(
        build_schedule_context(scheduled_execution_time=datetime(2021, 5, 8, 9, 30))
    ).run_requests[0].run_config == {
        "start": "2021-05-07T02:15:00+00:00",
        "end": "2021-05-08T02:15:00+00:00",
    }

    @repository
    def _repo():
        return [my_schedule]


def test_hourly_schedule():
    @hourly_partitioned_config(start_date=datetime(2021, 5, 5))
    def my_partitioned_config(start, end):
        return {"start": str(start), "end": str(end)}

    keys = my_partitioned_config.get_partition_keys()
    assert keys[0] == "2021-05-05-00:00"
    assert keys[1] == "2021-05-05-01:00"

    partitions = my_partitioned_config.partitions_def.get_partitions()
    assert partitions[0].value == time_window("2021-05-05T00:00:00", "2021-05-05T01:00:00")

    assert my_partitioned_config.get_run_config_for_partition_key(keys[0]) == {
        "start": "2021-05-05T00:00:00+00:00",
        "end": "2021-05-05T01:00:00+00:00",
    }

    my_schedule_default = schedule_for_partitioned_config(my_partitioned_config)
    assert my_schedule_default.cron_schedule == "0 * * * *"

    my_schedule = schedule_for_partitioned_config(my_partitioned_config, minute_of_hour=30)
    assert my_schedule.cron_schedule == "30 * * * *"

    assert my_schedule.evaluate_tick(
        build_schedule_context(
            scheduled_execution_time=datetime.strptime("2021-05-08", DATE_FORMAT)
        )
    ).run_requests[0].run_config == {
        "start": "2021-05-07T23:00:00+00:00",
        "end": "2021-05-08T00:00:00+00:00",
    }

    @repository
    def _repo():
        return [my_schedule]


def test_hourly_schedule_with_offsets():
    @hourly_partitioned_config(start_date=datetime(2021, 5, 5), minute_offset=20)
    def my_partitioned_config(start, end):
        return {"start": str(start), "end": str(end)}

    keys = my_partitioned_config.get_partition_keys()
    assert keys[0] == "2021-05-05-00:20"
    assert keys[1] == "2021-05-05-01:20"

    partitions = my_partitioned_config.partitions_def.get_partitions()
    assert partitions[0].value == time_window("2021-05-05T00:20:00", "2021-05-05T01:20:00")

    assert my_partitioned_config.get_run_config_for_partition_key(keys[0]) == {
        "start": "2021-05-05T00:20:00+00:00",
        "end": "2021-05-05T01:20:00+00:00",
    }

    my_schedule = schedule_for_partitioned_config(my_partitioned_config, minute_of_hour=30)
    assert my_schedule.cron_schedule == "30 * * * *"

    assert my_schedule.evaluate_tick(
        build_schedule_context(
            scheduled_execution_time=datetime.strptime("2021-05-08", DATE_FORMAT)
        )
    ).run_requests[0].run_config == {
        "start": "2021-05-07T22:20:00+00:00",
        "end": "2021-05-07T23:20:00+00:00",
    }

    @repository
    def _repo():
        return [my_schedule]


def test_weekly_schedule():
    @weekly_partitioned_config(start_date="2021-05-05")
    def my_partitioned_config(start, end):
        return {"start": str(start), "end": str(end)}

    keys = my_partitioned_config.get_partition_keys()
    assert keys[0] == "2021-05-09"
    assert keys[1] == "2021-05-16"

    partitions = my_partitioned_config.partitions_def.get_partitions()
    assert partitions[0].value == time_window("2021-05-09", "2021-05-16")

    assert my_partitioned_config.get_run_config_for_partition_key(keys[0]) == {
        "start": "2021-05-09T00:00:00+00:00",
        "end": "2021-05-16T00:00:00+00:00",
    }

    my_schedule = schedule_for_partitioned_config(
        my_partitioned_config, hour_of_day=9, minute_of_hour=30, day_of_week=2
    )
    assert my_schedule.cron_schedule == "30 9 * * 2"

    assert my_schedule.evaluate_tick(
        build_schedule_context(
            scheduled_execution_time=datetime.strptime("2021-05-21", DATE_FORMAT)
        )
    ).run_requests[0].run_config == {
        "start": "2021-05-09T00:00:00+00:00",
        "end": "2021-05-16T00:00:00+00:00",
    }

    @repository
    def _repo():
        return [my_schedule]


def test_weekly_schedule_with_offsets():
    @weekly_partitioned_config(
        start_date="2021-05-05", minute_offset=10, hour_offset=13, day_offset=3
    )
    def my_partitioned_config(start, end):
        return {"start": str(start), "end": str(end)}

    keys = my_partitioned_config.get_partition_keys()
    assert keys[0] == "2021-05-05"
    assert keys[1] == "2021-05-12"

    partitions = my_partitioned_config.partitions_def.get_partitions()
    assert partitions[0].value == time_window("2021-05-05T13:10:00", "2021-05-12T13:10:00")

    assert my_partitioned_config.get_run_config_for_partition_key(keys[0]) == {
        "start": "2021-05-05T13:10:00+00:00",
        "end": "2021-05-12T13:10:00+00:00",
    }

    my_schedule = schedule_for_partitioned_config(
        my_partitioned_config, hour_of_day=9, minute_of_hour=30, day_of_week=2
    )
    assert my_schedule.cron_schedule == "30 9 * * 2"

    assert my_schedule.evaluate_tick(
        build_schedule_context(
            scheduled_execution_time=datetime.strptime("2021-05-21", DATE_FORMAT)
        )
    ).run_requests[0].run_config == {
        "start": "2021-05-12T13:10:00+00:00",
        "end": "2021-05-19T13:10:00+00:00",
    }

    @repository
    def _repo():
        return [my_schedule]


def test_monthly_schedule():
    @monthly_partitioned_config(start_date="2021-05-05")
    def my_partitioned_config(start, end):
        return {"start": str(start), "end": str(end)}

    keys = my_partitioned_config.get_partition_keys()
    assert keys[0] == "2021-06-01"
    assert keys[1] == "2021-07-01"

    partitions = my_partitioned_config.partitions_def.get_partitions()
    assert partitions[0].value == time_window("2021-06-01", "2021-07-01")

    assert my_partitioned_config.get_run_config_for_partition_key(keys[0]) == {
        "start": "2021-06-01T00:00:00+00:00",
        "end": "2021-07-01T00:00:00+00:00",
    }

    my_schedule = schedule_for_partitioned_config(
        my_partitioned_config, hour_of_day=9, minute_of_hour=30, day_of_month=2
    )
    assert my_schedule.cron_schedule == "30 9 2 * *"

    assert my_schedule.evaluate_tick(
        build_schedule_context(
            scheduled_execution_time=datetime.strptime("2021-07-21", DATE_FORMAT)
        )
    ).run_requests[0].run_config == {
        "start": "2021-06-01T00:00:00+00:00",
        "end": "2021-07-01T00:00:00+00:00",
    }

    @repository
    def _repo():
        return [my_schedule]


def test_monthly_schedule_with_offsets():
    @monthly_partitioned_config(
        start_date="2021-05-05", minute_offset=15, hour_offset=16, day_offset=12
    )
    def my_partitioned_config(start, end):
        return {"start": str(start), "end": str(end)}

    keys = my_partitioned_config.get_partition_keys()
    assert keys[0] == "2021-05-12"
    assert keys[1] == "2021-06-12"

    partitions = my_partitioned_config.partitions_def.get_partitions()
    assert partitions[0].value == time_window("2021-05-12T16:15:00", "2021-06-12T16:15:00")

    assert my_partitioned_config.get_run_config_for_partition_key(keys[0]) == {
        "start": "2021-05-12T16:15:00+00:00",
        "end": "2021-06-12T16:15:00+00:00",
    }

    my_schedule = schedule_for_partitioned_config(
        my_partitioned_config, hour_of_day=9, minute_of_hour=30, day_of_month=2
    )
    assert my_schedule.cron_schedule == "30 9 2 * *"

    assert my_schedule.evaluate_tick(
        build_schedule_context(
            scheduled_execution_time=datetime.strptime("2021-06-21", DATE_FORMAT)
        )
    ).run_requests[0].run_config == {
        "start": "2021-05-12T16:15:00+00:00",
        "end": "2021-06-12T16:15:00+00:00",
    }

    @repository
    def _repo():
        return [my_schedule]
