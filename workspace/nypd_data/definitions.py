import dagster
import dagster_dbt
import dagster_prometheus

from nypd_data.assets import notify_prometheus_failure_sensor, notify_prometheus_status_sensor, DBT_PROJECT_ARGS
from nypd_data.jobs import *


all_assets = dagster.load_assets_from_current_module(group_name='nypd')
nypd_build_schedule = dagster.ScheduleDefinition(
    job=all_assets_job,
    cron_schedule="0 0 * * *",
)

definitions = dagster.Definitions(
    assets=all_assets,
    jobs=[nypd_data_job, all_assets_job],
    schedules=[nypd_build_schedule],
    sensors=[notify_prometheus_failure_sensor, notify_prometheus_status_sensor],
    resources={
        "dbt": dagster_dbt.DbtCliResource(**DBT_PROJECT_ARGS),
        "prometheus": dagster_prometheus.PrometheusResource(gateway="http://dag-monitor:9090"),
    }
)

## https://github.com/slopp/dagteam
