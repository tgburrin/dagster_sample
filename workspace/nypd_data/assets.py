import http.client
import os
import urllib.parse
import logging
import json
import decimal
import datetime
import ssl

from typing import Optional, Any

import dagster_dbt
import psycopg2
import psycopg2.extras

import dagster
from dagster import asset, op, graph, graph_asset, OpExecutionContext, AssetExecutionContext, \
    run_failure_sensor, RunFailureSensorContext, \
    run_status_sensor, RunStatusSensorContext
from dagster_prometheus import PrometheusResource, prometheus_resource

from dagster_dbt import dbt_assets, DbtProject, DbtCliResource

import prometheus_client as pc

import dbt_core_interface as dbtci

# https://data.cityofnewyork.us/resource/uip8-fykc.json
# ?$offset=1000

# https://data.cityofnewyork.us/Public-Safety/NYPD-Arrest-Data-Year-to-Date-/uip8-fykc/about_data

from utils.json_utils import CustomJsonEncoder, CustomJsonDecoder

NYC_DATA_HOST = "data.cityofnewyork.us"
OUTPUT_FILE = "/working_dir/nypd_arrests_{dt}.json"
COLUMN_MAP = {
    'arrest_key': {"position": 0, "name": "arrest_key", "type": "int"},
    'arrest_date': {"position": 0, "name": "arrest_date", "type": "date"},
    'pd_cd': {"position": 0, "name": "arrest_code", "type": "int"},
    'pd_desc': {"position": 0, "name": "arrest_code_description", "type": "text"},
    'ky_cd': {"position": 0, "name": "arrest_class_code", "type": "int"},
    'ofns_desc': {"position": 0, "name": "arrest_class_code_description", "type": "text"},
    'law_code': {"position": 0, "name": "law_code", "type": "text"},
    'law_cat_cd': {"position": 0, "name": "law_code_description", "type": "text"},
    'arrest_boro': {"position": 0, "name": "borough_code", "type": "text"},  # char
    'arrest_precinct': {"position": 0, "name": "precinct", "type": "int"},
    'jurisdiction_code': {"position": 0, "name": "jurisdiction_code", "type": "int"},
    'age_group': {"position": 0, "name": "age_group", "type": "text"},
    'perp_sex': {"position": 0, "name": "gender", "type": "text"},  # char
    'perp_race': {"position": 0, "name": "race", "type": "text"},
    'latitude': {"position": 0, "name": "lat", "type": "text"},
    'longitude': {"position": 0, "name": "long", "type": "text"},
}

DBT_PROJECT_ARGS = {
    "project_dir": "/dbt_projects/dbt_nypd_data",
    "profiles_dir": "/dbt_projects/profiles",
    "profile": "dbt_nypd_data",
    "target": "dev",
}


dbt_nypd_data_project = dagster_dbt.DbtProject(**DBT_PROJECT_ARGS)


@run_failure_sensor
def notify_prometheus_failure_sensor(context: RunFailureSensorContext, prometheus: PrometheusResource):
    pc.Info("error_message", context.failure_event.message, registry=prometheus.registry)
    prometheus.push_to_gateway(job=context.dagster_run.job_name)


@run_status_sensor(run_status=dagster.DagsterRunStatus.SUCCESS)
def notify_prometheus_status_sensor(context: RunStatusSensorContext, prometheus: PrometheusResource):
    #prometheus.delete_from_gateway(job=context.dagster_run.job_name)
    pc.Info("complete", context.dagster_run.job_name, registry=prometheus.registry)
    prometheus.push_to_gateway(job=context.dagster_run.job_name)


def download_arrest_data() -> Optional[str]:
    n = datetime.datetime.now(datetime.UTC)

    fn = OUTPUT_FILE.format(dt=n.strftime("%Y%m%d"))
    tfn = OUTPUT_FILE.format(dt=n.strftime("%Y%m%d")+"_tmp")

    os.makedirs(os.path.dirname(fn), exist_ok=True)

    # Disables strict ssl checking on this public resource
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    if os.path.isfile(fn):
        logging.log(logging.INFO, "Arrest data already downloaded")
        return fn

    with open(tfn, "w+") as f:
        rowcount = 0
        endpoint = "/resource/uip8-fykc.json"
        cli = http.client.HTTPSConnection(host=NYC_DATA_HOST, context=ctx)
        url_args = {"$order": "arrest_key"}
        req_args = urllib.parse.urlencode(url_args)
        req_string = f'{endpoint}?{req_args}'
        running = True
        while running:
            logging.log(logging.DEBUG, f"Requesting: {req_string}")
            cli.request("GET", req_string)
            resp = cli.getresponse()
            if resp.status not in [200]:
                logging.log(logging.DEBUG, f"Returned {resp.status}")
                logging.log(logging.DEBUG, f"Headers {resp.headers}")
                raise ValueError(resp.read().decode())

            payload = resp.read()
            rows = json.loads(payload, cls=CustomJsonDecoder)
            if rows:
                for r in rows:
                    f.write(json.dumps(r) + "\n")

                if '$offset' not in url_args:
                    url_args['$offset'] = 0
                rowcount += len(rows)
                url_args['$offset'] += len(rows)
                req_args = urllib.parse.urlencode(url_args)
                req_string = f'{endpoint}?{req_args}'
                # running = False
            else:
                running = False

        logging.log(logging.DEBUG, f"Returned {rowcount} rows")
        cli.close()

    os.rename(tfn, fn)
    return fn


def cleanup_downloaded(fn: str, cnt: int):
    if cnt:
        logging.log(logging.INFO, f"Removing {fn}")
        os.remove(fn)
        return True
    return False


def load_nypd_arrest_records(fn) -> Optional[int]:
    record_count = 0

    # column_fields = set([v.get('name', k) for k, v in COLUMN_MAP.items()])
    # with psycopg2.connect("postgresql://analytics:passw0rd@dag-postgres/analytics") as dbc:
    #     with dbc.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
    #         cur.execute("select count(*) as total from nypd_arrests")
    #         for r in cur.fetchall():
    #             record_count = r['total']

    if record_count:
        logging.log(logging.INFO, f"There are {record_count} records already in the database")
        return record_count
    else:
        logging.log(logging.INFO, f"Doing a truncate and load")

    with open(fn) as ifd:
        with psycopg2.connect("postgresql://analytics:passw0rd@dag-postgres/analytics") as dbc:
            dbc.autocommit = False
            with dbc.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("truncate table nypd_arrests")
            with dbc.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                for line in ifd:
                    ad = json.loads(line)
                    # rec_fields = set(ad.keys())
                    # missing_fields = column_fields.difference(rec_fields)
                    # if missing_fields:
                    #     print(f"Record is missing some mapped fields {', '.join(missing_fields)}")
                    #     continue

                    row = {}
                    for k, v in ad.items():
                        m = COLUMN_MAP.get(k)
                        if m is None:
                            logging.log(logging.DEBUG, f"Skipping {k}")
                            continue

                        if m['type'] == "int" and v is not None:
                            v = int(v)
                        elif m['type'] == "decimal":
                            v = decimal.Decimal(v)
                        elif m['type'] == 'epoch_ts':
                            v = datetime.datetime.utcfromtimestamp(v).replace(tzinfo=datetime.timezone.utc)
                        elif m['type'] == 'date':
                            v = datetime.datetime.strptime(v, "%Y-%m-%dT%H:%M:%S.%f").date()

                        if k != m['name']:
                            row[m['name']] = v
                        else:
                            row[k] = v

                    row['location'] = f"({row.pop('lat')}, {row.pop('long')})"
                    if row['age_group'] == '(null)':
                        row['age_group'] = None
                    elif row['age_group'] == "<18":
                        row['age_group'] = "[,18)"
                    elif row['age_group'] == "65+":
                        row['age_group'] = "[65,]"
                    else:
                        row['age_group'] = f"[{','.join(row['age_group'].split('-'))}]"

                    if row['gender'] == '(null)':
                        row['gender'] = None

                    fields_list = sorted(row.keys())
                    fields_str = ",".join(fields_list)
                    fields_places = ",".join(["%s"] * len(fields_list))

                    # if not row.get('arrest_code'):
                    #     print(f"Skipping record {row['arrest_key']} based on missing field", file=sys.stderr)
                    #     continue

                    sql = f"insert into nypd_arrests ({fields_str}) values ({fields_places}) on conflict do nothing"
                    cur.execute(sql, [row[f] for f in fields_list])
                    record_count += 1
            dbc.commit()

    return record_count


@asset
def nypd_raw_data() -> int:
    fn = download_arrest_data()
    cnt = load_nypd_arrest_records(fn)
    cleanup_downloaded(fn, cnt)
    return cnt


@dbt_assets(
    manifest=dbt_nypd_data_project.manifest_path
)
def nypd_dbt_project(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

# @asset
# def nypd_dbt_project(nypd_raw_data: int) -> bool:
#     if not nypd_raw_data:
#         return True
#
#     proj = dbtci.DbtProject(**DBT_PROJECT_ARGS)
#     res = proj.build()
#     if not res.success:
#         raise Exception(str(res.exception))
#
#     return True
