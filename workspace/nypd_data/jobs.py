from dagster import job, define_asset_job
from nypd_data.assets import nypd_raw_data, nypd_dbt_project

# op jobs and asset jobs are different
@job
def nypd_data_job():
    cnt = nypd_raw_data()
    nypd_dbt_project()


# Materializes all assets
all_assets_job = define_asset_job(name="run_nypd_data_model")
