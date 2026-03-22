#!/usr/bin/env python3

import dbt_core_interface as dbtci

cli = dbtci.DbtProject(
    project_dir="/dbt_projects/dbt_nypd_data",
    profiles_dir="/dbt_projects/profiles",
    profile="dbt_nypd_data",
    target="dev"
)

print(cli.build())
