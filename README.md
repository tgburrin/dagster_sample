# Dagster Sample Project
This is a sample project that leverages dagster + dbt to pull public nypd data and process it into some simple tables

## Setup

After cloning the directory run the following commands
Build the dagster docker image
```
./dagster_image/build.sh
```
Compile the dbt project and build the manifest.json
```
docker run \
  --rm -ti \
  -v ./dbt_projects:/dbt_projects  \
  dagster-custom \
  dbt parse --profiles-dir /dbt_projects/profiles --project-dir /dbt_projects/dbt_nypd_data
```
Start the project (prometheus, postgres, dagster webserver, and a dagster worker)
```
docker-compose up
```
