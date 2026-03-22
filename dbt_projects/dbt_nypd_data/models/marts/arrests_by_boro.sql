{{
    config(
        materialized = 'table',
        indexes = [
            {"columns": ["arrest_date", "borough_code"], "unique": True}
        ]
    )
}}

select
    arrest_date,
    borough_code,
    count(*) as total
from
    {{  source('nypd_data_job', 'nypd_arrests') }}
group by arrest_date, borough_code
order by arrest_date
