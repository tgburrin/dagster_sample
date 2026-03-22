{{
    config(
        materialized = 'table',
        indexes = [
            {"columns": ["arrest_date"], "unique": True}
        ]
    )
}}

select
    b.arrest_date,
    sum(b.total)::bigint as total
from
    {{  ref('arrests_by_boro') }} b
group by b.arrest_date
order by b.arrest_date
