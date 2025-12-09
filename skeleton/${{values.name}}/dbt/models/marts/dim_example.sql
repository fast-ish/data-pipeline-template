with staged as (
    select * from {{ ref('stg_example') }}
)

select
    id as example_id,
    name as example_name,
    created_at,
    updated_at
from staged
