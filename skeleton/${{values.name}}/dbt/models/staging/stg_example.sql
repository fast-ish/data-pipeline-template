with source as (
    select * from {{ source('raw', 'example') }}
),

staged as (
    select
        id,
        name,
        created_at,
        updated_at
    from source
)

select * from staged
