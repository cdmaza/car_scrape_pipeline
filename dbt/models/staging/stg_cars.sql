with source as (
    select * from {{ source('raw', 'staging_cars') }}
),

cleaned as (
    select
        source_id,
        trim(title) as title,
        cast(nullif(regexp_replace(price, '[^0-9.]', '', 'g'), '') as decimal(12,2)) as price,
        cast(nullif(year, '') as integer) as year,
        cast(nullif(regexp_replace(mileage, '[^0-9]', '', 'g'), '') as integer) as mileage,
        trim(location) as location,
        trim(lower(brand)) as brand,
        trim(lower(model)) as model,
        source,
        extracted_at,
        current_timestamp as transformed_at
    from source
    where title is not null
)

select * from cleaned
