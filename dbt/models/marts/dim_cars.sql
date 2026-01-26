with staged as (
    select * from {{ ref('stg_cars') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['source_id', 'source']) }} as car_id,
        source_id,
        title,
        brand,
        model,
        year,
        source,
        extracted_at,
        transformed_at
    from staged
)

select * from final
