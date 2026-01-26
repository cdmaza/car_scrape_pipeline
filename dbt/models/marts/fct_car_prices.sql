with staged as (
    select * from {{ ref('stg_cars') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['source_id', 'source']) }} as car_id,
        price,
        mileage,
        location,
        source,
        extracted_at,
        transformed_at,
        case
            when price < 30000 then 'budget'
            when price < 80000 then 'mid-range'
            when price < 150000 then 'premium'
            else 'luxury'
        end as price_segment,
        case
            when mileage is null then 'unknown'
            when mileage < 50000 then 'low'
            when mileage < 100000 then 'medium'
            else 'high'
        end as mileage_segment
    from staged
    where price is not null
)

select * from final
