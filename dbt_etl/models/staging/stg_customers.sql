with source as (
    select * from "etl_metadata"."public"."customers"
),

staged as (
    select
        customer_id,
        trim(name) as customer_name,
        email,
        phone,
        address,
        TO_TIMESTAMP(created_at) as created_at
    from source
)

select * from staged