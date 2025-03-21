with source as (
    select * from "etl_metadata"."public"."customers"
),

staged as (
    select
        customer_id,
        trim(name) as customer_name,
        case
            when email is null then 'unknown@example.com'
            when email = 'invalid_email' then 'unknown@example.com'
            else lower(trim(email))
        end as email,
        case
            when phone is null then 'unknown'
            when phone = 'invalid_phone' then 'unknown'
            else trim(phone)
        end as phone,
        coalesce(address, 'unknown') as address,
        TO_TIMESTAMP(created_at / 1000) as created_at
    from source
)

select * from staged