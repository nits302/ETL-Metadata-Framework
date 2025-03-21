{{
  config(
    materialized='table',
    schema='load'
  )
}}

with source as (
    select * from {{ ref('stg_orders') }}
),

transformed as (
    select
        order_id,
        customer_id,
        product_name,
        quantity,
        price,
        order_date,
        order_total,
        current_timestamp as loaded_at
    from source
)

select * from transformed 