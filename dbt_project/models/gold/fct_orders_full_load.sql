{{
  config(
    materialized='table',
    schema='gold'
  )
}}

with orders as (
    select * from {{ ref('sil_orders') }}
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
        loaded_at
    from orders
)

select * from transformed 