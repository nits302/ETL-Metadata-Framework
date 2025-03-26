{{
  config(
    materialized='incremental',
    unique_key='order_id',
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

{% if is_incremental() %}
    select * from transformed
    where loaded_at > (select max(loaded_at) from {{ this }})
{% else %}
    select * from transformed
{% endif %}